package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       string
	Key          string
	Value        string
	ClerkID      int64
	RequestCount int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv                   map[string]string

	clerkRequestMaxExecuteCount map[int64]int
	logListeners                map[int][]string
	chanToNotifyRes             map[string]notifyChanInfo //[reqID][chan]
	cacheRes                    map[string]executeRes
	executeIndex   int
}

type notifyChanInfo struct{
	c chan executeRes
	count int
}

type executeRes struct {
	ExecuteOp Op
	Value     string
	Err       Err
}

func (kv *KVServer) sendExecuteResOfReq(opID string,res executeRes){
	if chanInfo,ok :=kv.chanToNotifyRes[opID];ok{
		for j:=0;j< chanInfo.count;j++{
			chanInfo.c<-res
		}
	}
	delete(kv.chanToNotifyRes, opID)
}

func (kv *KVServer) notifyListener(index int,res executeRes){
	listenerList:=kv.logListeners[index]
	for i:=0;i<len(listenerList);i++{
		opID :=listenerList[i]
		kv.sendExecuteResOfReq(opID,res)
	}
	delete(kv.logListeners,index)
}

func (kv *KVServer) snapshotIfStateSizeExceed(){
	if kv.maxraftstate==-1{
		return
	}
	curSize:=kv.rf.RaftStateSize()
	if curSize>kv.maxraftstate*2/3{
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.executeIndex)
		e.Encode(kv.clerkRequestMaxExecuteCount)
		e.Encode(kv.cacheRes)
		e.Encode(kv.kv)
		data := w.Bytes()

		kv.rf.Snapshot(kv.executeIndex,data)
	}
}

func (kv *KVServer) executeSnapShot(msg raft.ApplyMsg){
	if msg.SnapshotIndex< kv.rf.GetSnapshotLastIndex(){
		return
	}

	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.executeIndex) != nil ||
		d.Decode(&kv.clerkRequestMaxExecuteCount) != nil ||
		d.Decode(&kv.cacheRes) != nil ||
		d.Decode(&kv.kv) != nil {
		fmt.Print("readSnapshot error")
	}
	kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
}

func (kv *KVServer) execute(msgChan chan raft.ApplyMsg) {
	for true {
		applyMsg := <-msgChan
		kv.mu.Lock()
		if applyMsg.SnapshotValid{
			kv.executeSnapShot(applyMsg)
			kv.mu.Unlock()
			continue
		}


		op := applyMsg.Command.(Op)
		lastOpID := kv.getOpID(op.ClerkID, op.RequestCount-1)
		opID := kv.getOpID(op.ClerkID, op.RequestCount)
		res := executeRes{ExecuteOp: op,}

		//check if the raft send the old command
		if applyMsg.CommandIndex <= kv.executeIndex {
			goto UNLOCKANDCONTINUE
		}
		//check if raft return command in disorder
		if applyMsg.CommandIndex>kv.executeIndex+1{
			go func(){
				msgChan<-applyMsg
			}()
			goto UNLOCKANDCONTINUE
		}

		//log valid, try to execute it
		kv.executeIndex++
		//already execute the request in this log, just continue
		if kv.checkDuplicate(op.ClerkID,op.RequestCount){
			goto UNLOCKANDCONTINUE
		}

		DPrintf(
			"server %d: op commited, commited index %d, index %d, type %s, clerkID %d, requestCount %d", kv.me,kv.executeIndex,applyMsg.CommandIndex,
			op.OpType, op.ClerkID, op.RequestCount,
		)
		switch op.OpType {
		case "Get":
			res.Value = kv.kv[op.Key]
			res.Err = OK
			DPrintf("server %d: get [%s]=%s", kv.me, op.Key, kv.kv[op.Key])
		case "Put":
			kv.kv[op.Key] = op.Value
			res.Err = OK
			DPrintf("server %d: put [%s]=%s", kv.me, op.Key, kv.kv[op.Key])
		case "Append":
			kv.kv[op.Key] += op.Value
			res.Err = OK
			DPrintf(
				"server %d: append [%s]=%s, opValue %s", kv.me, op.Key, kv.kv[op.Key],
				op.Value,
			)
		}

		kv.clerkRequestMaxExecuteCount[op.ClerkID]=op.RequestCount
		kv.cacheRes[opID]=res
		delete(kv.cacheRes,lastOpID)
		kv.notifyListener(applyMsg.CommandIndex,res)
		kv.sendExecuteResOfReq(opID,res)


		kv.snapshotIfStateSizeExceed()

		UNLOCKANDCONTINUE:
		kv.mu.Unlock()
		continue

	}

}

func (kv *KVServer) callRaftAndListen(op Op) chan executeRes {
	opID := kv.getOpID(op.ClerkID, op.RequestCount)
	res := executeRes{}
	index := -1
	isleader := true
	ok := true
	resChan := make(chan executeRes, 1)

	//check if the request has been executed (may be send by self or other server)
	if res,ok=kv.cacheRes[opID];ok{
		resChan <- res
		return resChan
	}

	//check if already send the req to raft
	if chanInfo,ok:=kv.chanToNotifyRes[opID];ok{
		chanInfo.count++
		resChan=chanInfo.c
		//do not return here, still required to send a new request to raft
		//return chanInfo.c
	}

	index, _, isleader = kv.rf.Start(op)
	if !isleader{
		res.Err =ErrWrongLeader
		resChan <- res
		return resChan
	}

	//already sent the req to raft, update listening message
	if _,ok:=kv.chanToNotifyRes[opID];!ok {
		kv.chanToNotifyRes[opID]=notifyChanInfo{c:resChan,count: 1}
	}
	kv.logListeners[index]=append(kv.logListeners[index],opID)

	return resChan
}

func (kv *KVServer) checkOpMatch(originalOp Op, res executeRes) bool {
	if res.Err ==ErrWrongLeader{
		return false
	}
	if originalOp == res.ExecuteOp {
		return true
	}
	return false
}

func (kv *KVServer) checkDuplicate(ClerkID int64, RequestCount int) bool {
	if kv.clerkRequestMaxExecuteCount[ClerkID] >= RequestCount {
		return true
	}
	return false
}

func (kv *KVServer) getOpID(ClerkID int64, RequestCount int) string {
	return strconv.FormatInt(ClerkID, 10) + strconv.Itoa(RequestCount)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		OpType:       "Get",
		Key:          args.Key,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}
	executeResListener := kv.callRaftAndListen(op)
	kv.mu.Unlock()
	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		reply.Err = ErrWrongLeader //should this be ErrWrongLeader?
		return
	}

	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		OpType:       args.Op,
		Key:          args.Key,
		Value:        args.Value,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}
	executeResListener := kv.callRaftAndListen(op)
	kv.mu.Unlock()
	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		reply.Err = ErrWrongLeader //should this be ErrWrongLeader?
		return
	}
	reply.Err = res.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//func (kv *KVServer) receiveOldApplyMsgFromRaft() {
//	receiveChan:=make(chan raft.ApplyMsg)
//	go kv.rf.GetApplyMsgWithLock(receiveChan)
//	for{
//		msg,ok:=<-receiveChan
//		if !ok{
//			break
//		}
//		op := msg.Command.(Op)
//		if kv.clerkRequestMaxExecuteCount[op.ClerkID]<op.RequestCount{
//			kv.clerkRequestMaxExecuteCount[op.ClerkID]=op.RequestCount
//		}
//	}
//}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(
	servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.clerkRequestMaxExecuteCount = make(map[int64]int)
	kv.logListeners = make(map[int][]string)
	kv.chanToNotifyRes = make(map[string]notifyChanInfo)
	kv.cacheRes = make(map[string]executeRes)
	kv.executeIndex = 0
	//kv.receiveOldApplyMsgFromRaft()

	go kv.execute(kv.applyCh)
	return kv
}
