package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

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
	clerkRequestMaxCount map[int64]int
	executeResObserver   map[int][]chan executeRes //[index][]notifier
	opIndexInLog         map[string]int
	cacheRes             map[string]executeRes

	term           int
	executeIndex   int
}

type executeRes struct {
	index     int
	executeOp Op
	value     string
	err       Err
}

func (kv *KVServer) checkLeaderState(){
	for true{
		time.Sleep(time.Millisecond * 300)
		kv.mu.Lock()
		term,_:=kv.rf.GetState()
		if term>kv.term{
			kv.termChange(term)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) execute() {

	for true {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		if applyMsg.CommandIndex <= kv.executeIndex {
			kv.mu.Unlock()
			continue
		}

		if applyMsg.CommandIndex>kv.executeIndex+1{
			go func(){
				time.Sleep(100*time.Millisecond)
				kv.applyCh<-applyMsg
			}()
			kv.mu.Unlock()
			continue
		}

		op := applyMsg.Command.(Op)
		res := executeRes{
			index:     applyMsg.CommandIndex,
			executeOp: op,
		}
		DPrintf(
			"server %d: op commited, commited index %d, index %d, type %s, clerkID %d, requestCount %d", kv.me,kv.executeIndex,applyMsg.CommandIndex,
			op.OpType, op.ClerkID, op.RequestCount,
		)

		switch op.OpType {
		case "Get":
			res.value = kv.kv[op.Key]
			res.err = OK
			DPrintf("server %d: get [%s]=%s", kv.me, op.Key, kv.kv[op.Key])
		case "Put":
			kv.kv[op.Key] = op.Value
			res.err = OK
			DPrintf("server %d: put [%s]=%s", kv.me, op.Key, kv.kv[op.Key])
		case "Append":
			kv.kv[op.Key] += op.Value
			res.err = OK
			DPrintf(
				"server %d: append [%s]=%s, opValue %s", kv.me, op.Key, kv.kv[op.Key],
				op.Value,
			)
		}
		observerList := kv.executeResObserver[res.index]
		delObserverKey := kv.getOpID(op.ClerkID, op.RequestCount-1)
		addObserverKey := kv.getOpID(op.ClerkID, op.RequestCount)
		delete(kv.executeResObserver, res.index)
		delete(kv.opIndexInLog, delObserverKey)
		delete(kv.cacheRes, delObserverKey)
		kv.cacheRes[addObserverKey] = res
		kv.executeIndex = applyMsg.CommandIndex

		kv.mu.Unlock()

		for i := 0; i < len(observerList); i++ {
			observerList[i] <- res
		}

	}

}

func (kv *KVServer) termChange(newTerm int){
	kv.term=newTerm
	res:=executeRes{err: ErrWrongLeader}
	for _,resChanList:=range kv.executeResObserver{
		for _,resChan:=range resChanList{
			resChan<-res
		}
	}
	kv.executeResObserver=make(map[int][]chan executeRes)
}

func (kv *KVServer) callRaftAndListen(op Op) (bool, chan executeRes) {
	index := -1
	isleader := true
	ok := true
	term:=0
	opID := kv.getOpID(op.ClerkID, op.RequestCount)
	resChan := make(chan executeRes, 1)

	isDuplicate := kv.checkDuplicate(op.ClerkID, op.RequestCount)
	if !isDuplicate {
		index, term, isleader = kv.rf.Start(op)
		if term>kv.term{
			kv.termChange(term)
		}
		if !isleader {
			return isleader, nil
		}
		kv.opIndexInLog[opID] = index
		kv.clerkRequestMaxCount[op.ClerkID] = op.RequestCount
		kv.executeResObserver[index] = append(kv.executeResObserver[index], resChan)
	} else {
		if index, ok = kv.opIndexInLog[opID]; !ok {
			fmt.Printf("fatal error! opIndexInLog not exist!")
			os.Exit(-1)
		}
		index = kv.opIndexInLog[opID]

		if index <= kv.executeIndex {
			go func() {
				res := executeRes{}
				ok := true
				if res, ok = kv.cacheRes[opID]; !ok {
					res.err=ErrWrongLeader  //maybe has bug??
					//fmt.Printf("fatal error! cache not exist!")
					//os.Exit(-1)
				}
				resChan <- res
			}()
		} else {
			kv.executeResObserver[index] = append(kv.executeResObserver[index], resChan)
		}
	}
	return isleader, resChan
}

func (kv *KVServer) checkOpMatch(originalOp Op, res executeRes) bool {
	if res.err==ErrWrongLeader{
		return false
	}
	if originalOp == res.executeOp {
		return true
	}
	return false
}

func (kv *KVServer) checkDuplicate(ClerkID int64, RequestCount int) bool {
	//if _, ok := kv.clerkRequestMaxCount[ClerkID]; !ok {
	//	kv.clerkRequestMaxCount[ClerkID] = -1
	//}
	if kv.clerkRequestMaxCount[ClerkID] >= RequestCount {
		return true
	}
	return false
}

func (kv *KVServer) getOpID(ClerkID int64, RequestCount int) string {
	return strconv.FormatInt(ClerkID, 10) + strconv.Itoa(RequestCount)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("server %d: receive get", kv.me)
	kv.mu.Lock()
	op := Op{
		OpType:       "Get",
		Key:          args.Key,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}
	isleader, executeResListener := kv.callRaftAndListen(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		reply.Err = ErrWrongLeader //should this be ErrWrongLeader?
		return
	}

	reply.Err = res.err
	reply.Value = res.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Printf("server %d: receive putappend", kv.me)
	// Your code here.
	kv.mu.Lock()
	op := Op{
		OpType:       args.Op,
		Key:          args.Key,
		Value:        args.Value,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}

	isleader, executeResListener := kv.callRaftAndListen(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		//DPrintf(
		//	"server %d: committed op not matched %s request, clerkID %d, requestCount %d, index %d",
		//	kv.me, args.Op,
		//	args.ClerkID, args.RequestCount, index,
		//)
		reply.Err = ErrWrongLeader //should this be ErrWrongLeader?
		return
	}
	reply.Err = res.err
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
//
//		if kv.clerkRequestMaxCount[op.ClerkID]<op.RequestCount{
//			kv.clerkRequestMaxCount[op.ClerkID]=op.RequestCount
//		}
//	}
//}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
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
	kv.clerkRequestMaxCount = make(map[int64]int)
	kv.executeResObserver = make(map[int][]chan executeRes)
	kv.opIndexInLog = make(map[string]int)
	kv.cacheRes = make(map[string]executeRes)
	kv.executeIndex = 0

	//kv.receiveOldApplyMsgFromRaft()

	go kv.execute()
	go kv.checkLeaderState()

	return kv
}
