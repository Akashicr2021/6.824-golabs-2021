package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
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
	clerkRequestMaxCount map[int64]int
	executeObserver      map[int][]chan executeRes
}

type executeRes struct {
	index     int
	executeOp Op
	value     string
	err       Err
}

func (kv *KVServer) execute() {

	for true {
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		res := executeRes{
			index:     applyMsg.CommandIndex,
			executeOp: op,
		}
		DPrintf("server %d: op commited, type %s, clerkID %d, requestCount %d",kv.me,op.OpType,op.ClerkID,op.RequestCount)
		kv.mu.Lock()
		switch op.OpType {
		case "Get":
			res.value = kv.kv[op.Key]
			res.err = OK
		case "Put":
			kv.kv[op.Key] = op.Value
			res.err = OK
		case "Append":
			kv.kv[op.Key] += op.Value
			res.err = OK
		}
		observerList := kv.executeObserver[res.index]
		delete(kv.executeObserver, res.index)
		kv.mu.Unlock()

		for i := 0; i < len(observerList); i++ {
			observerList[i] <- res
		}

	}

}

func (kv *KVServer) listenExecute(index int) chan executeRes {
	ret := make(chan executeRes, 1)
	kv.executeObserver[index] = append(kv.executeObserver[index], ret)
	return ret
}

func (kv *KVServer) checkOpMatch(originalOp Op, res executeRes) bool {
	if originalOp == res.executeOp {
		return true
	}
	return false
}

//func (kv *KVServer) checkDuplicate(ClerkID int64,RequestCount int) bool {
//	if kv.clerkRequestMaxCount[ClerkID]>=RequestCount{
//		return true
//	}
//	return false
//}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf(
		"server %d: receive Get request, clerkID %d, requestCount %d", kv.me,
		args.ClerkID, args.RequestCount,
	)
	op := Op{
		OpType:       "Get",
		Key:          args.Key,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf(
		"server %d: wait for Get request, clerkID %d, requestCount %d, index %d", kv.me,
		args.ClerkID, args.RequestCount, index,
	)
	executeResListener := kv.listenExecute(index)
	kv.mu.Unlock()

	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		DPrintf(
			"server %d: committed op not matched Get request, clerkID %d, requestCount %d, index %d", kv.me,
			args.ClerkID, args.RequestCount, index,
		)
		reply.Err = ErrWrongLeader //should this be ErrWrongLeader?
		return
	}

	reply.Err = res.err
	reply.Value = res.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf(
		"server %d: receive %s request, clerkID %d, requestCount %d", kv.me,args.Op,
		args.ClerkID, args.RequestCount,
	)
	op := Op{
		OpType:       args.Op,
		Key:          args.Key,
		Value:        args.Value,
		ClerkID:      args.ClerkID,
		RequestCount: args.RequestCount,
	}

	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	executeResListener := kv.listenExecute(index)
	kv.mu.Unlock()

	res := <-executeResListener

	if !kv.checkOpMatch(op, res) {
		DPrintf(
			"server %d: committed op not matched %s request, clerkID %d, requestCount %d, index %d", kv.me,args.Op,
			args.ClerkID, args.RequestCount, index,
		)
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
	kv.executeObserver = make(map[int][]chan executeRes)

	go kv.execute()

	return kv
}
