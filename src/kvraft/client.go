package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex    int
	clerkID        int64
	requestCounter int
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.requestCounter=1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{
		Key:          key,
		ClerkID:      ck.clerkID,
		RequestCount: ck.requestCounter,
	}
	reply := &GetReply{}
	ck.requestCounter++

	retReply:=ck.RequestUntilSuccess("KVServer.Get", args, reply)

	return retReply.(*GetReply).Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		ClerkID:      ck.clerkID,
		RequestCount: ck.requestCounter,
	}
	reply := &PutAppendReply{}
	ck.requestCounter++

	ck.RequestUntilSuccess("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) RequestUntilSuccess(
	funcName string, args interface{}, reply interface{},
) interface{}{

	timeoutFunc := func(timeoutChan chan bool) {
		time.Sleep(1 * time.Second)
		timeoutChan <- true
	}
	remoteCallFunc := func(
		remoteCallRetChan chan bool, args interface{}, reply interface{},
	) {
		ck.mu.Lock()
		leaderIndex:=ck.leaderIndex
		ck.mu.Unlock()
		ok := ck.servers[leaderIndex].Call(funcName, args, reply)
		remoteCallRetChan <- ok
	}

	var ret interface{}
LOOP:
	for true {
		timeoutChan := make(chan bool, 1)
		remoteCallRetChan := make(chan bool, 1)
		newReply:=reply.(ReplyInterface).copy()
		go timeoutFunc(timeoutChan)
		go remoteCallFunc(remoteCallRetChan, args, newReply)

		select {
		case <-timeoutChan:
			//;
		case ok := <-remoteCallRetChan:
			if !ok {
				//;do not send to the same server, send the request to other server
			}
			if newReply.(ReplyInterface).getErr() == OK {
				ret=newReply
				break LOOP
			}
		}
		ck.mu.Lock()
		ck.leaderIndex = (ck.leaderIndex + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
	return ret
}
