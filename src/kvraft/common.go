package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID      int64
	RequestCount int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID      int64
	RequestCount int
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetReply) getErr() Err{
	return r.Err
}

func (r *PutAppendReply) getErr() Err{
	return r.Err
}

func (r *GetReply) copy() interface{}{
	newReply:=*r
	return &newReply
}

func (r *PutAppendReply) copy() interface{}{
	newReply:=*r
	return &newReply
}

type ReplyInterface interface {
	getErr() Err
	copy() interface{}
}
