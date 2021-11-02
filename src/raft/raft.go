package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Term    int
	Command interface{}
}

var logger *log.Logger

type AppendEntryArgs struct {
	Term   int
	Leader int

	LogEntries   []logEntry
	PreLogIndex  int
	PreLogTerm   int
	LeaderCommit int
}

type AppendEntryReply struct {
	ServerIndex int
	Term        int
	Leader      int

	Success                bool
	ConflictTerm           int
	ConflictTermFirstIndex int
	LogLen                 int
}

type CommitEntryArgs struct {
	CommitIndex int
}

type CommitEntryReply struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logEntries  []logEntry

	leader         int
	timeOut4Leader bool

	commandChan chan interface{}
	nextIndex   []int
	applyChan   chan ApplyMsg
	commitIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	if rf.leader == rf.me {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
		logger.Print("readPersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		logger.Printf("node %d: recover from persist, log %v", rf.me, rf.logEntries)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int, lastIncludedIndex int, snapshot []byte,
) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type voteCounter struct {
	count       int
	votedServer []int
	mu          sync.Mutex
}

func (rf *Raft) updateTerm(newTerm int, leader int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.leader = leader
	rf.persist()
}

func (rf *Raft) CommitEntry(args *CommitEntryArgs, reply *CommitEntryReply) {
	rf.mu.Lock()
	rf.commitEntriesUntilIndex(args.CommitIndex)
	rf.mu.Unlock()
}

func (rf *Raft) commitEntriesUntilIndex(index int) {
	for i := rf.commitIndex + 1; i <= index; i++ {
		msg := ApplyMsg{}
		msg.Command = rf.logEntries[i].Command
		msg.CommandIndex = i
		msg.CommandValid = true
		rf.applyChan <- msg
		//logger.Printf("node %d: commit log %v", rf.me, msg.Command)
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
		rf.persist()
	}
}

func (rf *Raft) sendCommitEntry(
	server int, args *CommitEntryArgs, reply *CommitEntryReply,
) bool {
	ok := rf.peers[server].Call("Raft.CommitEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = false
	if rf.leader == rf.me {
		isLeader = true
		term = rf.currentTerm
		index = len(rf.logEntries) + len(rf.commandChan)
		rf.commandChan <- command
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) checkAppendEntryArgs(args *AppendEntryArgs, reply *AppendEntryReply) bool{
	//reject if the Term is old
	if args.Term < rf.currentTerm {
		logger.Printf(
			"node %d: reject append, current term is %d but the request term is %d",
			rf.me, rf.currentTerm, args.Term,
		)
		return false
	}

	reply.LogLen = len(rf.logEntries)
	reply.ConflictTermFirstIndex = reply.LogLen
	//reject if pre info is wrong
	if args.PreLogIndex >= len(rf.logEntries) {
		logger.Printf("node %d: reject append, preLogIndex is %d, len of log is %d", rf.me, args.PreLogIndex, len(rf.logEntries))
		return false
	}
	if rf.logEntries[args.PreLogIndex].Term != args.PreLogTerm {
		reply.ConflictTerm = rf.logEntries[args.PreLogIndex].Term
		for i := args.PreLogIndex; i > 0; i-- {
			if rf.logEntries[i-1].Term != reply.ConflictTerm {
				reply.ConflictTermFirstIndex = i
				break
			}
		}
		logger.Printf(
			"node %d: reject append, last log term in local is %d but the last log term in args is %d",
			rf.me, rf.logEntries[args.PreLogIndex].Term, args.PreLogTerm,
		)
		return false
	}
	return true
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//code to handle heart beat
	if args.Term >= rf.currentTerm {
		rf.timeOut4Leader = false
		if args.Term > rf.currentTerm {
			rf.updateTerm(args.Term, args.Leader)
		}
	}

	reply.Term = rf.currentTerm
	reply.Leader = rf.leader
	reply.ServerIndex = rf.me
	//accept the append entry
	reply.Success = rf.checkAppendEntryArgs(args,reply)
	if !reply.Success{
		return
	}

	isDiff:=false
	for i:=0;i<len(args.LogEntries);i++{
		syncIndex:=args.PreLogIndex+i+1
		//log len is different
		if syncIndex==len(rf.logEntries){
			isDiff=true
			break
		}
		//term is different
		if rf.logEntries[syncIndex]!=args.LogEntries[i]{
			isDiff=true
			break
		}
	}
	//trunc the logs
	if isDiff{
		rf.logEntries=append(rf.logEntries[0:args.PreLogIndex+1],args.LogEntries...)
		if len(args.LogEntries) > 0 {
			logger.Printf("node %d: accept append, term %d, leaderCommit %d, preLogIndex %d, newly logEntries %v", rf.me, args.Term, args.LeaderCommit, args.PreLogIndex, args.LogEntries)
			logger.Printf("node %d: current log %v", rf.me, rf.logEntries)
		}
	}
	if args.LeaderCommit>rf.commitIndex{
		rf.commitEntriesUntilIndex(args.LeaderCommit)
	}
}

func (rf *Raft) sendAppendEntry(
	server int, args *AppendEntryArgs, reply *AppendEntryReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) appendEntrySender(peerIndex int, args AppendEntryArgs, counter *voteCounter) {
	reply := AppendEntryReply{}
	sendRes := rf.sendAppendEntry(peerIndex, &args, &reply)
	if sendRes {
		rf.appendEntryReplyHandler(peerIndex, args, &reply, counter)
	}
}

func (rf *Raft) appendEntryReplyHandler(
	serverIndex int, args AppendEntryArgs, reply *AppendEntryReply, counter *voteCounter,
) {
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term, reply.Leader)
	}
	//only handle the situation when reply term == leader term == args term
	if rf.leader == rf.me && args.Term == rf.currentTerm {
		if reply.Success {
			next := args.PreLogIndex + len(args.LogEntries) + 1
			//the if is needed because the reply can be old
			if rf.nextIndex[serverIndex] < next {
				rf.nextIndex[serverIndex] = next
			}
			counter.mu.Lock()
			counter.count++
			counter.votedServer = append(counter.votedServer, serverIndex)
			if counter.count > len(rf.peers)/2 {
				index := args.PreLogIndex + len(args.LogEntries)
				//TODO: reduce unnecessary log
				//logger.Printf("node %d, leader commit to %d, current log is %v",rf.me,index,rf.logEntries)
				rf.commitEntriesUntilIndex(index)
			}
			counter.mu.Unlock()
		} else {
			//update the next index to sync
			//case 1: the log of follower is so short, then the syncIndex is the logLen
			//case 2: there exist conflict log entry. syncIndex is the first index of entry with conflicted term
			syncIndex := reply.LogLen
			if syncIndex > reply.ConflictTermFirstIndex {
				syncIndex = reply.ConflictTermFirstIndex
			}
			rf.nextIndex[serverIndex] = syncIndex
			args=rf.genAppendEntryArgs4SpecificPeer(serverIndex,args)
			go rf.appendEntrySender(serverIndex, args, counter)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) genAppendEntryArgs4SpecificPeer(peerIndex int, basicArgs AppendEntryArgs) AppendEntryArgs {
	syncIndex := rf.nextIndex[peerIndex]
	if basicArgs.PreLogIndex == -1 {
		basicArgs.PreLogIndex = syncIndex - 1
		basicArgs.PreLogTerm = rf.logEntries[syncIndex-1].Term
		basicArgs.LeaderCommit = rf.commitIndex
		basicArgs.LogEntries = append([]logEntry{}, rf.logEntries[syncIndex:len(rf.logEntries)]...)
	} else {
		syncEndIndex:=basicArgs.PreLogIndex+1
		basicArgs.PreLogIndex = syncIndex - 1
		basicArgs.PreLogTerm = rf.logEntries[syncIndex-1].Term
		//add the conflicted entries
		tmpLogEntries := make([]logEntry, syncEndIndex-syncIndex)
		copy(tmpLogEntries, rf.logEntries[syncIndex:syncEndIndex])
		basicArgs.LogEntries = append(tmpLogEntries, basicArgs.LogEntries...)
	}
	return basicArgs
}

func (rf *Raft) appendEntryTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		//if the server is not the leader, stop append entry
		if rf.leader != rf.me {
			rf.commandChan = make(chan interface{}, 1000)
			rf.mu.Unlock()
			break
		}
		rf.timeOut4Leader = false
		//append the new entry into the local logs
		if len(rf.commandChan) > 0 {
			newLogEntry := logEntry{Term: rf.currentTerm, Command: <-rf.commandChan}
			rf.logEntries = append(rf.logEntries, newLogEntry)
			rf.persist()
		}
		counter := voteCounter{
			votedServer: []int{rf.me},
			count:       1,
		}
		basicArgs := AppendEntryArgs{
			Term:       rf.currentTerm,
			Leader:     rf.me,
			LogEntries: make([]logEntry, 0),

			PreLogIndex: -1,
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args:=rf.genAppendEntryArgs4SpecificPeer(i,basicArgs)
			go rf.appendEntrySender(i, args, &counter)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 200)
	}
}

func (rf *Raft) requestVoteSender(peerIndex int, args *RequestVoteArgs, counter *voteCounter) {
	reply := RequestVoteReply{}
	sendRes := rf.sendRequestVote(peerIndex, args, &reply)
	if sendRes {
		rf.requestVoteReplyHandler(peerIndex, args, &reply, counter)
	}
}

func (rf *Raft) requestVoteReplyHandler(
	serverIndex int, args *RequestVoteArgs, reply *RequestVoteReply, counter *voteCounter,
) {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	if reply.VoteGranted {
		counter.count++
	}
	if counter.count > len(rf.peers)/2 {
		rf.mu.Lock()
		if args.Term == rf.currentTerm && rf.leader == -1 {
			logger.Printf(
				"node %d: become the leader in Term %d \n", rf.me, args.Term,
			)
			rf.leader = rf.me
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = len(rf.logEntries)
			}
			go rf.appendEntryTicker()
		}
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	//update the current Term if current Term is old
	if rf.currentTerm < args.Term {
		rf.updateTerm(args.Term, -1)
	}

	reply.Term = rf.currentTerm
	//candidate Term is old, reject
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	//has voted
	if rf.votedFor != -1 {
		//has voted for other candidate, reject
		if rf.votedFor != args.CandidateID {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
		}
		return
	} else { //has not voted, try to vote the candidate
		lastLogIndex := len(rf.logEntries) - 1
		//candidate log is old, reject
		//compare the LastLogIndex rather than last applied, because some logs are committed
		//but are not applied for the time being
		logger.Printf("node %d: my last log %v, candidate last log term %d",rf.me,rf.logEntries[lastLogIndex],args.LastLogTerm)
		if args.LastLogTerm < rf.logEntries[lastLogIndex].Term {
			reply.VoteGranted = false
			logger.Printf("node %d: reject vote, my last log term is %d but the leader last log term %d", rf.me, rf.logEntries[lastLogIndex].Term, args.LastLogTerm)
			return
		}

		if args.LastLogTerm == rf.logEntries[lastLogIndex].Term && args.LastLogIndex < lastLogIndex {
			reply.VoteGranted = false
			logger.Printf("node %d: reject vote, last log term are same, my last log index is %d but the leader last log index %d", rf.me, lastLogIndex, args.LastLogIndex)
			return
		}

		//vote for candidate
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.timeOut4Leader = false //reset timeout here???
		reply.VoteGranted = true
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(
	server int, args *RequestVoteArgs, reply *RequestVoteReply,
) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		rf.timeOut4Leader = true
		rf.mu.Unlock()

		timeout := time.Duration(500 + rand.Int()%300)
		time.Sleep(time.Millisecond * timeout)
		rf.mu.Lock()
		timeOut4Leader := rf.timeOut4Leader
		rf.mu.Unlock()

		if timeOut4Leader == true {
			rf.mu.Lock()
			rf.updateTerm(rf.currentTerm+1, -1)
			rf.votedFor = rf.me
			rf.persist()
			args := RequestVoteArgs{
				CandidateID:  rf.me,
				Term:         rf.currentTerm,
				LastLogIndex: len(rf.logEntries) - 1,
				LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
			}
			logger.Printf(
				"node %d: timeout, start new election in Term %d\n", rf.me,
				rf.currentTerm,
			)
			rf.mu.Unlock()

			counter := voteCounter{count: 1}
			//send vote request
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.requestVoteSender(i, &args, &counter)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	if logger == nil {
		logger = log.New(ioutil.Discard, "[DEBUG] ", 0)
	}

	rf.currentTerm = 1
	initialTerm := logEntry{Term: 1}
	rf.logEntries = []logEntry{initialTerm}
	rf.votedFor = -1
	rf.leader = -1

	rf.nextIndex = make([]int, len(peers))
	rf.commandChan = make(chan interface{}, 1000) //may cause block and dead lock
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
