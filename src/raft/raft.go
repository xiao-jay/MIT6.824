package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
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

//
// A Go object implementing a single Raft peer.

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int //当前节点的term
	role        string
	voteFor     int
	lastReceive time.Time
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	var flag bool
	if rf.role == LEADER {
		flag = true
	}
	return rf.currentTerm, flag
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	//LastLogIndex int
	//LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.voteFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("%+v,%+v", *args, *reply)
		return
	}
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm, rf.voteFor = args.Term, -1
	}

	rf.voteFor = args.CandidateID
	reply.VoteGranted, reply.Term = true, rf.currentTerm
	DPrintf("%+v,%+v,votefor: %d votefrom: %d", *args, *reply, rf.voteFor, rf.me)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
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

// Kill 把这个rf.dead赋值并且放进一个地址中
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().Unix())
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		start := time.Now()
		time.Sleep(time.Duration(rand.Intn(500)+200) * time.Millisecond)
		// 领导人过期了，开始重新选举
		if rf.lastReceive.Before(start) {
			rf.AttemptElection()
		}
	}
}

func (rf *Raft) AttemptElection() {
	DPrintf("election start me:%v, role:%v term :%v", rf.me, rf.role, rf.currentTerm)
	numVote := 1
	rf.role = CANDIDATE
	// 给自己投票
	rf.voteFor = rf.me
	// term++
	rf.currentTerm++
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args, reply := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me,
			}, &RequestVoteReply{}
			// 得先判断rpc是否成功
			if rf.sendRequestVote(i, args, reply) {
				rf.mu.Lock()
				if rf.role == CANDIDATE {
					if reply.VoteGranted {
						numVote++
						if numVote > len(rf.peers)/2 {
							rf.role = LEADER
							DPrintf("leader is %d, term :%d", rf.me, rf.currentTerm)
						} else if reply.Term > rf.currentTerm {
							rf.role = FOLLOWER
							rf.currentTerm, rf.voteFor = reply.Term, -1
						}
					}
				}
				rf.mu.Unlock()
			}

		}(i)
	}
}

type appendEntriesArgs struct {
	Term     int
	LeaderID int
}

type appendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) HeartBeat(args *appendEntriesArgs, reply *appendEntriesReply) {
	//DPrintf("enter heart beat")

	if args.Term > rf.currentTerm && rf.role != LEADER {
		rf.currentTerm = args.Term
		rf.lastReceive = time.Now()
		reply.Success = true
	} else {
		// 说明leader已经out了,应该直接丢弃
		rf.lastReceive = time.Now()
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendHeartbeat(server int, args *appendEntriesArgs, reply *appendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}

// LeaderOperation AppendEntries 用于心跳
func (rf *Raft) LeaderOperation() {
	for {
		if rf.role == LEADER {
			for i := range rf.peers {
				go func(i int) {
					args, reply := &appendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderID: rf.me,
					}, &appendEntriesReply{}
					if rf.sendHeartbeat(i, args, reply) {
						if reply.Term > rf.currentTerm {
							rf.role = FOLLOWER
							rf.currentTerm = reply.Term
						}
					}

				}(i)
			}
		}
		time.Sleep(time.Millisecond * 10)
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 1,
		role:        FOLLOWER,
		voteFor:     -1,
		lastReceive: time.Now(),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.LeaderOperation()

	return rf
}
