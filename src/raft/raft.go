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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Entry log entry
type Entry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       raftState
	voteGranted []bool

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTime  time.Duration
	electionTimer *time.Timer

	heartbeatTime  time.Duration
	heartBeatTimer *time.Timer

	applyCh chan ApplyMsg
}

type raftState int

const (
	FOLLOWER  raftState = 0
	CANDIDATE           = 1
	LEADER              = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
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

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

// must used in lock
func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1

	// don't need clear timer buffer since it will be take cared in heartbeat process
	rf.heartBeatTimer.Stop()
}

func upToDate(term1 int, index1 int, term2 int, index2 int) bool {
	return term1 > term2 || (term1 == term2 && index1 >= index2)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		rf.stepDown(args.Term)
	}

	granted := false
	logLen := len(rf.log)

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		upToDate(args.LastLogTerm, args.LastLogIndex, logTerm(rf.log, logLen), logLen) {
		granted = true
		rf.votedFor = args.CandidateID

		rf.electionTimer.Reset(rf.electionTime)

	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	success := false
	matchIndex := 0

	if rf.currentTerm < args.Term ||
		(rf.currentTerm == args.Term && rf.state != FOLLOWER) {
		rf.stepDown(args.Term)
	}
	if rf.currentTerm == args.Term {
		if args.PrevLogIndex == 0 ||
			(len(rf.log) >= args.PrevLogIndex &&
				logTerm(rf.log, args.PrevLogIndex) == args.PrevLogTerm) {
			success = true
			var index = args.PrevLogIndex
			for i := 0; i < len(args.Entries); i++ {
				index++
				if logTerm(rf.log, index) != args.Entries[i].Term {
					rf.log = rf.log[:index]
					rf.log = append(rf.log, args.Entries[i])
				}
			}
			matchIndex = index
			rf.commitIndex = min(len(rf.log), max(rf.commitIndex, args.LeaderCommit))
		}
	}
	reply.Success = success
	reply.Term = rf.currentTerm
	reply.MatchIndex = matchIndex
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) handleRequestVoteReply(peer int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.stepDown(reply.Term)
	}

	// if already become leader, no need to handle such message
	if rf.state == CANDIDATE && rf.currentTerm == reply.Term {
		rf.voteGranted[peer] = reply.VoteGranted

		// try to become leader
		voted := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.voteGranted[i] {
				voted++
			}
		}
		// fmt.Printf("vote from %d to %d for term %d, result %t \n", peer, rf.me, rf.currentTerm, reply.VoteGranted)
		if 2*voted > len(rf.peers) { // become leader succeed

			nextIndex := len(rf.log) + 1
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = 0
			}

			fmt.Printf("%d become leader for term %d \n", rf.me, rf.currentTerm)
			rf.state = LEADER
			// immediatly send heart beat
			rf.heartBeatTimer.Reset(0)
			// stop election timer
			rf.electionTimer.Stop()
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.stepDown(reply.Term)
	}
	if rf.state == LEADER && rf.currentTerm == reply.Term {
		if reply.Success {
			rf.matchIndex[peer] = max(rf.matchIndex[peer], reply.MatchIndex)
			rf.nextIndex[peer] = reply.MatchIndex + 1
		} else {
			rf.nextIndex[peer] = max(1, rf.nextIndex[peer]-1)
		}
	}
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func logTerm(log []Entry, index int) int {
	if index < 1 || index > len(log) {
		return 0
	}
	return log[index-1].Term
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.votedFor = -1

	peerLength := len(peers)
	rf.voteGranted = make([]bool, peerLength)
	rf.nextIndex = make([]int, peerLength)
	rf.log = make([]Entry, 0)
	rf.nextIndex = make([]int, peerLength)
	for i := 0; i < peerLength; i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, peerLength)

	rf.currentTerm = 0
	rf.electionTime = time.Duration(rand.Intn(200)+150) * time.Millisecond
	rf.heartbeatTime = time.Duration(20) * time.Millisecond

	// start election timer
	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(200)) * time.Millisecond)

	// create heart beat timer but without start it
	rf.heartBeatTimer = time.NewTimer(0)
	<-rf.heartBeatTimer.C

	// election routine, select master
	go func() {
		for {
			<-rf.electionTimer.C
			rf.mu.Lock() // concurrent from rpc relpy
			// after election timer trigger, before lock to update state
			if rf.state == LEADER {
				rf.mu.Unlock()
				continue
			}
			rf.electionTimer.Reset(rf.electionTime)

			fmt.Printf("state %d \n", rf.state)
			rf.state = CANDIDATE
			rf.currentTerm++
			for i := 0; i < len(rf.peers); i++ {
				rf.voteGranted[i] = false
			}
			rf.voteGranted[rf.me] = true
			rf.votedFor = rf.me

			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = len(rf.log)
			args.LastLogTerm = logTerm(rf.log, args.LastLogIndex)
			rf.mu.Unlock()

			for i := 0; i < len(peers); i++ {
				if i != rf.me {
					go func(i int) {
						reply := RequestVoteReply{}
						rf.sendRequestVote(i, &args, &reply)
						rf.handleRequestVoteReply(i, &reply)
					}(i)
				}
			}
		}
	}()

	// heartbeat routine, also for replicate logs
	go func() {
		for {
			<-rf.heartBeatTimer.C

			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				continue
			}
			rf.heartBeatTimer.Reset(rf.heartbeatTime)

			for i := 0; i < len(peers); i++ {
				if i != rf.me {
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderID = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = logTerm(rf.log, args.PrevLogIndex)

					// append log to peer util nextIndex adjust to the right value
					var lastLogIndex int
					if rf.matchIndex[i] < rf.nextIndex[i]-1 {
						lastLogIndex = args.PrevLogIndex
					} else {
						lastLogIndex = len(rf.log)
					}
					args.Entries = rf.log[args.PrevLogIndex:lastLogIndex]

					go func(i int) {
						reply := AppendEntriesReply{}
						rf.sendAppendEntries(i, &args, &reply)
						rf.handleAppendEntriesReply(i, &reply)
					}(i)
				}
			}
			rf.mu.Unlock()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
