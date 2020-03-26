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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
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
	CommandValid bool // Snapshot request if CommandValid is false
	Snapshot     []byte
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

	snapshotLastIndex int
	snapshotLastTerm  int

	commitIndex int

	nextIndex  []int
	matchIndex []int

	electionElapsed  int
	heartbeatElapsed int

	electionTimeout  int
	heartbeatTimeout int

	applyCh chan ApplyMsg

	ticktime int
}

type raftState int

const (
	FOLLOWER  raftState = 0
	CANDIDATE           = 1
	LEADER              = 2
)

const (
	configElectionTimeout  int = 15
	configHeartBeatTimeout int = 1
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.raftState())
}

func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	return w.Bytes()
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
	var log []Entry
	var snapshotLastIndex int
	var snapshotLastTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil {
		panic("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIndex = snapshotLastIndex
		rf.snapshotLastTerm = snapshotLastTerm

		rf.commitIndex = rf.snapshotLastIndex

		DPrintf("server %d recover, term @%d, votedFor %d, log length %d, snap index %d, snap term @%d",
			rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.snapshotLastIndex, rf.snapshotLastTerm)

		applyMsg := ApplyMsg{
			CommandValid: false,
			Snapshot:     rf.persister.ReadSnapshot(),
		}
		rf.applyCh <- applyMsg
	}
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

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term       int
	MatchIndex int
}

// must used in lock
func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	if rf.state != FOLLOWER {
		DPrintf("leader %d step down to follower caused by get term %d", rf.me, term)
		rf.electionElapsed = 0
	}
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.persist()
}

func upToDate(term1 int, index1 int, term2 int, index2 int) bool {
	return term1 > term2 || (term1 == term2 && index1 >= index2)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("server %d RequestVote %v", rf.me, args)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {
		rf.stepDown(args.Term)
	}

	granted := false
	lastLogIndex := rf.snapshotLastIndex + len(rf.log)

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		upToDate(args.LastLogTerm, args.LastLogIndex, rf.logTerm(lastLogIndex), lastLogIndex) {
		granted = true
		rf.votedFor = args.CandidateID
	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("follower %d handle append entries, prev term index [@%d - %d], len entries %d",
		rf.me, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term ||
		(rf.currentTerm == args.Term && rf.state != FOLLOWER) {
		rf.stepDown(args.Term)
	}

	reply.Success = false

	if rf.currentTerm == args.Term {

		rf.electionElapsed = 0

		// logTerm gurantee snapshot or 0 match
		if rf.logTerm(args.PrevLogIndex) == args.PrevLogTerm {

			reply.Success = true

			var index = args.PrevLogIndex
			for i := 0; i < len(args.Entries); i++ {
				index++
				if rf.logTerm(index) != args.Entries[i].Term {
					rf.log = rf.log[:index-rf.snapshotLastIndex-1]
					rf.log = append(rf.log, args.Entries[i])
				}
			}

			commitIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.advanceCommitIndexTo(commitIndex)

			reply.MatchIndex = index
			DPrintf("follower %d prev [@%d~%d] match, try to append %d entry, reply index [%d]",
				rf.me, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries), reply.MatchIndex)
		} else {
			reply.Success = false
			pos := args.PrevLogIndex - rf.snapshotLastIndex - 1
			if pos >= len(rf.log) {
				reply.MatchIndex = rf.snapshotLastIndex + len(rf.log)
			} else {
				unmatchTerm := rf.logTerm(args.PrevLogIndex)
				nextIndex := args.PrevLogIndex
				for nextIndex > rf.snapshotLastIndex && rf.logTerm(nextIndex) == unmatchTerm {
					nextIndex--
				}
				reply.MatchIndex = nextIndex
			}
			DPrintf("follower %d reply prev index %d mismatch [leader @%d, me @%d] and ask for next index [%d]",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.logTerm(args.PrevLogIndex), reply.MatchIndex)
		}
	}

	rf.persist()
	reply.Term = rf.currentTerm
}

// InstallSnapshot install snapshot from leader
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("follower %d handle InstallSnapshot  [@%d - %d] from leader %d",
		rf.me, args.LastIncludedTerm, args.LastIncludedIndex, args.LeaderId)

	// not my leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("follower %d reject install before leader term %d < my term %d",
			rf.me, args.Term, rf.currentTerm)
		return
	}

	rf.electionElapsed = 0

	// I already have all the information contained in the snapshot
	if rf.snapshotLastIndex >= args.LastIncludedIndex ||
		rf.logTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		DPrintf("follower %d reject install already has snapshot info,"+
			"follwer snap term index [@%d - %d], last term index [@%d - %d]",
			rf.me, rf.snapshotLastIndex, rf.snapshotLastTerm,
			args.LastIncludedIndex, rf.logTerm(args.LastIncludedIndex))
		reply.Term = rf.currentTerm
		reply.MatchIndex = args.LastIncludedIndex
		return
	}

	// Now I need to install the snapshot
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.Term
	rf.log = rf.log[len(rf.log):]
	rf.commitIndex = rf.snapshotLastIndex

	rf.persister.SaveStateAndSnapshot(rf.raftState(), args.Data)

	ApplyMsg := ApplyMsg{
		CommandValid: false,
		Snapshot:     args.Data,
	}
	rf.applyCh <- ApplyMsg

	DPrintf("follower %d install snapshot", rf.me)

	reply.Term = rf.currentTerm
	reply.MatchIndex = args.LastIncludedIndex
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("===> call install snapshot api server %d, args %v", server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isLeader = true
		entry := Entry{}
		entry.Term = rf.currentTerm
		entry.Command = command
		rf.log = append(rf.log, entry)
		index = rf.snapshotLastIndex + len(rf.log)
		term = rf.currentTerm

		// 在这里犯了一个严重的问题, 没有在这里调用replicate, 导致lab3测试出现可用性问题,
		// 一度绝望
		// 看测试200*4个请求,每个心跳50ms, 4*200*50 = 40s
		// 无论如何都会超时 30s
		// lab上给的测试结果1s完成是怎么做到的?

		// replicate逻辑如果在这里不调用而只等待50ms的心跳来提交信息
		// 那么每一条命令的提交都至少需要消耗50ms
		// 命令提交之后应当立即replicate到follower并反馈客户端
		// 否则性能取决于心跳是可笑的

		// Raft论文中关于broadcast time的描述
		// > Raft RPCS typically require the recipient to persist
		// > information to stable storage, so the broadcast tiem may range
		// > from 0.5ms to 20ms
		// 如果按最小计算和50ms心跳,那么本来50ms可以提交100个请求变成了只能提交一个请求

		// Raft 论文中关于replicate的描述
		// > Once a leader has been elected, it begins servicing client requests
		// > The leader appends the command to its log as a new entry, then issues
		// > AppendEntries RPCS in parallel to each of the other servers to replicate
		// > the entry
		// 无需等待心跳

		// 最开始犯这个错误可能是因为follow https://raft.github.io/
		// Raft Visualization 上的表现

		rf.replicate()
		rf.persist()
	}

	return index, term, isLeader
}

// TakeSnapshot allow caller to take snapshot at any time
func (rf *Raft) TakeSnapshot(snapshot []byte) {
	// A bug occur here
	// logTerm depends on snapshotLastIndex, so we must not modify it
	// before we change state

	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapIndex := rf.commitIndex
	snapTerm := rf.logTerm(snapIndex)
	logPos := rf.logPos(rf.commitIndex)

	rf.snapshotLastIndex = snapIndex
	rf.snapshotLastTerm = snapTerm
	rf.log = rf.log[logPos+1:]

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	state := w.Bytes()

	DPrintf("server %d TakeSnapshot [@%d- %d]",
		rf.me, rf.snapshotLastTerm, rf.snapshotLastIndex)
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("server %d being killed", rf.me)
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
		if 2*voted > len(rf.peers) { // become leader succeed
			nextIndex := len(rf.log) + 1
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = 0
			}
			rf.state = LEADER
			rf.electionElapsed = 0
			DPrintf("%d become leader of term %d", rf.me, rf.currentTerm)

			// > Once a candidate wins an election, it
			// > becomes leader. It then sends heartbeat messages to all of
			// > the other servers to establish its authority and prevent new
			// > elections.
			// 成为leader之后立即发送replicate mesage
			rf.replicate()
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("leader %d handle follower %d AppendEntriesReply %v", rf.me, peer, reply)

	if rf.currentTerm < reply.Term {
		rf.stepDown(reply.Term)
		return
	}

	if rf.state == LEADER && rf.currentTerm == reply.Term {
		if reply.Success {
			rf.matchIndex[peer] = max(rf.matchIndex[peer], reply.MatchIndex)
			rf.matchIndex[rf.me] = rf.snapshotLastIndex + len(rf.log)
			rf.nextIndex[peer] = reply.MatchIndex + 1

			if rf.matchIndex[peer] > rf.commitIndex {
				matchCopy := make([]int, len(rf.matchIndex))
				copy(matchCopy, rf.matchIndex)
				sort.Ints(matchCopy)
				DPrintf("leader %d handle append reply from %d term index [@%d-%d], match index change to [%v]",
					rf.me,
					peer,
					reply.Term, reply.MatchIndex,
					rf.matchIndex)
				advanceIndex := matchCopy[len(rf.peers)/2]

				// > If there exists an N such that N > commitIndex, a majority
				// > of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// > set commitIndex = N

				// 这里是否会发生 advanceIndex > rf.commitIndex, 但是term却不是当前term的情况呢?
				// 答案是,是的,会发生

				// 注意论文中的这一段描述

				// > However, a leader cannot immediately conclude that an entry
				// > from a previous term is committed once it is stored
				// > on a majority of servers

				// > Raft never commits log entries from previous terms by counting replicas.
				// > Only log entries from the *leader's current term* are committed by counting
				// > replicas. Once an entry from the current term has been committed in this way,
				// > sthen all prior entries are committed indirectly because of the Log Matching
				// > Property (即一个term和index相同,那么之前的term和indx都相同)

				// 这对应的是图8, 非常的subtle
				// 大概就是在index2的时候,因为挂掉的原因有term2和term3当了不同的leader
				// 并且给index2给了不同的内容
				// 总共5个server, term2的那个派发了1个server(包含自己就是2个), 所以还剩3个可以选出term3
				// 这个时候term3下线, term2上线又被选成了leader term4
				// 此时term4把 index2 apply给了第三个server,达成majority条件
				// 即 advanceIndex > rf.commitIndex, 但 term = term2
				// 这个时候知道term4 leader如果挂了
				// term3 leader重新选成了leader term5
				// 能把自己的状态给复制出去
				// 相当于index2的内容需要被改掉,违背了index apply之后不许变动这个前提

				// term3 leader在term4挂掉后还能选成leader是因为term4并没有把自己的log复制出去
				// 选举时无法通过 upToDate (term更大或者log更长) 来禁止term3这样的重新选举为leader

				if advanceIndex > rf.commitIndex && rf.logTerm(advanceIndex) == rf.currentTerm {
					rf.advanceCommitIndexTo(advanceIndex)
				}
			}
		} else {
			rf.nextIndex[peer] = max(1, reply.MatchIndex)
		}
	}
}

func (rf *Raft) handleInstallSnapsnotReply(peer int, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("leader %d handle install snapshot reply [@%d~%d]",
		rf.me, reply.Term, reply.MatchIndex)

	if rf.currentTerm < reply.Term {
		rf.stepDown(reply.Term)
	} else {

		rf.nextIndex[peer] = max(rf.nextIndex[peer], reply.MatchIndex+1)
		rf.matchIndex[peer] = max(rf.matchIndex[peer], reply.MatchIndex)
		DPrintf("leader %d handle peer %d install snapshot reply match index %d,"+
			" set next index %d and match index %d",
			rf.me, peer, reply.MatchIndex, rf.nextIndex[peer], rf.matchIndex[peer])
	}
}

func (rf *Raft) advanceCommitIndexTo(commitIndex int) {
	if commitIndex > rf.commitIndex {
		if rf.state == LEADER {
			DPrintf("leader(@%d) %d advance commit from %d to %d",
				rf.me, rf.currentTerm, rf.commitIndex, commitIndex)
		} else {
			DPrintf("follower %d advance commit from %d to %d", rf.me, rf.commitIndex, commitIndex)
		}
		for i := rf.commitIndex; i < commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Command = rf.log[i-rf.snapshotLastIndex].Command
			applyMsg.CommandIndex = i + 1
			applyMsg.CommandValid = true
			rf.applyCh <- applyMsg
			rf.commitIndex++
		}
	}
}

func (rf *Raft) resetElectionTimeout(electionTimeout int) {
	rf.electionTimeout = electionTimeout + rand.Intn(electionTimeout)
}

// must be called in already rf locked function
func (rf *Raft) replicate() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {

			// If leader hold the log entry need, then send append entries request
			// else send install snapshot

			// A dead dependency bug happened here
			// Client sad 23 mismacth and ask for 22
			// But 22 is already in snapshot
			// So try install snapshot
			// Client says I don't need the snapshot, because I already have 22, give me 23

			// To get out of it, one way is set next index to 23 in such case when follower
			// reply I alreay have 22

			DPrintf("leader %d store follower %d need index %d, leader snapshot index %d, len log %d",
				rf.me, i, rf.nextIndex[i], rf.snapshotLastIndex, len(rf.log))
			if rf.nextIndex[i] <= rf.snapshotLastIndex {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotLastIndex,
					LastIncludedTerm:  rf.snapshotLastTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				go func(peer int) {
					DPrintf("leader %d send snap shot [@%d~%d] to follower %d",
						rf.me, rf.snapshotLastTerm, rf.snapshotLastIndex, peer)
					reply := &InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(peer, args, reply)
					if ok {
						rf.handleInstallSnapsnotReply(peer, reply)
					} else {
						DPrintf("leader %d send snapshot to %d failed ", rf.me, peer)
					}
				}(i)
			} else {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logTerm(rf.nextIndex[i] - 1),
				}

				if rf.matchIndex[i] < args.PrevLogIndex {
					args.Entries = make([]Entry, 0)
				} else {
					args.Entries = rf.log[args.PrevLogIndex-rf.snapshotLastIndex:]
				}

				if len(args.Entries) == 0 {
					if rf.matchIndex[i] < args.PrevLogIndex {
						DPrintf("leader %d (snap [@%d~%d]) send test match index to %d (match %d), prev term index [@%d~%d]",
							rf.me, rf.snapshotLastTerm, rf.snapshotLastIndex,
							i, rf.matchIndex[i],
							args.PrevLogTerm, args.PrevLogIndex)
					} else {
						DPrintf("leader %d know follow %d is up to date (last index %d), just heartbeat",
							rf.me, i, rf.snapshotLastIndex+len(rf.log))
					}
				} else {
					DPrintf("leader %d (snap [@%d~%d]) send append entries(len(%d)) to %d (match %d), prev term index [@%d~%d]",
						rf.me, rf.snapshotLastTerm, rf.snapshotLastIndex,
						len(args.Entries),
						i, rf.matchIndex[i],
						args.PrevLogTerm, args.PrevLogIndex)
				}

				go func(peer int) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(peer, &args, &reply)
					if ok {
						rf.handleAppendEntriesReply(peer, &reply)
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server %d tick to %d", rf.me, rf.ticktime)
	rf.ticktime++

	if rf.state == LEADER {
		rf.heartbeatElapsed++
		// send heart beat if it is leader
		if rf.heartbeatElapsed >= rf.heartbeatTimeout {
			rf.heartbeatElapsed = 0
			rf.replicate()
		}
	} else {
		rf.electionElapsed++
		// send election if it is candidate or it is follower
		if rf.electionElapsed >= rf.electionTimeout {
			rf.electionElapsed = 0
			rf.resetElectionTimeout(configElectionTimeout)

			rf.state = CANDIDATE
			// legal term starts from 1
			rf.currentTerm++
			for i := 0; i < len(rf.peers); i++ {
				rf.voteGranted[i] = false
			}
			rf.voteGranted[rf.me] = true
			rf.votedFor = rf.me
			rf.persist()

			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = len(rf.log)
			args.LastLogTerm = rf.logTerm(args.LastLogIndex)

			DPrintf("tick %d, server %d vote for self and try to become leader of term @%d",
				rf.ticktime, rf.me, args.Term)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(i int) {
						reply := RequestVoteReply{}
						DPrintf("tick %d, server %d ask for vote to server %d", rf.ticktime, rf.me, i)
						ok := rf.sendRequestVote(i, &args, &reply)
						if ok {
							rf.handleRequestVoteReply(i, &reply)
						}
					}(i)
				}
			}
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

func (rf *Raft) logPos(index int) int {
	return index - rf.snapshotLastIndex - 1
}

func (rf *Raft) logTerm(index int) int {
	pos := rf.logPos(index)

	if pos == -1 {
		return rf.snapshotLastTerm
	}

	if pos < -1 || pos >= len(rf.log) {
		return -1
	}

	return rf.log[pos].Term
}

func (rf *Raft) containsEntry(index int) bool {
	pos := index - rf.snapshotLastIndex - 1
	if pos < 0 || pos >= len(rf.log) {
		return false
	}
	return true
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

	rf.heartbeatTimeout = configHeartBeatTimeout
	rf.resetElectionTimeout(configElectionTimeout)

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0

	DPrintf("server %d up", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("server %d read persist complete", rf.me)

	go func() {
		for {
			time.Sleep(50 * time.Millisecond)
			rf.tick()
		}
	}()
	return rf
}
