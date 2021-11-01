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
	"6.824/labs/labgob"
	"6.824/labs/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
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
// Snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg

	VoteFor int //persist fields,capital
	Term    int
	Log     []LogEntry

	CommitIndex          int
	appliedLast          int
	nextIndex            []int
	matchIndex           []int
	majorityNum          int
	lastReceiveHeartBeat time.Time //for election TimeOut,update when receive heartbeat
	state                int

	LastIncludedIndex int
	LastIncludedTerm  int
	//isSnapshot        bool

	appendEntryCh chan bool //optimization 采用chan代替long loop
	followerTime  map[int]time.Time
	// Your data here (2A, 2B, 2C).1
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) GetLock() {
	rf.mu.Lock()
}
func (rf *Raft) ReleaseLock() {
	rf.mu.Unlock()
}

const ElectionTimeOut = 300
const HeartBeat = 120
const (
	Leader = iota
	Candidate
	Follower
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// warn GetState不加锁,因为后续在处理请求时,会频繁调用GetState()函数,影响raft layer的performance,并且读操作此时不需要线程安全，data race is acceptable
	term := rf.Term
	isLeader := rf.state == Leader
	return term, isLeader
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
	defer DPrintf("rf [%v] persist term: %v len(log): %v ", rf.me, rf.Term, len(rf.Log))
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//rf.mu.Lock()
	e.Encode(rf.Log)
	e.Encode(rf.Term)
	e.Encode(rf.VoteFor)
	//rf.mu.Unlock()
	data := w.Bytes()
	rf.Persister.SaveRaftState(data)
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
	var Log []LogEntry
	var Term int
	var VoteFor int
	if d.Decode(&Log) != nil ||
		d.Decode(&Term) != nil || d.Decode(&VoteFor) != nil {
		//error...
		DPrintf("Decode error happen\n")
	} else {
		//rf.mu.Lock()
		rf.Log = Log
		rf.Term = Term
		rf.VoteFor = VoteFor
		//rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int
	LastEntryTerm int //vote restriction,enable leader completeness property
	LastEntryIdx  int
	CandidateId   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	RequestTerm int
	IsSuccess   bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) getLastLogEntryWithNoLock() *LogEntry {
	logEntry := LogEntry{}
	if len(rf.Log) > 0 {
		return &rf.Log[len(rf.Log)-1]
	}
	logEntry.Index = rf.LastIncludedIndex
	logEntry.Term = rf.LastIncludedTerm
	return &logEntry
}
func (rf *Raft) initialLeaderUnlock() {
	DPrintf("rf [%v] initialLeaderUnlock begin", rf.me)
	lastLogEntry := rf.getLastLogEntryWithNoLock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogEntry.Index + 1
		rf.matchIndex[i] = -1
	}
	rf.state = Leader
	rf.appendEntryCh <- true
	DPrintf("rf [%v] appendEntryCh finish", rf.me)
	//go rf.doSendHeartBeat()
}
func (rf *Raft) startElection() {
	DPrintf("rf [%v] startElection", rf.me)
	rf.mu.Lock()
	rf.lastReceiveHeartBeat = time.Now()
	rf.Term++
	rf.VoteFor = rf.me
	logEntry := rf.getLastLogEntryWithNoLock()
	requestVoteArgs := RequestVoteArgs{
		Term:          rf.Term,
		LastEntryTerm: logEntry.Term,
		LastEntryIdx:  logEntry.Index,
		CandidateId:   rf.me,
	}
	rf.persist()
	rf.mu.Unlock() //warn 由于在选举过程中candidate可能会变为follower,因此提前解锁
	voteCount := 1 //TODO voteCount加锁
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if rf.state == Candidate {
				go func(i int) { // warn go function closure
					var requestVoteReply RequestVoteReply
					ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
					if ok {
						voteGranted := rf.handleRequestVoteReply(&requestVoteReply)
						if voteGranted == 1 {
							DPrintf("rf [%v] grant vote to rf [%v]", i, rf.me)
							voteCount += voteGranted
							if voteCount == rf.majorityNum { // warn 如果rf已经从candidate当选为leader,不再重新initialAfterElection
								rf.initialLeaderUnlock() // warn rf从candidate变为leader后应立即向其他server发送AppendEntries Rpc,避免其他server发起选举
							}
						} else {
							DPrintf("rf [%v] grant no vote to rf [%v]", i, rf.me)
						}
					}
				}(i)
			} else {
				return
			}
		}
	}
}
func (rf *Raft) handleRequestVoteReply(requestVoteReply *RequestVoteReply) int {
	rf.mu.Lock()
	DPrintf("rf [%v] handleRequestVoteReply begin %v", rf.me, requestVoteReply)
	defer DPrintf("rf [%v] handleRequestVoteReply finish %v", rf.me, requestVoteReply)
	defer rf.mu.Unlock()
	if requestVoteReply.Term > rf.Term {
		rf.becomeFollower(requestVoteReply.Term)
		return 0
	}
	//warn to deal with rpc delay,leader处理relay必须在自己的term内
	if requestVoteReply.RequestTerm < rf.Term {
		return 0
	}
	if requestVoteReply.IsSuccess {
		return 1
	} else {
		return 0
	}

}
func (rf *Raft) becomeFollower(term int) {
	//rf.mu.Lock()  warn liveLock Problem
	//defer rf.mu.Unlock()
	DPrintf("rf [%v] become follower %v ", rf.me, rf)
	rf.Term = term
	rf.state = Follower
	rf.VoteFor = -1
	rf.persist()
}
func (reply *RequestVoteReply) returnFailureVote(term int, requestTerm int) {
	reply.RequestTerm = requestTerm
	reply.Term = term
	reply.IsSuccess = false
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("rf [%v] RequestVote called begin", rf.me)
	DPrintf("rf [%v] args:%v", rf.me, args)
	DPrintf("rf [%v] detail: %v", rf.me, rf)
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("rf [%v] RequestVote called finish,%v", rf.me, reply)
	if args.Term < rf.Term {
		reply.returnFailureVote(rf.Term, args.Term)
		return
	}
	lastLogEntry := rf.getLastLogEntryWithNoLock()
	if args.Term > rf.Term { //warn : rf需要先更新term,然后再返回,不然会出现没有更新term然后返回的情况,即该if块应在以下if块前执行
		rf.Term = args.Term
		rf.VoteFor = -1
		rf.state = Follower
	}
	if args.LastEntryTerm < lastLogEntry.Term {
		reply.returnFailureVote(rf.Term, args.Term)
		return
	}
	if args.LastEntryTerm == lastLogEntry.Term && args.LastEntryIdx < lastLogEntry.Index {
		reply.returnFailureVote(rf.Term, args.Term)
		return
	}
	if rf.VoteFor != -1 {
		reply.returnFailureVote(rf.Term, args.Term)
	} else {
		reply.Term = rf.Term
		reply.IsSuccess = true
		reply.RequestTerm = args.Term
		rf.VoteFor = args.CandidateId
		rf.lastReceiveHeartBeat = time.Now()
	}
	return
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

type AppendEntriesArgs struct {
	//For Consistency Check Information
	Term        int
	PrevLogIdx  int
	PrevLogTerm int
	LeaderId    int

	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	RequestTerm   int
	ConflictTerm  int
	ConflictIndex int
	IsSuccess     bool
}

type InstallSnapshotArgs struct {
	Snapshot          []byte
	Term              int
	LeaderId          int
	LastIncludedIndex int
}
type InstallSnapshotReply struct {
	IsSuccess         bool
	Term              int
	LastIncludedIndex int
}

//type Snapshot struct {
//	State             map[int]map[string]string
//	OldConfig shardmaster.Config
//	NewConfig shardmaster.Config
//	ShardConfigNum [shardmaster.NShards]int
//	LastIncludedIndex int
//	LastIncludedTerm  int
//	ClientMap         map[int]map[int64]int
//}

func (rf *Raft) TrimLogEntryWithNoLock(lastIncludedIndex int, lastIncludedTerm int) {
	// warn change lock 被调用时kv有锁,这里rf必须上锁,因为log必须一致
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf("rf [%v] TrimLogEntry lastIncludedIndex %v len(rf.Log):%v detail : %v", rf.me, lastIncludedIndex, len(rf.Log), rf)
	if len(rf.Log) > 0 { // warn trim时已经确认是最新的installSnapshot rpc,但是raft可能包含stale log entry,因此要discard
		//trimIndex := lastIncludedIndex
		//if rf.isSnapshot{
		trimIndex := lastIncludedIndex - rf.LastIncludedIndex - 1 //change 当server restart后save snapshot时,其实isSnapshot是true,但是由于restart,变成了false,
		// 这里修改,如果没有snapshot,lastIncludedIndex =-1,trimIndex依然成立
		//}/
		if trimIndex < len(rf.Log) {
			rf.Log = rf.Log[trimIndex+1:]
		} else {
			rf.Log = rf.Log[len(rf.Log):]
		}
	}
	rf.LastIncludedTerm = lastIncludedTerm
	rf.LastIncludedIndex = lastIncludedIndex
	//rf.isSnapshot = true // warn rf Snapshot更新必须放在最后,不然会被上面代码用到
	DPrintf("rf [%v] TrimLogEntry lastIncludedIndex %v len(rf.Log):%v", rf.me, lastIncludedIndex, len(rf.Log))
}

//func (rf *Raft) GetSnapshot(data []byte) (Snapshot, bool) {
//	snapshot := Snapshot{}
//	if data == nil || len(data) < 1 { // bootstrap without any state?
//		return snapshot, false
//	}
//	r := bytes.NewBuffer(data)
//	d := labgob.NewDecoder(r)
//	var ClientMap map[int]map[int64]int
//	var State map[int]map[string]string
//	var LastIncludedTerm int
//	var LastIncludedIndex int
//	//DPrintf("rf [%v] restoreSnapshot size%v",kv.me,len(data))
//	if d.Decode(&State) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil || d.Decode(&ClientMap) != nil {
//		//error...
//		DPrintf("Decode Snapshot error happen\n")
//	} else {
//		snapshot = Snapshot{
//			State:             State,
//			LastIncludedIndex: LastIncludedIndex,
//			LastIncludedTerm:  LastIncludedTerm,
//			ClientMap:         ClientMap,
//		}
//	}
//	return snapshot, true
//}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("rf [%v] InstallSnapshot begin,detail:%v", rf.me, rf)
	DPrintf("args.Term: %v args.LastIncludedIndex: %v", args.Term, args.LastIncludedIndex)
	defer DPrintf("rf [%v] InstallSnapshot finish", rf.me)
	if rf.Term > args.Term || rf.LastIncludedIndex >= args.LastIncludedIndex { // change deal old rpc
		reply.IsSuccess = false
		reply.Term = rf.Term
		return
	}
	data := args.Snapshot
	reply.Term = rf.Term
	reply.LastIncludedIndex = args.LastIncludedIndex
	reply.IsSuccess = true
	applyMsg := ApplyMsg{
		CommandValid: false,
		Command:      data,
		CommandIndex: 0,
		CommandTerm:  0,
	}
	DPrintf("rf%v send applyMsg channel begin %v", rf.me, applyMsg) //TODO snapshot解码放在server端
	rf.applyCh <- applyMsg
	DPrintf("rf%v send applyMsg channel finish %v", rf.me, applyMsg)
	// change 如果此时crash,snapshot 和lastIncluded是不匹配的,但是snapshot中的server state和 lastIncludedIndex是匹配的
	//snapshot, ok := rf.GetSnapshot(data) //
	//if ok {
	//	reply.Term = rf.Term
	//	reply.LastIncludedIndex = args.LastIncludedIndex
	//	reply.IsSuccess = true
	//	applyMsg := ApplyMsg{
	//		CommandValid: false,
	//		Command:      snapshot,
	//		CommandIndex: 0,
	//		CommandTerm:  0,
	//	}
	//	DPrintf("rf%v send applyMsg channel begin %v", rf.me, applyMsg)
	//	rf.applyCh <- applyMsg
	//	DPrintf("rf%v send applyMsg channel finish %v", rf.me, applyMsg)
	//	// change 如果此时crash,snapshot 和lastIncluded是不匹配的,但是snapshot中的server state和 lastIncludedIndex是匹配的
	//} else {
	//	fmt.Printf("data == nil || len(data) < 1")
	//}

}
func (rf *Raft) HandleInstallSnapshotReply(reply *InstallSnapshotReply, server int) {
	DPrintf("rf [%v] HandleInstallSnapshotReply begin lastIncludedIndex %v Term %v nextIndex %v", rf.me, rf.LastIncludedIndex, rf.Term, rf.nextIndex[server])

	DPrintf("reply.Term : %v rf.Term %v reply.LastIncludedIndex %v rf.LastIncludedIndex %v", reply.Term, rf.Term, reply.LastIncludedIndex, rf.LastIncludedIndex)
	if reply.Term != rf.Term || reply.LastIncludedIndex != rf.LastIncludedIndex {
		return
	}
	if reply.IsSuccess {
		rf.nextIndex[server] = rf.LastIncludedIndex + 1
	}
	DPrintf("rf [%v] HandleInstallSnapshotReply finish lastIncludedIndex %v Term %v nextIndex %v", rf.me, rf.LastIncludedIndex, rf.Term, rf.nextIndex[server])
	return
}
func (rf *Raft) initialInstallSnapshotArgs() (InstallSnapshotArgs, InstallSnapshotReply) {
	snapshot := rf.Persister.ReadSnapshot()
	args := InstallSnapshotArgs{
		Snapshot:          snapshot,
		Term:              rf.Term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
	}
	reply := InstallSnapshotReply{}
	return args, reply
}
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("rf [%v] SendInstallSnapshot to %v,detail:lastIncludedIndex : %v", rf.me, server, args.LastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) initialAppendEntriesArgs(server int) AppendEntriesArgs {
	rf.mu.Lock()
	DPrintf("rf [%v] initialAppendEntriesArgs begin rf [%v],nextIndex %v lastIncludedIndex:%v detail:%v", rf.me, server, rf.nextIndex[server], rf.LastIncludedIndex, rf)
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{}
	lastLogEntry := rf.getLastLogEntryWithNoLock()
	DPrintf("lastLogEntry.Index:%v", lastLogEntry.Index)
	if rf.nextIndex[server] <= rf.LastIncludedIndex { // 对于lag behind的server,仍然要定期发送appendEntries rpc, appendEntires rpc和 installSnapshot RPC相分离
		go func() {
			args, reply := rf.initialInstallSnapshotArgs()
			ok := rf.SendInstallSnapshot(server, &args, &reply)
			if ok {
				rf.HandleInstallSnapshotReply(&reply, server)
			}
		}()
		args.PrevLogIdx = lastLogEntry.Index
		args.PrevLogTerm = lastLogEntry.Term
		args.LeaderId = rf.me
		args.LeaderCommit = rf.CommitIndex
		return args
	}

	prevLogIndex := -1
	if lastLogEntry.Index+1 == rf.nextIndex[server] {
		args.PrevLogIdx = lastLogEntry.Index
		args.PrevLogTerm = lastLogEntry.Term
		args.Entries = rf.Log[len(rf.Log):]
	} else {
		prevLogIndex = rf.nextIndex[server] - rf.LastIncludedIndex - 2 //change optimization isSnapshot
		//if rf.isSnapshot {
		//
		//} else {
		//	prevLogIndex = rf.nextIndex[server] - 1
		//}
		if prevLogIndex == -1 {
			args.PrevLogIdx = rf.LastIncludedIndex
			args.PrevLogTerm = rf.LastIncludedTerm
		} else {
			args.PrevLogIdx = rf.Log[prevLogIndex].Index
			args.PrevLogTerm = rf.Log[prevLogIndex].Term // stupid error
		}
		args.Entries = rf.Log[prevLogIndex+1:]
	}
	args.LeaderId = rf.me
	args.LeaderCommit = rf.CommitIndex
	DPrintf("rf [%v] initialAppendEntriesArgs finish rf [%v],args.PrevLogTerm %v,args.PrevLogIdx %v", rf.me, server, args.PrevLogTerm, args.PrevLogIdx)
	return args
}
func (rf *Raft) updateLeaderCommitIndexWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf [%v] updateLeaderCommitIndex begin commitIndex: %v", rf.me, rf.CommitIndex)
	var matchIndex []int
	for i, index := range rf.matchIndex {
		if i != rf.me {
			matchIndex = append(matchIndex, index)
		}
	}
	sort.Ints(matchIndex)
	DPrintf("rf [%v] matchIndex %v", rf.me, matchIndex)
	oldCommitIndex := rf.CommitIndex
	rf.CommitIndex = matchIndex[rf.majorityNum-1]
	absIndex := rf.CommitIndex - rf.LastIncludedIndex - 1 //change warn leader只更新自己任期内的commitIndex
	if absIndex > -1 && rf.Log[absIndex].Term == rf.Term && rf.CommitIndex > oldCommitIndex {
		go rf.doSendAppendEntries(rf.Term, false) // change after commitIndex update,send commitIndex to follower
		go rf.UpdateAppliedLastWithLock()
	}
	DPrintf("rf [%v] updateLeaderCommitIndex finish commitIndex: %v", rf.me, rf.CommitIndex)
}
func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
	DPrintf("rf [%v] handleAppendEntriesReply rf [%v] begin ,%v,%v,nextIndex before %v", rf.me, server, args, reply, rf.nextIndex[server])
	defer DPrintf("rf [%v] handleAppendEntriesReply rf [%v] finish ,update nextIndex%v", rf.me, server, rf.nextIndex[server])
	if rf.Term > reply.RequestTerm { //warn deal network delay,old stale leader,old rpc
		return
	}
	if rf.Term < reply.Term {
		rf.becomeFollower(reply.Term)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.followerTime[server] = time.Now() // warn reset follower lastReceiveHeartBeat
	if reply.IsSuccess {
		if len(args.Entries) > 0 {
			if args.Entries[len(args.Entries)-1].Index > rf.matchIndex[server] { //change deal old rpc
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index //update nextIndex,matchIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				if args.Entries[len(args.Entries)-1].Term == rf.Term { //
					go rf.updateLeaderCommitIndexWithLock() //warn 此处的condition确保leader只更新自己term的commitIndex
				}
			}
		}
	} else {
		if reply.ConflictTerm == 0 {
			if reply.ConflictIndex <= rf.LastIncludedIndex {
				args, reply := rf.initialInstallSnapshotArgs()
				ok := rf.SendInstallSnapshot(server, &args, &reply)
				if ok {
					rf.HandleInstallSnapshotReply(&reply, server)
				}
				return
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			for i := len(rf.Log) - 1; i >= 0; i-- { //optimization
				if rf.Log[i].Term == reply.ConflictTerm {
					rf.nextIndex[server] = i
					return
				} else if rf.Log[i].Term < reply.ConflictTerm {
					if reply.ConflictIndex <= rf.LastIncludedIndex {
						args, reply := rf.initialInstallSnapshotArgs()
						ok := rf.SendInstallSnapshot(server, &args, &reply)
						if ok {
							rf.HandleInstallSnapshotReply(&reply, server)
						}
						return
					}
					rf.nextIndex[server] = reply.ConflictIndex
					return
				}
			}
		}
	}
}
func (rf *Raft) updateLogEntryWithNoLock(args *AppendEntriesArgs, matchPrevLogIndex int) {
	//warn if no conflict,must no delete,maybe old rpc,deal with AppendEntries RPC reorder,raft delete entries only when exists conflict,
	DPrintf("rf [%v] matchPrevLogIndex:%v", rf.me, matchPrevLogIndex)
	j := 0
	i := matchPrevLogIndex + 1
	for ; i < len(rf.Log); i++ {
		if j < len(args.Entries) && rf.Log[i].Term == args.Entries[j].Term {
			j++
		} else {
			break
		}
	}
	if j < len(args.Entries) { // big warn bug waste,如果不加condition,可能已经被commit的log会被删除掉,
		rf.Log = rf.Log[:i]
		rf.Log = append(rf.Log, args.Entries[j:]...)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("rf [%v] AppendEntries called begin detail: %v %v %v", rf.me, rf, args, reply)
	rf.mu.Lock()
	DPrintf("rf [%v] AppendEntries called get lock detail: %v %v %v", rf.me, rf, args, reply)
	//if len(args.Entries) > 0{
	//	for _,entry := range args.Entries{
	//		DPrintf("rf [%v] entry[%v]:",rf.me,entry.Command)
	//	}
	//}
	defer DPrintf("rf [%v] AppendEntries called Finish detail: %v %v %v", rf.me, rf, args, reply)
	defer rf.mu.Unlock()
	if args.Term > rf.Term { // warn persistent
		rf.Term = args.Term
		rf.state = Follower
		rf.persist()
	}
	if rf.Term > args.Term { //warn  old leader rpc
		rf.returnAppendEntriesWithNoLock(args, reply, false, false)
		return
	}
	lastLogEntry := rf.getLastLogEntryWithNoLock()
	DPrintf("rf [%v] AppendEntries : lastLogEntry.Index:%v", rf.me, lastLogEntry.Index)
	if args.PrevLogIdx > lastLogEntry.Index {
		rf.returnAppendEntriesWithNoLock(args, reply, true, false)
		reply.ConflictIndex = lastLogEntry.Index + 1
		reply.ConflictTerm = 0
		return
	}
	if args.PrevLogIdx < rf.LastIncludedIndex { // warn change deal same term old rpc
		//if args.Entries[len(args.Entries)-1].Index <= rf.LastIncludedIndex{
		//reply.IsSuccess = true
		reply.IsSuccess = true
		return
		//}
	}
	if args.PrevLogIdx == rf.LastIncludedIndex { //warn args.PrevLog可能小于rf.LastIncludedIndex,old rpc
		rf.updateLogEntryWithNoLock(args, -1)
		if len(args.Entries) > 0 {
			rf.persist()
		}
		rf.returnAppendEntriesWithNoLock(args, reply, true, true)
		rf.updateFollowerCommitIndexWithNoLock(args)
		return
	} else {
		//prevLogIndex := args.PrevLogIdx // change optimization snapshot
		//if rf.isSnapshot {
		prevLogIndex := args.PrevLogIdx - rf.LastIncludedIndex - 1
		//}
		prevLogEntry := rf.Log[prevLogIndex]
		if prevLogEntry.Index != args.PrevLogIdx {
			DPrintf("prevLogEntry.Index != args.PrevLogIdx")
		}
		DPrintf("rf [%v] prevLogEntry term: %v index : %v", rf.me, prevLogEntry.Term, prevLogEntry.Index)
		//DPrintf("args.PrevLogTerm: %v",args.PrevLogTerm)
		if prevLogEntry.Term == args.PrevLogTerm {
			rf.updateLogEntryWithNoLock(args, prevLogIndex)
			if len(args.Entries) > 0 {
				rf.persist()
			}
			rf.returnAppendEntriesWithNoLock(args, reply, true, true)
			rf.updateFollowerCommitIndexWithNoLock(args) //change warn updateFollowerCommitIndexWithNoLock必须和appendEntries同步,不能单独放在另一个goroutine里,不然
			//会发生多个updateFollowerCommitIndexWithNoLock并发进行的状况,导致commitIndex减少
			return
		} else {
			rf.returnAppendEntriesWithNoLock(args, reply, true, false)
			conflictIndex := prevLogIndex
			for ; conflictIndex > -1; conflictIndex-- {
				if rf.Log[conflictIndex].Term != prevLogEntry.Term {
					break
				}
			}
			reply.ConflictIndex = conflictIndex + 1
			reply.ConflictTerm = prevLogEntry.Term
			return
		}

	}
}
func (rf *Raft) doSendAppendEntries(term int, isHeartBeat bool) {
	DPrintf("rf [%v] doSendAppendEntries,state:%v,term:%v", rf.me, rf.state, rf.Term)
	duration := time.Millisecond
	if isHeartBeat {
		duration = time.Duration(HeartBeat/2) * time.Millisecond
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			followerTime := rf.followerTime[i]
			rf.mu.Unlock()
			if time.Now().Sub(followerTime) > duration {
				go func(i int) { // deal with closure parameter reference
					args := rf.initialAppendEntriesArgs(i)
					args.Term = term //warn args.term在循环过程中可能会更新,因此要固定,escape stale leader append,only current leader can make follower deal appendEntries call
					reply := AppendEntriesReply{}
					if len(args.Entries) > 0 {
						DPrintf("rf [%v] send entry %v to rf [%v]", rf.me, len(args.Entries), i)
					}
					ok := rf.SendAppendEntries(i, &args, &reply)
					if ok {
						rf.handleAppendEntriesReply(&args, &reply, i)
					}
				}(i)
			}
		} else {
			rf.lastReceiveHeartBeat = time.Now() //warn leader在每次AppendEntries时都需要resetTimeOut,不然由于timeOutElapse该leader会重新发起选举
		}
	}
}
func (rf *Raft) doSendHeartBeat() {
	//warn 如果rpc无响应,一直retry,会浪费计算资源,所以选择按照heartbeat的时间retry
	for {
		<-rf.appendEntryCh
		DPrintf("rf [%v] appendEntryCh is called", rf.me)
		term := rf.Term
		for {
			if rf.killed() { // warn confuse log
				DPrintf("rf.killed")
				break
			}
			if rf.state == Leader {
				DPrintf("rf [%v] doSendHeartBeat detail:%v", rf.me, rf)
				go rf.doSendAppendEntries(term, true)
			} else {
				break
			}
			time.Sleep(time.Millisecond * HeartBeat)
		}
	}
}
func (rf *Raft) returnAppendEntriesWithNoLock(args *AppendEntriesArgs, reply *AppendEntriesReply, isResetTimeOut bool, isSuccess bool) {
	reply.Term = rf.Term
	reply.RequestTerm = args.Term
	reply.IsSuccess = isSuccess
	if isResetTimeOut {
		rf.lastReceiveHeartBeat = time.Now()
	}
}
func (rf *Raft) updateFollowerCommitIndexWithNoLock(args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.CommitIndex {
		DPrintf("rf [%v] updateFollowerCommitIndex begin %v", rf.me, rf.CommitIndex)
		lastLogEntry := rf.getLastLogEntryWithNoLock() //warn check
		//rf.CommitIndex = args.LeaderCommit;
		if args.LeaderCommit > lastLogEntry.Index { // warn min is must,因为logEntry里可能包含没有commit的entry
			rf.CommitIndex = lastLogEntry.Index
			fmt.Printf("args.LeaderCommit > lastLogEntry.Index")
			//TODO check why this scenario happen
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		go rf.UpdateAppliedLastWithLock()
		DPrintf("rf [%v] updateFollowerCommitIndex end %v", rf.me, rf.CommitIndex)
	}
}
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) UpdateAppliedLastWithLock() {
	DPrintf("rf [%v] UpdateAppliedLastWithLock begin", rf.me)
	rf.mu.Lock() //change warn 必须加lock,因为在UpdateAppliedLast时可能会save snapshot,len(rf.Log)会由于snapshot发生改变,产生索引越界错误
	defer rf.mu.Unlock()
	DPrintf("rf [%v] UpdateAppliedLast get lock CommitIndex%v appliedLast%v", rf.me, rf.CommitIndex, rf.appliedLast)
	if rf.CommitIndex > rf.appliedLast {
		appliedLastIndex := rf.appliedLast
		if appliedLastIndex <= rf.LastIncludedIndex { // warn 当server restart,appliedLastIndex = -1,so condition need
			appliedLastIndex = -1
		} else {
			appliedLastIndex = appliedLastIndex - rf.LastIncludedIndex - 1
		}
		DPrintf("rf [%v] appliedLastIndex %v", rf.me, appliedLastIndex)
		commitIndex := rf.CommitIndex - rf.LastIncludedIndex - 1
		for i := appliedLastIndex + 1; i <= commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: rf.Log[i].Index,
				CommandTerm:  rf.Log[i].Term,
			}
			DPrintf("rf [%v] send applyMsg channel%v begin", rf.me, applyMsg)
			rf.applyCh <- applyMsg
			DPrintf("rf [%v] send applyMsg channel %v finish", rf.me, applyMsg)
		}
	}
	rf.appliedLast = rf.CommitIndex
	DPrintf("rf [%v] UpdateAppliedLast finish CommitIndex%v appliedLast%v", rf.me, rf.CommitIndex, rf.appliedLast)
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
func (rf *Raft) initialServer() {
	for {
		if !rf.killed() {
			timeOut := time.Millisecond * time.Duration(ElectionTimeOut+rand.Intn(ElectionTimeOut)) //warn 必须sleep timeOut,避免server在一个ElectionTimeOut结束后连续发起选举
			time.Sleep(timeOut)
			//DPrintf("rf%v state%v",rf.me,rf.state)
			rf.mu.Lock()
			switch rf.state {
			case Candidate:
				DPrintf("candidate [%v] startElection detail:%v", rf.me, rf)
				go rf.startElection()
			case Follower:
				//if time.Now().Sub(rf.lastReceiveHeartBeat) > (timeOut - time.Millisecond*time.Duration(10)) {
				if time.Now().Sub(rf.lastReceiveHeartBeat) > timeOut {
					rf.state = Candidate
					//DPrintf("follower %v startElection",rf)
					go rf.startElection() // warn follower转为candidate后应立即startElection,不再等待新的ElectionTimeOUt
				}
			}
			rf.mu.Unlock()
		} else {
			return
		}
	}
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock() // add lock to deal with concurrency request from client
	defer rf.mu.Unlock()
	lastLogEntry := rf.getLastLogEntryWithNoLock()
	index := lastLogEntry.Index + 1
	term := rf.Term
	isLeader := rf.state == Leader
	if isLeader {
		logEntry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.Log = append(rf.Log, logEntry)
		rf.persist()
		go rf.doSendAppendEntries(rf.Term, false)
		//rf.printLog()
		DPrintf("rf [%v] start command  detail: index %v term %v command %v", rf.me, index, term, command)
	}
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
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		Persister: persister,

		me:                   me,
		dead:                 0,
		VoteFor:              -1,
		Term:                 0,
		CommitIndex:          -1,
		appliedLast:          -1,
		Log:                  nil,
		nextIndex:            make([]int, len(peers)),
		matchIndex:           make([]int, len(peers)),
		lastReceiveHeartBeat: time.Now(),
		state:                Follower,
		applyCh:              applyCh,
		LastIncludedIndex:    -1,
		LastIncludedTerm:     0,
		appendEntryCh:        make(chan bool),
		followerTime:         make(map[int]time.Time),
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.followerTime[i] = time.Now()
		}
	}
	serverNum := len(rf.peers)
	if serverNum%2 == 1 {
		rf.majorityNum = serverNum/2 + 1
	} else {
		rf.majorityNum = serverNum / 2
	}
	rf.readPersist(persister.ReadRaftState()) // warn rf restart ,restore persistent
	DPrintf("rf [%v] restart detail : %v", rf.me, rf)
	go rf.initialServer()
	go rf.doSendHeartBeat()
	// Your initialization code here (2A, 2B, 2C).
	//fmt.Printf("rf initialize begin")
	// initialize from state persisted before a crash

	return rf
}
