package raft

//
// this is an outline of the API that raft Must expose to
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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HeartbeatInterval   = 100 * time.Millisecond
	ElectionTimeoutBase = 300
	Follower            = "follower"
	Candidate           = "candidate"
	Leader              = "leader"
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

//
// A Go object implementing a single Raft peer.
//
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	//non-volatile field
	currentTerm int
	log         []*Entry
	voteFor     int
	//volatile field
	lastHeartBeat     time.Time
	LastApplied       int
	commitIndex       int
	matchIndex        []int
	nextIndex         []int
	ApplyCh           chan ApplyMsg
	state             string
	quorum            int
	lastIncludedIndex int
	lastIncludedTerm  int
	cond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server Must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) getRaftState() []byte {
	var data []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.log) != nil || e.Encode(rf.voteFor) != nil || e.Encode(rf.lastIncludedIndex) != nil || e.Encode(rf.lastIncludedTerm) != nil {
		fmt.Println("encode error")
	} else {
		data = w.Bytes()
	}
	return data
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.Persister.SaveRaftState(rf.getRaftState())

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []*Entry
	var voteFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.voteFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it comMunicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer DPrintf("CondInstallSnapshot : %v", rf)
	DPrintf("CondInstallSnapshot : %v,lastIncludedIndex : %v lastIncludedTerm : %v", rf, lastIncludedIndex, lastIncludedTerm)
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	logIndex := lastIncludedIndex - rf.lastIncludedIndex - 1
	if logIndex >= len(rf.log) {
		rf.log = rf.log[len(rf.log):]
	} else if rf.log[logIndex].Term == lastIncludedTerm {
		rf.log = rf.log[logIndex+1:]
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.LastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.Persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	return true
}
func (rf *Raft) trimLogWithNoLock(lastIncludedIndex int) {
	logIndex := lastIncludedIndex - rf.lastIncludedIndex - 1
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.log[logIndex].Term
	rf.log = rf.log[logIndex+1:]
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as Much as possible.
func (rf *Raft) SnapshotWithNoLock(index int, snapshot []byte) {
	DPrintf("Snapshot begin : %v,lastIncludedIndex : %v", rf, index)
	defer DPrintf("Snapshot end : %v", rf)
	if rf.LastApplied < index {
		panic("Snapshot : rf.LastApplied < index")
	}
	rf.trimLogWithNoLock(index)
	rf.Persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	return
}
func (rf *Raft) CanSnapshot() bool{
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.LastApplied > rf.lastIncludedIndex
}
type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
	LeaderId          int
}
type InstallSnapshotReply struct {
	ArgsTerm int
	Succeed  bool
	ReplyId  int
}

func (rf *Raft) handleInstallSnapshot(server int) {
	rf.Mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.Persister.ReadSnapshot(),
		LeaderId:          rf.me,
	}
	rf.Mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	rf.Mu.Lock()
	if ok && reply.ArgsTerm == rf.currentTerm && reply.Succeed {
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}
	rf.Mu.Unlock()
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Mu.Lock()
	defer DPrintf("InstallSnapshot end : rf[%v] reply : %v", rf.me, reply)
	DPrintf("InstallSnapshot begin : rf[%v] args : %v", rf.me, args)
	DPrintf("InstallSnapshot %v", rf)
	reply.ReplyId = rf.me
	if args.LastIncludedIndex <= rf.commitIndex { //strong check
		rf.Mu.Unlock()
		return
	}
	reply.Succeed = true
	reply.ArgsTerm = args.Term
	rf.Mu.Unlock()
	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.ApplyCh <- applyMsg
}

//
// example RequestVote RPC arguments structure.
// field names Must start with capital letters!
//
type safeCounter struct {
	Mu    sync.Mutex
	count int
}

func (rf *Raft) getLastEntryWithNoLock() *Entry {
	var entry *Entry
	if len(rf.log) == 0 {
		entry = &Entry{
			Index:   rf.lastIncludedIndex,
			Term:    rf.lastIncludedTerm,
			Command: nil,
		}
	} else {
		entry = rf.log[len(rf.log)-1]
	}
	return entry
}
func (rf *Raft) startElection() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer DPrintf("startElection detail: %v", rf)
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.state = Candidate
	rf.persist()
	lastEntry := rf.getLastEntryWithNoLock()
	lastLogIndex, lastLogTerm := lastEntry.Index, lastEntry.Term
	counter := &safeCounter{
		count: 1,
	}
	for i := range rf.peers {
		if i != rf.me {
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			go rf.leaderElection(i, args, reply, counter)
		}
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names Must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ArgsTerm  int
	ReplyTerm int
	Vote      bool
	ReplyId   int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer DPrintf("RequestVote end : rf[%v]: %v", rf.me, reply)
	DPrintf("RequestVote begin : %v", rf)
	DPrintf("RequestVote detail : rf[%v]: %v:", rf.me, args)
	reply.ReplyId = rf.me
	if args.Term < rf.currentTerm {
		reply.ReplyTerm = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = Follower
		rf.persist()
	}
	lastEntry := rf.getLastEntryWithNoLock()
	if args.Term >= rf.currentTerm && (args.LastLogTerm > lastEntry.Term || (args.LastLogIndex >= lastEntry.Index && args.LastLogTerm == lastEntry.Term)) && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		reply.ArgsTerm = args.Term
		reply.Vote = true
		rf.voteFor = args.CandidateId
		rf.lastHeartBeat = time.Now()
		rf.persist()
		return
	} else {
		reply.Vote = false
		return
	}

	// Your code here (2A, 2B).
}
func (rf *Raft) initialLeaderWithNoLock() {
	rf.state = Leader
	lastEntry := rf.getLastEntryWithNoLock()
	for i := range rf.peers {
		rf.nextIndex[i] = lastEntry.Index + 1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) leaderElection(i int, args *RequestVoteArgs, reply *RequestVoteReply, counter *safeCounter) {
	for ok := rf.sendRequestVote(i, args, reply); ; ok = rf.sendRequestVote(i, args, reply) {
		if ok {
			rf.Mu.Lock()
			if reply.ReplyTerm > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = reply.ReplyTerm
				rf.voteFor = -1
				rf.persist()
			} else if reply.ArgsTerm == rf.currentTerm && reply.Vote {
				counter.Mu.Lock()
				counter.count += 1
				if counter.count == rf.quorum {
					rf.initialLeaderWithNoLock()
					go rf.replicateLog()
				}
				counter.Mu.Unlock()
			}
			rf.Mu.Unlock()
			return
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	ArgsTerm      int
	ReplyTerm     int
	IsSucceed     bool
	ConflictTerm  int
	ConflictIndex int
	ReplyId       int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer DPrintf("AppendEntries end : rf[%v] %v", rf.me, reply)
	DPrintf("AppendEntries begin : %v", rf)
	DPrintf("AppendEntries detail : rf[%v] %v", rf.me, args)
	reply.ReplyId = rf.me
	if args.Term < rf.currentTerm {
		reply.ReplyTerm = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteFor = -1
		rf.persist()
	}
	rf.lastHeartBeat = time.Now()
	reply.ArgsTerm = args.Term
	lastEntry := rf.getLastEntryWithNoLock()
	//分为4种情况，其中logIndex == -1 必succeed
	if args.PrevLogIndex > lastEntry.Index {
		reply.ConflictIndex = lastEntry.Index + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}
	if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
		panic("Fault AppendEntries RPC: args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm")
	}
	logIndex := args.PrevLogIndex - rf.lastIncludedIndex - 1

	if logIndex == -1 || rf.log[logIndex].Term == args.PrevLogTerm {
		i := logIndex + 1
		j := 0
		for i < len(rf.log) && j < len(args.Entries) {
			if rf.log[i].Term != args.Entries[j].Term {
				break
			}
			i++
			j++
		}
		if j < len(args.Entries) {
			rf.log = rf.log[:i]
			rf.log = append(rf.log, args.Entries[j:]...)
			rf.persist()
		}
		reply.IsSucceed = true
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.cond.Signal()
		}
		return
	} else {
		reply.ConflictTerm = rf.log[logIndex].Term
		conflictIndex := logIndex
		for ; conflictIndex > -1; conflictIndex-- {
			if rf.log[conflictIndex].Term != rf.log[logIndex].Term {
				break
			}
		}
		reply.ConflictIndex = rf.log[conflictIndex+1].Index
		reply.IsSucceed = false
		return
	}
}
func (rf *Raft) SendAll() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.state == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var prevLogIndex int
				var prevLogTerm int
				var entries []*Entry
				logIndex := rf.nextIndex[i] - 1 - rf.lastIncludedIndex - 1
				if logIndex < 0 {
					prevLogIndex = rf.lastIncludedIndex
					prevLogTerm = rf.lastIncludedTerm
					entries = rf.log[0:]
				} else {
					prevLogIndex = rf.log[logIndex].Index
					prevLogTerm = rf.log[logIndex].Term
					entries = rf.log[logIndex+1:]
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				go rf.handleAppendEntries(i, &args, &reply)

			}
		}
	}
}
func (rf *Raft) replicateLog() {
	for {
		rf.SendAll()
		time.Sleep(HeartbeatInterval)
	}
}
func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer DPrintf("handleAppendEntries finish : %v", rf)
	DPrintf("handleAppendEntries begin : %v", rf)
	DPrintf("handleAppendEntries detail args : %v", args)
	DPrintf("handleAppendEntries detail reply : %v", reply)
	if ok {
		if reply.ReplyTerm > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.ReplyTerm
			rf.voteFor = -1
			rf.persist()
			return
		}
		if reply.ArgsTerm != rf.currentTerm {
			return
		}
		if reply.IsSucceed {
			if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[server] {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.updateCommitIndexWithNoLock()
			}
			return
		} else {
			if reply.ConflictTerm == -1 {
				if reply.ConflictIndex <= rf.lastIncludedIndex {
					go rf.handleInstallSnapshot(server)
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
				return
			}
			logIndex := args.PrevLogIndex - 1 - rf.lastIncludedIndex
			for ; logIndex > -1; logIndex-- {
				if rf.log[logIndex].Term <= reply.ConflictTerm {
					break
				}
			}
			if logIndex > -1 && rf.log[logIndex].Term == reply.ConflictTerm {
				rf.nextIndex[server] = rf.log[logIndex].Index + 1
			} else {
				if reply.ConflictIndex <= rf.lastIncludedIndex {
					go rf.handleInstallSnapshot(server)
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
		}
	}
}
func (rf *Raft) updateCommitIndexWithNoLock() {
	//leader only commit its term log
	matchIndex := make([]int, len(rf.peers))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	candidateCommitIndex := matchIndex[rf.quorum]
	if candidateCommitIndex > rf.commitIndex && rf.log[candidateCommitIndex-rf.lastIncludedIndex-1].Term == rf.currentTerm {
		rf.commitIndex = candidateCommitIndex
		rf.cond.Signal()  //snapshot and logEntry Mutex collision
	}
}
func (rf *Raft) sendApplyMsg() {
	for !rf.killed(){
		rf.Mu.Lock()
		for rf.LastApplied >= rf.commitIndex{
			rf.cond.Wait()
		}
		if rf.LastApplied >= rf.commitIndex {
			panic("rf.LastApplied >= rf.commitIndex")
		}
		index := rf.LastApplied - rf.lastIncludedIndex - 1
		entry := rf.log[index+1]
		applyMsg := ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.LastApplied++ //由raft layer 负责，server layer维护自身的lastApplied
		DPrintf("%v applyMsg.CommandIndex : %v", rf, applyMsg.CommandIndex)
		rf.Mu.Unlock()
		rf.ApplyCh <- applyMsg
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() Must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package siMulates a lossy network, in which servers
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("rf[%v] start begin",rf.me)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	entry := rf.getLastEntryWithNoLock()
	index := entry.Index + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader && !rf.killed(){
		entry := Entry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, &entry)
		rf.persist()
		go rf.SendAll()
		DPrintf("StartCommand %v entryIndex : %v", rf, entry.Index)
	}
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
//
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
	for !rf.killed() {
		electionTimeout := time.Duration(ElectionTimeoutBase+rand.Intn(ElectionTimeoutBase)) * time.Millisecond
		time.Sleep(electionTimeout)
		rf.Mu.Lock()
		DPrintf("ticker rf[%v] : lastHeartBeat : %v", rf.me, rf.lastHeartBeat)
		if time.Now().Sub(rf.lastHeartBeat) >= electionTimeout && rf.state != Leader {
			go rf.startElection()
		}
		rf.Mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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
// Make() Must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		Mu:                sync.Mutex{},
		peers:             peers,
		Persister:         persister,
		me:                me,
		dead:              0,
		currentTerm:       0,
		log:               []*Entry{},
		voteFor:           -1,
		lastHeartBeat:     time.Now(),
		LastApplied:       0,
		commitIndex:       0,
		matchIndex:        make([]int, len(peers)),
		nextIndex:         make([]int, len(peers)),
		ApplyCh:           applyCh,
		state:             Follower,
		quorum:            len(peers)/2 + 1,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,

	}
	rf.cond = sync.NewCond(&rf.Mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.LastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg()
	DPrintf("Initialize rf %v",rf)
	return rf
}
