package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) String() string{
	return fmt.Sprintf("rf[%v] term : %v len(log) : %v voteFor : %v state : %v lastApplied : %v commitIndex : %v lastIncludedIndex : %v lastIncludedTerm : %v",rf.me,rf.currentTerm,len(rf.log),rf.voteFor,rf.state,
		rf.LastApplied,rf.commitIndex,rf.lastIncludedIndex,rf.lastIncludedTerm)
}
func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("term : %v candidateId : %v lastLogIndex : %v lastLogTerm : %v",args.Term,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
}
func (reply *RequestVoteReply) String() string  {
	return fmt.Sprintf("replyId : %v argsTerm : %v replyTerm : %v vote : %v",reply.ReplyId,reply.ArgsTerm,reply.ReplyTerm,reply.Vote)
}
func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("leaderId : %v term : %v len(Entries) : %v leaderCommit : %v prevLogIndex : %v prevLogTerm : %v",args.LeaderId,args.Term,len(args.Entries),args.LeaderCommit,args.PrevLogIndex,args.PrevLogTerm)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("replyId : %v argsTerm : %v replyTerm : %v isSucceed : %v conflictTerm : %v conflictIndex : %v",reply.ReplyId,reply.ArgsTerm,reply.ReplyTerm,reply.IsSucceed,reply.ConflictTerm,reply.ConflictIndex)
}
func (args *InstallSnapshotArgs) String() string{
	return fmt.Sprintf("leaderId : %v term : %v lastIncludedIndex : %v lastIncludedTerm : %v snapshot : %v",args.LeaderId,args.Term,args.LastIncludedIndex,args.LastIncludedTerm,args.Snapshot)
}
func (reply *InstallSnapshotReply) String() string{
	return fmt.Sprintf("followerId : %v argsTerm : %v succeed : %v",reply.ReplyId,reply.ArgsTerm,reply.Succeed)
}