package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func (request *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs Term %v,CandidateId %v, LastEntryIdx %v,LastEntryTerm %v", request.Term, request.CandidateId, request.LastEntryIdx, request.LastEntryTerm)
}
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply Term %v,RequestTerm %v, IsSuccess %v", reply.Term, reply.RequestTerm, reply.IsSuccess)
}
func (request *AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntriesArgs PrevLogIdx %v,LeaderId %v,Term %v", request.PrevLogIdx, request.LeaderId, request.Term)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply Term %v,RequestTerm %v,IsSuccess %v ConflictTerm %v ConflictIndex %v", reply.Term, reply.RequestTerm, reply.IsSuccess,reply.ConflictTerm,reply.ConflictIndex)
}


func (rf *Raft) String() string {
	return fmt.Sprintf("rf [%v]:term %v,state %v isdead %v LastIncludedIndex %v LastIncludedTerm %v len(rf.Log) : %v", rf.me, rf.Term, rf.state,rf.dead,rf.LastIncludedIndex,rf.LastIncludedTerm,len(rf.Log))
}
func (applyMsg *ApplyMsg) String() string {
	return fmt.Sprintf("Command %v,CommandIndex %v,CommandValid %v", applyMsg.Command, applyMsg.CommandIndex, applyMsg.CommandValid)
}
func (logEntry *LogEntry) String() string{
	return fmt.Sprintf("logEntry Command %v,Term %v,Index %v",logEntry.Command,logEntry.Term,logEntry.Index)
}
