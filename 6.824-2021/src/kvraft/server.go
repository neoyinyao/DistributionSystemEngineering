package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET       = "get"
	PUT       = "Put"
	APPEND    = "Append"
	NOTLEADER = "notLeader"
	OK        = "OK"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string
	ClientId  int64
	SerialNum int
	Key       string
	Value     string
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	state        map[string]string
	clientState  map[int64]int
	lastApplied int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("kv[%v] Get args : %v", kv.me, args)
	defer DPrintf("kv[%v] Get reply :%v", kv.me, reply)
	op := Op{
		Op:        GET,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
		Value:     "",
	}
	_, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		if kv.clientState[args.ClientId] > args.SerialNum {
			//old rpc
			kv.mu.Unlock()
			return
		} else if kv.clientState[args.ClientId] == args.SerialNum {
			//duplicate rpc
			reply.Err = OK
			reply.Value = kv.state[args.Key]
			reply.ServerId = kv.me
			kv.mu.Unlock()
			return
		}
		//else if args.SerialNum-kv.clientState[args.ClientId] > 1 {
		//	//fatal mental implementation cannot happen
		//	DPrintf("kv[%v] clientState : %v  args : %v ",kv.me,kv.clientState[args.ClientId],args)
		//	panic("rpc recall kv.clientState[args.ClientId] - args.SerialNum > 1")
		//}
		kv.mu.Unlock()
		for {
			_, isLeader = kv.rf.GetState()
			if !isLeader {
				reply.Err = NOTLEADER
				reply.ServerId = kv.me
				return
			}
			kv.mu.Lock()
			if kv.clientState[args.ClientId] == args.SerialNum {
				val, ok := kv.state[args.Key]
				if ok {
					reply.Value = val
				} else {
					reply.Value = ""
				}
				kv.mu.Unlock()
				reply.Err = OK
				reply.ServerId = kv.me
				return
			} else if kv.clientState[args.ClientId] > args.SerialNum {
				//fatal mental implementation cannot happen,old rpc cannot start command,cannot replicate
				panic("kv.clientState[args.ClientId] > args.SerialNum")
			}
			//else if args.SerialNum-kv.clientState[args.ClientId] > 1 {
			//	//fatal mental implementation cannot happen
			//	DPrintf("kv[%v] clientState : %v  args : %v ",kv.me,kv.clientState[args.ClientId],args)
			//	panic("rpc recall kv.clientState[args.ClientId] - args.SerialNum > 1")
			//}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		reply.Err = NOTLEADER
		reply.ServerId = kv.me
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("kv[%v] PutAppend args : %v", kv.me, args)
	defer DPrintf("kv[%v] PutAppend reply :%v", kv.me, reply)
	command := Op{
		Op:        args.Op,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
		Value:     args.Value,
	}
	_, _, isLeader := kv.rf.Start(command)
	if isLeader {
		kv.mu.Lock()
		if kv.clientState[args.ClientId] > args.SerialNum {
			kv.mu.Unlock()
			return
		} else if kv.clientState[args.ClientId] == args.SerialNum {
			reply.Err = OK
			reply.ServerId = kv.me
			kv.mu.Unlock()
			return
		}
		//else if args.SerialNum-kv.clientState[args.ClientId] > 1 {
		//	//fatal mental implementation cannot happen
		//	DPrintf("kv[%v] clientState : %v  args : %v ",kv.me,kv.clientState[args.ClientId],args)
		//	panic("rpc recall kv.clientState[args.ClientId] - args.SerialNum > 1")
		//}can happen
		kv.mu.Unlock()
		for {
			_, isLeader = kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				if kv.clientState[args.ClientId] == args.SerialNum {
					reply.Err = OK
					reply.ServerId = kv.me
					kv.mu.Unlock()
					return
				} else if kv.clientState[args.ClientId] > args.SerialNum {
					//fatal mental implementation cannot happen,old rpc cannot start command,cannot replicate
					panic("kv.clientState[args.ClientId] > args.SerialNum")
				}
				//else if args.SerialNum-kv.clientState[args.ClientId] > 1 {
				//	//fatal mental implementation cannot happen
				//	DPrintf("kv[%v] clientState : %v  args : %v ",kv.me,kv.clientState[args.ClientId],args)
				//	panic("rpc recall kv.clientState[args.ClientId] - args.SerialNum > 1")
				//}
				kv.mu.Unlock()
			} else {
				reply.Err = NOTLEADER
				reply.ServerId = kv.me
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	} else {
		reply.Err = NOTLEADER
		reply.ServerId = kv.me
	}
	// Your code here.
}
func (kv *KVServer) consumeApplyMsg() {
	for !kv.killed(){
		applyMsg := <-kv.rf.ApplyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			DPrintf("kv[%v] consumeApplyMsg clientState : %v  %v", kv.me, kv.clientState[op.ClientId], op)
			if op.SerialNum-kv.clientState[op.ClientId] > 1 {
				panic("op.SerialNum - kv.clientState[op.ClientId] > 1")
			} else if op.SerialNum == kv.clientState[op.ClientId] { //TODO justify duplicate rpc scenario：leader crash restart
				kv.rf.Mu.Lock()
				kv.lastApplied = applyMsg.CommandIndex //warn snapshot和log是一致的，此处必须放在双锁里
				kv.rf.Mu.Unlock()
				kv.mu.Unlock()
			} else if op.SerialNum < kv.clientState[op.ClientId] {
				panic("op.SerialNum < kv.clientState[op.ClientId]")
			} else {
				kv.rf.Mu.Lock()
				DPrintf("consumeApplyMsg get lock : kv[%v]",kv.me)
				kv.clientState[op.ClientId] = op.SerialNum
				if op.Op == PUT {
					kv.state[op.Key] = op.Value
				} else if op.Op == APPEND {
					_, ok := kv.state[op.Key]
					if ok {
						kv.state[op.Key] += op.Value
					} else {
						kv.state[op.Key] = op.Value
					}
				} else if op.Op == GET{

				} else {
					panic("PutAppend Unexpected Op")
				}
				kv.lastApplied = applyMsg.CommandIndex //warn snapshot和log是一致的，此处必须放在双锁里
				kv.rf.Mu.Unlock()
				DPrintf("consumeApplyMsg release lock : kv[%v]",kv.me)
				kv.mu.Unlock()
			}
		} else if applyMsg.SnapshotValid {
			kv.DealSnapshotApplyMsgWithLock(applyMsg.Snapshot,applyMsg.SnapshotIndex,applyMsg.SnapshotTerm)
		} else {
			panic("applyMsg neither CommandValid nor SnapshotValid")
		}
	}
}

type Snapshot struct {
	content []byte
	lastIncludedIndex int
	lastIncludedTerm int
}
func (kv *KVServer) DealSnapshotApplyMsgWithLock(snapshot []byte,lastIncludedIndex int,lastIncludedTerm int){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ok := kv.rf.CondInstallSnapshot(lastIncludedTerm,lastIncludedIndex,snapshot)
	if ok{
		kv.restoreSnapshot(snapshot)
	}
}
func (kv *KVServer) saveSnapshot() []byte {
	var data []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.state) != nil || e.Encode(kv.clientState) != nil {
		fmt.Println("encode snapshot error")
	} else {
		data = w.Bytes()
	}
	return data
}
func (kv *KVServer) restoreSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state map[string]string
	var clientState map[int64]int
	if d.Decode(&state) != nil ||
		d.Decode(&clientState) != nil {
		fmt.Println("decode snapshot error")
	} else {
		kv.state = state
		kv.clientState = clientState
	}
}

func (kv *KVServer) monitorSnapshot(){
	for !kv.killed(){
		if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate && kv.rf.CanSnapshot(){
			kv.mu.Lock()
			kv.rf.Mu.Lock()
			DPrintf("monitorSnapshot get lock : kv[%v]",kv.me)
			data := kv.saveSnapshot()
			kv.rf.SnapshotWithNoLock(kv.lastApplied,data)
			kv.rf.Mu.Unlock()
			DPrintf("monitorSnapshot release lock : kv[%v]",kv.me)
			kv.mu.Unlock()
		}
		time.Sleep(300 * time.Millisecond)
	}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientState = make(map[int64]int)
	kv.state = make(map[string]string)
	kv.restoreSnapshot(kv.rf.Persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.consumeApplyMsg()
	go kv.monitorSnapshot()
	return kv
}
