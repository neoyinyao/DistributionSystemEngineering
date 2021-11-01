package kvraft
//warn return must match unlock
import (
	"6.824/labs/labgob"
	"6.824/labs/labrpc"
	"6.824/labs/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
	Id    int64
	Seq   int
}
type ckEntry struct {
	lastAppliedSeq int
	ckCh           chan int
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate      int // snapshot if log grows this big
	state             map[string]string
	clientMap         map[int64]*ckEntry
	lastAppliedIndex int
	lastAppliedTerm  int
	// Your definitions here.
}
type Snapshot struct {
	State             map[string]string
	LastIncludedIndex int
	LastIncludedTerm  int
	ClientMap         map[int64]int
}


func (kv *KVServer) initialCkMap(clientId int64) {
	DPrintf("server [%v] initialCkMap",kv.me)
	kv.mu.Lock()
	DPrintf("server [%v] initialCkMap get Lock",kv.me)
	defer kv.mu.Unlock()
	_, ok := kv.clientMap[clientId]
	if !ok {
		entry := ckEntry{
			lastAppliedSeq: -1,
			ckCh:           make(chan int),
		}
		kv.clientMap[clientId] = &entry
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//Get Rpc does not enter log
	// warn No longtime lock here,raft need lock
	kv.initialCkMap(args.Id)
	if args.Seq < kv.clientMap[args.Id].lastAppliedSeq{
		kv.mu.Lock()
		key := args.Key
		value, ok := kv.state[key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}
	Command := Op{
		Op:    GET,
		Key:   args.Key,
		Value: "",
		Id:    args.Id,
		Seq:   args.Seq,
	}
	clientId := args.Id
	_, _, isLeaderRequest := kv.rf.Start(Command)
	if !isLeaderRequest {
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("Server [%v] begin Get %v %v", kv.me, args, reply)
		defer DPrintf("Server [%v] Finish Get %v %v", kv.me, args, reply)
		kv.mu.Lock()
		ckCh := kv.clientMap[clientId].ckCh
		kv.mu.Unlock()
		for {
			select {
			case seq := <-ckCh:
				if seq == args.Seq {
					key := Command.Key
					kv.mu.Lock()
					value, ok := kv.state[key]
					kv.mu.Unlock()
					if ok {
						reply.Err = OK
						reply.Value = value
					} else {
						reply.Err = ErrNoKey
					}
					return
				}
			default:
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					reply.Err = ErrWrongLeader
					return
				}
				time.Sleep(10 * time.Millisecond) // Reduce CPU Cost
			}
		}
	}

	// Your code here.
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Command := Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
		Id:    args.Id,
		Seq:   args.Seq,
	}
	clientId := args.Id
	_, _, isLeaderRequest := kv.rf.Start(Command)
	if !isLeaderRequest {
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("Server [%v] begin PutAppend %v %v", kv.me, args, reply)
		defer DPrintf("Server [%v] finish PutAppend %v %v", kv.me, args, reply)
		kv.initialCkMap(clientId)
		if args.Seq <= kv.clientMap[args.Id].lastAppliedSeq{
			reply.Err = OK
			return
		}
		kv.mu.Lock()
		ckCh := kv.clientMap[clientId].ckCh
		kv.mu.Unlock()
		for {
			select {
			case seq := <- ckCh: //warn seq做比较,因此出来的seq可能是老的rpc
				if seq == args.Seq {
					reply.Err = OK
					return
				}
			default:
				_, isLeader := kv.rf.GetState() //warn detect server leader state change
				if !isLeader {
					reply.Err = ErrWrongLeader
					return
				}
				time.Sleep(10 * time.Millisecond) // Reduce CPU Cost
			}
		}
	}

	// Your code here.
}
func (kv *KVServer) Leader2Client(clientId int64, seq int) {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf("server [%v] Leader2Client begin seq [%v]", kv.me,seq)
		kv.mu.Lock()
		ckCh := kv.clientMap[clientId].ckCh
		kv.mu.Unlock()
		ckCh <- seq
		DPrintf("server [%v] Leader2Client finish seq [%v]", kv.me,seq)
	}
}
func (kv *KVServer) updateFromRF() {
	for {
		appliedMsg := <-kv.applyCh
		if !appliedMsg.CommandValid {
			//snapshot := appliedMsg.Command.(raft.Snapshot)//warn change snapshot install block,wrong change
			snapshot,ok := kv.readSnapshotWithNoLock(appliedMsg.Command.([]byte))
			if ok{
				go func(Snapshot){//warn change snapshot install must run in a goroutine,no block,escape dead lock,如果此时updateAppliedLast拿到了锁，
					//snapshot block会导致updateAppliedLast无法释放锁，造成死锁，channel可以看成是锁的一种．
					if snapshot.LastIncludedIndex > kv.rf.LastIncludedIndex{//warn 别的goroutine,比如appendEntries,updateAppliedLast可能拿到了rf lock，防止死锁
						//warn old rpc可能会到达condition内，因此下面再加condition
						kv.rf.GetLock()
						kv.mu.Lock() //warn change add condition snapshot.LastIncludedIndex > kv.lastAppliedIndex,escape 已经被applied的entry,被snapshot抹掉
						if snapshot.LastIncludedIndex > kv.rf.LastIncludedIndex && snapshot.LastIncludedIndex > kv.lastAppliedIndex{ //warn change old duplicate install snapshot rpc deal
							kv.saveSnapshotWithNoLock(&snapshot) // change warn kv必须首先saveSnapshot再restore
							kv.restoreSnapshotWithNoLock(&snapshot)
						}
						kv.mu.Unlock()
						kv.rf.ReleaseLock()
					}
				}(snapshot)
				// return
			}
			continue // warn no return channel block
		}
		DPrintf("server [%v] updateFromRF begin", kv.me)
		Command := appliedMsg.Command.(Op)
		DPrintf("server [%v] updateFromRF %v", kv.me, Command)
		clientId, currSeq := Command.Id, Command.Seq // warn duplicate Rpc Detect
		kv.initialCkMap(clientId)                    //warn Follower initial
		DPrintf("server [%v] initialCkMap finished", kv.me)
		kv.mu.Lock()
		lastSeq := kv.clientMap[clientId].lastAppliedSeq
		DPrintf("server [%v] Command :%v lastSeq:%v currSeq:%v",kv.me,Command,lastSeq,currSeq)
		if currSeq > lastSeq && currSeq - lastSeq != 1{
			DPrintf("currSeq - lastSeq != 1")
		}
		if currSeq < lastSeq{
			DPrintf("currSeq < lastSeq") // big warn condition appear old rpc when logEntry committed but donot send client reply rpc,entry committed twice
		}
		if currSeq <= lastSeq { //warn 此处应是 <= not ==
			DPrintf("server [%v] detect duplicate rpc Id:%v seq:%v", kv.me, clientId, currSeq)
		} else {
			if Command.Op != GET {
				class, key, value := Command.Op, Command.Key, Command.Value
				if class == PUT {
					kv.state[key] = value
				} else {
					currValue, ok := kv.state[key]
					if ok {
						kv.state[key] = currValue + value
					} else {
						kv.state[key] = value
					}
				}
			}
			kv.clientMap[clientId].lastAppliedSeq = currSeq //  warn lastAppliedSeq必须递增
		}
		kv.lastAppliedIndex = appliedMsg.CommandIndex
		kv.lastAppliedTerm = appliedMsg.CommandTerm
		kv.mu.Unlock()
		go kv.Leader2Client(clientId, currSeq) //warn client之间互不影响,新的leader在处理老的request时,老的request可能被老的leader处理了,也可能没有,如果处理完成了,updateleader会一直阻塞,影响别的client的可用
		DPrintf("server [%v] updateFromRF finish", kv.me)
	}
}

func (kv *KVServer) readSnapshotWithNoLock(data []byte)(Snapshot,bool){
	var snapshot Snapshot
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("Decode error happen\n")
		return snapshot,false
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
	var State map[string]string
	var LastIncludedIndex int
	var LastIncludedTerm  int
	var ClientMap         map[int64]int
	if d.Decode(&State) != nil ||
		d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil || d.Decode(&ClientMap) != nil{
		//error...
		DPrintf("Decode error happen\n")
		return snapshot,false
	} else {
		snapshot = Snapshot{
			State:             State,
			LastIncludedIndex: LastIncludedIndex,
			LastIncludedTerm:  LastIncludedTerm,
			ClientMap:         ClientMap,
		}
	}
	return snapshot,true
}
func (kv *KVServer) saveSnapshotWithNoLock(snapshot *Snapshot) {
	DPrintf("kv [%v] saveSnapshot begin", kv.me)
	//kv.mu.Lock() // change add lock deal concurrent state read and write
	wServer := new(bytes.Buffer)
	eServer := labgob.NewEncoder(wServer)
	eServer.Encode(snapshot.State)
	eServer.Encode(snapshot.LastIncludedIndex)
	eServer.Encode(snapshot.LastIncludedTerm)
	eServer.Encode(snapshot.ClientMap)
	dataServer := wServer.Bytes()
	//kv.mu.Unlock()
	kv.rf.TrimLogEntryWithNoLock(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
	wRfState := new(bytes.Buffer)
	eRfState := labgob.NewEncoder(wRfState)
	eRfState.Encode(kv.rf.Log)
	eRfState.Encode(kv.rf.Term)
	eRfState.Encode(kv.rf.VoteFor)
	dataRfState := wRfState.Bytes() //change warn rf的state persistent应该在trim 修改log之后
	kv.rf.Persister.SaveStateAndSnapshot(dataRfState, dataServer)
	DPrintf("kv [%v] saveSnapshot finish,snapshot.LastIncludedIndex : %v", kv.me, snapshot.LastIncludedIndex)
}
func (kv *KVServer) monitorSnapshot() {
	for {
		if kv.maxraftstate > 0 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate && kv.lastAppliedIndex > kv.rf.LastIncludedIndex{//warn change save snapshot condition change
			DPrintf("kv [%v] start snapshot", kv.me)
			kv.rf.GetLock()//由于save snapshot和updateAppliedLast可能并发进行,此时updateAppliedLast获得了rf Lock,而trim需要rf lock,uAL需要kv的Lock,产生死锁
			//因此必须先获得rf锁,使得save snapshot goroutine和updateAppliedLast goroutine互斥
			kv.mu.Lock()
			if kv.lastAppliedIndex > kv.rf.LastIncludedIndex{ //warn change add condition 因为monitorSnapshot和installSnapshot会抢占锁，
				//monitorSnapshot获得锁之后kv.rf.LastIncludedIndex可能已经改变了，导致trim发生索引越界错误
				clientMap := make(map[int64]int) // warn clientMap in snapshot, duplicate rpc detect
				for key, entry := range kv.clientMap {
					clientMap[key] = entry.lastAppliedSeq
				}
				snapShot := Snapshot{
					State:             kv.state,
					LastIncludedIndex: kv.lastAppliedIndex,
					LastIncludedTerm:  kv.lastAppliedTerm,
					ClientMap:         clientMap,
				}
				// warn saveSnapshot必须上锁,因为updateAppliedLast会并发进行,appliedChannel 会更新server的state,导致clientMap发生改变,产生duplicate rpc无法识别的错误
				kv.saveSnapshotWithNoLock(&snapShot)
			}
			kv.mu.Unlock()
			kv.rf.ReleaseLock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *KVServer) restoreSnapshotWithNoLock(snapshot *Snapshot) {
	DPrintf("kv [%v] restoreSnapshot begin",kv.me)
	defer DPrintf("kv [%v] restoreSnapshot finish",kv.me)
	kv.state = snapshot.State
	for key, val := range snapshot.ClientMap {
		entry := ckEntry{
			lastAppliedSeq: val,
			ckCh:           make(chan int),
		}
		kv.clientMap[key] = &entry
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
	kv.state = make(map[string]string)
	kv.clientMap = make(map[int64]*ckEntry)
	data := persister.ReadSnapshot()
	snapshot,ok := kv.readSnapshotWithNoLock(data)
	if ok{
		kv.rf.LastIncludedIndex = snapshot.LastIncludedIndex // change warn restart需要修改rf的snapshot state,restore snapshot时,rf的state必须改变
		kv.rf.LastIncludedTerm = snapshot.LastIncludedTerm //
		kv.restoreSnapshotWithNoLock(&snapshot)
		//kv.rf.TrimLogEntryWithLock(snapshot.LastIncludedIndex,snapshot.LastIncludedTerm)// warn change restart不需要trim
	}
	// You may need initialization code here.
	go kv.monitorSnapshot()
	go kv.updateFromRF()
	return kv
}
