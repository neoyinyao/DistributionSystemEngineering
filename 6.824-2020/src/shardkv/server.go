package shardkv

import (
	"6.824/labs/labgob"
	"6.824/labs/labrpc"
	"6.824/labs/raft"
	"6.824/labs/shardmaster"
	"bytes"
	"sync"
	"time"
)

const (
	GET            = "Get"
	PUT            = "Put"
	APPEND         = "Append"
	CONFIG         = "Config"
	HandOffShard   = "HandOffShard"
	DeprecateShard = "DeprecateShard"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shard      int
	Type       string
	Key        string
	Value      string
	ClientId   int64
	Seq        int
	Config     shardmaster.Config
	ConfigNum  int
	ShardState map[string]string
	ClientMap  map[int64]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	sm               *shardmaster.Clerk
	oldConfig        shardmaster.Config
	newConfig        shardmaster.Config
	clientMap        map[int]map[int64]*clientMapEntry
	state            map[int]map[string]string
	shardConfigNum   [shardmaster.NShards]int
	lastAppliedIndex int
	lastAppliedTerm  int
	isKilled         bool
	// Your definitions here.
}
type clientMapEntry struct {
	seq int
	ch  chan int
}
type Snapshot struct {
	State             map[int]map[string]string
	LastIncludedIndex int
	LastIncludedTerm  int
	ClientMap         map[int]map[int64]int
	OldConfig         shardmaster.Config
	NewConfig         shardmaster.Config
	ShardConfigNum    [shardmaster.NShards]int
}

func (kv *ShardKV) pollConfigWithLongLoop() {
loop:
	for {
		if kv.isKilled {
			return
		}
		_, isLeader := kv.rf.GetState()
		if isLeader { //only Leader Commit newConfig
			nextConfig := kv.sm.Query(kv.newConfig.Num + 1) //commit config must one by one
			if nextConfig.Num > kv.newConfig.Num {          //warn condition required
				for _, configNum := range kv.shardConfigNum {
					if configNum < kv.newConfig.Num {
						continue loop
					}
				}
				Command := Op{
					Type:   CONFIG,
					Config: nextConfig,
				}
				DPrintf("%v begin commit new config", kv)
				kv.rf.Start(Command)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type HandOffShardArgs struct {
	ConfigNum  int //check
	Shard      int
	ShardState map[string]string
	ClientMap  map[int64]int //duplicate rpc detect
}
type HandOffShardsReply struct {
	Err string
}

func (kv *ShardKV) HandOffShard(args *HandOffShardArgs, reply *HandOffShardsReply) {
	if kv.shardConfigNum[args.Shard] >= args.ConfigNum {
		reply.Err = OK
		return
	}
	if args.ConfigNum != kv.newConfig.Num { //warn if one replication group lag big,then maybe args.ConfigNum > kv.newConfig.Num,so must check
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Command := Op{
		Shard:      args.Shard,
		ConfigNum:  args.ConfigNum,
		ShardState: args.ShardState,
		ClientMap:  args.ClientMap,
		Type:       HandOffShard,
	}
	DPrintf("%v begin commit handOffShard shard:%v configNum:%v", kv, args.Shard, args.ConfigNum)
	kv.rf.Start(Command)
}
func (kv *ShardKV) updateAddShardsWithNoLock(args *HandOffShardArgs) {
	if kv.shardConfigNum[args.Shard] >= args.ConfigNum {
		return
	}
	if kv.newConfig.Num != args.ConfigNum {
		return
	}
	kv.shardConfigNum[args.Shard] = args.ConfigNum
	kv.state[args.Shard] = args.ShardState
	shardClientMap := make(map[int64]*clientMapEntry)
	for clientId, clientSeq := range args.ClientMap {
		entry := clientMapEntry{
			seq: clientSeq,
			ch:  make(chan int),
		}
		shardClientMap[clientId] = &entry
	}
	kv.clientMap[args.Shard] = shardClientMap
}
func (kv *ShardKV) sendHandOffShard(args HandOffShardArgs, config *shardmaster.Config) {
	destinationGID := config.Shards[args.Shard]
	destinationServers := config.Groups[destinationGID]
	// try each server for the shard.
	for si := 0; si < len(destinationServers); si++ {
		srv := kv.make_end(destinationServers[si])
		var reply HandOffShardsReply
		ok := srv.Call("ShardKV.HandOffShard", &args, &reply)
		if ok && (reply.Err == OK) {
			Command := Op{
				Shard:     args.Shard,
				Type:      DeprecateShard,
				ConfigNum: args.ConfigNum,
			}
			DPrintf("%v begin commit deprecateShard shard:%v configNum:%v", kv, args.Shard, args.ConfigNum)
			kv.rf.Start(Command)
			return
		}
		if ok && (reply.Err == ErrWrongLeader) {
			continue
		}
		// ... not ok, or ErrWrongLeader
	}
}

func (kv *ShardKV) updateDeprecateShardConfigNumWithLongLoop() {
	for {
		if kv.isKilled {
			return
		}
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			oldShards := kv.oldConfig.Shards
			newShards := kv.newConfig.Shards
			var deprecateShards []int
			for shard, newGID := range newShards {
				if oldShards[shard] == kv.gid && newGID != kv.gid {
					deprecateShards = append(deprecateShards, shard)
				}
			}
			for _, shard := range deprecateShards {
				if kv.shardConfigNum[shard] != kv.newConfig.Num {
					clientMap := make(map[int64]int)
					for clientId, entry := range kv.clientMap[shard] {
						clientMap[clientId] = entry.seq
					}
					shardState := make(map[string]string)
					for key, val := range kv.state[shard] {
						shardState[key] = val
					}
					args := HandOffShardArgs{
						ConfigNum:  kv.newConfig.Num,
						Shard:      shard,
						ShardState: shardState,
						ClientMap:  clientMap,
					}
					config := shardmaster.Config{
						Num:    kv.newConfig.Num,
						Shards: kv.newConfig.Shards,
						Groups: kv.newConfig.Groups,
					}
					go kv.sendHandOffShard(args, &config)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) monitorSnapshotWithLongLoop() {
	for {
		if kv.isKilled {
			return
		}
		if kv.maxraftstate > 0 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate && kv.lastAppliedIndex > kv.rf.LastIncludedIndex { //warn change save snapshot condition change
			DPrintf("%v start snapshot", kv)
			kv.rf.GetLock() //由于save snapshot和updateAppliedLast可能并发进行,此时updateAppliedLast获得了rf Lock,而trim需要rf lock,uAL需要kv的Lock,产生死锁
			//因此必须先获得rf锁,使得save snapshot goroutine和updateAppliedLast goroutine互斥
			kv.mu.Lock()
			if kv.lastAppliedIndex > kv.rf.LastIncludedIndex { //warn change add condition 因为monitorSnapshot和installSnapshot会抢占锁，
				//monitorSnapshot获得锁之后kv.rf.LastIncludedIndex可能已经改变了，导致trim发生索引越界错误
				clientMap := make(map[int]map[int64]int) // warn clientMap in snapshot, duplicate rpc detect
				for shard := 0; shard < shardmaster.NShards; shard++ {
					shardClientMap := make(map[int64]int)
					clientMap[shard] = shardClientMap
				}
				for shard, shardEntry := range kv.clientMap {
					for clientId, clientMapEntry := range shardEntry {
						clientMap[shard][clientId] = clientMapEntry.seq
					}
				}

				snapShot := Snapshot{
					State:             kv.state,
					LastIncludedIndex: kv.lastAppliedIndex,
					LastIncludedTerm:  kv.lastAppliedTerm,
					ClientMap:         clientMap,
					OldConfig:         kv.oldConfig,
					NewConfig:         kv.newConfig,
					ShardConfigNum:    kv.shardConfigNum,
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
func (kv *ShardKV) readSnapshotWithNoLock(data []byte) (Snapshot, bool) {
	var snapshot Snapshot
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("Decode error happen\n")
		return snapshot, false
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
	var State map[int]map[string]string
	var LastIncludedIndex int
	var LastIncludedTerm int
	var ClientMap map[int]map[int64]int
	var OldConfig shardmaster.Config
	var NewConfig shardmaster.Config
	var ShardConfigNum [shardmaster.NShards]int
	if d.Decode(&State) != nil ||
		d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil ||
		d.Decode(&ClientMap) != nil || d.Decode(&OldConfig) != nil || d.Decode(&NewConfig) != nil || d.Decode(&ShardConfigNum) != nil {
		//error...
		DPrintf("Decode error happen\n")
		return snapshot, false
	} else {
		snapshot = Snapshot{
			State:             State,
			LastIncludedIndex: LastIncludedIndex,
			LastIncludedTerm:  LastIncludedTerm,
			ClientMap:         ClientMap,
			OldConfig:         OldConfig,
			NewConfig:         NewConfig,
			ShardConfigNum:    ShardConfigNum,
		}
		return snapshot, true
	}

}
func (kv *ShardKV) saveSnapshotWithNoLock(snapshot *Snapshot) {
	DPrintf("%v saveSnapshot begin", kv)
	//kv.mu.Lock() // change add lock deal concurrent state read and write
	wServer := new(bytes.Buffer)
	eServer := labgob.NewEncoder(wServer)
	eServer.Encode(snapshot.State)
	eServer.Encode(snapshot.LastIncludedIndex)
	eServer.Encode(snapshot.LastIncludedTerm)
	eServer.Encode(snapshot.ClientMap)
	eServer.Encode(snapshot.OldConfig)
	eServer.Encode(snapshot.NewConfig)
	eServer.Encode(snapshot.ShardConfigNum)
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
	DPrintf("%v saveSnapshot finish,snapshot.LastIncludedIndex : %v", kv, snapshot.LastIncludedIndex)
}
func (kv *ShardKV) restoreSnapshotWithNoLock(snapshot *Snapshot) {
	DPrintf("%v restoreSnapshot begin", kv)
	defer DPrintf("%v restoreSnapshot finish", kv)
	kv.state = snapshot.State
	for shard, shardClientMap := range snapshot.ClientMap {
		for clientId, clientSeq := range shardClientMap {
			entry := clientMapEntry{
				seq: clientSeq,
				ch:  make(chan int),
			}
			kv.clientMap[shard][clientId] = &entry
		}
	}
	kv.oldConfig = snapshot.OldConfig
	kv.newConfig = snapshot.NewConfig
	kv.shardConfigNum = snapshot.ShardConfigNum
}

func (kv *ShardKV) updateFromRaftWithLongLoop() {
	for {
		if kv.isKilled {
			return
		}
		appliedMsg := <-kv.applyCh
		if !appliedMsg.CommandValid {
			snapshot, ok := kv.readSnapshotWithNoLock(appliedMsg.Command.([]byte)) //warn change snapshot install block,wrong change
			if ok {
				go func(Snapshot) { //warn change snapshot install must run in a goroutine,no block,escape dead lock,如果此时updateAppliedLast拿到了锁，
					//snapshot block会导致updateAppliedLast无法释放锁，造成死锁，channel可以看成是锁的一种．
					if snapshot.LastIncludedIndex > kv.rf.LastIncludedIndex { //warn 别的goroutine,比如appendEntries,updateAppliedLast可能拿到了rf lock，防止死锁
						//warn old rpc可能会到达condition内，因此下面再加condition
						kv.rf.GetLock()
						kv.mu.Lock()                                                                                                  //warn change add condition snapshot.LastIncludedIndex > kv.lastAppliedIndex,escape 已经被applied的entry,被snapshot抹掉
						if snapshot.LastIncludedIndex > kv.rf.LastIncludedIndex && snapshot.LastIncludedIndex > kv.lastAppliedIndex { //warn change old duplicate install snapshot rpc deal
							kv.saveSnapshotWithNoLock(&snapshot) // change warn kv必须首先saveSnapshot再restore
							kv.restoreSnapshotWithNoLock(&snapshot)
						}
						kv.mu.Unlock()
						kv.rf.ReleaseLock()
					}
				}(snapshot)
			}
			continue
		}
		op := appliedMsg.Command.(Op)
		DPrintf("%v updateFromRaft begin:%v ", kv, &op)
		kv.mu.Lock()
		kv.initialClientMapWithNoLock(op.ClientId, op.Shard)
		_, ok := kv.rf.GetState()
		switch op.Type {
		case PUT:
			if op.Seq > kv.clientMap[op.Shard][op.ClientId].seq && (op.ConfigNum == kv.newConfig.Num) {
				DPrintf("%v update state ClientId %v Seq %v Key:%v,Value:%v", kv, op.ClientId, op.Seq, op.Key, op.Value)
				kv.state[op.Shard][op.Key] = op.Value
				kv.clientMap[op.Shard][op.ClientId].seq = op.Seq
			}
			DPrintf("%v update state ClientId %v Seq %v Key:%v,Value:%v,ConfigNum:%v", kv, op.ClientId, op.Seq, op.Key, op.Value, op.ConfigNum)
			ch := kv.clientMap[op.Shard][op.ClientId].ch
			if ok {
				go func() {
					ch <- op.Seq
				}()
			}
		case APPEND:
			if op.Seq > kv.clientMap[op.Shard][op.ClientId].seq && (op.ConfigNum == kv.newConfig.Num) { //second condition must,see error1
				DPrintf("%v update state ClientId %v Seq %v Key:%v,Value:%v", kv, op.ClientId, op.Seq, op.Key, op.Value)
				currValue, ok := kv.state[op.Shard][op.Key]
				if ok {
					kv.state[op.Shard][op.Key] = currValue + op.Value
				} else {
					kv.state[op.Shard][op.Key] = op.Value
				}
				kv.clientMap[op.Shard][op.ClientId].seq = op.Seq
			}
			DPrintf("%v update state ClientId %v Seq %v Key:%v,Value:%v,ConfigNum:%v", kv, op.ClientId, op.Seq, op.Key, op.Value, op.ConfigNum)
			ch := kv.clientMap[op.Shard][op.ClientId].ch
			if ok {
				go func() {
					ch <- op.Seq
				}()
			}
		case GET:
			if op.Seq > kv.clientMap[op.Shard][op.ClientId].seq {
				kv.clientMap[op.Shard][op.ClientId].seq = op.Seq
			}
			ch := kv.clientMap[op.Shard][op.ClientId].ch
			if ok {
				go func() {
					ch <- op.Seq
				}()
			}
		case CONFIG:
			if kv.newConfig.Num == op.Config.Num-1 { //warn duplicate commit config because of leader step down
				kv.oldConfig = kv.newConfig
				kv.newConfig = op.Config
				oldShards := kv.oldConfig.Shards
				newShards := kv.newConfig.Shards
				for shard, newGID := range newShards {
					if (oldShards[shard] == kv.gid || oldShards[shard] == 0) && newGID == kv.gid { //no change shard
						kv.shardConfigNum[shard] = kv.newConfig.Num
					} else if oldShards[shard] != kv.gid && newGID != kv.gid { //non-interest shard
						kv.shardConfigNum[shard] = kv.newConfig.Num
					}
				}
			}
		case HandOffShard:
			shardState := make(map[string]string) //warn copy must,for map or slice in go are reference,log persistent may change raw log
			for key, val := range op.ShardState {
				shardState[key] = val
			}
			clientMap := make(map[int64]int)
			for key, val := range op.ClientMap {
				clientMap[key] = val
			}
			HandOffShard := HandOffShardArgs{
				ConfigNum:  op.ConfigNum,
				Shard:      op.Shard,
				ShardState: shardState,
				ClientMap:  clientMap,
			}
			if HandOffShard.Shard == 8 {
				value, ok := HandOffShard.ShardState["0"]
				if ok {
					DPrintf("%v", value)
				}
			}
			kv.updateAddShardsWithNoLock(&HandOffShard)
		case DeprecateShard:
			if kv.shardConfigNum[op.Shard] < op.ConfigNum && kv.newConfig.Num == op.ConfigNum {
				kv.shardConfigNum[op.Shard] = op.ConfigNum
				kv.state[op.Shard] = nil //challenge1
			}
		}
		kv.lastAppliedIndex = appliedMsg.CommandIndex
		kv.lastAppliedTerm = appliedMsg.CommandTerm
		kv.mu.Unlock()
		DPrintf("%v updateFromRaft end", kv)
	}
}

func (kv *ShardKV) initialClientMapWithNoLock(clientId int64, shard int) {
	_, ok := kv.clientMap[shard][clientId]
	if !ok {
		entry := clientMapEntry{
			seq: -1,
			ch:  make(chan int),
		}
		kv.clientMap[shard][clientId] = &entry
	}
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	DPrintf("%v Get begin: %v", kv, args)
	defer DPrintf("%v Get end: %v %v ", kv, args, reply)
	if args.ConfigNum != kv.newConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//if kv.newConfig.Shards[args.Shard] != kv.gid {
	//	reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	if kv.shardConfigNum[args.Shard] != args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.initialClientMapWithNoLock(args.ClientId, args.Shard)
	if args.Seq <= kv.clientMap[args.Shard][args.ClientId].seq {
		reply.Err = OK
		value, ok := kv.state[args.Shard][args.Key]
		if ok {
			reply.Value = value
		} else {
			reply.Value = ""
		}
		kv.mu.Unlock()
		return
	}
	ch := kv.clientMap[args.Shard][args.ClientId].ch
	kv.mu.Unlock()
	Command := Op{
		Type:     GET,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Shard:    args.Shard,
	}
	kv.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				if args.ConfigNum != kv.newConfig.Num {
					reply.Err = ErrWrongGroup
					return
				}
				reply.Err = OK
				value, ok := kv.state[args.Shard][args.Key]
				if ok {
					reply.Value = value
				} else {
					reply.Value = ""
				}
				return
			}
		default:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.Err = ErrWrongLeader
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// Your code here.
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	DPrintf("%v PutAppend begin: %v", kv, args)
	defer DPrintf("%v PutAppend end: %v %v", kv, args, reply)
	if args.ConfigNum != kv.newConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//if kv.newConfig.Shards[args.Shard] != kv.gid {
	//	reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	if kv.shardConfigNum[args.Shard] != args.ConfigNum { // warn condition must,handOffShard的configNum必须等于args.ConfigNum
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.initialClientMapWithNoLock(args.ClientId, args.Shard)
	if args.Seq <= kv.clientMap[args.Shard][args.ClientId].seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	ch := kv.clientMap[args.Shard][args.ClientId].ch
	kv.mu.Unlock()
	Command := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		Seq:       args.Seq,
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
	}
	kv.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				//error1 corner case,当replication group的Leader在handOffShard后，crash，newLeader可能仍然没有更新config，client也没有更新Config，此时newLeader依然能够接收客户端请求
				//但是因为newConfig已经被commit了，也apply了，但是newLeader对客户端的回应必须是ErrWrongGroup,不能更新newLeader的state，不然会出现data loss，这在客户端看来是不可接受的。
				if args.ConfigNum != kv.newConfig.Num {
					reply.Err = ErrWrongGroup
					return
				}
				reply.Err = OK
				return
			}
		default:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.Err = ErrWrongLeader
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.isKilled = true
	DPrintf("%v kill", kv)
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.sm = shardmaster.MakeClerk(masters)
	kv.clientMap = make(map[int]map[int64]*clientMapEntry)
	kv.state = make(map[int]map[string]string)
	for i := 0; i < shardmaster.NShards; i++ {
		shardState := make(map[string]string)
		shardClientMap := make(map[int64]*clientMapEntry)
		kv.state[i] = shardState
		kv.clientMap[i] = shardClientMap
	}
	kv.lastAppliedTerm = 0   //warn condition snapshot
	kv.lastAppliedIndex = -1 //
	data := persister.ReadSnapshot()
	snapshot, ok := kv.readSnapshotWithNoLock(data)
	if ok {
		kv.rf.LastIncludedIndex = snapshot.LastIncludedIndex // change warn restart需要修改rf的snapshot state,restore snapshot时,rf的state必须改变
		kv.rf.LastIncludedTerm = snapshot.LastIncludedTerm   //
		kv.restoreSnapshotWithNoLock(&snapshot)
		//kv.rf.TrimLogEntryWithLock(snapshot.LastIncludedIndex,snapshot.LastIncludedTerm)// warn change restart不需要trim
	}
	go kv.updateFromRaftWithLongLoop()
	go kv.pollConfigWithLongLoop()
	go kv.monitorSnapshotWithLongLoop()
	go kv.updateDeprecateShardConfigNumWithLongLoop()
	return kv
}
