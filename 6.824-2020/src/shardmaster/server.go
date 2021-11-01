package shardmaster

import (
	"6.824/labs/raft"
	"sort"
	"time"
)
import "6.824/labs/labrpc"
import "sync"
import "6.824/labs/labgob"

type clientMapEntry struct {
	seq int
	ch  chan int
}
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	groupNum  int
	clientMap map[int64]*clientMapEntry
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	// Your data here.
	Servers map[int][]string
	GIDs    []int
	GID     int
	Shard   int
	Num     int
	Type    string
	Id      int64
	Seq     int
}

func (sm *ShardMaster) initialClientMapWithNoLock(clientId int64) {
	_, ok := sm.clientMap[clientId]
	if !ok {
		ch := make(chan int)
		entry := clientMapEntry{
			seq: -1,
			ch:  ch,
		}
		sm.clientMap[clientId] = &entry
	}
}

func (sm *ShardMaster) updateFromRaft() {
	for {
		applyMsg := <-sm.applyCh
		op := applyMsg.Command.(Op)
		DPrintf("sm [%v] applyMsg %v",sm.me,&op)
		clientId := op.Id
		clientSeq := op.Seq
		sm.mu.Lock()
		sm.initialClientMapWithNoLock(clientId)
		if clientSeq > sm.clientMap[clientId].seq { //update config only with new rpc
			sm.clientMap[clientId].seq = clientSeq
			//config := op.Config
			switch op.Type {
			case Join:
				sm.handleJoin(&op)
			case Leave:
				sm.handleLeave(&op)
			case Move:
				sm.handleMove(&op)
			}
		}
		ch := sm.clientMap[clientId].ch
		sm.mu.Unlock()
		_, isLeader := sm.rf.GetState()
		if isLeader {
			go func() { //goroutine concurrent do not block
				ch <- clientSeq
			}()
		}
	}
}
func shards2map(shards [NShards]int) map[int][]int {
	shardsMap := make(map[int][]int)
	for shard, gid := range shards {
		_, ok := shardsMap[gid]
		if ok {
			shardsMap[gid] = append(shardsMap[gid], shard)
		} else {
			shardsMap[gid] = []int{shard}
		}
	}
	return shardsMap
}
func Contain(slice []int, element int) bool {
	for _, val := range slice {
		if val == element {
			return true
		}
	}
	return false
}
func (sm *ShardMaster) handleJoin(op *Op) {
	currConfig := sm.configs[len(sm.configs)-1]
	newGroupNum := len(currConfig.Groups) + len(op.Servers)
	var shardsDeserve int
	var reAllocateShardsNum int
	shardsDeserve = NShards / newGroupNum
	if newGroupNum > NShards{
		shardsDeserve = 1
		reAllocateShardsNum = NShards - len(currConfig.Groups)
	}else {
		reAllocateShardsNum = (NShards / newGroupNum) * len(op.Servers)
	}

	var reAllocateShards []int
	var addGIDs []int
	for gid := range op.Servers {
		addGIDs = append(addGIDs,gid)
	}
	sort.Ints(addGIDs)
	var newShards [NShards]int
	for idx, shard := range currConfig.Shards {
		newShards[idx] = shard
	}
	if len(currConfig.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			reAllocateShards = append(reAllocateShards, i)
		}
	} else {
		shardsMap := shards2map(newShards)
		var currGIDs []int
		for gid := range currConfig.Groups{
			currGIDs = append(currGIDs,gid)
		}
		sort.Ints(currGIDs)
		loop:
			for _,gid := range currGIDs{
				shards,ok := shardsMap[gid]
				if ok{
					for len(shards) > shardsDeserve {
						if len(reAllocateShards) >= reAllocateShardsNum{
							break loop
						}
						reAllocateShards = append(reAllocateShards, shards[0])
						shards = shards[1:]
					}
				}
			}

	}
	var step int
	for _, shard := range reAllocateShards {
		newShards[shard] = addGIDs[step]
		step++
		if step == len(addGIDs){
			step = 0
		}
	}

	newGroups := make(map[int][]string)
	for key, val := range currConfig.Groups {
		newGroups[key] = val
	}
	for gid,servers := range op.Servers{
		newGroups[gid] = servers
	}


	args := JoinArgs{
		Id:      op.Id,
		Seq:     op.Seq,
		Servers: op.Servers,
	}
	config := Config{
		Num:    currConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, config)
	DPrintf("sm [%v] Join: %v shardsMap:%v newGroups :%v", sm.me, &args, shards2map(newShards), newGroups)
}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	//initialize
	sm.mu.Lock()
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	DPrintf("sm [%v] begin Join: %v", sm.me, args)
	defer DPrintf("sm [%v] end Join: %v ,reply.WrongLeader %v", sm.me, args, reply.WrongLeader)
	clientSeq := args.Seq
	clientId := args.Id
	sm.initialClientMapWithNoLock(clientId)
	if clientSeq <= sm.clientMap[clientId].seq { //deal old\duplicate rpc
		reply.WrongLeader = false
		DPrintf("sm [%v] Join: %v ,old/duplicate rpc", sm.me, args)
		sm.mu.Unlock()
		return
	}
	ch := sm.clientMap[clientId].ch
	sm.mu.Unlock()
	Command := Op{
		Servers: args.Servers,
		Type:    Join,
		Id:      clientId,
		Seq:     clientSeq,
	}
	sm.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				reply.Err = OK
				return
			}
		default:
			_, isLeader := sm.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				DPrintf("sm [%v] Join: %v leader step down", sm.me, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Your code here.
}
func (sm *ShardMaster) handleLeave(op *Op) {
	currConfig := sm.configs[len(sm.configs)-1]
	leaveGIDs := op.GIDs


	var newShards [NShards]int
	for idx, shard := range currConfig.Shards {
		newShards[idx] = shard
	}
	newGroups := make(map[int][]string)
	for key, val := range currConfig.Groups {
		newGroups[key] = val
	}

	shardsMap := shards2map(newShards)
	var idleGIDs []int
	for gid := range currConfig.Groups{
		_,ok := shardsMap[gid]
		if !ok{
			idleGIDs = append(idleGIDs,gid)
		}
	}
	sort.Ints(idleGIDs) //map遍历是无序的，为了保证一致性，必须排序
	var idleGID int
	var reAllocateShards []int
	for _,leaveGID := range leaveGIDs{
		if !Contain(idleGIDs,leaveGID){
			if len(idleGIDs) > 0{
				idleGID = idleGIDs[0]
				idleGIDs = idleGIDs[1:]
				for shard,currGID := range newShards{
					if currGID == leaveGID{
						newShards[shard] = idleGID
					}
				}
			}else{
				reAllocateShards = append(reAllocateShards,shardsMap[leaveGID]...)
			}
		}
		delete(newGroups,leaveGID)
	}

	var newGIDs []int
	for gid := range newGroups{
		newGIDs = append(newGIDs,gid)
	}
	var i int
	for shard := range newShards {
		if len(newGIDs) == 0{
			newShards[shard] = 0
			continue
		}
		if Contain(reAllocateShards,shard){
			newShards[shard] = newGIDs[i]
			i++
			if i == len(newGIDs){
				i = 0
			}
		}

	}

	config := Config{
		Num:    currConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	args := LeaveArgs{
		Id:   op.Id,
		Seq:  op.Seq,
		GIDs: op.GIDs,
	}
	sm.configs = append(sm.configs, config)
	DPrintf("sm [%v] Leave: %v shardsMap:%v newGroups :%v", sm.me, &args, shards2map(newShards), newGroups)
}
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	//initialize
	sm.mu.Lock()
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	DPrintf("sm [%v] begin Leave: %v", sm.me, args)
	defer DPrintf("sm [%v] end Leave: %v reply.WrongLeader %v", sm.me, args, reply.WrongLeader)
	clientSeq := args.Seq
	clientId := args.Id
	sm.initialClientMapWithNoLock(clientId)
	if clientSeq <= sm.clientMap[clientId].seq { //deal old\duplicate rpc
		reply.WrongLeader = false
		DPrintf("sm [%v] Leave: %v ,old/duplicate rpc", sm.me, args)
		sm.mu.Unlock()
		return
	}

	Command := Op{
		GIDs: args.GIDs,
		Type: Leave,
		Id:   args.Id,
		Seq:  args.Seq,
	}
	ch := sm.clientMap[clientId].ch
	sm.mu.Unlock()
	sm.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				reply.WrongLeader = false
				return
			}
		default:
			_, isLeader := sm.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				DPrintf("sm [%v] Leave: %v leader step down", sm.me, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Your code here.
}
func (sm *ShardMaster) handleMove(op *Op) {
	currConfig := sm.configs[len(sm.configs)-1]
	moveShard := op.Shard
	moveGid := op.GID
	var newShards [NShards]int
	for shard, gid := range currConfig.Shards {
		newShards[shard] = gid
	}
	newGroups := make(map[int][]string)
	for shard := range newShards {
		if shard == moveShard {
			newShards[shard] = moveGid
		}

	}
	for _, gid := range newShards {
		newGroups[gid] = currConfig.Groups[gid]
	}

	config := Config{
		Num:    currConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	args := MoveArgs{
		Id:    op.Id,
		Seq:   op.Seq,
		Shard: op.Shard,
		GID:   op.GID,
	}
	sm.configs = append(sm.configs, config)
	DPrintf("sm [%v] Move: %v shardsMap:%v newGroups :%v", sm.me, &args, shards2map(newShards), newGroups)
}
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	//initialize
	sm.mu.Lock()
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	DPrintf("sm [%v] begin Move: %v", sm.me, args)
	defer DPrintf("sm [%v] end Move: %v reply.WrongLeader %v", sm.me, args, reply.WrongLeader)
	clientSeq := args.Seq
	clientId := args.Id
	sm.initialClientMapWithNoLock(clientId)
	if clientSeq <= sm.clientMap[clientId].seq { //deal old\duplicate rpc
		reply.WrongLeader = false
		DPrintf("sm [%v] Move: %v old/duplicate rpc", sm.me, args)
		sm.mu.Unlock()
		return
	}
	//generate new config

	Command := Op{
		Shard: args.Shard,
		GID:   args.GID,
		Type:  Move,
		Id:    args.Id,
		Seq:   args.Seq,
	}
	ch := sm.clientMap[clientId].ch //deal concurrent map error
	sm.mu.Unlock()
	//start rf Command and wait command commit
	sm.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				reply.WrongLeader = false

				return
			}
		default:
			_, isLeader := sm.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				DPrintf("sm [%v] Move: %v leader step down", sm.me, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Your code here.
}
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	_,isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintf("sm [%v] begin Query: %v", sm.me, args)
	defer DPrintf("sm [%v] end Query: %v reply.WrongLeader %v reply.Config.Num %v", sm.me, args, reply.WrongLeader, reply.Config.Num)
	clientSeq := args.Seq
	clientId := args.Id
	configNum := args.Num
	sm.mu.Lock()
	sm.initialClientMapWithNoLock(clientId)
	if clientSeq <= sm.clientMap[clientId].seq { //deal old\duplicate rpc
		reply.WrongLeader = false
		if configNum == -1 || configNum == len(sm.configs){
			configNum = len(sm.configs) - 1
		}
		reply.Config = sm.configs[configNum]
		DPrintf("sm [%v] Query: %v old/duplicate rpc", sm.me, args)
		sm.mu.Unlock()
		return
	}

	//warn Query 必须进log，得到majority agreement，不然由于stale leader，会得到stale data
	// ，虽然会得到stale data，但是对client来说，依然是linear的
	ch := sm.clientMap[clientId].ch
	sm.mu.Unlock()
	Command := Op{
		Num:  args.Num,
		Type: Query,
		Id:   args.Id,
		Seq:  args.Seq,
	}
	sm.rf.Start(Command)
	for {
		select {
		case seq := <-ch:
			if seq == args.Seq {
				reply.WrongLeader = false
				if configNum == -1 || configNum >= len(sm.configs){
					configNum = len(sm.configs) - 1
				}
				reply.Config = sm.configs[configNum]
				DPrintf("sm [%v] Query: %v shardsMap:%v Groups :%v", sm.me, args, shards2map(sm.configs[configNum].Shards), sm.configs[configNum].Groups)
				return
			}
		default:
			_, isLeader := sm.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				DPrintf("sm [%v] Query: %v leader step down", sm.me, args)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Num = 0
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.clientMap = make(map[int64]*clientMapEntry)
	go sm.updateFromRaft()
	// Your code here.

	return sm
}
