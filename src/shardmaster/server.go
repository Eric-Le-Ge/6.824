package shardmaster

import "../labrpc"
import "../labgob"
import "../raft"
import "log"
import "sort"
import "sync"
import "sync/atomic"
import "time"

const ApplyWaitTimeOut = 480 * time.Millisecond
const ApplySendTimeOut = 20 * time.Millisecond

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32    // 1 if shardMaster is killed
	configs []Config // indexed by config num
	doneApplyCh map[int]chan Serial // channels to send back apply finished message at given index
	queryResults map[int] Config // channels to send back query results
	clientSerial map[int64]int64 // highest processed serial number of client
}


type Op struct {
	// Your data here.
	Type string // Join, Leave, Move, Query
	Args interface{}
}

func (sm *ShardMaster) apply(_type string, serial Serial, args interface{}) (int, bool) {
	sm.mu.Lock()
	if serial.Number <= sm.clientSerial[serial.ClientId] {
		sm.mu.Unlock()
		return -1, true
	}

	index, _, isLeader := sm.rf.Start(Op{
		Type: _type,
		Args: args,
	})

	if !isLeader || sm.killed() {
		sm.mu.Unlock()
		return -1, false
	}
	doneCh := make(chan Serial)
	sm.doneApplyCh[index] = doneCh
	sm.mu.Unlock()

	select {
		case appliedSerial := <- doneCh: {
			sm.mu.Lock()
			defer sm.mu.Unlock()
			if appliedSerial == serial{
				return index, true
			}
			return -1, false
		}
		case <- time.After(ApplyWaitTimeOut):
			return -1, false
 	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, success := sm.apply("Join", args.Serial, *args)
	reply.WrongLeader = !success
	if success {
		reply.Err = OK
	}
}

type gidShardCount struct {
	gid int
	shardCount int
}

type gidShardCounts []gidShardCount

func (g gidShardCounts) Len() int {
	return len(g)
}

func (g gidShardCounts) Less(i, j int) bool {
	if g[i].shardCount == g[j].shardCount {
		return g[i].gid > g[j].gid
	}
	return g[i].shardCount > g[j].shardCount
}

func (g gidShardCounts) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

//
// applies a join to sm, called with lock held
//
func (sm *ShardMaster) join(args *JoinArgs) {
	oldConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	for i:=0; i<NShards; i++ {newConfig.Shards[i]=oldConfig.Shards[i]}
	nOld := len(oldConfig.Groups)
	nDelta := len(args.Servers)
	quota := NShards / (nOld + nDelta)
	overflow := NShards % (nOld + nDelta)
	if nOld == 0 {
		ind := 0
		for gid, servers := range args.Servers {
			distribute := quota
			if overflow > 0 {
				overflow --
				distribute ++
			}
			for i:=0;i<distribute;i++{
				newConfig.Shards[ind] = gid
				ind ++
			}
			newConfig.Groups[gid] = servers
		}
	} else {
		shardQueue := make([]int, 0, NShards)
		gidToShards := map[int][]int{}
		for ind, gid := range oldConfig.Shards {
			if arr, ok := gidToShards[gid]; ok {
				gidToShards[gid] = append(arr, ind)
			} else {
				gidToShards[gid] = []int{ind}
			}
		}
		gids := make(gidShardCounts, 0, len(oldConfig.Groups))
		for gid, servers := range oldConfig.Groups {
			shardCount := 0
			if arr, ok := gidToShards[gid]; ok {
				shardCount = len(arr)
			}
			gids = append(gids, gidShardCount{
				gid:        gid,
				shardCount: shardCount,
			})
			newConfig.Groups[gid] = servers
		}
		sort.Sort(gids)
		ind := 0
		for _, _gidShardCount := range gids {
			distribute := quota
			if overflow > 0 {
				overflow--
				distribute++
			}
			for _gidShardCount.shardCount > distribute {
				_gidShardCount.shardCount--
				var shard int
				shard, gidToShards[_gidShardCount.gid] = gidToShards[_gidShardCount.gid][0], gidToShards[_gidShardCount.gid][1:]
				shardQueue = append(shardQueue, shard)
			}
			for _gidShardCount.shardCount < distribute {
				_gidShardCount.shardCount++
				newConfig.Shards[shardQueue[ind]] = _gidShardCount.gid
				ind ++
			}
		}
		for gid, servers := range args.Servers {
			distribute := quota
			if overflow > 0 {
				overflow --
				distribute ++
			}
			for i:=0;i<distribute;i++{
				newConfig.Shards[shardQueue[ind]] = gid
				ind ++
			}
			newConfig.Groups[gid] = servers
		}
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, success := sm.apply("Leave", args.Serial, *args)
	reply.WrongLeader = !success
	if success {
		reply.Err = OK
	}
}

func (sm *ShardMaster) leave(args *LeaveArgs) {
	oldConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	nOld := len(oldConfig.Groups)
	nDelta := -len(args.GIDs)
	if nOld + nDelta == 0 {
		sm.configs = append(sm.configs, newConfig)
		return
	}
	for i:=0; i<NShards; i++ {newConfig.Shards[i]=oldConfig.Shards[i]}
	quota := NShards / (nOld + nDelta)
	overflow := NShards % (nOld + nDelta)
	shardQueue := make([]int, 0, NShards)
	gidToShards := map[int][]int{}
	for ind, gid := range oldConfig.Shards {
		if arr, ok := gidToShards[gid]; ok {
			gidToShards[gid] = append(arr, ind)
		} else {
			gidToShards[gid] = []int{ind}
		}
	}
	gids := make(gidShardCounts, 0, len(oldConfig.Groups))
	for gid, servers := range oldConfig.Groups {
		shardCount := 0
		if arr, ok := gidToShards[gid]; ok {
			shardCount = len(arr)
		}
		gids = append(gids, gidShardCount{
			gid:        gid,
			shardCount: shardCount,
		})
		newConfig.Groups[gid] = servers
	}
	sort.Sort(gids)
	for _, gid := range args.GIDs {
		if arr, ok := gidToShards[gid]; ok {
			for _, shard := range arr {
				shardQueue = append(shardQueue, shard)
			}
		}
		delete(newConfig.Groups, gid)
	}
	ind := 0
	for _, _gidShardCount := range gids {
		if _, ok := newConfig.Groups[_gidShardCount.gid]; !ok {
			continue
		}
		distribute := quota
		if overflow > 0 {
			overflow--
			distribute++
		}
		for _gidShardCount.shardCount > distribute {
			_gidShardCount.shardCount--
			var shard int
			shard, gidToShards[_gidShardCount.gid] = gidToShards[_gidShardCount.gid][0], gidToShards[_gidShardCount.gid][1:]
			shardQueue = append(shardQueue, shard)
		}
		for _gidShardCount.shardCount < distribute {
			_gidShardCount.shardCount++
			newConfig.Shards[shardQueue[ind]] = _gidShardCount.gid
			ind ++
		}
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, success := sm.apply("Move", args.Serial, *args)
	reply.WrongLeader = !success
	if success {
		reply.Err = OK
	}
}

func (sm *ShardMaster) move(args *MoveArgs) {
	oldConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	for i:=0; i<NShards; i++ {newConfig.Shards[i]=oldConfig.Shards[i]}
	newConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	index, success := sm.apply("Query", args.Serial, *args)
	reply.WrongLeader = !success
	if success {
		// if query is already applied
		sm.mu.Lock()
		if index == -1 {
			reply.Config = sm.query(args)
		} else {
			reply.Config = sm.queryResults[index]
		}
		sm.mu.Unlock()
		reply.Err = OK
	}
}

func (sm *ShardMaster) query(args *QueryArgs) Config {
	if args.Num == -1 {
		return sm.configs[len(sm.configs)-1]
	}
	return sm.configs[args.Num]
}

//
// a long running go routine that applies messages from ApplyChannel,
// simplified version of kvraft.operateApply
//
func (sm *ShardMaster) operateApply() {
	for msg := range sm.applyCh {
		op := msg.Command.(Op)
		sm.mu.Lock()
		switch op.Type {
		case "Join": {
			args := op.Args.(JoinArgs)
			if args.Serial.Number > sm.clientSerial[args.Serial.ClientId] {
				sm.clientSerial[args.Serial.ClientId] = args.Serial.Number
				sm.join(&args)
				doneCh := sm.doneApplyCh[msg.CommandIndex]
				sm.mu.Unlock()
				select {
				case doneCh <- args.Serial:
				case <- time.After(ApplySendTimeOut):
				}
			} else {
				sm.mu.Unlock()
			}
		}
		case "Leave": {
			args := op.Args.(LeaveArgs)
			if args.Serial.Number > sm.clientSerial[args.Serial.ClientId] {
				sm.clientSerial[args.Serial.ClientId] = args.Serial.Number
				sm.leave(&args)
				doneCh := sm.doneApplyCh[msg.CommandIndex]
				sm.mu.Unlock()
				select {
				case doneCh <- args.Serial:
				case <- time.After(ApplySendTimeOut):
				}
			} else {
				sm.mu.Unlock()
			}
		}
		case "Move": {
			args := op.Args.(MoveArgs)
			if args.Serial.Number > sm.clientSerial[args.Serial.ClientId] {
				sm.clientSerial[args.Serial.ClientId] = args.Serial.Number
				sm.move(&args)
				doneCh := sm.doneApplyCh[msg.CommandIndex]
				sm.mu.Unlock()
				select {
				case doneCh <- args.Serial:
				case <- time.After(ApplySendTimeOut):
				}
			} else {
				sm.mu.Unlock()
			}
		}
		case "Query": {
			args := op.Args.(QueryArgs)
			if args.Serial.Number > sm.clientSerial[args.Serial.ClientId] {
				sm.clientSerial[args.Serial.ClientId] = args.Serial.Number
				sm.queryResults[msg.CommandIndex] = sm.query(&args)
				doneCh := sm.doneApplyCh[msg.CommandIndex]
				sm.mu.Unlock()
				select {
				case doneCh <- args.Serial:
				case <- time.After(ApplySendTimeOut):
				}
			} else {
				sm.mu.Unlock()
			}
		}
		default:
			log.Fatal("Unrecognized op type")
		}
		if sm.killed() {return}
	}
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
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
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
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(Serial{})
	sm.mu.Lock()
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.clientSerial = make(map[int64]int64)
	sm.doneApplyCh = make(map[int]chan Serial)
	sm.queryResults = make(map[int]Config)

	// Your code here.

	go sm.operateApply()
	sm.mu.Unlock()

	return sm
}
