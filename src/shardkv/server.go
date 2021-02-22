package shardkv


import "bytes"
import "log"
import "sync/atomic"
import "sync"
import "time"
import "../raft"
import "../labgob"
import "../labrpc"
import "../shardmaster"

const ApplyWaitTimeOut = 480 * time.Millisecond
const ApplySendTimeOut = 20 * time.Millisecond
const WaitPollInterval = 20 * time.Millisecond
const ClientPollInterval = 100 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string // "Put" or "Append" or "Get" or "Config" or "GC"
	Key   string
	Value string
	Shard int // used for GC
	Config shardmaster.Config
	SerialNumber int64 // used to prevent duplicate requests.
	ClientId     int64 // used to prevent duplicate requests.
}

type Result struct {
	Err Err      // error for the apply result
	Value string // for gets, include the value in the result
}

type Group struct {
	Gid int      // gid of the group
	Servers []string // servers in the group
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck          *shardmaster.Clerk // clerk to talk to shardmaster
	stateMachine map[string]string // state machine - an in memory map
	historyState map[int]map[string]string // historical entries in state machine
	historySerial map[int]map[int64]int64 // historical entries in state machine
	historyNeed  map[int]map[int]Group // historical entries needed by gid for garbage collection
	snapshotConfigNum int // highest config number present in snapshot for garbage collection
	snapshotTracker map[int]map[string]int // tracker of snapshot progress in each machine for garbage collection
	applyIndex   int // highest index that has been applied to the state machine
	clientSerial map[int64]int64 // highest processed serial number of client
	doneCh       map[int]chan Result // channel to send back apply status
	currentConfig shardmaster.Config // current sharding configuration of the cluster
	targetConfig  shardmaster.Config // target configuration, if an update is taking place
	blockShard   map[int]bool // shards to block applying during a configuration change
}

// common code used by Get and PutAppend to validate a request, and
// start a log entry if request is valid. Called with lock held.
func (kv *ShardKV) validateAndStart(serial Serial, key string, value string, op string) (term int, index int, err Err) {
	if serial.Number <= kv.clientSerial[serial.ClientId] {
		err = ErrOldRequest
		return
	}
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		err = ErrWrongLeader
		return
	}
	shardId := key2shard(key)
	if kv.targetConfig.Shards[shardId] != kv.gid {
		err = ErrWrongGroup
		return
	}
	index, _, isLeader = kv.rf.Start(Op{
		Op:    op,
		Key:   key,
		Value: value,
		SerialNumber: serial.Number,
		ClientId: serial.ClientId,
	})
	err = OK
    return
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	term, index, err := kv.validateAndStart(args.Serial, args.Key, "", GetOp)
	reply.Err = err
	if err != OK {
		kv.mu.Unlock()
		return
	}
	doneCh := make(chan Result)
	kv.doneCh[index] = doneCh
	kv.mu.Unlock()

	select {
		case result := <- doneCh: {
			kv.mu.Lock()
			currentTerm, isLeader := kv.rf.GetState()
			if !isLeader || currentTerm != term {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			// some applies may not be successful. For example,
			// accessing a shard after it no longer belongs to
			// this group
			reply.Err = result.Err
			if reply.Err == OK || reply.Err == ErrNoKey {
				reply.Value = result.Value
			}
			kv.mu.Unlock()
		}
		case <- time.After(ApplyWaitTimeOut): {
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	term, index, err := kv.validateAndStart(args.Serial, args.Key, args.Value, args.Op)
	reply.Err = err
	if err != OK {
		kv.mu.Unlock()
		return
	}
	doneCh := make(chan Result)
	kv.doneCh[index] = doneCh
	kv.mu.Unlock()

	select {
		case result := <- doneCh: {
			kv.mu.Lock()
			currentTerm, isLeader := kv.rf.GetState()
			if !isLeader || currentTerm != term {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			// some applies may not be successful. For example,
			// accessing a shard after it no longer belongs to
			// this group
			reply.Err = result.Err
			kv.mu.Unlock()
		}
		case <- time.After(ApplyWaitTimeOut): {
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum >= kv.currentConfig.Num {
		reply.Err = ErrWait
		return
	}
	data := make(map[string]string)
	for k, v := range kv.historyState[args.ConfigNum] {
		if key2shard(k) == args.Shard {
			data[k] = v
		}
	}
	reply.Err = OK
	reply.Data = data
	reply.ClientSerial = kv.historySerial[args.ConfigNum]
}

//
// Rpc to ask for snapshot number for garbage collection.
//
func (kv *ShardKV) QuerySnapshotNum(args *QuerySnapshotNumArgs, reply *QuerySnapshotNumReply) {
	kv.mu.Lock()
	reply.Num = kv.snapshotConfigNum
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// Save the current raft state into a snapshot. called with lock held.
//
func (kv *ShardKV) saveSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.clientSerial)
	e.Encode(kv.currentConfig)
	e.Encode(kv.historyState)
	e.Encode(kv.historySerial)
	e.Encode(kv.historyNeed)
	data := w.Bytes()
	kv.rf.TruncateLog(data, kv.applyIndex)
	if kv.currentConfig.Num > kv.snapshotConfigNum {
		kv.snapshotConfigNum = kv.currentConfig.Num
	}
}

//
// function for goroutine called by custer members to retrieve
// a shard
//
func (kv *ShardKV) installShard(shard int, wg *sync.WaitGroup) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer wg.Done()
	if kv.targetConfig.Num == 1 {
		kv.blockShard[shard] = false
		return
	}
	gid := kv.currentConfig.Shards[shard]
	servers := kv.currentConfig.Groups[gid]
	args := InstallShardArgs {
		Shard:     shard,
		ConfigNum: kv.currentConfig.Num,
	}
	for {
		for si := 0; si < len(servers); si++ {

			srv := kv.make_end(servers[si])
			var reply InstallShardReply
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.InstallShard", &args, &reply)
			kv.mu.Lock()
			if ok && (reply.Err == OK) {
				for k, v := range reply.Data {
					kv.stateMachine[k] = v
				}
				kv.blockShard[shard] = false
				for k, v := range reply.ClientSerial {
					if vThis, ok := kv.clientSerial[k]; !ok || vThis < v {
						kv.clientSerial[k] = v
					}
				}
				return
			}
			if ok && (reply.Err == ErrWait) {
				break
			}
		}
		time.Sleep(ClientPollInterval)
	}
}

//
// helper function to coordinate the process of asking for shards asynchronously
//
func (kv *ShardKV) updateConfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var wg sync.WaitGroup
	for shard := 0; shard < len(kv.targetConfig.Shards); shard++ {
		if kv.targetConfig.Shards[shard] == kv.gid && kv.currentConfig.Shards[shard] != kv.gid {
			wg.Add(1)
			go kv.installShard(shard, &wg)
		}
	}
	kv.mu.Unlock()
	wg.Wait()
	kv.mu.Lock()
	kv.currentConfig = kv.targetConfig
}

func (kv *ShardKV) operateConfig() {
	for !kv.killed() {
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader || kv.currentConfig.Num != kv.targetConfig.Num || newConfig.Num == kv.currentConfig.Num {
			kv.mu.Unlock()
			time.Sleep(ClientPollInterval)
			continue
		}
		if newConfig.Num - kv.currentConfig.Num > 1 {
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()
			newConfig = kv.mck.Query(currentNum+1)
			kv.mu.Lock()
		}
		_, term, _ := kv.rf.Start(Op{
			Op:           UpdateConfig,
			Config:       newConfig,
		})
		for {
			kv.mu.Unlock()
			time.Sleep(WaitPollInterval)
			kv.mu.Lock()
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != term || !isLeader || kv.currentConfig.Num >= newConfig.Num {
				break
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) querySnapshotNum(wg *sync.WaitGroup, server string, gid int) {
	srv := kv.make_end(server)
	var reply QuerySnapshotNumReply
	ok := srv.Call("ShardKV.QuerySnapshotNum", &QuerySnapshotNumArgs{}, &reply)
	if ok {
		kv.mu.Lock()
		if _, ok := kv.snapshotTracker[gid]; !ok {
			kv.snapshotTracker[gid] = make(map[string]int)
		}
		if reply.Num > kv.snapshotTracker[gid][server] {
			kv.snapshotTracker[gid][server] = reply.Num
		}
		kv.mu.Unlock()
	}
	wg.Done()
}

//
// a long running go routine that garbage collects histories
//
func (kv *ShardKV) operateGC() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if isLeader {
			// refresh snapshot nums for each server
			var wg sync.WaitGroup
			for gid, servers := range kv.currentConfig.Groups {
				for _, server := range servers {
					wg.Add(1)
					go kv.querySnapshotNum(&wg, server, gid)
				}
			}
			kv.mu.Unlock()
			c := make(chan struct{})
			go func() {
				defer close(c)
				wg.Wait()
			}()
			select {
			case <-c:
			case <-time.After(ApplyWaitTimeOut):
			}
			kv.mu.Lock()
			for num, shard2group := range kv.historyNeed {
				for shard, group := range shard2group {
					clear := len(group.Servers) == 0
					if groupNums, ok := kv.snapshotTracker[group.Gid]; ok {
						clear = true
						for _, server := range group.Servers {
							if snapshotNum, ok := groupNums[server]; !ok || snapshotNum <= num {
								clear = false
								break
							}
						}
					}
					if clear {
						kv.rf.Start(Op{
							Op:           GCOp,
							Shard:        shard,
							Config:       shardmaster.Config{
								Num: num,
							},
						})
					}
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(ApplyWaitTimeOut)
	}
}

//
// installs a snapshot. Called with lock held.
//
func (kv *ShardKV) installSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateMachine map[string]string
	var clientSerial map[int64]int64
	var currentConfig shardmaster.Config
	var historyState map[int]map[string]string
	var historySerial map[int]map[int64]int64
	var historyNeed map[int]map[int]Group
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&clientSerial) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&historyState) != nil ||
		d.Decode(&historySerial) != nil ||
		d.Decode(&historyNeed) != nil {
		log.Fatal("snapshot decode error")
	} else {
		kv.stateMachine = stateMachine
		kv.clientSerial = clientSerial
		kv.currentConfig = currentConfig
		kv.targetConfig = currentConfig
		kv.historyState = historyState
		kv.historySerial = historySerial
		kv.historyNeed = historyNeed
		kv.snapshotConfigNum = currentConfig.Num
	}
}

//
// a long running go routine that applies messages from ApplyChannel
//
func (kv *ShardKV) operateApply() {
	for msg := range kv.applyCh {
		if kv.killed() {return}
		kv.mu.Lock()
		if !msg.CommandValid {
			// make sure we don't install a snapshot in the middle of a config update
			for kv.currentConfig.Num != kv.targetConfig.Num {
				kv.mu.Unlock()
				time.Sleep(WaitPollInterval)
				kv.mu.Lock()
			}
			kv.installSnapshot(msg.Command.([]byte))
			kv.applyIndex = msg.CommandIndex
		} else {
			cmd := msg.Command.(Op)
			if cmd.Op == GetOp || cmd.Op == PutOp || cmd.Op == AppendOp {
				if cmd.SerialNumber > kv.clientSerial[cmd.ClientId] {
					shard := key2shard(cmd.Key)
					// wait for shards in transit
					for value, ok := kv.blockShard[shard]; ok && value; value, ok = kv.blockShard[shard]{
						kv.mu.Unlock()
						time.Sleep(WaitPollInterval)
						kv.mu.Lock()
					}
					if cmd.SerialNumber > kv.clientSerial[cmd.ClientId] {
						// Fail operations on shards to be removed
						kv.applyIndex = msg.CommandIndex
						result := Result{Err: ErrWrongGroup}
						if kv.targetConfig.Shards[shard] == kv.gid {
							result.Err = OK
							kv.clientSerial[cmd.ClientId] = cmd.SerialNumber
							switch cmd.Op {
							case GetOp:
								{
									if val, ok := kv.stateMachine[cmd.Key]; ok {
										result.Value = val
									} else {
										result.Err = ErrNoKey
										result.Value = ""
									}
								}
							case AppendOp:
								{
									if _, ok := kv.stateMachine[cmd.Key]; ok {
										kv.stateMachine[cmd.Key] += cmd.Value
									} else {
										kv.stateMachine[cmd.Key] = cmd.Value
									}
								}
							case PutOp:
								kv.stateMachine[cmd.Key] = cmd.Value
							}
						}
						doneCh := kv.doneCh[msg.CommandIndex]
						kv.mu.Unlock()
						select {
						case doneCh <- result:
						case <- time.After(ApplySendTimeOut):
						}
						kv.mu.Lock()
					}
				}
			} else if cmd.Op == UpdateConfig {
				if cmd.Config.Num == kv.targetConfig.Num + 1 {
					for kv.currentConfig.Num != kv.targetConfig.Num {
						kv.mu.Unlock()
						time.Sleep(WaitPollInterval)
						kv.mu.Lock()
					}
					kv.applyIndex = msg.CommandIndex
					kv.targetConfig = cmd.Config
					for shard := 0; shard < len(cmd.Config.Shards); shard++ {
						if cmd.Config.Shards[shard] == kv.gid && kv.currentConfig.Shards[shard] != kv.gid {
							kv.blockShard[shard] = true
						} else if kv.targetConfig.Shards[shard] != kv.gid && kv.currentConfig.Shards[shard] == kv.gid {
							if value, ok := kv.historyState[kv.currentConfig.Num]; !ok || value == nil {
								kv.historyState[kv.currentConfig.Num] = make(map[string]string)
							}
							if value, ok := kv.historyNeed[kv.currentConfig.Num]; !ok || value == nil {
								kv.historyNeed[kv.currentConfig.Num] = make(map[int]Group)
							}
							kv.historyNeed[kv.currentConfig.Num][shard] = Group{
								Gid:     kv.targetConfig.Shards[shard],
								Servers: kv.targetConfig.Groups[kv.targetConfig.Shards[shard]],
							}
							if v, ok := kv.snapshotTracker[kv.targetConfig.Shards[shard]]; !ok || v == nil {
								kv.snapshotTracker[kv.targetConfig.Shards[shard]] = make(map[string]int)
							}
							for _, server := range kv.targetConfig.Groups[kv.targetConfig.Shards[shard]] {
								if _, ok := kv.snapshotTracker[kv.targetConfig.Shards[shard]][server]; !ok {
									kv.snapshotTracker[kv.targetConfig.Shards[shard]][server] = 0
								}
							}
							var kset []string
							for k, _ := range kv.stateMachine {
								if key2shard(k) == shard {
									kset = append(kset, k)
								}
							}
							for _, k := range kset {
								kv.historyState[kv.currentConfig.Num][k] = kv.stateMachine[k]
								delete(kv.stateMachine, k)
							}
							if _, ok := kv.historySerial[kv.currentConfig.Num]; !ok {
								serialCopy := map[int64]int64{}
								for k, v := range kv.clientSerial {
									serialCopy[k] = v
								}
								kv.historySerial[kv.currentConfig.Num] = serialCopy
							}
						}
					}
					go kv.updateConfig()
				}
			} else if cmd.Op == GCOp {
				kv.applyIndex = msg.CommandIndex
				if shard2gid, ok := kv.historyNeed[cmd.Config.Num]; ok {
					delete(shard2gid, cmd.Shard)
					if len(shard2gid) == 0 {
						delete(kv.historyNeed, cmd.Config.Num)
					}
				}
				if stateMachine, ok := kv.historyState[cmd.Config.Num]; ok {
					for k := range stateMachine {
						if key2shard(k) == cmd.Shard {
							delete(stateMachine, k)
						}
					}
					if len(stateMachine) == 0 {
						delete(kv.historyState, cmd.Config.Num)
						delete(kv.historySerial, cmd.Config.Num)
					}
				}
			} else {
				log.Fatalf("Unrecognized Op: %v", cmd.Op)
			}
			if kv.maxraftstate != -1 {
				// logs may be temporarily large as we do not truncate the log until
				// configuration change has happened.
				if stateSize := kv.rf.GetStateSize(); stateSize >= kv.maxraftstate &&
					kv.currentConfig.Num == kv.targetConfig.Num {
					kv.saveSnapshot()
				}
			}
		}
		kv.mu.Unlock()
	}
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(Serial{})
	labgob.Register(Group{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.mu.Lock()
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.currentConfig = shardmaster.Config{
		Groups: map[int][]string{},
	}
	kv.historyState = make(map[int]map[string]string)
	kv.historySerial = make(map[int]map[int64]int64)
	kv.historyNeed = make(map[int]map[int]Group)
	kv.snapshotTracker = make(map[int]map[string]int)
	kv.targetConfig = kv.currentConfig
	kv.doneCh = make(map[int]chan Result)
	kv.stateMachine = make(map[string]string)
	kv.clientSerial = make(map[int64]int64)
	kv.blockShard = make(map[int]bool)
	data := persister.ReadSnapshot()
	if len(data) > 0 {
		kv.installSnapshot(data)
	}

	go kv.operateApply()
	go kv.operateConfig()
	if maxraftstate != -1 {
		go kv.operateGC()
	}
	kv.mu.Unlock()

	return kv
}
