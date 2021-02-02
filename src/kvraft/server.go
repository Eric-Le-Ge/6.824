package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const PollInterval = 20 * time.Millisecond
const PollingTimeOut = 480 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	 Op    string // "Put" or "Append" or "Get".
	 Key   string
	 Value string
	 SerialNumber int64 // used to prevent duplicate requests.
	 ClientId     int64 // used to prevent duplicate requests.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine map[string]string // state machine - an in memory map
	applyIndex int // highest index that has been applied to the state machine
	clientSerial map[int64]int64 // highest processed serial number of client
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// for get, if it is an old request, there is no need to apply anything to
	// the log. Read the value immediately
	if args.SerialNumber <= kv.clientSerial[args.ClientId] {
		DPrintf("Handled duplicate Read from %v, (serial number %v)", args.ClientId, args.SerialNumber)
		if val, ok := kv.stateMachine[args.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		return
	}
	index, _, _ := kv.rf.Start(Op{
		Op:    GetOp,
		Key:   args.Key,
		Value: "",
		SerialNumber: args.SerialNumber,
		ClientId: args.ClientId,
	})
	DPrintf("Started get op, index %v, term %v", index, term)
	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Unlock()
		time.Sleep(PollInterval)
		kv.mu.Lock()
		curTerm, isLeader := kv.rf.GetState()
		// if leadership information changes or call times out, abort call
		if curTerm != term || !isLeader || time.Since(startTime) > PollingTimeOut {
			reply.Err = ErrWrongLeader
			return
		}
		// get is applied. Read value and report success
		if kv.applyIndex >= index {
			reply.Err = OK
			DPrintf("Handled Read from %v, (serial number %v)", args.ClientId, args.SerialNumber)
			if val, ok := kv.stateMachine[args.Key]; ok {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
			return
		}
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check the serial number to prevent reapplying a same request.
	// this is a loose check mainly for performance. request with the same
	// serial number may still be written two times to the log, however,
	// correctness will not be affected as the operation with the same serial
	// number won't be applied twice to the state machine.
	if args.SerialNumber <= kv.clientSerial[args.ClientId] {
		reply.Err = ErrOldRequest
		return
	}
	index, term, isLeader := kv.rf.Start(Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
		SerialNumber: args.SerialNumber,
		ClientId: args.ClientId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Started putAppend op, index %v, term %v", index, term)
	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Unlock()
		time.Sleep(PollInterval)
		DPrintf("putappend waiting on term %v, %v", term, kv.me)
		kv.mu.Lock()
		curTerm, isLeader := kv.rf.GetState()
		// if leadership information changes or call times out, abort call
		if curTerm != term || !isLeader || time.Since(startTime) > PollingTimeOut{
			reply.Err = ErrWrongLeader
			return
		}
		// putAppend is applied. Report success
		if kv.applyIndex >= index {
			reply.Err = OK
			return
		}
	}
	reply.Err = ErrWrongLeader
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
// a long running go routine that applies messages from ApplyChannel
//
func (kv *KVServer) operateApply() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if !msg.CommandValid {
			r := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(r)
			var stateMachine map[string]string
			var clientSerial map[int64]int64
			if d.Decode(&stateMachine) != nil ||
			   d.Decode(&clientSerial) != nil {
				log.Fatal("snapshot decode error")
			} else {
				kv.stateMachine = stateMachine
				kv.clientSerial = clientSerial
				kv.applyIndex = msg.CommandIndex
			}
		} else {
			cmd := msg.Command.(Op)
			if msg.CommandValid && cmd.SerialNumber > kv.clientSerial[cmd.ClientId] {
				kv.clientSerial[cmd.ClientId] = cmd.SerialNumber
				kv.applyIndex = msg.CommandIndex
				DPrintf("Applied %s from %v (serial %v) at index %v", cmd.Op, cmd.ClientId, cmd.SerialNumber, msg.CommandIndex)
				switch cmd.Op {
				case GetOp:
					break
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
			if kv.maxraftstate != -1 {
				if stateSize := kv.rf.GetStateSize(); stateSize >= kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.stateMachine)
					e.Encode(kv.clientSerial)
					data := w.Bytes()
					kv.rf.TruncateLog(data, kv.applyIndex)
				}
			}
		}
		kv.mu.Unlock()
		if kv.killed() {return}
	}
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
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.stateMachine = make(map[string]string)
	kv.clientSerial = make(map[int64]int64)

	// You may need initialization code here.
	go kv.operateApply()
	kv.mu.Unlock()

	return kv
}
