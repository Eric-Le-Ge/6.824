package kvraft

import (
	"../labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var idAlloc int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int
	serialAlloc int64
	cachedLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = int(atomic.AddInt64(&idAlloc, 1))
	ck.cachedLeader = int(nrand()) % len(ck.servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	sid := int(atomic.AddInt64(&ck.serialAlloc, 1))
	server := ck.cachedLeader
	for {
		args := GetArgs{
			Key:          key,
			SerialNumber: sid,
			ClientId:     ck.id,
		}
		reply := GetReply{Err: OK}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.cachedLeader = server
			return reply.Value
		}
		// TODO: implement redirection
		ck.cachedLeader = int(nrand()) % len(ck.servers)
		server = ck.cachedLeader
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	sid := int(atomic.AddInt64(&ck.serialAlloc, 1))
	server := ck.cachedLeader
	for {
		args := PutAppendArgs{
			Key:          key,
			Value:        value,
			Op:           op,
			SerialNumber: sid,
			ClientId:     ck.id,
		}
		reply := PutAppendReply{Err: OK}
		DPrintf("client called with args %v", args)
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrOldRequest) {
			ck.cachedLeader = server
			return
		}
		ck.cachedLeader = int(nrand()) % len(ck.servers)
		server = ck.cachedLeader
		time.Sleep(PollInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, GetOp)
}
