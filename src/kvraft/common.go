package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOldRequest  = "ErrOldRequest"
)

type Err string

const (
	PutOp = "Put"
	AppendOp = "Append"
	GetOp = "Get"
)


// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append".
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNumber int64 // used to prevent duplicate requests.
	ClientId int64 // id of the client.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SerialNumber int64 // used to prevent duplicate requests.
	ClientId int64 // id of the client.
}

type GetReply struct {
	Err   Err
	Value string
}
