package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOldRequest  = "ErrOldRequest"
)

type Err string

type OpType string

const (
	PutOp OpType = "Put"
	AppendOp = "Append"
	GetOp = "Get"
)

type Command struct {
	Op   OpType // "Put" or "Append" or "Get".
	Key   string
	Value string
	SerialNumber int // used to prevent duplicate requests.
	ClientId     int // used to prevent duplicate requests.
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    OpType // "Put" or "Append".
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNumber int // used to prevent duplicate requests.
	ClientId int // id of the client.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SerialNumber int // used to prevent duplicate requests.
	ClientId int // id of the client.
}

type GetReply struct {
	Err   Err
	Value string
}
