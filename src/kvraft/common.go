package raftkv

const (
	OK             = "OK"
	ErrTimeout     = "ErrTimeout"
	ErrApplyFailed = "ErrApplyFailed"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClinetID int64
	Seq      int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClinetID int64
	Seq      int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	KeyNotExist bool
	Value       string
}
