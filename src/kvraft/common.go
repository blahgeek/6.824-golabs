package raftkv

type StatusType int

const (
	STATUS_OK           StatusType = iota
	STATUS_WRONG_LEADER StatusType = iota
)

type OpType int

const (
	OP_GET    OpType = iota
	OP_PUT    OpType = iota
	OP_APPEND OpType = iota
)

type Op struct {
	Type  OpType
	Key   string
	Value string // empty for GET

	Client int64 // which client sends this OP?
	Id     int64 // unique for this client, monotone increase
}

type OpReply struct {
	Status StatusType
	Value  string
}
