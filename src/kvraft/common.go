package raftkv

import "raftsc"

type OpData struct {
	Key   string
	Value string
}

type OpReplyData string

const (
	OP_GET    raftsc.OpType = iota
	OP_PUT    raftsc.OpType = iota
	OP_APPEND raftsc.OpType = iota
)
