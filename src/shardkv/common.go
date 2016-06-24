package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

import "raftsc"
import "shardmaster"

const (
	OP_PUT       raftsc.OpType = iota
	OP_APPEND    raftsc.OpType = iota
	OP_GET       raftsc.OpType = iota
	OP_PULL      raftsc.OpType = iota
	OP_NEWCONFIG raftsc.OpType = iota
)

type OpData struct {
	Key   string
	Value string
	// used by OP_PULL
	ConfigNum int
	ShardNum  int
	Shard     map[string]string
	// used by OP_NEWCONFIG
	Config shardmaster.Config
}

type OpReplyData struct {
	IsWrongGroup bool
	Value        string
}
