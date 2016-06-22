package shardmaster

//
// Shardmaster clerk.
//

import "raftsc"
import "labrpc"

type Clerk struct {
	*raftsc.RaftClient
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	c := raftsc.MakeClient(servers, "ShardMaster")
	return &Clerk{c}
}

func (ck *Clerk) Query(num int) Config {
	ret := ck.Exec(OP_QUERY, OpData{ConfigNum: num})
	return ret.(Config)
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Exec(OP_JOIN, OpData{Servers: servers})
}

func (ck *Clerk) Leave(gids []int) {
	op_data := OpData{Servers: make(map[int][]string)}
	for _, gid := range gids {
		op_data.Servers[gid] = nil
	}
	ck.Exec(OP_LEAVE, op_data)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Exec(OP_MOVE, OpData{Shard: shard, GID: gid})
}
