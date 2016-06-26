package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "raftsc"
import "labrpc"
import "sync/atomic"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd

	shard_client_id int64
	shard_op_id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) Exec(typ raftsc.OpType, data OpData) string {
	atomic.AddInt64(&ck.shard_op_id, 1)
	shard := key2shard(data.Key)
	for {
		if ck.config.Num > 0 {
			var servers []*labrpc.ClientEnd
			for _, server_name := range ck.config.Groups[ck.config.Shards[shard]] {
				servers = append(servers, ck.make_end(server_name))
			}
			client := raftsc.MakeClient(servers, "ShardKV")
			client.SetClientID(ck.shard_client_id) // prevent too big snapshot (too many clien IDs)
			data.ShardOpClient = ck.shard_client_id
			data.ShardOpId = ck.shard_op_id

			result := client.Exec(typ, data)
			op_result := result.(OpReplyData)
			if !op_result.IsWrongGroup {
				return op_result.Value
			}
			time.Sleep(10 * time.Millisecond)
		}
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Exec(OP_GET, OpData{Key: key})
}

func (ck *Clerk) Put(key, value string) {
	ck.Exec(OP_PUT, OpData{Key: key, Value: value})
}

func (ck *Clerk) Append(key, value string) {
	ck.Exec(OP_APPEND, OpData{Key: key, Value: value})
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		sm:              shardmaster.MakeClerk(masters),
		make_end:        make_end,
		shard_client_id: nrand(),
	}
}
