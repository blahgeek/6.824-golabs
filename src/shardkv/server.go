package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "raftsc"
import "sync"
import "log"
import "os"
import "fmt"
import "time"
import "deepcopy"
import "encoding/gob"

type PushingShard struct {
	ConfigNum int // for which config
	Shard     map[string]string
	Group     int
	Servers   []string
	LastOp    map[int64]int64
}

type ShardKVImpl struct {
	mu     sync.Mutex
	logger *log.Logger

	make_end func(string) *labrpc.ClientEnd
	gid      int

	config               shardmaster.Config        // the latest config, which I am using
	shards               map[int]map[string]string // data
	shards_latest_config map[int]int               // de-dup PULL op
	pushing_shards       map[int]PushingShard

	shards_client_last_op map[int]map[int64]int64 // client's last op id for each shard
}

func (kv *ShardKVImpl) pushShardSingle(shard int, ps PushingShard) (ok bool) {
	kv.logger.Printf("Pushing shard %v to group %v, config=%v\n", shard, ps.Group, ps.ConfigNum)
	var servers []*labrpc.ClientEnd
	for _, server_name := range ps.Servers {
		servers = append(servers, kv.make_end(server_name))
	}
	client := raftsc.MakeClient(servers, "ShardKV")
	client.SetClientID(-1) // do not check PULL for dup
	push_ok, push_ret := client.DoExec(OP_PULL, OpData{
		ConfigNum:         ps.ConfigNum,
		ShardNum:          shard,
		Shard:             ps.Shard,
		ShardClientLastOp: ps.LastOp,
	}, false) // do not retry
	if push_ok && push_ret.(OpReplyData).IsWrongGroup == false {
		kv.logger.Printf("Push shard %v to group %v done\n", shard, ps.Group)
		ok = true
		// delete(kv.pushing_shards, shard)
	} else {
		kv.logger.Printf("Push shard %v to group %v failed (network ok=%v)\n", shard, ps.Group, push_ok)
		ok = false
	}
	return
}

func (kv *ShardKVImpl) pushShards() (updated bool) {
	updated = false
	for shard, _ := range kv.config.Shards {
		kv.mu.Lock()
		ps, exist := kv.pushing_shards[shard]
		if !exist {
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		ok := kv.pushShardSingle(shard, ps)

		kv.mu.Lock()
		if ok && kv.pushing_shards[shard].ConfigNum == ps.ConfigNum {
			updated = true
			delete(kv.pushing_shards, shard)
		}
		kv.mu.Unlock()
	}
	return
}

func (kv *ShardKVImpl) preparePush() {
	if kv.config.Num == 1 {
		// initial configuration
		for shard, grp := range kv.config.Shards {
			if grp == kv.gid && kv.shards_latest_config[shard] < 1 {
				kv.logger.Printf("Initializing: add shard %v\n", shard)
				kv.shards[shard] = map[string]string{}
				kv.shards_latest_config[shard] = 1
			}
		}
	}
	// kv.pushing_shards = map[int]PushingShard{}
	for shard, grp := range kv.config.Shards {
		kv.logger.Printf("TEST shard %v, %v vs %v, nil? %v\n", shard, grp, kv.gid, kv.shards[shard] == nil)
		if grp != kv.gid && kv.shards[shard] != nil {
			kv.pushing_shards[shard] = PushingShard{
				ConfigNum: kv.config.Num,
				Shard:     kv.shards[shard],
				LastOp:    kv.shards_client_last_op[shard],
				Group:     grp,
				Servers:   kv.config.Groups[grp],
			}
			delete(kv.shards, shard)
			delete(kv.shards_client_last_op, shard)
		}
	}
}

func (kv *ShardKVImpl) ApplyOp(typ raftsc.OpType, data interface{}, dup bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op_data := data.(OpData)
	var reply_data OpReplyData

	if typ == OP_PULL {
		kv.logger.Printf("Applying PULL: shard=%v, config=%v (shard's latest=%v)\n", op_data.ShardNum, op_data.ConfigNum, kv.shards_latest_config[op_data.ShardNum])
		if op_data.ConfigNum > kv.config.Num {
			kv.logger.Printf("My config is not ready (%v vs %v)\n", op_data.ConfigNum, kv.config.Num)
			reply_data.IsWrongGroup = true
			return reply_data
		}
		if op_data.ConfigNum > kv.shards_latest_config[op_data.ShardNum] {
			if kv.shards[op_data.ShardNum] != nil {
				panic("WTF, shard while receiving PULL should be nil")
			}
			if op_data.Shard == nil {
				panic("WTF, shard in data should not be nil")
			}
			kv.shards[op_data.ShardNum] = deepcopy.Iface(op_data.Shard).(map[string]string)
			kv.shards_client_last_op[op_data.ShardNum] = deepcopy.Iface(op_data.ShardClientLastOp).(map[int64]int64)
			kv.shards_latest_config[op_data.ShardNum] = op_data.ConfigNum
			kv.preparePush()
		}
		return reply_data // unused
	}

	if typ == OP_NEWCONFIG {
		if !dup && op_data.Config.Num > kv.config.Num {
			kv.logger.Printf("Applying new config: %v\n", op_data.Config.Num)
			kv.config = deepcopy.Iface(op_data.Config).(shardmaster.Config)
			kv.preparePush()
		}
		return nil
	}

	target_shard := key2shard(op_data.Key)
	p_shard := kv.shards[target_shard]
	kv.logger.Printf("Applying OP %v (shard %v)[%v], id=%v-%v\n", typ, target_shard, kv.config.Shards[target_shard], op_data.ShardOpClient, op_data.ShardOpId)
	if kv.config.Num == 0 || kv.config.Shards[target_shard] != kv.gid || p_shard == nil {
		kv.logger.Printf("Wrong group")
		reply_data.IsWrongGroup = true
		return reply_data
	}

	if kv.shards_client_last_op[target_shard] == nil {
		kv.shards_client_last_op[target_shard] = make(map[int64]int64)
	}
	shard_dup := kv.shards_client_last_op[target_shard][op_data.ShardOpClient] >= op_data.ShardOpId
	if !shard_dup {
		kv.shards_client_last_op[target_shard][op_data.ShardOpClient] = op_data.ShardOpId
	}

	if !shard_dup { // do not check `dup`
		if typ == OP_PUT {
			p_shard[op_data.Key] = op_data.Value
		} else if typ == OP_APPEND {
			p_shard[op_data.Key] = p_shard[op_data.Key] + op_data.Value
		}
	}
	// kv.logger.Println(kv.shards)

	reply_data.Value = p_shard[op_data.Key]
	return reply_data
}

func (kv *ShardKVImpl) EncodeSnapshot(enc *gob.Encoder) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	enc.Encode(kv.shards)
	enc.Encode(kv.shards_latest_config)
	enc.Encode(kv.pushing_shards)
	enc.Encode(kv.shards_client_last_op)
	enc.Encode(kv.config)
}

func (kv *ShardKVImpl) DecodeSnapshot(dec *gob.Decoder) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// THIS IS IMPORTANT
	// Decode MERGE maps
	kv.shards = nil
	kv.shards_latest_config = nil
	kv.pushing_shards = nil
	kv.shards_client_last_op = nil
	dec.Decode(&kv.shards)
	dec.Decode(&kv.shards_latest_config)
	dec.Decode(&kv.pushing_shards)
	dec.Decode(&kv.shards_client_last_op)
	dec.Decode(&kv.config)
}

func (kv *ShardKVImpl) Free() {
	kv.mu.Lock()
	kv.shards = nil
	kv.pushing_shards = nil
	kv.shards_latest_config = nil
}

type ShardKV struct {
	*raftsc.RaftServer
	rf *raft.Raft
}

func StartServer(servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {

	gob.Register(OpData{})
	gob.Register(OpReplyData{})

	kv := &ShardKVImpl{
		logger:                log.New(os.Stderr, fmt.Sprintf("[ShardKV G%v(%v)]", gid, me), log.LstdFlags),
		make_end:              make_end,
		gid:                   gid,
		shards:                map[int]map[string]string{},
		shards_latest_config:  map[int]int{},
		shards_client_last_op: map[int]map[int64]int64{},
		pushing_shards:        map[int]PushingShard{},
	}
	server := raftsc.StartServer(kv, servers, me, persister, maxraftstate)

	// watch for configs
	if me == 0 {
		go func() {
			var last_config shardmaster.Config
			master_clerk := shardmaster.MakeClerk(masters)
			server_clerk := raftsc.MakeClient(servers, "ShardKV")
			for {
				config := master_clerk.Query(last_config.Num + 1)
				if config.Num != last_config.Num {
					server_clerk.Exec(OP_NEWCONFIG, OpData{Config: config})
					time.Sleep(50 * time.Millisecond)
					last_config = config
				}
			}
		}()
	}

	go func() {
		for {
			updated := kv.pushShards()
			if updated {
				server.SnapshotAndClean()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return &ShardKV{server, server.Raft()}
}
