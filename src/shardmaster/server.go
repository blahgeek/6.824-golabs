package shardmaster

import "raft"
import "labrpc"
import "encoding/gob"
import "log"
import "fmt"
import "sort"
import "raftsc"
import "deepcopy"

type ShardMasterImpl struct {
	configs []Config
	logger  *log.Logger
}

func (sm *ShardMasterImpl) rebalance(config *Config) {
	sm.logger.Printf("Re-balance: %v\n", config)

	var unallocated_shards []int
	for s, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			unallocated_shards = append(unallocated_shards, s)
		}
	}

	var groups []int
	group_to_shards := map[int][]int{}
	for grp, _ := range config.Groups {
		groups = append(groups, grp)
		group_to_shards[grp] = nil
	}
	for shard, grp := range config.Shards {
		if _, ok := config.Groups[grp]; ok {
			group_to_shards[grp] = append(group_to_shards[grp], shard)
		}
	}
	sort.Ints(groups)

	target_shards_nums := make([]int, len(groups))
	for idx := 0; idx < len(groups); idx += 1 {
		target_shards_nums[idx] = len(config.Shards) / len(config.Groups)
		if idx < len(config.Shards)%len(config.Groups) {
			target_shards_nums[idx] += 1
		}
	}

	for idx, grp := range groups {
		shards := group_to_shards[grp]
		if len(shards) > target_shards_nums[idx] {
			unallocated_shards = append(unallocated_shards, shards[target_shards_nums[idx]:]...)
		}
	}

	for idx, grp := range groups {
		for i := 0; i < target_shards_nums[idx]-len(group_to_shards[grp]); i += 1 {
			config.Shards[unallocated_shards[0]] = grp
			unallocated_shards = unallocated_shards[1:]
		}
	}

	if len(unallocated_shards) != 0 {
		panic("WTF")
	}
}

func (sm *ShardMasterImpl) ApplyOp(typ raftsc.OpType, data interface{}, dup bool) interface{} {
	op_data := data.(OpData)
	if typ == OP_QUERY {
		sm.logger.Printf("Query %v...\n", op_data.ConfigNum)
		if op_data.ConfigNum < 0 || op_data.ConfigNum >= len(sm.configs) {
			op_data.ConfigNum = len(sm.configs) - 1
		}
		return sm.configs[op_data.ConfigNum]
	}

	if !dup {
		// make a new config
		config := deepcopy.Iface(sm.configs[len(sm.configs)-1]).(Config)
		config.Num += 1

		switch typ {
		case OP_JOIN:
			sm.logger.Printf("Joining %v...\n", op_data.Servers)
			for k, v := range op_data.Servers {
				config.Groups[k] = v
			}
			sm.rebalance(&config)
		case OP_LEAVE:
			sm.logger.Printf("Leaving %v...\n", op_data.Servers)
			for k, _ := range op_data.Servers {
				delete(config.Groups, k)
			}
			sm.rebalance(&config)
		case OP_MOVE:
			sm.logger.Printf("Leaving %v -> %v...\n", op_data.Shard, op_data.GID)
			config.Shards[op_data.Shard] = op_data.GID
		default:
		}
		sm.configs = append(sm.configs, config)
	}
	return nil
}

func (sm *ShardMasterImpl) EncodeSnapshot(enc *gob.Encoder) {
	enc.Encode(sm.configs)
}

func (sm *ShardMasterImpl) DecodeSnapshot(dec *gob.Decoder) {
	sm.configs = nil
	dec.Decode(&sm.configs)
}

func (sm *ShardMasterImpl) Free() {
	sm.configs = nil
}

type ShardMaster struct {
	*raftsc.RaftServer
	rf *raft.Raft
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	gob.Register(OpData{})
	gob.Register(Config{})

	sm := &ShardMasterImpl{
		configs: make([]Config, 1),
		logger:  log.New(raft.GetLoggerWriter(), fmt.Sprintf("[ShardMaster%v] ", me), log.LstdFlags),
	}
	sm.configs[0].Num = 0
	sm.configs[0].Groups = make(map[int][]string)
	for i := 0; i < len(sm.configs[0].Shards); i += 1 {
		sm.configs[0].Shards[i] = 0
	}

	server := raftsc.StartServer(fmt.Sprintf("ShardMaster%v", me), sm, servers, me, persister, -1)
	return &ShardMaster{server, server.Raft()}
}
