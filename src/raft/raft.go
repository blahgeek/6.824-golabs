package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	crand "crypto/rand"
	"deepcopy"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"labrpc"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func GetLoggerWriter() io.Writer {
	if enable_log := os.Getenv("GOLAB_ENABLE_LOG"); enable_log != "" {
		return os.Stderr
	} else {
		return ioutil.Discard
	}
}

func majority(n int) int {
	return n/2 + 1
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type RaftState int

const (
	FOLLOWER  RaftState = iota
	CANDIDATE RaftState = iota
	LEADER    RaftState = iota
)

const (
	MIN_TIMEOUT        = time.Millisecond * 100
	MAX_TIMEOUT        = time.Millisecond * 300
	HEARTBEAT_INTERVAL = time.Millisecond * 50
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state RaftState

	currentTerm int
	votedFor    int
	logs        []interface{}
	logs_term   []int

	snapshotedCount int // if snapshotedCount = n, logs_term[n-1] is also stored

	commitCount  int
	appliedCount int

	nextIndex  []int
	matchCount []int

	applyCh chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	granted_votes_count int
	timer               *time.Timer

	logger *log.Logger
	killed int32
}

func (rf *Raft) DeleteOldLogs(lastIndex int, snapshot []byte) { // called by server
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex -= 1 // yes, all indexes number starts from 1 outside

	if lastIndex+1 < rf.snapshotedCount {
		return
	}

	rf.snapshotedCount = lastIndex + 1

	if rf.snapshotedCount > rf.appliedCount {
		panic("Snapshoted count > applied count")
	}
	rf.persister.SaveSnapshot(snapshot)

	rf.logger.Printf("Deleting old logs up to %v\n", rf.snapshotedCount)
	for i := 0; i < rf.snapshotedCount; i += 1 {
		if i < len(rf.logs) {
			rf.logs[i] = nil
		} else {
			panic("snapshotedCount should not be larger than logs")
		}
	}
	rf.persist()
}

func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Hour) // will be override soon
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	new_timeout := HEARTBEAT_INTERVAL
	if rf.state != LEADER {
		val, _ := crand.Int(crand.Reader, big.NewInt(int64(MAX_TIMEOUT-MIN_TIMEOUT)))
		new_timeout = MIN_TIMEOUT + time.Duration(val.Int64())
	}
	rf.logger.Printf("Resetting timer to %v (I'm %v)\n", new_timeout, rf.state)
	rf.timer.Reset(new_timeout)
}

func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		rf.logger.Printf("Timeout, start a new election, new term = %v\n", rf.currentTerm+1)
		// start new election
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.persist()
		rf.sendRequestVotes()
		rf.granted_votes_count = 1
	} else {
		rf.logger.Printf("Timeout, send heartbeat.\n")
		rf.sendAppendEntriesAll()
	}
	rf.resetTimer()
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Got vote result: %v\n", reply)

	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	// reply.term == rf.currentTerm
	if rf.state == CANDIDATE && reply.Granted {
		rf.granted_votes_count += 1
		if rf.granted_votes_count >= majority(len(rf.peers)) {
			rf.state = LEADER
			for i := 0; i < len(rf.peers); i += 1 {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchCount[i] = 0
			}
			rf.resetTimer()
		}
	}
}

func (rf *Raft) handleAppendEntriesResult(reply AppendEntriesReply, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Got append entries result: %v\n", reply)

	if rf.state != LEADER {
		rf.logger.Printf("I'm not leader now... return\n")
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[peer] = reply.IndexHint
		if rf.nextIndex[peer] > len(rf.logs) {
			rf.nextIndex[peer] = 0
			panic("nextIndex should not be more than leader's logs")
		}
		rf.matchCount[peer] = rf.nextIndex[peer]
		this_match_count_peers := 1
		for i := 0; i < len(rf.peers); i += 1 {
			if i == rf.me {
				continue
			}
			if rf.matchCount[i] >= rf.matchCount[peer] {
				this_match_count_peers += 1
			}
		}
		if this_match_count_peers >= majority(len(rf.peers)) &&
			rf.commitCount < rf.matchCount[peer] &&
			rf.logs_term[rf.matchCount[peer]-1] == rf.currentTerm {
			rf.logger.Printf("Update commit count to %v\n", rf.matchCount[peer])
			rf.commitCount = rf.matchCount[peer]
			go rf.doCommitLogs()
		}
	} else {
		rf.nextIndex[peer] = reply.IndexHint
		if rf.nextIndex[peer] < 0 {
			panic("nextIndex should not go negative")
		}
		rf.sendAppendEntriesAll()
	}

}

func (rf *Raft) doCommitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	real_commit_count := rf.commitCount
	if real_commit_count > len(rf.logs) {
		real_commit_count = len(rf.logs)
	}

	if rf.appliedCount < rf.snapshotedCount {
		rf.logger.Printf("Applying snapshot up to %v\n", rf.snapshotedCount)
		rf.appliedCount = rf.snapshotedCount
		snapshot := rf.persister.ReadSnapshot()
		rf.applyCh <- ApplyMsg{Index: rf.snapshotedCount - 1 + 1, // attension
			UseSnapshot: true, Snapshot: snapshot}
	}

	for i := rf.appliedCount; i < real_commit_count; i += 1 {
		rf.logger.Printf("Applying cmd %v\n", i)
		// in the paper, index starts from 1
		rf.appliedCount += 1
		if rf.logs[i] == nil {
			panic("Logs after snapshotedCount should not be nil")
		}
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: deepcopy.Iface(rf.logs[i])}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// this is important
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.snapshotedCount)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)

	enc.Encode(rf.logs[rf.snapshotedCount:])
	enc.Encode(rf.logs_term[rf.snapshotedCount:])
	if rf.snapshotedCount > 0 {
		enc.Encode(rf.logs_term[rf.snapshotedCount-1])
	}

	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	if data == nil {
		return
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	dec.Decode(&rf.snapshotedCount)
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)

	var logs_tmp []interface{}
	var logs_term_tmp []int
	dec.Decode(&logs_tmp)
	dec.Decode(&logs_term_tmp)
	rf.logs = make([]interface{}, len(logs_tmp)+rf.snapshotedCount)
	rf.logs_term = make([]int, len(logs_term_tmp)+rf.snapshotedCount)
	copy(rf.logs[rf.snapshotedCount:], logs_tmp)
	copy(rf.logs_term[rf.snapshotedCount:], logs_term_tmp)

	if rf.snapshotedCount > 0 {
		dec.Decode(&rf.logs_term[rf.snapshotedCount-1])
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int
	CandidateID int
	LogCount    int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term    int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true
	if len(rf.logs) > 0 {
		// rf.logs_term[len(rf.logs)-1] will always there, no matter snapshotedCount
		if rf.logs_term[len(rf.logs)-1] > args.LastLogTerm ||
			(rf.logs_term[len(rf.logs)-1] == args.LastLogTerm && len(rf.logs) > args.LogCount) {
			may_grant_vote = false
		}
	}
	rf.logger.Printf("Got vote request: %v, may grant vote: %v\n", args, may_grant_vote)

	if args.Term < rf.currentTerm {
		rf.logger.Printf("Got vote request with term = %v, reject\n", args.Term)
		reply.Term = rf.currentTerm
		reply.Granted = false
		return
	}

	if args.Term == rf.currentTerm {
		rf.logger.Printf("Got vote request with current term, now voted for %v\n", rf.votedFor)
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateID
			rf.persist()
		}
		reply.Granted = (rf.votedFor == args.CandidateID)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.logger.Printf("Got vote request with term = %v, follow it\n", args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if may_grant_vote {
			rf.votedFor = args.CandidateID
			rf.persist()
		}
		rf.resetTimer()

		reply.Granted = (rf.votedFor == args.CandidateID)
		reply.Term = args.Term
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).

// returns true if labrpc says the RPC was delivered.

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }
func (rf *Raft) sendRequestVotes() {
	req_args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		LogCount:    len(rf.logs),
	}
	if req_args.LogCount > 0 {
		req_args.LastLogTerm = rf.logs_term[len(rf.logs)-1]
	}

	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			ok := rf.peers[p].Call("Raft.RequestVote", req_args, &reply)
			if ok {
				rf.handleVoteResult(reply)
			}
		}(peer)
	}
}

// AppendEntries

type AppendEntriesArgs struct {
	Term         int
	Snapshot     []byte // if present, it's snapshot up to PrevLogCount
	PrevLogCount int
	PrevLogTerm  int
	Entries      []interface{}
	EntryTerms   []int
	// Entry             interface{}
	// EntryTerm         int
	Leader            int
	LeaderCommitCount int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	IndexHint int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.logger.Printf("Got append request with term = %v, drop it\n", args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.logger.Printf("Got append request with term = %v, update and follow and append\n", args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.Leader

		reply.Term = args.Term

		if len(rf.logs) < args.PrevLogCount && args.Snapshot != nil {
			rf.logger.Printf("Installing snapshot up to %v\n", args.PrevLogCount)
			rf.persister.SaveSnapshot(args.Snapshot)
			rf.logs = make([]interface{}, args.PrevLogCount)
			rf.logs_term = make([]int, args.PrevLogCount)
			rf.logs_term[args.PrevLogCount-1] = args.PrevLogTerm
			rf.snapshotedCount = args.PrevLogCount
		}

		if args.PrevLogCount < rf.snapshotedCount {
			rf.logger.Printf("Log already snapshoted, ignore\n")
			reply.IndexHint = rf.snapshotedCount
			reply.Success = true
		} else if args.PrevLogCount > 0 && (len(rf.logs) < args.PrevLogCount || rf.logs_term[args.PrevLogCount-1] != args.PrevLogTerm) {
			rf.logger.Printf("But not match: %v\n", args)
			reply.IndexHint = len(rf.logs)
			if reply.IndexHint >= args.PrevLogCount {
				reply.IndexHint = args.PrevLogCount
			}
			for reply.IndexHint > rf.snapshotedCount {
				if rf.logs_term[reply.IndexHint-1] == args.PrevLogTerm {
					break
				}
				reply.IndexHint--
			}
			reply.Success = false
		} else if args.Entries != nil {
			rf.logger.Printf("Appending log from %v (count %v) (I've got %v already)\n", args.PrevLogCount, len(args.Entries), len(rf.logs))
			for _, e := range args.Entries {
				if e == nil {
					panic("Entries in AppendEntries cannot be nil")
				}
			}

			for idx := args.PrevLogCount; idx < args.PrevLogCount+len(args.Entries); idx += 1 {
				if idx < len(rf.logs) && rf.logs_term[idx] != args.EntryTerms[idx-args.PrevLogCount] {
					rf.logs = rf.logs[:idx]
					rf.logs_term = rf.logs_term[:idx]
				}
				if idx >= len(rf.logs) {
					if idx > len(rf.logs) {
						panic("WTF")
					}
					rf.logs = append(rf.logs, args.Entries[idx-args.PrevLogCount])
					rf.logs_term = append(rf.logs_term, args.EntryTerms[idx-args.PrevLogCount])
					rf.logger.Printf("Replacing %v\n", idx)
				}
			}

			if len(rf.logs) >= args.LeaderCommitCount && rf.commitCount < args.LeaderCommitCount {
				rf.commitCount = args.LeaderCommitCount
				go rf.doCommitLogs()
			}
			reply.IndexHint = args.PrevLogCount + len(args.Entries)
			reply.Success = true
		} else {
			rf.logger.Printf("It's a heartbeat...\n")
			if len(rf.logs) >= args.LeaderCommitCount {
				rf.commitCount = args.LeaderCommitCount
				go rf.doCommitLogs()
			}
			reply.IndexHint = args.PrevLogCount
			reply.Success = true
		}

		rf.persist()
		rf.resetTimer()
	}
}

func (rf *Raft) sendAppendEntriesAll() {
	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}

		var args AppendEntriesArgs
		args.Leader = rf.me
		args.Term = rf.currentTerm
		args.PrevLogCount = rf.nextIndex[peer]
		if args.PrevLogCount < rf.snapshotedCount {
			args.PrevLogCount = rf.snapshotedCount
			args.Snapshot = rf.persister.ReadSnapshot()
		}

		if args.PrevLogCount > 0 {
			args.PrevLogTerm = rf.logs_term[args.PrevLogCount-1]
		}
		if args.PrevLogCount < len(rf.logs) {
			args.Entries = make([]interface{}, len(rf.logs)-args.PrevLogCount)
			args.EntryTerms = make([]int, len(rf.logs)-args.PrevLogCount)
			copy(args.Entries, rf.logs[args.PrevLogCount:])
			copy(args.EntryTerms, rf.logs_term[args.PrevLogCount:])
		}
		args.LeaderCommitCount = rf.commitCount

		go func(p int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.peers[p].Call("Raft.AppendEntries", args, &reply)
			if ok {
				rf.handleAppendEntriesResult(reply, p)
			}
		}(peer, args)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	index := len(rf.logs)
	rf.logs = append(rf.logs, command)
	rf.logs_term = append(rf.logs_term, rf.currentTerm)
	rf.persist()

	rf.logger.Printf("New command, append at %v with term = %v\n", index, rf.currentTerm)

	// in the paper, index starts from 1
	return index + 1, rf.currentTerm, rf.state == LEADER
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	if !atomic.CompareAndSwapInt32(&rf.killed, 0, 1) {
		return
	}

	// this is a dirty (but smart) hack...
	// which prevent future functions running...
	// while also wait for all currently running functions to complete
	rf.mu.Lock()

	// free up memory
	rf.logs = nil
	rf.logs_term = nil
}

func (rf *Raft) SetLoggerPrefix(name string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger = log.New(GetLoggerWriter(), fmt.Sprintf("[%v-Raft%v] ", name, rf.me), log.LstdFlags)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.logger = log.New(GetLoggerWriter(), fmt.Sprintf("[Node %v] ", me), log.LstdFlags)

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]interface{}, 0)
	rf.logs_term = make([]int, 0)
	rf.commitCount = 0
	rf.appliedCount = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchCount = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist()
	rf.resetTimer()

	return rf
}
