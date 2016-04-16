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

import "sync"
import "labrpc"
import "time"
import crand "crypto/rand"
import "math/big"
import "log"
import "os"
import "fmt"

// import "bytes"
// import "encoding/gob"

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
	MIN_TIMEOUT        = time.Millisecond * 200
	MAX_TIMEOUT        = time.Millisecond * 500
	HEARTBEAT_INTERVAL = time.Millisecond * 20
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state RaftState

	currentTerm int
	votedFor    int

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	granted_votes_count int
	timer               *time.Timer

	logger *log.Logger
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
			rf.resetTimer()
		}
	}
}

func (rf *Raft) handleAppendEntriesResult(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Got append entries result: %v\n", reply)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int
	CandidateID int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term    int
	Granted bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.logger.Printf("Got vote request with term = %v, reject\n", args.Term)
		reply.Term = args.Term
		reply.Granted = false
		return
	}
	if args.Term == rf.currentTerm {
		rf.logger.Printf("Got vote request with current term, now voted for %v\n", rf.votedFor)
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateID
		}
		reply.Granted = (rf.votedFor == args.CandidateID)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.logger.Printf("Got vote request with term = %v, follow it\n", args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.resetTimer()

		reply.Granted = true
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
	req_args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me}
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
	Term int

	// TODO
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.logger.Printf("Got append request with term = %v, update and follow and append\n", args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetTimer()

		reply.Term = args.Term
		reply.Success = true
		return
	} else {
		rf.logger.Printf("Got append request with term = %v, drop it\n", args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}

func (rf *Raft) sendAppendEntriesAll() {
	req_args := AppendEntriesArgs{Term: rf.currentTerm}
	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply AppendEntriesReply
			ok := rf.peers[p].Call("Raft.AppendEntries", req_args, &reply)
			if ok {
				rf.handleAppendEntriesResult(reply)
			}
		}(peer)
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
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	rf.logger = log.New(os.Stderr, fmt.Sprintf("[Node %v] ", me), log.LstdFlags)

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
