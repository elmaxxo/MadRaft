use futures::{channel::mpsc, stream::FuturesUnordered, Future, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task,
    time::{self, *},
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Pointer},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,

    // Last term this peer voted for. All vote requests with term <= this value are rejected.
    term_voted: u64,

    // Time at which election timeout starts counting.
    election_timeout_reset: Instant,
}

/// State of a raft peer.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct State {
    term: u64,
    role: Role,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl State {
    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
    fn set_role(&mut self, role: Role) {
        self.role = role;
    }
    fn set_term(&mut self, term: u64) {
        assert!(self.term <= term);
        self.term = term;
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    // Your data here.
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft({})", self.me)
    }
}

/// Send vote request to the peer.
async fn send_vote_request(peer: SocketAddr, req: RequestVoteArgs) -> io::Result<RequestVoteReply> {
    let net = net::NetLocalHandle::current();
    let timeout = time::Duration::from_secs(1);
    net.call_timeout(peer, req, timeout).await
}

/// Create a set of futures that broadcast vote request to all peers.
fn broadcast_vote_request(
    peers: &[SocketAddr],
    req: &RequestVoteArgs,
) -> FuturesUnordered<impl Future<Output = io::Result<RequestVoteReply>>> {
    let rpcs = FuturesUnordered::new();
    peers
        .iter()
        .enumerate()
        .filter(|(peer_idx, _)| *peer_idx != req.candidate)
        .for_each(|(_, peer)| {
            let (peer, req) = (peer.clone(), req.clone());
            info!("sending vote request to {peer:?}");
            rpcs.push(async move { send_vote_request(peer, req).await });
        });
    rpcs
}

/// Send append entries to the peer.
async fn send_append_entries(
    peer: SocketAddr,
    req: AppendEntriesArgs,
) -> io::Result<AppendEntriesReply> {
    let net = net::NetLocalHandle::current();
    let timeout = time::Duration::from_secs(1);
    info!("sending append entries to {peer:?}");
    net.call_timeout(peer, req, timeout).await
}

/// Create a set of futures that broadcast append entries to all peers.
fn broadcast_append_entries(
    peers: &[SocketAddr],
    req: &AppendEntriesArgs,
    me: usize,
) -> FuturesUnordered<impl Future<Output = io::Result<AppendEntriesReply>>> {
    let rpcs = FuturesUnordered::new();
    peers
        .iter()
        .enumerate()
        .filter(|(peer_idx, _)| *peer_idx != me)
        .for_each(|(_, peer)| {
            let (peer, req) = (peer.clone(), req.clone());
            rpcs.push(async move { send_append_entries(peer, req).await });
        });
    rpcs
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            state: State::default(),
            term_voted: 0,
            election_timeout_reset: Instant::now(),
        }));
        let mut handle = RaftHandle { inner };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();
        handle.transfer_state(0, Role::Follower);

        (handle, recver)
    }

    /// Transfer raft state.
    fn transfer_state(&mut self, term: u64, role: Role) {
        let mut raft = self.raft();
        raft.set_term(term);
        raft.set_role(role);
        drop(raft);

        info!("transfering state to ({role:?}, {term})");
        match role {
            Role::Leader => self.spawn_heartbeat_task(term),
            Role::Candidate => self.spawn_election_task(term),
            Role::Follower => self.spawn_election_timeout_task(term),
        }
    }

    // Spawn a task broadcasting heartbeats to all peers by timeout.
    // This task can only be performed by a peer who considers himself as a leader.
    fn spawn_heartbeat_task(&mut self, term: u64) {
        let mut handle = self.clone();
        task::spawn(async move {
            info!("running heartbeat task");
            let state = handle.raft().state();
            loop {
                let mut rpcs = {
                    let raft = handle.raft();
                    let req = AppendEntriesArgs { term };
                    broadcast_append_entries(&raft.peers, &req, raft.me)
                };

                info!("broadcasting heartbeats");
                while let Some(Ok(res)) = rpcs.next().await {
                    let raft = handle.raft();
                    if raft.state_changed(&state) {
                        return;
                    }

                    // We've just discovered there is a leader or a candidate with a higher
                    // term, transfer state to follower. (3.3)
                    if res.term > term {
                        drop(raft);
                        return handle.transfer_state(res.term, Role::Follower);
                    }
                }

                time::sleep(Raft::generate_heartbeat_timeout()).await;
                if handle.raft().state_changed(&state) {
                    return;
                }
            }
        })
        .detach()
    }

    // Spawn a task performing election for the current peer with specified term.
    // This task can only be performed by a candidate.
    fn spawn_election_task(&mut self, term: u64) {
        let mut handle = self.clone();
        task::spawn(async move {
            // Vote for this peer.
            let mut votes = 1;
            let (mut rpcs, state) = {
                let mut raft = handle.raft();
                raft.term_voted = term;
                let req = RequestVoteArgs {
                    candidate: raft.me,
                    term,
                };
                info!("broadcasting vote requests");
                (broadcast_vote_request(&raft.peers, &req), raft.state())
            };

            // Note: not all of the errors should affect the election.
            // For instance, if one of the peers timed out, we still can win the election.
            // If we didn't won the election and didn't discover a new leader, we will perform a
            // new election started by the election timeout taskthe .
            while let Some(Ok(res)) = rpcs.next().await {
                info!("got reply: {res:?}");
                let raft = handle.raft();
                if raft.state_changed(&state) {
                    return;
                }

                if res.vote_granted {
                    votes += 1;
                }

                // Check for majority.
                if votes >= raft.peers.len() / 2 + 1 {
                    info!("peer {} won election", raft.me);
                    drop(raft);
                    return handle.transfer_state(term, Role::Leader);
                }
            }
            info!("peer {} lost election", handle.raft().me);
        })
        .detach();

        // Spawn an election timeout task to handle communication errors.
        self.spawn_election_timeout_task(term);
    }

    // Spawn an task tracking for election timeout.
    // This task can only be performed by a follower peer.
    fn spawn_election_timeout_task(&mut self, term: u64) {
        let mut handle = self.clone();
        task::spawn(async move {
            info!("running leader tracker task");
            let state = {
                let mut raft = handle.raft();
                raft.reset_election_timeout();
                raft.state()
            };

            loop {
                let timeout = Raft::generate_election_timeout();
                time::sleep(timeout.clone()).await;

                let raft = handle.raft();
                if raft.state_changed(&state) {
                    return;
                }

                if raft.timed_out(timeout) {
                    info!("election timeout's been reached");
                    drop(raft);
                    return handle.transfer_state(term + 1, Role::Candidate);
                }
            }
        })
        .detach();
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.state.is_leader()
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        todo!()
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        todo!()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let persist: Persist = todo!("persist state");
        let snapshot: Vec<u8> = todo!("persist snapshot");
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                todo!("restore snapshot");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                todo!("restore state");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let mut this = this.clone();
            async move { this.request_vote(args).await }
        });

        let that = self.clone();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let mut this = that.clone();
            async move { this.append_entries(args).await.unwrap() }
        });
        // add more RPC handers here
    }

    // Rpc handler for request vote message.
    async fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        info!("running request vote rpc: {args:?}");
        let mut get_reply = || {
            let term = self.raft().term();
            // Don't vote for stale terms.
            if args.term < term {
                return RequestVoteReply {
                    vote_granted: false,
                    term,
                };
            }

            // We've just discovered there is a leader or a candidate with a higher
            // term, transfer state to follower. (3.3)
            if args.term > term {
                self.transfer_state(args.term, Role::Follower);
            }

            // Don't vote twice.
            let mut raft = self.raft();
            if raft.term_voted >= args.term {
                return RequestVoteReply {
                    vote_granted: false,
                    term: args.term,
                };
            }

            // Vote for the candidate.
            raft.term_voted = args.term;
            RequestVoteReply {
                vote_granted: true,
                term: args.term,
            }
        };
        let reply = get_reply();
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        // self.persist().await.expect("failed to persist");
        info!("finished request vote rpc: {reply:?}");
        reply
    }

    // Rpc handler for append entries message.
    async fn append_entries(&mut self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        info!("running append entries rpc: {args:?}");
        let reply = {
            let mut raft = self.raft();
            let term = raft.term();
            // Reset election timeout if we've git a message from the leader.
            if raft.term() <= args.term {
                raft.reset_election_timeout();
            }

            // We've just discovered there is a leader or a candidate with a higher
            // term, transfer state to follower. (3.3)
            if term < args.term {
                drop(raft);
                self.transfer_state(args.term, Role::Follower);
            }

            AppendEntriesReply { term }
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        // self.persist().await.expect("failed to persist");
        info!("finished append entries rpc: {reply:?}");
        Ok(reply)
    }

    fn raft(&self) -> MutexGuard<Raft> {
        self.inner.lock().unwrap()
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }
        todo!("start agreement");
    }

    // Here is an example to apply committed message.
    fn apply(&self) {
        let msg = ApplyMsg::Command {
            data: todo!("apply msg"),
            index: todo!("apply msg"),
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    fn generate_election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    fn generate_heartbeat_timeout() -> Duration {
        Duration::from_millis(50)
    }

    fn timed_out(&self, election_timeout: Duration) -> bool {
        let now = time::Instant::now();
        self.election_timeout_reset + election_timeout < now
    }

    fn set_term(&mut self, term: u64) {
        self.state.set_term(term)
    }

    fn term(&self) -> u64 {
        self.state.term
    }

    fn set_role(&mut self, role: Role) {
        self.state.set_role(role)
    }

    fn state(&self) -> State {
        self.state
    }

    /// Check if the state changed.
    /// State checks must be performed after awaiting, as awaiting can change the state.
    fn state_changed(&self, state: &State) -> bool {
        self.state != *state
    }

    fn reset_election_timeout(&mut self) {
        self.election_timeout_reset = Instant::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    candidate: usize,
    term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    vote_granted: bool,
    term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: u64,
}
