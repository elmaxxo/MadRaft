use super::log::Log;
use futures::{channel::mpsc, stream::FuturesUnordered, Future, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task::{self, Task},
    time::{self, *},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self},
    io,
    net::SocketAddr,
    ops::AddAssign,
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
    #[error("no such index {0}")]
    NoSuchIndex(usize),
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

    tasks: Vec<Task<()>>,

    // Last term this peer voted for. All vote requests with term <= this value are rejected.
    term_voted: u64,

    // Time at which election timeout starts counting.
    election_timeout_reset: Instant,

    log: Log<Vec<u8>>,

    last_committed: u64,

    next_index: HashMap<SocketAddr, u64>,
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
async fn send_vote_request(
    peer: SocketAddr,
    req: RequestVoteArgs,
    timeout: Duration,
) -> io::Result<RequestVoteReply> {
    let net = net::NetLocalHandle::current();
    info!("sending vote request to {peer:?}");
    net.call_timeout(peer, req, timeout).await
}

/// Create a set of futures that broadcast vote request to all peers.
fn broadcast_vote_request(
    peers: &[SocketAddr],
    req: &RequestVoteArgs,
    timeout: &Duration,
) -> FuturesUnordered<impl Future<Output = io::Result<RequestVoteReply>>> {
    let rpcs = FuturesUnordered::new();
    peers
        .iter()
        .enumerate()
        .filter(|(peer_idx, _)| *peer_idx != req.candidate)
        .for_each(|(_, peer)| {
            let (peer, req, timeout) = (peer.clone(), req.clone(), timeout.clone());
            rpcs.push(async move { send_vote_request(peer, req, timeout).await });
        });
    rpcs
}

/// Send append entries to the peer.
async fn send_append_entries(
    peer: SocketAddr,
    req: AppendEntryArgs,
    timeout: Duration,
) -> io::Result<AppendEntryReply> {
    let net = net::NetLocalHandle::current();
    info!("sending append entries to {peer:?}");
    net.call_timeout(peer, req, timeout).await
}

/// Create a set of futures that broadcast append entries to all peers.
fn broadcast_append_entries(
    peers: &[SocketAddr],
    req: &AppendEntryArgs,
    me: usize,
    timeout: &Duration,
) -> FuturesUnordered<impl Future<Output = io::Result<AppendEntryReply>>> {
    let rpcs = FuturesUnordered::new();
    peers
        .iter()
        .enumerate()
        .filter(|(peer_idx, _)| *peer_idx != me)
        .for_each(|(_, peer)| {
            let (peer, req, timeout) = (peer.clone(), req.clone(), timeout.clone());
            rpcs.push(async move { send_append_entries(peer, req, timeout).await });
        });
    rpcs
}

impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        // Log starts with 1 index.
        let next_index = peers.iter().map(|peer| (peer.clone(), 1)).collect();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            state: State::default(),
            tasks: vec![],
            term_voted: 0,
            election_timeout_reset: Instant::now(),
            log: Log::new(),
            last_committed: 0,
            next_index,
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
        info!(
            "{}: transfering state to ({role:?}, {term})",
            raft.describe()
        );
        raft.abort_tasks();
        raft.set_term(term);
        raft.set_role(role);
        drop(raft);

        match role {
            Role::Leader => self.spawn_leader_tasks(term),
            Role::Candidate => self.spawn_election_task(term),
            Role::Follower => self.spawn_election_timeout_task(term),
        }
    }

    fn spawn_leader_tasks(&mut self, term: u64) {
        self.spawn_heartbeat_task(term);
        let (me, peers) = {
            let raft = self.raft();
            (raft.me, raft.peers.clone())
        };
        for (idx, peer) in peers.iter().enumerate() {
            if idx != me {
                self.spawn_replication_task(*peer);
            }
        }
    }

    // Spawn a task broadcasting heartbeats to all peers by timeout.
    // This task can only be performed by a peer who considers himself as a leader.
    fn spawn_heartbeat_task(&mut self, term: u64) {
        let mut handle = self.clone();
        self.raft().spawn_task(async move {
            info!("{}: running heartbeat task", handle.raft().describe());
            loop {
                let timeout = Raft::heartbeat_timeout();
                let mut rpcs = {
                    let raft = handle.raft();
                    let req = AppendEntryArgs {
                        term,
                        last_committed: raft.last_committed,
                        index: raft.log.len(),
                        entry: vec![],
                    };
                    broadcast_append_entries(&raft.peers, &req, raft.me, &timeout)
                };

                info!("{}: broadcasting heartbeats", handle.raft().describe());
                while let Some(Ok(res)) = rpcs.next().await {
                    // We've just discovered there is a leader or a candidate with a higher
                    // term, transfer state to follower. (3.3)
                    if res.term > term {
                        return handle.transfer_state(res.term, Role::Follower);
                    }
                }

                time::sleep(timeout).await;
            }
        })
    }

    // Spawn a task performing election for the current peer with specified term.
    // This task can only be performed by a candidate.
    fn spawn_election_task(&mut self, term: u64) {
        let mut handle = self.clone();
        self.raft().spawn_task(async move {
            // Vote for this peer.
            let mut votes = 1;
            let mut rpcs = {
                let mut raft = handle.raft();
                raft.term_voted = term;
                let req = RequestVoteArgs {
                    candidate: raft.me,
                    term,
                };
                broadcast_vote_request(&raft.peers, &req, &Raft::generate_election_timeout())
            };

            // Note: not all of the errors should affect the election.
            // For instance, if one of the peers timed out, we still can win the election.
            // If we didn't won the election and didn't discover a new leader, the election
            // timeout task will start a new election.
            info!("{}: broadcasting vote requests", handle.raft().describe());
            while let Some(Ok(res)) = rpcs.next().await {
                info!("got reply: {res:?}");
                if res.vote_granted {
                    votes += 1;
                }

                // Check for majority.
                if handle.raft().is_majority(votes) {
                    info!("{}: won election", handle.raft().describe());
                    return handle.transfer_state(term, Role::Leader);
                }
            }
            info!("{}: lost election", handle.raft().describe());
        });

        // Spawn an election timeout task to handle split votes and communication errors.
        self.spawn_election_timeout_task(term);
    }

    // Spawn an task tracking for election timeout.
    // This task can only be performed by a follower or a candidate peer.
    fn spawn_election_timeout_task(&mut self, term: u64) {
        let mut handle = self.clone();
        self.raft().spawn_task(async move {
            info!("{}: running leader tracker task", handle.raft().describe());
            handle.raft().reset_election_timeout();

            loop {
                let timeout = Raft::generate_election_timeout();
                time::sleep(timeout.clone()).await;

                if handle.raft().timed_out(timeout) {
                    info!(
                        "{}: election timeout's been reached",
                        handle.raft().describe()
                    );
                    return handle.transfer_state(term + 1, Role::Candidate);
                }
            }
        })
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.raft();
        if !raft.state.is_leader() {
            let leader = (raft.me + 1) % raft.peers.len();
            return Err(Error::NotLeader(leader));
        }
        info!("{}: starting agreement {:?}", raft.describe(), cmd);
        let term = raft.term();
        let index = raft.log.append(term, cmd.to_vec());
        Ok(Start { index, term })
    }

    pub fn spawn_replication_task(&self, peer: SocketAddr) {
        let mut handle = self.clone();
        let term = self.raft().term();

        self.raft().spawn_task(async move {
            loop {
                let (index, term_and_entry) = {
                    let raft = handle.raft();
                    let next_index = raft.next_index[&peer];
                    let term_and_entry = raft
                        .log
                        .term_and_entry(next_index)
                        .map(|(t, e)| (t, e.to_vec()));
                    (next_index, term_and_entry)
                };

                let Some((entry_term, entry)) = term_and_entry else {
                    time::sleep(Raft::log_inspection_timeout()).await;
                    continue;
                };

                let req = AppendEntryArgs {
                    index,
                    term: entry_term,
                    last_committed: handle.raft().last_committed,
                    entry,
                };
                let rpc = send_append_entries(peer, req, Raft::heartbeat_timeout());
                if let Ok(resp) = rpc.await {
                    let mut raft = handle.raft();
                    if resp.success {
                        raft.advance_next_index(&peer);
                        raft.try_commit_new_entries();
                    } else {
                        todo!()
                    }
                };

                time::sleep(Raft::log_inspection_timeout()).await;
            }
        })
    }

    //pub fn spawn_command_replication_task(&self, index: LogIndex) {
    //    let mut handle = self.clone();
    //    let mut raft = self.raft();
    //    let Some(entry) = raft.log.get(index).cloned() else {
    //        // Entry has been overwritten.
    //        return;
    //    };
    //    let payload = entry.payload.clone();
    //    let term = raft.term();
    //    raft.spawn_task(async move {
    //        let req = AppendEntriesArgs { term, payload };
    //        loop {
    //            if handle.raft().log.is_commited(index) {
    //                return;
    //            }
    //
    //            let mut rpcs = {
    //                let raft = handle.raft();
    //                info!(
    //                    "{}: broadcasting append entries {:?}",
    //                    raft.describe(),
    //                    &req
    //                );
    //                broadcast_append_entries(
    //                    &raft.peers,
    //                    &req,
    //                    raft.me,
    //                    &Raft::generate_heartbeat_timeout(),
    //                )
    //            };
    //
    //            let mut confirmed = 1;
    //            while let Some(Ok(res)) = rpcs.next().await {
    //                if term < res.term {
    //                    return handle.transfer_state(res.term, Role::Follower);
    //                }
    //
    //                confirmed += 1;
    //                let mut raft = handle.raft();
    //                if raft.is_majority(confirmed) {
    //                    info!("{}: commiting index {:?}", raft.describe(), index);
    //                    raft.log.commit(index);
    //                    raft.apply(entry);
    //                    return;
    //                }
    //            }
    //        }
    //    })
    //}

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
        net.add_rpc_handler(move |args: AppendEntryArgs| {
            let mut this = that.clone();
            async move { this.append_entries(args).await.unwrap() }
        });
        // add more RPC handers here
    }

    // Rpc handler for request vote message.
    async fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        info!(
            "{}: running request vote rpc: {args:?}",
            self.raft().describe()
        );
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
        info!(
            "{}: processed request vote rpc: {reply:?}",
            self.raft().describe()
        );
        reply
    }

    // Rpc handler for append entries message.
    async fn append_entries(&mut self, args: AppendEntryArgs) -> Result<AppendEntryReply> {
        info!(
            "{}: running append entries rpc: {args:?}",
            self.raft().describe()
        );
        let reply = {
            let mut raft = self.raft();
            let term = raft.term();
            let last_committed = raft.last_committed;
            let is_candidate = raft.state.role == Role::Candidate;
            // Reset election timeout if we've get a message from the leader.
            if raft.term() <= args.term {
                raft.reset_election_timeout();
            }
            drop(raft);

            // We've lost this election
            // or
            // we've just discovered there is a leader or a candidate with a higher
            // term, transfer state to follower. (3.3)
            if (is_candidate && term == args.term) || term < args.term {
                self.transfer_state(args.term, Role::Follower);
            }

            if last_committed < args.last_committed {
                let mut raft = self.raft();
                info!(
                    "{}: new last committed {}",
                    raft.describe(),
                    args.last_committed
                );
                raft.last_committed = args.last_committed;
            }

            if !args.entry.is_empty() {
                // TODO: add checks and overwriting
                let mut raft = self.raft();
                info!("{}: appending new entry {:?}", raft.describe(), &args.entry);
                let index = raft.log.append(args.term, args.entry);
                assert!(index == args.index);
                raft.apply(index);
            }

            AppendEntryReply {
                term: self.raft().term(),
                success: true,
            }
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        // self.persist().await.expect("failed to persist");
        info!(
            "{}: processed append entries rpc: {reply:?}",
            self.raft().describe()
        );
        Ok(reply)
    }

    fn raft(&self) -> MutexGuard<Raft> {
        self.inner.lock().unwrap()
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    // Here is an example to apply committed message.
    fn apply(&self, index: u64) {
        let entry = self.log.entry(index).clone();
        info!("applying {:?} index {:?}", &entry, index);
        let msg = ApplyMsg::Command {
            data: entry.expect("no such entry").clone(),
            index,
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    fn generate_election_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(150..300))
    }

    // A helper function for spawning tasks and store them.
    fn spawn_task<F>(&mut self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.push(task::spawn(f))
    }

    //fn commit(&mut self, index: u64) {}

    fn advance_next_index(&mut self, peer: &SocketAddr) {
        self.next_index.get_mut(peer).map(|x| x.add_assign(1));
    }

    fn try_commit_new_entries(&mut self) {
        let mut last_sent: Vec<_> = self
            .next_index
            .iter()
            .map(|(_, x)| x.saturating_sub(1))
            .collect();

        last_sent.sort();
        let new_last_committed = last_sent[last_sent.len() / 2];
        let old_last_committed = self.last_committed;

        if old_last_committed < new_last_committed {
            info!(
                "{}: committing new index {}",
                self.describe(),
                new_last_committed
            );
            self.last_committed = new_last_committed;
        }

        for idx in old_last_committed + 1..=new_last_committed {
            self.apply(idx)
        }
    }

    fn abort_tasks(&mut self) {
        self.tasks.clear()
    }

    fn log_inspection_timeout() -> Duration {
        Self::heartbeat_timeout() / 2
    }

    fn heartbeat_timeout() -> Duration {
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

    fn reset_election_timeout(&mut self) {
        self.election_timeout_reset = Instant::now();
    }

    fn describe(&self) -> String {
        format!(
            "raft(peer: {}, role: {:?}, term: {}, committed: {})",
            self.me, self.state.role, self.state.term, self.last_committed,
        )
    }

    fn is_majority(&self, n: usize) -> bool {
        n >= self.peers.len() / 2 + 1
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
struct AppendEntryArgs {
    term: u64,
    index: u64,
    last_committed: u64,
    entry: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntryReply {
    term: u64,
    success: bool,
}
