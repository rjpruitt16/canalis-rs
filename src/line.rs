//! The line: Canalis's durable waiting room for tenants who can't be
//! assigned an instance yet. Stored on this node's own disk (redb), not in
//! Valkey -- Canalis holds and forwards work, it doesn't process it, so the
//! line only has to survive a crash of *this* process, and it's bounded on
//! purpose: past the configured limits, Canalis blocks with a 429 instead of
//! growing without end. Horizontal scale comes from running more Canalis
//! nodes, each with its own line, not from sharing one.

use crate::JobRequest;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

// Global arrival order across every tenant: seq -> user_id. Draining picks
// the oldest entry here, so whoever has waited longest is served first.
const ORDER: TableDefinition<u64, &str> = TableDefinition::new("order");
// One tenant's own jobs, FIFO by seq: (user_id, seq) -> job JSON.
const TENANT_JOBS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("tenant_jobs");
const TENANT_COUNTS: TableDefinition<&str, u64> = TableDefinition::new("tenant_counts");
// (user_id, idempotent_key) -> seq, while the job is still in line, so a
// retried submission doesn't take a second place in line.
const QUEUED_KEYS: TableDefinition<(&str, &str), u64> = TableDefinition::new("queued_keys");
// (user_id, idempotent_key) -> CachedResult JSON, kept for RESULT_TTL.
const RESULTS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("results");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const NEXT_SEQ: &str = "next_seq";
const TOTAL_JOBS: &str = "total_jobs";
const TOTAL_BYTES: &str = "total_bytes";

/// Where the line stops accepting work. Every limit is a block, not a crash:
/// a full line answers 429 with Retry-After and keeps serving everyone
/// already in it.
#[derive(Clone, Debug)]
pub struct LineLimits {
    pub max_jobs: u64,
    pub max_per_tenant: u64,
    pub max_bytes: u64,
    pub retry_after_seconds: u64,
    pub result_ttl_seconds: u64,
}

impl Default for LineLimits {
    fn default() -> Self {
        Self {
            max_jobs: 10_000,
            max_per_tenant: 1_000,
            max_bytes: 256 * 1024 * 1024,
            retry_after_seconds: 5,
            result_ttl_seconds: 300,
        }
    }
}

impl LineLimits {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            max_jobs: crate::env_u64("CANALIS_LINE_MAX_JOBS", d.max_jobs),
            max_per_tenant: crate::env_u64("CANALIS_LINE_MAX_PER_TENANT", d.max_per_tenant),
            max_bytes: crate::env_u64("CANALIS_LINE_MAX_BYTES", d.max_bytes),
            retry_after_seconds: crate::env_u64(
                "CANALIS_LINE_RETRY_AFTER_SECONDS",
                d.retry_after_seconds,
            ),
            result_ttl_seconds: crate::env_u64("CANALIS_RESULT_TTL_SECONDS", d.result_ttl_seconds),
        }
    }
}

/// The finished outcome of a drained job -- everything the still-waiting
/// connection (or GET /jobs/{key}) needs to rebuild the response it would
/// have gotten synchronously.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CachedResult {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

#[derive(Serialize, Deserialize)]
struct StoredResult {
    result: CachedResult,
    expires_at: u64,
}

/// A job taken out of the line, carrying its original seq so a failed
/// handoff can put it back in exactly the same place, and the path the
/// caller used ("/proxy" or "/jobs") so it's handed to the node the same way.
pub struct QueuedJob {
    pub seq: u64,
    pub job: JobRequest,
    pub path: String,
}

// What's stored per job. `job` is flattened so an entry written before
// `path` existed (a bare JobRequest) still reads back, as a /jobs job.
#[derive(Serialize, Deserialize)]
struct StoredJob {
    #[serde(default = "default_path")]
    path: String,
    #[serde(flatten)]
    job: JobRequest,
}

fn default_path() -> String {
    "/jobs".into()
}

#[derive(Debug)]
pub enum Enqueued {
    /// Newly in line; `ahead` jobs are in front of it. `seq` is its place
    /// in the global order, for position updates.
    Queued { ahead: u64, seq: u64 },
    /// Same (user_id, idempotent_key) is already waiting; not queued twice.
    AlreadyQueued { ahead: u64, seq: u64 },
    /// Same key already finished and its result is still cached.
    AlreadyDone(CachedResult),
}

#[derive(Debug)]
pub enum LineError {
    Full { reason: &'static str, limit: u64 },
    Storage(String),
}

impl std::fmt::Display for LineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineError::Full { reason, limit } => write!(f, "line full ({reason}, limit {limit})"),
            LineError::Storage(msg) => write!(f, "line storage error: {msg}"),
        }
    }
}

impl std::error::Error for LineError {}

macro_rules! impl_storage_from {
    ($($t:ty),+) => {$(
        impl From<$t> for LineError {
            fn from(err: $t) -> Self {
                LineError::Storage(err.to_string())
            }
        }
    )+};
}
impl_storage_from!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    serde_json::Error
);

/// Status of one (user_id, idempotent_key), for GET /jobs/{key}.
pub enum Lookup {
    Done(CachedResult),
    Queued { ahead: u64 },
    Unknown,
}

pub struct Line {
    db: Database,
    limits: LineLimits,
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

impl Line {
    /// Opens (or creates) the line at `path`. Whatever was in line when the
    /// process last died is still there: redb commits are durable (fsync)
    /// before `commit()` returns.
    pub fn open(path: impl AsRef<Path>, limits: LineLimits) -> Result<Self, LineError> {
        // A configured CANALIS_LINE_PATH whose directory doesn't exist yet
        // shouldn't stop Canalis from starting.
        if let Some(dir) = path.as_ref().parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| {
                LineError::Storage(format!("create {}: {e}", dir.display()))
            })?;
        }
        let db = Database::create(path)?;
        let txn = db.begin_write()?;
        {
            // Create every table up front so read transactions never hit
            // a "table doesn't exist" error on a brand-new file.
            txn.open_table(ORDER)?;
            txn.open_table(TENANT_JOBS)?;
            txn.open_table(TENANT_COUNTS)?;
            txn.open_table(QUEUED_KEYS)?;
            txn.open_table(RESULTS)?;
            txn.open_table(META)?;
        }
        txn.commit()?;
        Ok(Self { db, limits })
    }

    pub fn limits(&self) -> &LineLimits {
        &self.limits
    }

    /// Puts a job in line, unless it's a duplicate or the line is full.
    /// The dedup check runs before the limits, so retrying a job that's
    /// already waiting never gets a 429 for it.
    pub fn enqueue(&self, job: &JobRequest, path: &str) -> Result<Enqueued, LineError> {
        let payload = serde_json::to_vec(&StoredJob { path: path.into(), job: job.clone() })?;
        let idem = (job.user_id.as_str(), job.idempotent_key.as_str());
        let txn = self.db.begin_write()?;
        let outcome = {
            let mut order = txn.open_table(ORDER)?;
            let mut tenant_jobs = txn.open_table(TENANT_JOBS)?;
            let mut counts = txn.open_table(TENANT_COUNTS)?;
            let mut queued_keys = txn.open_table(QUEUED_KEYS)?;
            let results = txn.open_table(RESULTS)?;
            let mut meta = txn.open_table(META)?;

            if let Some(stored) = results.get(idem)? {
                let stored: StoredResult = serde_json::from_slice(stored.value())?;
                if stored.expires_at > now_unix() {
                    return Ok(Enqueued::AlreadyDone(stored.result));
                }
            }
            if let Some(seq) = queued_keys.get(idem)? {
                let seq = seq.value();
                let ahead = order.range(..seq)?.count() as u64;
                return Ok(Enqueued::AlreadyQueued { ahead, seq });
            }

            let total_jobs = meta.get(TOTAL_JOBS)?.map(|v| v.value()).unwrap_or(0);
            let total_bytes = meta.get(TOTAL_BYTES)?.map(|v| v.value()).unwrap_or(0);
            let tenant_count = counts.get(job.user_id.as_str())?.map(|v| v.value()).unwrap_or(0);

            if total_jobs >= self.limits.max_jobs {
                return Err(LineError::Full { reason: "line_jobs", limit: self.limits.max_jobs });
            }
            if tenant_count >= self.limits.max_per_tenant {
                return Err(LineError::Full {
                    reason: "line_tenant",
                    limit: self.limits.max_per_tenant,
                });
            }
            if total_bytes + payload.len() as u64 > self.limits.max_bytes {
                return Err(LineError::Full { reason: "line_bytes", limit: self.limits.max_bytes });
            }

            let seq = meta.get(NEXT_SEQ)?.map(|v| v.value()).unwrap_or(0);
            meta.insert(NEXT_SEQ, seq + 1)?;
            meta.insert(TOTAL_JOBS, total_jobs + 1)?;
            meta.insert(TOTAL_BYTES, total_bytes + payload.len() as u64)?;
            counts.insert(job.user_id.as_str(), tenant_count + 1)?;
            order.insert(seq, job.user_id.as_str())?;
            tenant_jobs.insert((job.user_id.as_str(), seq), payload.as_slice())?;
            queued_keys.insert(idem, seq)?;

            Enqueued::Queued { ahead: total_jobs, seq }
        };
        txn.commit()?;
        Ok(outcome)
    }

    /// Takes the longest-waiting job out of line, skipping tenants whose
    /// backlog is already being drained (their next job will be picked up
    /// by that drain, against the same sticky assignment).
    pub fn pop_oldest(&self, skip: &HashSet<String>) -> Result<Option<QueuedJob>, LineError> {
        let txn = self.db.begin_write()?;
        let found = {
            let order = txn.open_table(ORDER)?;
            let mut found = None;
            for entry in order.range::<u64>(..)? {
                let (seq, user_id) = entry?;
                if !skip.contains(user_id.value()) {
                    found = Some((seq.value(), user_id.value().to_string()));
                    break;
                }
            }
            found
        };
        let Some((seq, user_id)) = found else {
            return Ok(None);
        };
        let job = remove_job(&txn, &user_id, seq)?;
        txn.commit()?;
        Ok(job.map(|stored| QueuedJob { seq, job: stored.job, path: stored.path }))
    }

    /// Takes `user_id`'s own oldest job out of line, if they have one.
    pub fn pop_for(&self, user_id: &str) -> Result<Option<QueuedJob>, LineError> {
        let txn = self.db.begin_write()?;
        let seq = {
            let tenant_jobs = txn.open_table(TENANT_JOBS)?;
            let mut range = tenant_jobs.range((user_id, 0u64)..=(user_id, u64::MAX))?;
            match range.next() {
                Some(entry) => Some(entry?.0.value().1),
                None => None,
            }
        };
        let Some(seq) = seq else {
            return Ok(None);
        };
        let job = remove_job(&txn, user_id, seq)?;
        txn.commit()?;
        Ok(job.map(|stored| QueuedJob { seq, job: stored.job, path: stored.path }))
    }

    /// Puts a job back at its original place in line -- used when a job was
    /// taken out but couldn't be handed to an instance after all. Not
    /// subject to the limits: it was already admitted once.
    pub fn requeue(&self, queued: &QueuedJob) -> Result<(), LineError> {
        let payload =
            serde_json::to_vec(&StoredJob { path: queued.path.clone(), job: queued.job.clone() })?;
        let user_id = queued.job.user_id.as_str();
        let txn = self.db.begin_write()?;
        {
            let mut order = txn.open_table(ORDER)?;
            let mut tenant_jobs = txn.open_table(TENANT_JOBS)?;
            let mut counts = txn.open_table(TENANT_COUNTS)?;
            let mut queued_keys = txn.open_table(QUEUED_KEYS)?;
            let mut meta = txn.open_table(META)?;

            let total_jobs = meta.get(TOTAL_JOBS)?.map(|v| v.value()).unwrap_or(0);
            let total_bytes = meta.get(TOTAL_BYTES)?.map(|v| v.value()).unwrap_or(0);
            let tenant_count = counts.get(user_id)?.map(|v| v.value()).unwrap_or(0);
            meta.insert(TOTAL_JOBS, total_jobs + 1)?;
            meta.insert(TOTAL_BYTES, total_bytes + payload.len() as u64)?;
            counts.insert(user_id, tenant_count + 1)?;
            order.insert(queued.seq, user_id)?;
            tenant_jobs.insert((user_id, queued.seq), payload.as_slice())?;
            queued_keys.insert((user_id, queued.job.idempotent_key.as_str()), queued.seq)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn store_result(
        &self,
        user_id: &str,
        idempotent_key: &str,
        result: &CachedResult,
    ) -> Result<(), LineError> {
        let stored = StoredResult {
            result: result.clone(),
            expires_at: now_unix() + self.limits.result_ttl_seconds,
        };
        let payload = serde_json::to_vec(&stored)?;
        let txn = self.db.begin_write()?;
        txn.open_table(RESULTS)?.insert((user_id, idempotent_key), payload.as_slice())?;
        txn.commit()?;
        Ok(())
    }

    pub fn lookup(&self, user_id: &str, idempotent_key: &str) -> Result<Lookup, LineError> {
        let txn = self.db.begin_read()?;
        let key = (user_id, idempotent_key);
        if let Some(stored) = txn.open_table(RESULTS)?.get(key)? {
            let stored: StoredResult = serde_json::from_slice(stored.value())?;
            if stored.expires_at > now_unix() {
                return Ok(Lookup::Done(stored.result));
            }
        }
        if let Some(seq) = txn.open_table(QUEUED_KEYS)?.get(key)? {
            let ahead = txn.open_table(ORDER)?.range(..seq.value())?.count() as u64;
            return Ok(Lookup::Queued { ahead });
        }
        Ok(Lookup::Unknown)
    }

    #[cfg(test)]
    pub fn get_result(
        &self,
        user_id: &str,
        idempotent_key: &str,
    ) -> Result<Option<CachedResult>, LineError> {
        match self.lookup(user_id, idempotent_key)? {
            Lookup::Done(result) => Ok(Some(result)),
            _ => Ok(None),
        }
    }

    /// Drops cached results past their TTL. Returns how many were removed.
    pub fn expire_results(&self) -> Result<usize, LineError> {
        let now = now_unix();
        let txn = self.db.begin_write()?;
        let removed = {
            let mut results = txn.open_table(RESULTS)?;
            let mut removed = 0;
            results.retain(|_, payload| {
                let keep = serde_json::from_slice::<StoredResult>(payload)
                    .map(|stored| stored.expires_at > now)
                    .unwrap_or(false);
                if !keep {
                    removed += 1;
                }
                keep
            })?;
            removed
        };
        txn.commit()?;
        Ok(removed)
    }

    /// How many jobs are ahead of each of `seqs`, in one pass over the line
    /// rather than one count per waiting caller. Seqs no longer in line are
    /// left out of the result.
    pub fn ranks(&self, seqs: &HashSet<u64>) -> Result<std::collections::HashMap<u64, u64>, LineError> {
        let txn = self.db.begin_read()?;
        let order = txn.open_table(ORDER)?;
        let mut ranks = std::collections::HashMap::with_capacity(seqs.len());
        for (rank, entry) in order.range::<u64>(..)?.enumerate() {
            let seq = entry?.0.value();
            if seqs.contains(&seq) {
                ranks.insert(seq, rank as u64);
                if ranks.len() == seqs.len() {
                    break;
                }
            }
        }
        Ok(ranks)
    }

    /// Jobs currently in line.
    pub fn len(&self) -> Result<u64, LineError> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(META)?.get(TOTAL_JOBS)?.map(|v| v.value()).unwrap_or(0))
    }
}

// Removes one job from every table that references it, inside the caller's
// transaction. Returns None (and drops the entry) if the stored payload is
// unparseable -- a corrupt entry shouldn't jam the line forever.
fn remove_job(
    txn: &redb::WriteTransaction,
    user_id: &str,
    seq: u64,
) -> Result<Option<StoredJob>, LineError> {
    let mut order = txn.open_table(ORDER)?;
    let mut tenant_jobs = txn.open_table(TENANT_JOBS)?;
    let mut counts = txn.open_table(TENANT_COUNTS)?;
    let mut queued_keys = txn.open_table(QUEUED_KEYS)?;
    let mut meta = txn.open_table(META)?;

    order.remove(seq)?;
    let payload = match tenant_jobs.remove((user_id, seq))? {
        Some(payload) => payload.value().to_vec(),
        None => return Ok(None),
    };

    let total_jobs = meta.get(TOTAL_JOBS)?.map(|v| v.value()).unwrap_or(0);
    let total_bytes = meta.get(TOTAL_BYTES)?.map(|v| v.value()).unwrap_or(0);
    meta.insert(TOTAL_JOBS, total_jobs.saturating_sub(1))?;
    meta.insert(TOTAL_BYTES, total_bytes.saturating_sub(payload.len() as u64))?;

    let tenant_count = counts.get(user_id)?.map(|v| v.value()).unwrap_or(0);
    if tenant_count <= 1 {
        counts.remove(user_id)?;
    } else {
        counts.insert(user_id, tenant_count - 1)?;
    }

    match serde_json::from_slice::<StoredJob>(&payload) {
        Ok(stored) => {
            queued_keys.remove((user_id, stored.job.idempotent_key.as_str()))?;
            Ok(Some(stored))
        }
        Err(err) => {
            tracing::error!("dropping unparseable queued job for {user_id}: {err}");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn job(user_id: &str, key: &str) -> JobRequest {
        JobRequest {
            user_id: user_id.into(),
            idempotent_key: key.into(),
            url: "https://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            webhook_url: None,
        }
    }

    fn open(limits: LineLimits) -> (Line, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let line = Line::open(dir.path().join("line.redb"), limits).unwrap();
        (line, dir)
    }

    #[test]
    fn oldest_job_across_all_tenants_comes_out_first() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("b", "1"), "/jobs").unwrap();
        line.enqueue(&job("a", "1"), "/jobs").unwrap();
        line.enqueue(&job("b", "2"), "/jobs").unwrap();

        let first = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!((first.job.user_id.as_str(), first.job.idempotent_key.as_str()), ("b", "1"));
        let second = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!(second.job.user_id, "a");
        assert_eq!(line.len().unwrap(), 1);
    }

    #[test]
    fn pop_oldest_skips_tenants_already_being_drained() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("busy", "1"), "/jobs").unwrap();
        line.enqueue(&job("waiting", "1"), "/jobs").unwrap();

        let skip: HashSet<String> = ["busy".to_string()].into();
        let popped = line.pop_oldest(&skip).unwrap().unwrap();
        assert_eq!(popped.job.user_id, "waiting");
    }

    #[test]
    fn pop_for_is_fifo_within_one_tenant() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs").unwrap();
        line.enqueue(&job("other", "1"), "/jobs").unwrap();
        line.enqueue(&job("a", "2"), "/jobs").unwrap();

        assert_eq!(line.pop_for("a").unwrap().unwrap().job.idempotent_key, "1");
        assert_eq!(line.pop_for("a").unwrap().unwrap().job.idempotent_key, "2");
        assert!(line.pop_for("a").unwrap().is_none());
        assert_eq!(line.len().unwrap(), 1, "the other tenant's job stays in line");
    }

    #[test]
    fn requeue_puts_a_job_back_in_its_original_place() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs").unwrap();
        line.enqueue(&job("b", "1"), "/jobs").unwrap();

        let taken = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        line.requeue(&taken).unwrap();

        let again = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!(again.job.user_id, "a", "a requeued job must not lose its place to b");
    }

    #[test]
    fn a_retried_submission_does_not_take_a_second_place_in_line() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "same"), "/jobs").unwrap();
        line.enqueue(&job("b", "1"), "/jobs").unwrap();

        let again = line.enqueue(&job("a", "same"), "/jobs").unwrap();
        assert!(matches!(again, Enqueued::AlreadyQueued { ahead: 0, seq: 0 }));
        assert_eq!(line.len().unwrap(), 2);
    }

    #[test]
    fn same_key_from_different_tenants_are_different_jobs() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "k1"), "/jobs").unwrap();
        let other = line.enqueue(&job("b", "k1"), "/jobs").unwrap();
        assert!(matches!(other, Enqueued::Queued { .. }));
        assert_eq!(line.len().unwrap(), 2);
    }

    #[test]
    fn a_finished_job_resubmitted_returns_its_cached_result() {
        let (line, _dir) = open(LineLimits::default());
        let result = CachedResult { status: 200, content_type: "text/plain".into(), body: "ok".into() };
        line.store_result("a", "k", &result).unwrap();

        match line.enqueue(&job("a", "k"), "/jobs").unwrap() {
            Enqueued::AlreadyDone(cached) => assert_eq!(cached, result),
            other => panic!("expected the cached result, got {other:?}"),
        }
        assert_eq!(line.len().unwrap(), 0);
    }

    #[test]
    fn full_line_blocks_instead_of_growing() {
        let limits = LineLimits { max_jobs: 2, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("a", "1"), "/jobs").unwrap();
        line.enqueue(&job("b", "1"), "/jobs").unwrap();

        let err = line.enqueue(&job("c", "1"), "/jobs").unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_jobs", limit: 2 }));

        // Room frees up as soon as someone leaves the line.
        line.pop_oldest(&HashSet::new()).unwrap();
        line.enqueue(&job("c", "1"), "/jobs").unwrap();
    }

    #[test]
    fn one_tenant_cannot_take_the_whole_line() {
        let limits = LineLimits { max_per_tenant: 1, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("noisy", "1"), "/jobs").unwrap();

        let err = line.enqueue(&job("noisy", "2"), "/jobs").unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_tenant", .. }));
        line.enqueue(&job("quiet", "1"), "/jobs").expect("another tenant still gets in");
    }

    #[test]
    fn byte_limit_counts_queued_payloads() {
        // One serialized test job is ~120 bytes: room for one, not two.
        let limits = LineLimits { max_bytes: 200, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("a", "1"), "/jobs").unwrap();

        let err = line.enqueue(&job("b", "1"), "/jobs").unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_bytes", .. }));
    }

    #[test]
    fn lookup_reports_place_in_line_then_the_result() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs").unwrap();
        line.enqueue(&job("b", "1"), "/jobs").unwrap();

        assert!(matches!(line.lookup("b", "1").unwrap(), Lookup::Queued { ahead: 1 }));
        assert!(matches!(line.lookup("nobody", "1").unwrap(), Lookup::Unknown));

        line.pop_for("b").unwrap();
        let result = CachedResult { status: 200, content_type: String::new(), body: "done".into() };
        line.store_result("b", "1", &result).unwrap();
        assert!(matches!(line.lookup("b", "1").unwrap(), Lookup::Done(_)));
    }

    #[test]
    fn ranks_report_how_many_are_ahead_in_one_pass() {
        let (line, _dir) = open(LineLimits::default());
        let mut seqs = Vec::new();
        for user in ["a", "b", "c"] {
            match line.enqueue(&job(user, "1"), "/jobs").unwrap() {
                Enqueued::Queued { seq, .. } => seqs.push(seq),
                other => panic!("expected Queued, got {other:?}"),
            }
        }
        line.pop_for("a").unwrap();

        let wanted: HashSet<u64> = seqs.iter().copied().collect();
        let ranks = line.ranks(&wanted).unwrap();
        assert_eq!(ranks.get(&seqs[0]), None, "a left the line");
        assert_eq!(ranks[&seqs[1]], 0);
        assert_eq!(ranks[&seqs[2]], 1);
    }

    #[test]
    fn the_callers_path_comes_back_out_with_the_job() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/proxy").unwrap();
        let popped = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!(popped.path, "/proxy");

        line.requeue(&popped).unwrap();
        assert_eq!(line.pop_for("a").unwrap().unwrap().path, "/proxy", "requeue keeps it too");
    }

    #[test]
    fn opening_creates_a_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not").join("there").join("line.redb");
        let line = Line::open(&path, LineLimits::default()).unwrap();
        assert_eq!(line.len().unwrap(), 0);
        assert!(path.exists());
    }

    #[test]
    fn expired_results_are_swept() {
        let limits = LineLimits { result_ttl_seconds: 0, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        let result = CachedResult { status: 200, content_type: String::new(), body: String::new() };
        line.store_result("a", "1", &result).unwrap();

        assert!(line.get_result("a", "1").unwrap().is_none(), "a 0s TTL is expired immediately");
        assert_eq!(line.expire_results().unwrap(), 1);
    }

    // A real crash, not a clean shutdown: this test re-runs itself as a
    // child process that queues two jobs and then aborts (no destructors,
    // no flush-on-drop). The parent then reopens the same file and expects
    // both jobs to still be in line. Same write / kill / restart / read
    // check Aquifer's benchmarks use for SQLite and Pebble.
    #[test]
    fn jobs_in_line_survive_the_process_being_killed() {
        if let Ok(path) = std::env::var("CANALIS_LINE_CRASH_CHILD") {
            let line = Line::open(&path, LineLimits::default()).unwrap();
            line.enqueue(&job("a", "1"), "/jobs").unwrap();
            line.enqueue(&job("b", "1"), "/jobs").unwrap();
            std::process::abort();
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("line.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "line::tests::jobs_in_line_survive_the_process_being_killed", "--nocapture"])
            .env("CANALIS_LINE_CRASH_CHILD", &path)
            .status()
            .unwrap();
        assert!(!status.success(), "the child is supposed to die by abort");

        let line = Line::open(&path, LineLimits::default()).unwrap();
        assert_eq!(line.len().unwrap(), 2);
        assert_eq!(line.pop_oldest(&HashSet::new()).unwrap().unwrap().job.user_id, "a");
        assert_eq!(line.pop_oldest(&HashSet::new()).unwrap().unwrap().job.user_id, "b");
    }
}
