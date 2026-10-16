//! The line: Canalis's durable waiting room for tenants who can't be
//! assigned an instance yet. Stored on this node's own disk (redb), not in
//! Valkey -- Canalis holds and forwards work, it doesn't process it, so the
//! line only has to survive a crash of *this* process, and it's bounded on
//! purpose: past the configured limits, Canalis blocks with a 429 instead of
//! growing without end. Horizontal scale comes from running more Canalis
//! nodes, each with its own line, not from sharing one.

use crate::JobRequest;
use crate::assign::{DEFAULT_POOL, Tenant};
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
// Jobs taken out of line but not yet handed off (seq -> StoredJob), in the
// same transaction that took them. A crash in between leaves them here, and
// the next open puts them back in line instead of losing them.
const LEASED: TableDefinition<u64, &[u8]> = TableDefinition::new("leased");
// Jobs waiting per pool, so a new job's place in line is one lookup rather
// than a scan of the whole line. Rebuilt from ORDER on open.
const POOL_COUNTS: TableDefinition<&str, u64> = TableDefinition::new("pool_counts");

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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct CachedResult {
    pub status: u16,
    pub content_type: String,
    pub body: String,
    /// The instance's own response headers worth passing on (Retry-After,
    /// X-Aqueduct-*), other than the content type. Absent in results
    /// stored before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
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
    /// The pool it's waiting for an instance in.
    pub pool: String,
    /// When it joined the line (unix seconds; 0 if stored before this
    /// was recorded). Kept across a requeue.
    pub enqueued_at: u64,
}

// What's stored per job. `job` is flattened so an entry written before
// `path` existed (a bare JobRequest) still reads back, as a /jobs job.
#[derive(Serialize, Deserialize)]
struct StoredJob {
    #[serde(default = "default_path")]
    path: String,
    #[serde(default)]
    enqueued_at: u64,
    // Jobs stored before pools read back as the default pool.
    #[serde(default = "default_pool")]
    pool: String,
    #[serde(flatten)]
    job: JobRequest,
}

fn default_pool() -> String {
    DEFAULT_POOL.into()
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
    // Group commit: enqueues that arrive while one is being written wait
    // and go in the next transaction together, one disk flush for all of
    // them (the same idea as Aquifer's batched Pebble flush, without a
    // timer: at low load a job is written at once).
    batch: std::sync::Mutex<Batch>,
}

#[derive(Default)]
struct Batch {
    pending: Vec<PendingEnqueue>,
    writing: bool,
}

struct PendingEnqueue {
    job: JobRequest,
    path: String,
    pool: String,
    done: std::sync::Arc<Outcome>,
}

// Where a batched enqueue's result is handed back to the caller waiting
// for it.
type Outcome = (std::sync::Mutex<Option<Result<Enqueued, LineError>>>, std::sync::Condvar);

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
            txn.open_table(LEASED)?;
            // Rebuilt from the line itself, so a file written before this
            // table existed (or with counts that drifted) is right on open.
            let mut pool_counts = txn.open_table(POOL_COUNTS)?;
            pool_counts.retain(|_, _| false)?;
            let mut counted: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            for entry in txn.open_table(ORDER)?.range::<u64>(..)? {
                *counted.entry(Tenant::pool_of_key(entry?.1.value()).to_string()).or_default() += 1;
            }
            for (pool, n) in &counted {
                pool_counts.insert(pool.as_str(), *n)?;
            }
        }
        txn.commit()?;
        let line = Self { db, limits, batch: Default::default() };
        line.restore_leases()?;
        Ok(line)
    }

    // Back in line, at their original places: jobs that were taken out when
    // the process last died and never reached an instance (or never had
    // their result recorded). An instance that did get one sees the same
    // idempotency key again and answers from what it already has.
    fn restore_leases(&self) -> Result<(), LineError> {
        let leased: Vec<QueuedJob> = {
            let txn = self.db.begin_read()?;
            let table = txn.open_table(LEASED)?;
            let mut leased = Vec::new();
            for entry in table.range::<u64>(..)? {
                let (seq, payload) = entry?;
                let stored: StoredJob = serde_json::from_slice(payload.value())?;
                leased.push(QueuedJob { seq: seq.value(), job: stored.job, path: stored.path, pool: stored.pool, enqueued_at: stored.enqueued_at });
            }
            leased
        };
        if !leased.is_empty() {
            tracing::warn!("putting {} jobs that were in flight at the last shutdown back in line", leased.len());
        }
        for queued in &leased {
            self.requeue(queued)?;
        }
        Ok(())
    }

    /// A job taken out of line is done with: handed to an instance and its
    /// outcome recorded. Until then a restart puts it back in line.
    pub fn finish(&self, seq: u64) -> Result<(), LineError> {
        let txn = self.db.begin_write()?;
        txn.open_table(LEASED)?.remove(seq)?;
        txn.commit()?;
        Ok(())
    }

    /// Jobs taken out of line and not yet finished.
    pub fn leased(&self) -> Result<u64, LineError> {
        use redb::ReadableTableMetadata as _;
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(LEASED)?.len()?)
    }

    pub fn limits(&self) -> &LineLimits {
        &self.limits
    }

    /// Puts a job in line for an instance in `pool`, unless it's a
    /// duplicate or the line is full. The dedup check runs before the
    /// limits, so retrying a job that's already waiting never gets a 429
    /// for it. `ahead` counts only jobs waiting in the same pool.
    ///
    /// Durable once this returns. Calls that arrive while another is
    /// writing are committed together in the next transaction.
    pub fn enqueue(&self, job: &JobRequest, path: &str, pool: &str) -> Result<Enqueued, LineError> {
        let done: std::sync::Arc<Outcome> = std::sync::Arc::new((std::sync::Mutex::new(None), std::sync::Condvar::new()));
        let mut batch = self.batch.lock().unwrap_or_else(|e| e.into_inner());
        batch.pending.push(PendingEnqueue { job: job.clone(), path: path.into(), pool: pool.into(), done: done.clone() });
        if batch.writing {
            // Someone else is writing; they'll take this one next.
            drop(batch);
            let (slot, ready) = &*done;
            let mut result = slot.lock().unwrap_or_else(|e| e.into_inner());
            while result.is_none() {
                result = ready.wait(result).unwrap_or_else(|e| e.into_inner());
            }
            return result.take().expect("checked above");
        }
        // This caller writes, until nothing is left waiting.
        batch.writing = true;
        loop {
            let taken = std::mem::take(&mut batch.pending);
            if taken.is_empty() {
                batch.writing = false;
                break;
            }
            drop(batch);
            self.write_batch(taken);
            batch = self.batch.lock().unwrap_or_else(|e| e.into_inner());
        }
        drop(batch);
        let (slot, _) = &*done;
        slot.lock().unwrap_or_else(|e| e.into_inner()).take().expect("written in this caller's own batch")
    }

    // One transaction for every pending enqueue. If the commit itself
    // fails, every job in it gets that error (none of them went in).
    fn write_batch(&self, batch: Vec<PendingEnqueue>) {
        let deliver = |p: &PendingEnqueue, result: Result<Enqueued, LineError>| {
            let (slot, ready) = &*p.done;
            *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            ready.notify_one();
        };
        let txn = match self.db.begin_write() {
            Ok(txn) => txn,
            Err(err) => {
                let msg = err.to_string();
                batch.iter().for_each(|p| deliver(p, Err(LineError::Storage(msg.clone()))));
                return;
            }
        };
        let mut results = Vec::with_capacity(batch.len());
        for p in &batch {
            results.push(self.enqueue_in(&txn, &p.job, &p.path, &p.pool));
        }
        // A storage error may have left one job half-written: commit none.
        if let Some(Err(LineError::Storage(msg))) = results.iter().find(|r| matches!(r, Err(LineError::Storage(_)))) {
            let msg = msg.clone();
            drop(txn);
            batch.iter().for_each(|p| deliver(p, Err(LineError::Storage(msg.clone()))));
            return;
        }
        match txn.commit() {
            Ok(()) => batch.iter().zip(results).for_each(|(p, r)| deliver(p, r)),
            Err(err) => {
                let msg = err.to_string();
                batch.iter().for_each(|p| deliver(p, Err(LineError::Storage(msg.clone()))));
            }
        }
    }

    fn enqueue_in(&self, txn: &redb::WriteTransaction, job: &JobRequest, path: &str, pool: &str) -> Result<Enqueued, LineError> {
        let payload = serde_json::to_vec(&StoredJob {
            path: path.into(),
            enqueued_at: now_unix(),
            pool: pool.into(),
            job: job.clone(),
        })?;
        let tenant = Tenant::new(job.user_id.as_str(), pool).key();
        let idem = (tenant.as_str(), job.idempotent_key.as_str());
        let mut order = txn.open_table(ORDER)?;
        let mut tenant_jobs = txn.open_table(TENANT_JOBS)?;
        let mut counts = txn.open_table(TENANT_COUNTS)?;
        let mut queued_keys = txn.open_table(QUEUED_KEYS)?;
        let results = txn.open_table(RESULTS)?;
        let mut meta = txn.open_table(META)?;
        let mut pool_counts = txn.open_table(POOL_COUNTS)?;

        if let Some(stored) = results.get(idem)? {
            let stored: StoredResult = serde_json::from_slice(stored.value())?;
            if stored.expires_at > now_unix() {
                return Ok(Enqueued::AlreadyDone(stored.result));
            }
        }
        if let Some(seq) = queued_keys.get(idem)? {
            let seq = seq.value();
            let ahead = ahead_in_pool(&order, seq, pool)?;
            return Ok(Enqueued::AlreadyQueued { ahead, seq });
        }

        let total_jobs = meta.get(TOTAL_JOBS)?.map(|v| v.value()).unwrap_or(0);
        let total_bytes = meta.get(TOTAL_BYTES)?.map(|v| v.value()).unwrap_or(0);
        let tenant_count = counts.get(tenant.as_str())?.map(|v| v.value()).unwrap_or(0);

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
        // Everyone already waiting in this pool is ahead of a new job.
        let ahead = pool_counts.get(pool)?.map(|v| v.value()).unwrap_or(0);
        pool_counts.insert(pool, ahead + 1)?;
        counts.insert(tenant.as_str(), tenant_count + 1)?;
        order.insert(seq, tenant.as_str())?;
        tenant_jobs.insert((tenant.as_str(), seq), payload.as_slice())?;
        queued_keys.insert(idem, seq)?;

        Ok(Enqueued::Queued { ahead, seq })
    }

    /// Takes the longest-waiting job out of line, skipping tenants (line
    /// keys, see Tenant::key) whose backlog is already being drained --
    /// their next job will be picked up by that drain, against the same
    /// sticky assignment -- and pools with no instance free, so one full
    /// pool doesn't hold up the others.
    pub fn pop_oldest(&self, skip: &HashSet<String>, skip_pools: &HashSet<String>) -> Result<Option<QueuedJob>, LineError> {
        let txn = self.db.begin_write()?;
        let found = {
            let order = txn.open_table(ORDER)?;
            let mut found = None;
            for entry in order.range::<u64>(..)? {
                let (seq, user_id) = entry?;
                if !skip.contains(user_id.value()) && !skip_pools.contains(Tenant::pool_of_key(user_id.value())) {
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
        lease(&txn, seq, job.as_ref())?;
        txn.commit()?;
        Ok(job.map(|stored| QueuedJob { seq, job: stored.job, path: stored.path, pool: stored.pool, enqueued_at: stored.enqueued_at }))
    }

    /// Takes a tenant's own oldest job out of line (by line key, see
    /// Tenant::key), if they have one.
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
        lease(&txn, seq, job.as_ref())?;
        txn.commit()?;
        Ok(job.map(|stored| QueuedJob { seq, job: stored.job, path: stored.path, pool: stored.pool, enqueued_at: stored.enqueued_at }))
    }

    /// Puts a job back at its original place in line -- used when a job was
    /// taken out but couldn't be handed to an instance after all. Not
    /// subject to the limits: it was already admitted once.
    pub fn requeue(&self, queued: &QueuedJob) -> Result<(), LineError> {
        let payload =
            serde_json::to_vec(&StoredJob {
                path: queued.path.clone(),
                enqueued_at: queued.enqueued_at,
                pool: queued.pool.clone(),
                job: queued.job.clone(),
            })?;
        let tenant = Tenant::new(queued.job.user_id.as_str(), queued.pool.as_str()).key();
        let user_id = tenant.as_str();
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
            txn.open_table(LEASED)?.remove(queued.seq)?;
            let mut pool_counts = txn.open_table(POOL_COUNTS)?;
            let in_pool = pool_counts.get(queued.pool.as_str())?.map(|v| v.value()).unwrap_or(0);
            pool_counts.insert(queued.pool.as_str(), in_pool + 1)?;
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

    /// Status of a job by its tenant's line key (Tenant::key) and
    /// idempotency key.
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
            let ahead = ahead_in_pool(&txn.open_table(ORDER)?, seq.value(), Tenant::pool_of_key(user_id))?;
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
        // Places count within each pool: jobs waiting for a different
        // pool's instances aren't ahead of anyone here.
        let mut per_pool: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for entry in order.range::<u64>(..)? {
            let (seq, tenant) = entry?;
            let seq = seq.value();
            let rank = per_pool.entry(Tenant::pool_of_key(tenant.value()).to_string()).or_default();
            let this = *rank;
            *rank += 1;
            if seqs.contains(&seq) {
                ranks.insert(seq, this);
                if ranks.len() == seqs.len() {
                    break;
                }
            }
        }
        Ok(ranks)
    }

    /// How long the longest-waiting job has been in line, in seconds. None
    /// when the line is empty (or its oldest job predates this being
    /// recorded).
    pub fn oldest_wait_seconds(&self) -> Result<Option<u64>, LineError> {
        let txn = self.db.begin_read()?;
        let first = txn.open_table(ORDER)?.first()?.map(|(seq, user)| (seq.value(), user.value().to_string()));
        let Some((seq, user_id)) = first else {
            return Ok(None);
        };
        let Some(payload) = txn.open_table(TENANT_JOBS)?.get((user_id.as_str(), seq))? else {
            return Ok(None);
        };
        let stored: StoredJob = serde_json::from_slice(payload.value())?;
        Ok((stored.enqueued_at > 0).then(|| now_unix().saturating_sub(stored.enqueued_at)))
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
// Jobs waiting in `pool` before `seq`.
fn ahead_in_pool(order: &impl ReadableTable<u64, &'static str>, seq: u64, pool: &str) -> Result<u64, LineError> {
    let mut ahead = 0;
    for entry in order.range(..seq)? {
        if Tenant::pool_of_key(entry?.1.value()) == pool {
            ahead += 1;
        }
    }
    Ok(ahead)
}

fn lease(txn: &redb::WriteTransaction, seq: u64, job: Option<&StoredJob>) -> Result<(), LineError> {
    if let Some(job) = job {
        txn.open_table(LEASED)?.insert(seq, serde_json::to_vec(job)?.as_slice())?;
    }
    Ok(())
}

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

    if order.remove(seq)?.is_some() {
        let pool = Tenant::pool_of_key(user_id);
        let mut pool_counts = txn.open_table(POOL_COUNTS)?;
        let in_pool = pool_counts.get(pool)?.map(|v| v.value()).unwrap_or(0);
        if in_pool <= 1 {
            pool_counts.remove(pool)?;
        } else {
            pool_counts.insert(pool, in_pool - 1)?;
        }
    }
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
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "2"), "/jobs", DEFAULT_POOL).unwrap();

        let first = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert_eq!((first.job.user_id.as_str(), first.job.idempotent_key.as_str()), ("b", "1"));
        let second = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert_eq!(second.job.user_id, "a");
        assert_eq!(line.len().unwrap(), 1);
    }

    #[test]
    fn pop_oldest_skips_tenants_already_being_drained() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("busy", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("waiting", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let skip: HashSet<String> = ["busy".to_string()].into();
        let popped = line.pop_oldest(&skip, &HashSet::new()).unwrap().unwrap();
        assert_eq!(popped.job.user_id, "waiting");
    }

    #[test]
    fn pop_for_is_fifo_within_one_tenant() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("other", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("a", "2"), "/jobs", DEFAULT_POOL).unwrap();

        assert_eq!(line.pop_for("a").unwrap().unwrap().job.idempotent_key, "1");
        assert_eq!(line.pop_for("a").unwrap().unwrap().job.idempotent_key, "2");
        assert!(line.pop_for("a").unwrap().is_none());
        assert_eq!(line.len().unwrap(), 1, "the other tenant's job stays in line");
    }

    #[test]
    fn requeue_puts_a_job_back_in_its_original_place() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let taken = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        line.requeue(&taken).unwrap();

        let again = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert_eq!(again.job.user_id, "a", "a requeued job must not lose its place to b");
    }

    #[test]
    fn a_retried_submission_does_not_take_a_second_place_in_line() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "same"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let again = line.enqueue(&job("a", "same"), "/jobs", DEFAULT_POOL).unwrap();
        assert!(matches!(again, Enqueued::AlreadyQueued { ahead: 0, seq: 0 }));
        assert_eq!(line.len().unwrap(), 2);
    }

    #[test]
    fn same_key_from_different_tenants_are_different_jobs() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "k1"), "/jobs", DEFAULT_POOL).unwrap();
        let other = line.enqueue(&job("b", "k1"), "/jobs", DEFAULT_POOL).unwrap();
        assert!(matches!(other, Enqueued::Queued { .. }));
        assert_eq!(line.len().unwrap(), 2);
    }

    #[test]
    fn a_finished_job_resubmitted_returns_its_cached_result() {
        let (line, _dir) = open(LineLimits::default());
        let result = CachedResult { status: 200, content_type: "text/plain".into(), body: "ok".into(), ..Default::default() };
        line.store_result("a", "k", &result).unwrap();

        match line.enqueue(&job("a", "k"), "/jobs", DEFAULT_POOL).unwrap() {
            Enqueued::AlreadyDone(cached) => assert_eq!(cached, result),
            other => panic!("expected the cached result, got {other:?}"),
        }
        assert_eq!(line.len().unwrap(), 0);
    }

    #[test]
    fn full_line_blocks_instead_of_growing() {
        let limits = LineLimits { max_jobs: 2, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let err = line.enqueue(&job("c", "1"), "/jobs", DEFAULT_POOL).unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_jobs", limit: 2 }));

        // Room frees up as soon as someone leaves the line.
        line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap();
        line.enqueue(&job("c", "1"), "/jobs", DEFAULT_POOL).unwrap();
    }

    #[test]
    fn one_tenant_cannot_take_the_whole_line() {
        let limits = LineLimits { max_per_tenant: 1, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("noisy", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let err = line.enqueue(&job("noisy", "2"), "/jobs", DEFAULT_POOL).unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_tenant", .. }));
        line.enqueue(&job("quiet", "1"), "/jobs", DEFAULT_POOL).expect("another tenant still gets in");
    }

    #[test]
    fn byte_limit_counts_queued_payloads() {
        // One serialized test job is ~120 bytes: room for one, not two.
        let limits = LineLimits { max_bytes: 200, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();

        let err = line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap_err();
        assert!(matches!(err, LineError::Full { reason: "line_bytes", .. }));
    }

    #[test]
    fn lookup_reports_place_in_line_then_the_result() {
        let (line, _dir) = open(LineLimits::default());
        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();

        assert!(matches!(line.lookup("b", "1").unwrap(), Lookup::Queued { ahead: 1 }));
        assert!(matches!(line.lookup("nobody", "1").unwrap(), Lookup::Unknown));

        line.pop_for("b").unwrap();
        let result = CachedResult { status: 200, content_type: String::new(), body: "done".into(), ..Default::default() };
        line.store_result("b", "1", &result).unwrap();
        assert!(matches!(line.lookup("b", "1").unwrap(), Lookup::Done(_)));
    }

    #[test]
    fn ranks_report_how_many_are_ahead_in_one_pass() {
        let (line, _dir) = open(LineLimits::default());
        let mut seqs = Vec::new();
        for user in ["a", "b", "c"] {
            match line.enqueue(&job(user, "1"), "/jobs", DEFAULT_POOL).unwrap() {
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
        line.enqueue(&job("a", "1"), "/proxy", DEFAULT_POOL).unwrap();
        let popped = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
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
    fn the_oldest_wait_follows_the_front_of_the_line_and_survives_a_requeue() {
        let (line, _dir) = open(LineLimits::default());
        assert_eq!(line.oldest_wait_seconds().unwrap(), None, "empty line");

        line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
        line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();
        assert_eq!(line.oldest_wait_seconds().unwrap(), Some(0));

        let taken = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert!(taken.enqueued_at > 0);
        line.requeue(&taken).unwrap();
        assert_eq!(line.pop_for("a").unwrap().unwrap().enqueued_at, taken.enqueued_at, "a requeue keeps its arrival time");
    }

    #[test]
    fn expired_results_are_swept() {
        let limits = LineLimits { result_ttl_seconds: 0, ..LineLimits::default() };
        let (line, _dir) = open(limits);
        let result = CachedResult { status: 200, content_type: String::new(), body: String::new(), ..Default::default() };
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
            line.enqueue(&job("a", "1"), "/jobs", DEFAULT_POOL).unwrap();
            line.enqueue(&job("b", "1"), "/jobs", DEFAULT_POOL).unwrap();
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
        assert_eq!(line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap().job.user_id, "a");
        assert_eq!(line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap().job.user_id, "b");
    }

    #[test]
    fn a_job_taken_out_but_never_finished_is_back_in_line_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("line.redb");
        {
            let line = Line::open(&path, LineLimits::default()).unwrap();
            line.enqueue(&job("acme", "lost"), "/jobs", DEFAULT_POOL).unwrap();
            line.enqueue(&job("acme", "done"), "/jobs", DEFAULT_POOL).unwrap();
            let lost = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
            let done = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
            line.finish(done.seq).unwrap();
            assert_eq!((line.len().unwrap(), line.leased().unwrap()), (0, 1));
            assert_eq!(lost.job.idempotent_key, "lost");
            // Dropped here without finishing `lost`: the crash.
        }
        let line = Line::open(&path, LineLimits::default()).unwrap();
        assert_eq!((line.len().unwrap(), line.leased().unwrap()), (1, 0));
        let back = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert_eq!(back.job.idempotent_key, "lost");
    }

    #[test]
    fn requeue_ends_the_lease() {
        let dir = tempfile::tempdir().unwrap();
        let line = Line::open(dir.path().join("line.redb"), LineLimits::default()).unwrap();
        line.enqueue(&job("acme", "k"), "/jobs", DEFAULT_POOL).unwrap();
        let taken = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        line.requeue(&taken).unwrap();
        assert_eq!((line.len().unwrap(), line.leased().unwrap()), (1, 0));
    }

    #[test]
    fn a_full_pool_does_not_hold_up_another() {
        let dir = tempfile::tempdir().unwrap();
        let line = Line::open(dir.path().join("line.redb"), LineLimits::default()).unwrap();
        line.enqueue(&job("a", "1"), "/jobs", "gpu").unwrap();
        line.enqueue(&job("b", "1"), "/jobs", "gpu").unwrap();
        let cpu = line.enqueue(&job("c", "1"), "/jobs", DEFAULT_POOL).unwrap();
        assert!(matches!(cpu, Enqueued::Queued { ahead: 0, .. }), "jobs in another pool aren't ahead of it");

        let blocked: HashSet<String> = ["gpu".to_string()].into();
        let next = line.pop_oldest(&HashSet::new(), &blocked).unwrap().unwrap();
        assert_eq!((next.job.user_id.as_str(), next.pool.as_str()), ("c", DEFAULT_POOL));

        let gpu = line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap().unwrap();
        assert_eq!((gpu.job.user_id.as_str(), gpu.pool.as_str()), ("a", "gpu"));
        line.requeue(&gpu).unwrap();
        let ranks = line.ranks(&[gpu.seq].into()).unwrap();
        assert_eq!(ranks[&gpu.seq], 0, "back at the front of its pool");
    }

    #[test]
    fn the_same_key_in_two_pools_is_two_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let line = Line::open(dir.path().join("line.redb"), LineLimits::default()).unwrap();
        line.enqueue(&job("acme", "k"), "/jobs", "gpu").unwrap();
        let other = line.enqueue(&job("acme", "k"), "/jobs", DEFAULT_POOL).unwrap();
        assert!(matches!(other, Enqueued::Queued { .. }));
        assert_eq!(line.len().unwrap(), 2);
        let gpu_key = Tenant::new("acme", "gpu").key();
        assert!(matches!(line.lookup(&gpu_key, "k").unwrap(), Lookup::Queued { ahead: 0 }));
    }

    #[test]
    fn concurrent_enqueues_are_committed_together_and_all_land() {
        let dir = tempfile::tempdir().unwrap();
        let line = std::sync::Arc::new(Line::open(dir.path().join("line.redb"), LineLimits::default()).unwrap());
        let handles: Vec<_> = (0..50)
            .map(|i| {
                let line = line.clone();
                std::thread::spawn(move || line.enqueue(&job(&format!("u{i}"), "k"), "/jobs", DEFAULT_POOL).unwrap())
            })
            .collect();
        let mut aheads: Vec<u64> = handles
            .into_iter()
            .map(|h| match h.join().unwrap() {
                Enqueued::Queued { ahead, .. } => ahead,
                other => panic!("expected Queued, got {other:?}"),
            })
            .collect();
        aheads.sort();
        assert_eq!(aheads, (0..50).collect::<Vec<u64>>(), "every job gets its own place");
        assert_eq!(line.len().unwrap(), 50);
    }

    #[test]
    fn places_in_line_are_right_after_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("line.redb");
        {
            let line = Line::open(&path, LineLimits::default()).unwrap();
            line.enqueue(&job("a", "1"), "/jobs", "gpu").unwrap();
            line.enqueue(&job("b", "1"), "/jobs", "gpu").unwrap();
            line.enqueue(&job("c", "1"), "/jobs", DEFAULT_POOL).unwrap();
            line.pop_oldest(&HashSet::new(), &HashSet::new()).unwrap(); // a, gpu
        }
        let line = Line::open(&path, LineLimits::default()).unwrap();
        assert!(matches!(line.enqueue(&job("d", "1"), "/jobs", "gpu").unwrap(), Enqueued::Queued { ahead: 2, .. }), "b, plus a restored from its lease");
        assert!(matches!(line.enqueue(&job("e", "1"), "/jobs", DEFAULT_POOL).unwrap(), Enqueued::Queued { ahead: 1, .. }));
    }
}
