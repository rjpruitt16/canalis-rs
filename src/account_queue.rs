use crate::{JobRequest, Valkey};
use redis::AsyncCommands;

const PENDING_TENANTS_KEY: &str = "canalis:pending_tenants";

// Short TTL on purpose: this is a completed result sitting around for
// whoever's still waiting (the connection that queued it, or a future
// polling endpoint) to pick up -- not a permanent record. Separate,
// smaller lifetime than any longer-lived idempotency dedup marker would
// have (that's the next slice's concern, not this one's).
const RESULT_TTL_SECONDS: u64 = 300;

fn account_queue_key(user_id: &str) -> String {
    format!("canalis:account_queue:{user_id}")
}

fn result_key(idempotent_key: &str) -> String {
    format!("canalis:result:{idempotent_key}")
}

/// Durably queues a job for later draining -- survives a Canalis crash,
/// unlike holding it only in an in-memory retry loop. Called when the
/// pool is exhausted and there's genuinely nothing to assign right now.
/// canalis:pending_tenants tracks *which* tenants have something queued,
/// so draining doesn't need to scan every possible user_id to find work.
pub async fn enqueue(valkey: &mut Valkey, job: &JobRequest) -> redis::RedisResult<()> {
    let payload = serde_json::to_string(job).map_err(|e| {
        redis::RedisError::from((
            redis::ErrorKind::Client,
            "failed to serialize job for durable queueing",
            e.to_string(),
        ))
    })?;
    let _: () = valkey.lpush(account_queue_key(&job.user_id), payload).await?;
    valkey.sadd(PENDING_TENANTS_KEY, &job.user_id).await
}

/// Pops one pending tenant's oldest queued job, if any exist. Called when
/// a new instance frees up (see main.rs's register handler). No fairness
/// ordering across *which* tenant gets drained first -- dropped as a
/// requirement earlier ("the number of machines is the only mechanism
/// for serving N customers," not who's waited longest) -- but FIFO
/// *within* one tenant's own queue, via RPOP against a list built with
/// LPUSH.
pub async fn pop_one(valkey: &mut Valkey) -> redis::RedisResult<Option<JobRequest>> {
    let Some(user_id): Option<String> = valkey.spop(PENDING_TENANTS_KEY).await? else {
        return Ok(None);
    };

    let queue_key = account_queue_key(&user_id);
    let popped: Option<String> = valkey.rpop(&queue_key, None).await?;
    let Some(payload) = popped else {
        // Only reachable if pending_tenants and the queue itself somehow
        // disagreed -- they're always written together in enqueue, so
        // this shouldn't happen in practice. Treat as "nothing to drain"
        // rather than erroring the whole drain attempt over it.
        return Ok(None);
    };

    // If this tenant still has more queued after this pop, put them back
    // so a later drain picks up their next job too.
    let remaining: i64 = valkey.llen(&queue_key).await?;
    if remaining > 0 {
        let _: () = valkey.sadd(PENDING_TENANTS_KEY, &user_id).await?;
    }

    match serde_json::from_str(&payload) {
        Ok(job) => Ok(Some(job)),
        // A corrupted entry shouldn't be able to jam the whole drain
        // pipeline forever -- drop it and let the caller try the next one.
        Err(err) => {
            tracing::error!("dropping unparseable queued job for {user_id}: {err}");
            Ok(None)
        }
    }
}

/// Pops the next job directly from user_id's own queue, bypassing the
/// pending_tenants selection step in pop_one(). Used to drain the rest of
/// a tenant's backlog once pop_one() has already resolved which tenant
/// to drain and an address has been assigned for them -- every remaining
/// job in that same tenant's queue goes to the same sticky assignment, so
/// there's no reason to re-roll which tenant gets drained next.
///
/// Deliberately doesn't touch pending_tenants either way: pop_one()
/// already re-adds user_id there if more than one job was queued, so a
/// second, concurrent drain attempt landing on this same tenant mid-loop
/// just finds an empty queue and no-ops, rather than racing this one for
/// jobs.
pub async fn pop_for(valkey: &mut Valkey, user_id: &str) -> redis::RedisResult<Option<JobRequest>> {
    let popped: Option<String> = valkey.rpop(account_queue_key(user_id), None).await?;
    let Some(payload) = popped else {
        return Ok(None);
    };

    match serde_json::from_str(&payload) {
        Ok(job) => Ok(Some(job)),
        Err(err) => {
            tracing::error!("dropping unparseable queued job for {user_id}: {err}");
            Ok(None)
        }
    }
}

/// The cached outcome of a drained job -- everything the still-waiting
/// connection (or a future polling endpoint) needs to reconstruct the
/// same response it would have gotten synchronously.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct CachedResult {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

pub async fn store_result(
    valkey: &mut Valkey,
    idempotent_key: &str,
    result: &CachedResult,
) -> redis::RedisResult<()> {
    let payload = serde_json::to_string(result).map_err(|e| {
        redis::RedisError::from((
            redis::ErrorKind::Client,
            "failed to serialize result",
            e.to_string(),
        ))
    })?;
    valkey
        .set_ex(result_key(idempotent_key), payload, RESULT_TTL_SECONDS)
        .await
}

pub async fn get_result(
    valkey: &mut Valkey,
    idempotent_key: &str,
) -> redis::RedisResult<Option<CachedResult>> {
    let payload: Option<String> = valkey.get(result_key(idempotent_key)).await?;
    Ok(payload.and_then(|p| serde_json::from_str(&p).ok()))
}
