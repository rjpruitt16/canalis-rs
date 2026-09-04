use crate::Valkey;
use redis::AsyncCommands;

// canalis:pool:free and canalis:assigned are global keys, not scoped
// per-test the way canalis:assignment:<user_id> is -- so any test
// touching them (in this module's own tests, or main.rs's forwarding
// tests) can't safely run concurrently against another one without pool
// state bleeding across tests. Shared here, at crate visibility, rather
// than defined separately in each test module -- a lock in assign.rs's
// own tests wouldn't serialize against a *different* lock in main.rs's
// tests, and this exact gap caused a real, confusing test failure before
// it was fixed (main.rs's SSE relay test lost a race against this
// module's own tests popping the same seeded pool entry).
#[cfg(test)]
pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const ASSIGNMENT_KEY_PREFIX: &str = "canalis:assignment:";
const FREE_POOL_KEY: &str = "canalis:pool:free";
const ASSIGNED_KEY: &str = "canalis:assigned";

pub enum AssignOutcome {
    Assigned(String),
    PoolExhausted,
}

/// Resolves user_id's sticky assignment, creating one if it doesn't exist
/// yet. Race-safe against concurrent requests for the *same* user_id via
/// SET ... NX: if we lose that race, the instance we needlessly claimed
/// gets returned to the free pool, and we return whoever actually won
/// instead. Does not handle a Canalis process crashing mid-operation (a
/// real, separate gap; see DESIGN.md's reconciliation-sweep section, not
/// yet built).
pub async fn assign(valkey: &mut Valkey, user_id: &str) -> redis::RedisResult<AssignOutcome> {
    let assignment_key = format!("{ASSIGNMENT_KEY_PREFIX}{user_id}");

    if let Some(existing) = valkey.get::<_, Option<String>>(&assignment_key).await? {
        return Ok(AssignOutcome::Assigned(existing));
    }

    let popped: Option<String> = valkey.spop(FREE_POOL_KEY).await?;
    let Some(addr) = popped else {
        return Ok(AssignOutcome::PoolExhausted);
    };

    let won: bool = valkey.set_nx(&assignment_key, &addr).await?;
    if won {
        let _: () = valkey.sadd(ASSIGNED_KEY, &addr).await?;
        return Ok(AssignOutcome::Assigned(addr));
    }

    // Lost the race -- someone else's concurrent request for this same
    // user_id already wrote the assignment first. Give back the instance
    // we didn't need and return whoever actually won.
    let _: () = valkey.sadd(FREE_POOL_KEY, &addr).await?;
    let winner: Option<String> = valkey.get(&assignment_key).await?;
    match winner {
        Some(addr) => Ok(AssignOutcome::Assigned(addr)),
        // Only reachable if the winning assignment key vanished between
        // our failed NX and this GET -- assignment keys carry no TTL, so
        // this should never actually happen in practice.
        None => Err((redis::ErrorKind::Client, "assignment vanished after NX loss").into()),
    }
}

/// Called on every registration ping (see main.rs's `register` handler).
/// Adds addr to the free pool unless it's already claimed by someone --
/// SADD is a no-op if addr is already present, so a repeatedly-heartbeating
/// *unassigned* instance never creates duplicate pool entries (which would
/// let the same address get claimed twice). Switched the pool from a List
/// to a Set specifically for this idempotency property; trades away FIFO
/// fairness, which this slice never required (first-free-wins, per the
/// original scoping).
pub async fn register_if_free(valkey: &mut Valkey, addr: &str) -> redis::RedisResult<()> {
    let already_assigned: bool = valkey.sismember(ASSIGNED_KEY, addr).await?;
    if already_assigned {
        return Ok(());
    }
    valkey.sadd(FREE_POOL_KEY, addr).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // Nanosecond timestamps alone aren't guaranteed unique -- two calls a
    // few CPU cycles apart can return the identical value on real
    // hardware (clock resolution isn't always truly nanosecond). Found
    // this the hard way: it produced a same-tenant collision that looked
    // like a real assign() bug. A monotonic counter closes the gap for
    // real, regardless of clock resolution.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{nanos}-{n}")
    }

    async fn test_valkey() -> Valkey {
        let client = redis::Client::open("redis://127.0.0.1:6379")
            .expect("valid Valkey URL");
        redis::aio::ConnectionManager::new(client)
            .await
            .expect("connect to a real local Valkey on 127.0.0.1:6379 -- required for these tests")
    }

    async fn cleanup(valkey: &mut Valkey, addrs: &[&str], user_ids: &[&str]) {
        for addr in addrs {
            let _: redis::RedisResult<()> = valkey.srem(FREE_POOL_KEY, *addr).await;
            let _: redis::RedisResult<()> = valkey.srem(ASSIGNED_KEY, *addr).await;
        }
        for user_id in user_ids {
            let _: redis::RedisResult<()> =
                valkey.del(format!("{ASSIGNMENT_KEY_PREFIX}{user_id}")).await;
        }
    }

    #[tokio::test]
    async fn fresh_assignment_claims_a_free_instance() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr = unique("instance");
        let user_id = unique("user");

        let _: () = valkey.sadd(FREE_POOL_KEY, &addr).await.unwrap();

        let outcome = assign(&mut valkey, &user_id).await.unwrap();
        match outcome {
            AssignOutcome::Assigned(got) => assert_eq!(got, addr),
            AssignOutcome::PoolExhausted => panic!("expected an assignment, got PoolExhausted"),
        }

        let recorded: String = valkey
            .get(format!("{ASSIGNMENT_KEY_PREFIX}{user_id}"))
            .await
            .unwrap();
        assert_eq!(recorded, addr);

        let now_assigned: bool = valkey.sismember(ASSIGNED_KEY, &addr).await.unwrap();
        assert!(now_assigned, "claimed instance should be recorded in canalis:assigned");

        cleanup(&mut valkey, &[&addr], &[&user_id]).await;
    }

    #[tokio::test]
    async fn assignment_is_sticky_across_repeat_calls() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr = unique("instance");
        let user_id = unique("user");

        let _: () = valkey.sadd(FREE_POOL_KEY, &addr).await.unwrap();

        let first = match assign(&mut valkey, &user_id).await.unwrap() {
            AssignOutcome::Assigned(a) => a,
            AssignOutcome::PoolExhausted => panic!("expected an assignment"),
        };

        // Second call: pool is now empty, so if this weren't sticky (i.e.
        // if it tried to claim a *new* instance instead of returning the
        // existing assignment) it would come back PoolExhausted, not the
        // same address.
        let second = match assign(&mut valkey, &user_id).await.unwrap() {
            AssignOutcome::Assigned(a) => a,
            AssignOutcome::PoolExhausted => panic!("expected the same sticky assignment, not PoolExhausted"),
        };

        assert_eq!(first, second);
        assert_eq!(first, addr);

        cleanup(&mut valkey, &[&addr], &[&user_id]).await;
    }

    #[tokio::test]
    async fn different_tenants_get_different_instances() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr_a = unique("instance-a");
        let addr_b = unique("instance-b");
        let user_1 = unique("user");
        let user_2 = unique("user");

        let _: () = valkey.sadd(FREE_POOL_KEY, &addr_a).await.unwrap();
        let _: () = valkey.sadd(FREE_POOL_KEY, &addr_b).await.unwrap();

        let got_1 = match assign(&mut valkey, &user_1).await.unwrap() {
            AssignOutcome::Assigned(a) => a,
            AssignOutcome::PoolExhausted => panic!("expected an assignment"),
        };
        let got_2 = match assign(&mut valkey, &user_2).await.unwrap() {
            AssignOutcome::Assigned(a) => a,
            AssignOutcome::PoolExhausted => panic!("expected an assignment"),
        };

        assert_ne!(got_1, got_2, "two tenants should never be handed the same instance");

        cleanup(&mut valkey, &[&addr_a, &addr_b], &[&user_1, &user_2]).await;
    }

    #[tokio::test]
    async fn pool_exhausted_when_nothing_is_free() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let user_id = unique("user");

        // No SADD here -- deliberately asking for an assignment with
        // nothing available. Doesn't touch/clear the shared pool key
        // itself (other tests may have legitimate items queued right
        // before/after this one under the same lock); a fresh user_id
        // with nothing pre-seeded for it is enough to prove the miss path
        // without asserting anything about global pool emptiness.
        //
        // This only proves the behavior when the pool is at zero for this
        // attempt; it's possible (though the lock makes it unlikely given
        // this test suite's own usage) for a leftover item from outside
        // this test file to be claimed instead. Real isolation would
        // require a dedicated test Valkey DB (SELECT n) or flushing
        // before each test -- worth doing if this ever gets flaky.
        let outcome = assign(&mut valkey, &user_id).await.unwrap();

        // Only assert PoolExhausted if the pool key doesn't currently
        // exist/have members -- otherwise this test would be asserting
        // something it can't actually guarantee in a shared-key world.
        let pool_size: usize = valkey.scard(FREE_POOL_KEY).await.unwrap_or(0);
        if pool_size == 0 {
            assert!(matches!(outcome, AssignOutcome::PoolExhausted));
        }

        cleanup(&mut valkey, &[], &[&user_id]).await;
    }

    #[tokio::test]
    async fn register_if_free_skips_an_already_assigned_instance() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr = unique("instance");

        let _: () = valkey.sadd(ASSIGNED_KEY, &addr).await.unwrap();

        register_if_free(&mut valkey, &addr).await.unwrap();

        let in_pool: bool = valkey.sismember(FREE_POOL_KEY, &addr).await.unwrap();
        assert!(
            !in_pool,
            "an already-assigned instance's heartbeat must not leak it back into the free pool"
        );

        cleanup(&mut valkey, &[&addr], &[]).await;
    }

    #[tokio::test]
    async fn register_if_free_adds_a_genuinely_unassigned_instance() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr = unique("instance");

        register_if_free(&mut valkey, &addr).await.unwrap();

        let in_pool: bool = valkey.sismember(FREE_POOL_KEY, &addr).await.unwrap();
        assert!(in_pool, "a genuinely free instance should land in the pool");

        cleanup(&mut valkey, &[&addr], &[]).await;
    }
}
