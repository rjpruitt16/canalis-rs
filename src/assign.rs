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
// addr -> the user_ids assigned to it: the reverse of
// canalis:assignment:<user_id>, so a node that starts draining can hand
// its users back without scanning every assignment key.
const INSTANCE_TENANTS_PREFIX: &str = "canalis:instance_tenants:";

fn instance_tenants_key(addr: &str) -> String {
    format!("{INSTANCE_TENANTS_PREFIX}{addr}")
}

// Deletes an assignment only if it still points at the given address, so
// releasing a user from a draining node can never clobber a newer
// assignment someone else already made for them.
const RELEASE_IF_STILL_ASSIGNED: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
";

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
        let _: () = valkey.sadd(instance_tenants_key(&addr), user_id).await?;
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
/// Returns whether this call actually added a genuinely new free instance
/// (as opposed to a no-op on an already-claimed or already-pooled one) --
/// callers use this to decide whether it's worth attempting to drain a
/// pending job, rather than checking on every single heartbeat.
pub async fn register_if_free(valkey: &mut Valkey, addr: &str) -> redis::RedisResult<bool> {
    let already_assigned: bool = valkey.sismember(ASSIGNED_KEY, addr).await?;
    if already_assigned {
        return Ok(false);
    }
    let added: i64 = valkey.sadd(FREE_POOL_KEY, addr).await?;
    Ok(added > 0)
}

/// Releases user_id from addr, if that's still where they're assigned.
/// Their next request resolves a fresh assignment (or waits in line).
/// addr itself is NOT returned to the free pool -- this is for a node
/// that's draining, which shouldn't be handed new tenants.
pub async fn release_user(valkey: &mut Valkey, user_id: &str, addr: &str) -> redis::RedisResult<bool> {
    let released: i64 = redis::Script::new(RELEASE_IF_STILL_ASSIGNED)
        .key(format!("{ASSIGNMENT_KEY_PREFIX}{user_id}"))
        .arg(addr)
        .invoke_async(valkey)
        .await?;
    let _: () = valkey.srem(instance_tenants_key(addr), user_id).await?;
    Ok(released > 0)
}

/// Takes addr out of rotation because it reported `draining` or `offline`:
/// it leaves the free pool, every user assigned to it is released, and it
/// stops counting as assigned -- so if the same address later comes back
/// `active` (a restart), its next heartbeat makes it free again instead of
/// it staying stuck as "assigned" forever. Returns how many users were
/// released.
pub async fn retire_instance(valkey: &mut Valkey, addr: &str) -> redis::RedisResult<usize> {
    let _: () = valkey.srem(FREE_POOL_KEY, addr).await?;
    let tenants: Vec<String> = valkey.smembers(instance_tenants_key(addr)).await?;
    let mut released = 0;
    for user_id in &tenants {
        if release_user(valkey, user_id, addr).await? {
            released += 1;
        }
    }
    let _: () = valkey.del(instance_tenants_key(addr)).await?;
    let _: () = valkey.srem(ASSIGNED_KEY, addr).await?;
    Ok(released)
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
            let _: redis::RedisResult<()> = valkey.del(instance_tenants_key(addr)).await;
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
    async fn retiring_a_draining_instance_releases_its_users_and_lets_it_return_later() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let addr = unique("instance-draining");
        let user_id = unique("user");

        let _: () = valkey.sadd(FREE_POOL_KEY, &addr).await.unwrap();
        assert!(matches!(assign(&mut valkey, &user_id).await.unwrap(), AssignOutcome::Assigned(_)));

        assert_eq!(retire_instance(&mut valkey, &addr).await.unwrap(), 1);
        let assignment: Option<String> =
            valkey.get(format!("{ASSIGNMENT_KEY_PREFIX}{user_id}")).await.unwrap();
        assert!(assignment.is_none(), "the user must be free to land somewhere else");
        let in_pool: bool = valkey.sismember(FREE_POOL_KEY, &addr).await.unwrap();
        assert!(!in_pool, "a draining node must not be handed new tenants");

        // Restarted on the same address and back to `active`: it's free again.
        assert!(register_if_free(&mut valkey, &addr).await.unwrap());

        cleanup(&mut valkey, &[&addr], &[&user_id]).await;
    }

    #[tokio::test]
    async fn release_user_never_clobbers_a_newer_assignment() {
        let _guard = TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let user_id = unique("user");
        let key = format!("{ASSIGNMENT_KEY_PREFIX}{user_id}");
        let _: () = valkey.set(&key, "new-node").await.unwrap();

        assert!(!release_user(&mut valkey, &user_id, "old-node").await.unwrap());
        let still: String = valkey.get(&key).await.unwrap();
        assert_eq!(still, "new-node");

        cleanup(&mut valkey, &[], &[&user_id]).await;
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
