"""Prints the 4-day Canalis soak's failure schedule for the driver's
-scenarios flag (paste into fly.driver.toml).

Nodes: a and b are the Canalis nodes, q1 the Aquifer behind them. A leak
soak still needs churn, because leaks hide in reconnects and recovery
(peer HTTP/2 connections, line-ownership handoffs, Valkey reconnects, a line
that fills and drains), but less than a correctness soak: 8 failures a day.
Each day repeats one pattern in a seeded random order with jittered gaps,
so the same schedule comes out every time. Times are offsets from the
previous failure, as the driver wants them.
"""
import random

DAY = [
    "kill:a", "kill:b",       # kill -9: the line on disk, ownership moves
    "hold:a:5m",              # down past the 30s ownership lease: b takes a's lines
    "freeze:b:20s",           # a stall shorter than the lease
    "valkey:2m",              # the shared assignment table goes away
    "hold:q1:5m",             # nothing to hand work to: the line fills, then drains
    "kill:q1",                # the instance dies under Canalis
    "drain:a", "drain:b",     # a rolling restart, b right after a
]


def schedule(seed=7, days=4):
    rng = random.Random(seed)
    out = []
    for _ in range(days):
        todays = DAY[:]
        rng.shuffle(todays)
        # keep the rolling restart together
        todays.remove("drain:b")
        todays.insert(todays.index("drain:a") + 1, "drain:b")
        gap = 24 * 60 / (len(todays) - 1)
        for f in todays:
            minutes = 10 if f == "drain:b" else round(gap * rng.uniform(0.6, 1.4))
            out.append(f"{minutes}m:{f}")
    out[0] = "120m:" + out[0].split(":", 1)[1]  # a two-hour warm-up first
    return ",".join(out)


if __name__ == "__main__":
    print(schedule())
