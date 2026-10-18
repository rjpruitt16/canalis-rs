"""Prints the week's failure schedule for the driver's -scenarios flag.

Nodes: a and b are the Canalis nodes, q1 and q2 the Aquifers, s Spillway.
Each day repeats one pattern, in a seeded random order with jittered gaps,
so the same build gets the same schedule (rerun to see it; paste into
fly.driver.toml). Times are offsets from the previous failure, as the
driver wants them; the day's failures fill about 24 hours; the first comes after a two-hour warm-up.
"""
import random

DAY = [
    "kill:a", "kill:b", "kill:a", "kill:b",  # Canalis kill -9: line on disk, ownership moves
    "kill:q1", "kill:q2", "kill:s",          # instances die under Canalis
    "hold:a:5m",                             # a Canalis node down long enough for its claims to expire
    "freeze:b:20s",                          # a stall shorter than the 30s ownership lease
    "valkey:2m",                             # the shared assignment table goes away
    "hold:q1+q2:5m",                         # only Spillway left: the line fills
    "drain:a", "drain:b",                    # a rolling restart, b right after a
]
EVERY_OTHER_DAY = ["hold:q1+q2+s:3m"]        # nothing to deliver to: the line holds everything

def week(seed=7, days=7):
    rng = random.Random(seed)
    out = []
    for day in range(days):
        todays = DAY + (EVERY_OTHER_DAY if day % 2 else [])
        rng.shuffle(todays)
        # keep the rolling restart together
        todays.remove("drain:b")
        todays.insert(todays.index("drain:a") + 1, "drain:b")
        gap = 24 * 60 / (len(todays) - 1)
        for f in todays:
            minutes = 10 if f == "drain:b" else round(gap * rng.uniform(0.6, 1.4))
            out.append(f"{minutes}m:{f}")
    out[0] = "120m:" + out[0].split(":", 1)[1]
    return ",".join(out)

if __name__ == "__main__":
    print(week())
