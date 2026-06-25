"""
Raw asyncio -- Rate-Limited Notification Sender

Send 80 notifications (email / sms / push) concurrently.
Concurrency limit acts as a natural rate limiter.
Raw asyncio: gather with semaphore, track counts via shared lock.

Feature demonstrated: side-effect pipeline (for_each equivalent).
Run standalone:  python raw_notify.py
"""
import asyncio
import random
import time

# -- Simulation (seed=33, matches ps_notify.py) --------------------------------

N_NOTIFS    = 80
CONCURRENCY = 10    # max concurrent sends = effective rate limit

_rng = random.Random(33)
NOTIFICATIONS = [
    {
        "id": i,
        "type": _rng.choice(["email", "sms", "push"]),
        "recipient": f"user_{i}@example.com",
        "body": f"Notification body {i}",
    }
    for i in range(N_NOTIFS)
]
_SEND_DELAYS = {n["id"]: _rng.uniform(0.02, 0.07) for n in NOTIFICATIONS}


async def _send(notification: dict) -> None:
    await asyncio.sleep(_SEND_DELAYS[notification["id"]])


# -- Implementation ------------------------------------------------------------

async def run() -> dict[str, int]:
    sem = asyncio.Semaphore(CONCURRENCY)
    counts: dict[str, int] = {"email": 0, "sms": 0, "push": 0}
    lock = asyncio.Lock()

    async def send(notif: dict) -> None:
        async with sem:
            await _send(notif)
        async with lock:
            counts[notif["type"]] += 1

    tasks = [asyncio.create_task(send(n)) for n in NOTIFICATIONS]
    await asyncio.gather(*tasks)
    return counts


if __name__ == "__main__":
    t0 = time.monotonic()
    counts = asyncio.run(run())
    print(f"[raw asyncio]  sent {sum(counts.values())}  "
          f"email={counts['email']} sms={counts['sms']} push={counts['push']}  "
          f"in {time.monotonic()-t0:.3f}s")
