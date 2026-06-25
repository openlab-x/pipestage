"""
pipestage -- Rate-Limited Notification Sender

Send 80 notifications (email / sms / push) concurrently.
for_each is the natural terminal when you want side effects, not a collected list.

Feature demonstrated: for_each as primary terminal operation.
Run standalone:  python ps_notify.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=33, matches raw_notify.py) -------------------------------

N_NOTIFS    = 80
CONCURRENCY = 10

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
    counts: dict[str, int] = {"email": 0, "sms": 0, "push": 0}

    async def send(notif: dict) -> None:
        await _send(notif)
        counts[notif["type"]] += 1

    await (
        stream(NOTIFICATIONS)
        .for_each(send, concurrency=CONCURRENCY)
    )
    return counts


if __name__ == "__main__":
    t0 = time.monotonic()
    counts = asyncio.run(run())
    print(f"[pipestage]    sent {sum(counts.values())}  "
          f"email={counts['email']} sms={counts['sms']} push={counts['push']}  "
          f"in {time.monotonic()-t0:.3f}s")
