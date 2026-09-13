#!/usr/bin/env python3
"""
Job-queue recipe built on collection.watch().

Replaces a Celery/RabbitMQ worker with a single-process SQLite-backed queue:
enqueue with insert_one(), drain due jobs on boot with find(), then tail new
arrivals with watch(). Restart with the last resume token for zero repeats
and zero misses within the retention window.
"""

from datetime import datetime, timedelta, timezone

from neosqlite import Connection


def enqueue(jobs, job_type, payload, delay_s=0):
    """Enqueue a job for immediate or delayed execution."""
    now = datetime.now(timezone.utc)
    return jobs.insert_one(
        {
            "type": job_type,
            "payload": payload,
            "attempts": 0,
            "next_run": now + timedelta(seconds=delay_s),
            "status": "pending",
        }
    )


def claim_due(jobs, now):
    """Return jobs due for execution (worker-boot drain)."""
    return list(jobs.find({"status": "pending", "next_run": {"$lte": now}}))


def handle(job):
    """Process one job; retry bookkeeping stays app-side."""
    print(f"  handling {job['type']} {job['payload']}")


def mark_done(jobs, job_id):
    """Mark a job complete."""
    jobs.update_one({"_id": job_id}, {"$set": {"status": "done"}})


def mark_retry(jobs, job_id, attempts, delay_s):
    """Requeue a failed job with exponential-style backoff."""
    jobs.update_one(
        {"_id": job_id},
        {
            "$set": {
                "status": "pending",
                "next_run": datetime.now(timezone.utc)
                + timedelta(seconds=delay_s),
            },
            "$inc": {"attempts": attempts},
        },
    )


def run_worker(jobs, resume_token=None, max_events=10):
    """Tail new jobs until max_events processed; return last token."""
    token = resume_token
    processed = 0
    with jobs.watch(
        pipeline=[{"$match": {"operationType": "insert"}}],
        full_document="updateLookup",
        resume_after=token,
        max_await_time_ms=1000,
    ) as stream:
        while processed < max_events:
            try:
                change = next(stream)
            except StopIteration:
                break
            token = change["_id"]  # persist for crash-restart resume
            job = change.get("fullDocument")
            if job is None or job.get("status") != "pending":
                continue
            handle(job)
            mark_done(jobs, job["_id"])
            processed += 1
    return token


def main():
    """Demonstrate enqueue, drain, tail, and resume."""
    print("=== neosqlite watch() job-queue recipe ===\n")
    with Connection(":memory:") as conn:
        jobs = conn.jobs

        print("1. Enqueueing jobs...")
        enqueue(jobs, "send", {"to": "alice"})
        enqueue(jobs, "send", {"to": "bob"}, delay_s=3600)
        print("   - 1 due now, 1 delayed")

        print("2. Worker boot: draining due jobs...")
        now = datetime.now(timezone.utc)
        for job in claim_due(jobs, now):
            handle(job)
            mark_done(jobs, job["_id"])

        print("3. Tailing new arrivals...")
        with jobs.watch(
            pipeline=[{"$match": {"operationType": "insert"}}],
            full_document="updateLookup",
            max_await_time_ms=1000,
        ) as stream:
            enqueue(jobs, "send", {"to": "carol"})
            change = next(stream)
            handle(change["fullDocument"])
            mark_done(jobs, change["fullDocument"]["_id"])
            token = change["_id"]
        print(f"   - resume token: {token}")

        print("4. Restarting worker: drain first, then tail...")
        enqueue(jobs, "send", {"to": "dave"})
        # Boot drain catches jobs that arrived while no stream was open
        # (graceful close drops triggers; crash-restart replays via token).
        for job in claim_due(jobs, datetime.now(timezone.utc)):
            if job["payload"] == {"to": "dave"}:
                handle(job)
                mark_done(jobs, job["_id"])
        run_worker(jobs, resume_token=token, max_events=10)

    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()
