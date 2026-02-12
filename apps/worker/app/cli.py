import argparse
import re
import time
from datetime import datetime, timezone, timedelta

from .config import (
    RETRY_BACKOFF_MINUTES,
    IGNORE_SHEETS,
    APIFY_TOKEN,
    APIFY_COOLDOWN_TRIGGER_FAILURES,
    APIFY_COOLDOWN_HOURS,
)
from .db import (
    init_db,
    enqueue_handle,
    fetch_next_job,
    mark_job_failed,
    mark_job_retry,
    mark_job_success,
    upsert_handle_state,
    list_subscribers,
    get_feed_by_subscriber,
    log_run_start,
    log_run_finish,
    list_signal_posts_for_embedding,
    embedding_exists,
    upsert_post_embedding,
    run_retention_cleanup,
    ensure_feeders_for_subscriber,
    refresh_feeder_pair_metrics,
    rebuild_signal_aggregates_for_subscriber,
    fetch_next_post_job_batch,
    mark_post_job_success,
    mark_post_job_retry,
    mark_post_job_failed,
    mark_post_job_skipped,
    is_d7_hot,
    get_post_signal_map,
    get_conn,
    upsert_handle_profile_metric,
    get_apify_pause_until,
    record_apify_success,
    record_apify_failure,
)
from .sheets import list_sheet_titles, get_values, batch_update, upsert_handle_profile_snapshot
from .sync import sync_handle, sync_post_checkpoint_batch
from .apify import run_actor_details
from .embeddings import build_signal_texts, get_embedding
from .config import OPENAI_EMBED_MODEL, EMBED_ONLY_TAGS, EMBED_BATCH_LIMIT, EMBED_SIGNAL_TYPES
from .alerts import generate_alert_candidates


def _next_retry_time(attempt: int) -> datetime:
    idx = min(attempt - 1, len(RETRY_BACKOFF_MINUTES) - 1)
    minutes = RETRY_BACKOFF_MINUTES[idx]
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def _sanitize_error_message(err: Exception) -> str:
    msg = str(err or "")
    if not msg:
        return "Unknown error"
    if APIFY_TOKEN:
        msg = msg.replace(APIFY_TOKEN, "***")
    msg = re.sub(r"(token=)[^&\s]+", r"\1***", msg, flags=re.IGNORECASE)
    return msg


def _shortcode_from_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", u, flags=re.IGNORECASE)
    return m.group(1).lower() if m else ""


def schedule(run_type: str):
    init_db()
    for sub in list_subscribers():
        sheets = list_sheet_titles(sub["spreadsheet_id"])
        handle_sheets = [
            s for s in sheets
            if s not in IGNORE_SHEETS and s not in ("Feeder", "Billing/Usage")
        ]
        ensure_feeders_for_subscriber(sub["id"], handle_sheets)

        # Weekly cycle is profile/details refresh only.
        if run_type == "weekly":
            _refresh_followers(sub["id"], sub["spreadsheet_id"], handle_sheets)
            continue

        # Daily cycle enqueues post scrapes.
        for sheet in handle_sheets:
            enqueue_handle(sub["id"], sub["spreadsheet_id"], sheet, run_type)


def worker():
    init_db()
    post_batch_size = 10
    while True:
        pause_until = get_apify_pause_until()

        job = fetch_next_job()
        if job:
            handle = job["handle"]
            run_type = job["run_type"]
            subscriber_id = job["subscriber_id"]
            spreadsheet_id = job["spreadsheet_id"]
            run_log_id = log_run_start(subscriber_id, spreadsheet_id, handle, run_type)

            # Respect global Apify cooldown without consuming retries.
            if pause_until and pause_until > datetime.now(timezone.utc):
                mark_job_retry(job["id"], job["attempt"], pause_until, "Apify cooldown active")
                log_run_finish(run_log_id, "retry", 0, 0, 0, "Apify cooldown active")
                time.sleep(1)
                continue

            try:
                last_seen, apify_items, inserted_count, updated_count = sync_handle(
                    subscriber_id, spreadsheet_id, handle, handle, run_type
                )
                record_apify_success()
                upsert_handle_state(subscriber_id, handle, handle, "success", last_seen, None)
                mark_job_success(job["id"])
                log_run_finish(run_log_id, "success", apify_items, inserted_count, updated_count, None)
                feed = get_feed_by_subscriber(subscriber_id)
                if feed:
                    refresh_feeder_pair_metrics(feed["id"], 30)
            except Exception as exc:
                safe_error = _sanitize_error_message(exc)
                _, new_pause_until = record_apify_failure(
                    safe_error, APIFY_COOLDOWN_TRIGGER_FAILURES, APIFY_COOLDOWN_HOURS
                )
                attempt = job["attempt"] + 1
                if attempt <= len(RETRY_BACKOFF_MINUTES):
                    next_time = _next_retry_time(attempt)
                    if new_pause_until and new_pause_until > next_time:
                        next_time = new_pause_until
                    mark_job_retry(job["id"], attempt, next_time, safe_error)
                    upsert_handle_state(subscriber_id, handle, handle, "retry", None, safe_error)
                    log_run_finish(run_log_id, "retry", 0, 0, 0, safe_error)
                else:
                    mark_job_failed(job["id"], safe_error)
                    upsert_handle_state(subscriber_id, handle, handle, "failed", None, safe_error)
                    log_run_finish(run_log_id, "failed", 0, 0, 0, safe_error)
            time.sleep(1)
            continue

        post_jobs = fetch_next_post_job_batch(post_batch_size)
        if not post_jobs:
            time.sleep(5)
            continue

        # All jobs in batch share same subscriber+handle+checkpoint from DB fetch helper.
        anchor = post_jobs[0]

        if pause_until and pause_until > datetime.now(timezone.utc):
            for pj in post_jobs:
                mark_post_job_retry(pj["id"], pj["attempt"], pause_until, "Apify cooldown active")
            time.sleep(1)
            continue

        try:
            urls = [pj["post_url"] for pj in post_jobs]
            batch_results = sync_post_checkpoint_batch(
                subscriber_id=anchor["subscriber_id"],
                spreadsheet_id=anchor["spreadsheet_id"],
                handle=anchor["handle"],
                sheet_name=anchor["handle"],
                checkpoint=anchor["checkpoint"],
                post_urls=urls,
            )
            record_apify_success()

            for pj in post_jobs:
                if pj["checkpoint"] == "d21" and pj.get("requires_d7_hot"):
                    if not is_d7_hot(
                        subscriber_id=pj["subscriber_id"],
                        handle=pj["handle"],
                        post_url=pj["post_url"],
                    ):
                        mark_post_job_skipped(pj["id"], "D7 not hot; D21 skipped by gate")
                        continue

                res = batch_results.get(pj["post_url"])
                if res is None:
                    attempt = pj["attempt"] + 1
                    msg = "Post missing in batch result"
                    if attempt <= len(RETRY_BACKOFF_MINUTES):
                        mark_post_job_retry(pj["id"], attempt, _next_retry_time(attempt), msg)
                    else:
                        mark_post_job_failed(pj["id"], msg)
                else:
                    mark_post_job_success(pj["id"])
        except Exception as exc:
            safe_error = _sanitize_error_message(exc)
            _, new_pause_until = record_apify_failure(
                safe_error, APIFY_COOLDOWN_TRIGGER_FAILURES, APIFY_COOLDOWN_HOURS
            )
            for pj in post_jobs:
                attempt = pj["attempt"] + 1
                if attempt <= len(RETRY_BACKOFF_MINUTES):
                    next_time = _next_retry_time(attempt)
                    if new_pause_until and new_pause_until > next_time:
                        next_time = new_pause_until
                    mark_post_job_retry(pj["id"], attempt, next_time, safe_error)
                else:
                    mark_post_job_failed(pj["id"], safe_error)

        time.sleep(1)


def embeddings_run(subscriber_id: int | None):
    init_db()
    targets = [s for s in list_subscribers() if subscriber_id is None or s["id"] == subscriber_id]
    for sub in targets:
        rows = list_signal_posts_for_embedding(sub["id"], EMBED_ONLY_TAGS, EMBED_BATCH_LIMIT)
        for row in rows:
            try:
                signal_texts = build_signal_texts(row)
            except Exception as exc:
                print(f"[embeddings] skip row build failed: {_sanitize_error_message(exc)}")
                continue

            for signal_type in EMBED_SIGNAL_TYPES:
                if signal_type not in signal_texts:
                    continue
                try:
                    if embedding_exists(
                        subscriber_id=sub["id"],
                        handle=row.get("handle") or "",
                        post_url=row.get("post_url") or "",
                        embedding_model=OPENAI_EMBED_MODEL,
                        signal_type=signal_type,
                    ):
                        continue
                    text = signal_texts[signal_type]
                    emb = get_embedding(text)
                    upsert_post_embedding(
                        subscriber_id=sub["id"],
                        handle=row.get("handle") or "",
                        post_url=row.get("post_url") or "",
                        embedding_model=OPENAI_EMBED_MODEL,
                        signal_type=signal_type,
                        signal_version="v1",
                        niche_id=None,
                        metadata={
                            "velocity_tag": row.get("velocity_tag"),
                            "velocity_stage": row.get("velocity_stage"),
                            "velocity_percentile": row.get("velocity_percentile"),
                        },
                        source_text=text,
                        embedding=emb,
                    )
                except Exception as exc:
                    handle = row.get("handle") or "unknown"
                    post_url = row.get("post_url") or "unknown"
                    print(
                        f"[embeddings] skip handle={handle} post={post_url} signal={signal_type}: "
                        f"{_sanitize_error_message(exc)}"
                    )


def retention_run():
    init_db()
    run_retention_cleanup()


def alerts_run(subscriber_id: int | None):
    init_db()
    rebuild_signal_aggregates_for_subscriber(subscriber_id=subscriber_id, lookback_days=30)
    generate_alert_candidates(subscriber_id=subscriber_id, max_per_feed=3)


def aggregates_run(subscriber_id: int | None):
    init_db()
    rebuild_signal_aggregates_for_subscriber(subscriber_id=subscriber_id, lookback_days=30)


def _canonical_stage(stage: str, tag: str) -> str:
    st = (stage or "").strip().upper()
    tg = (tag or "").strip()
    if "👁" in tg:
        return "D2"
    if st in ("WATCH", "C1", "C1R"):
        return "D2"
    if st in ("D1",):
        return "D1"
    if st in ("D2",):
        return "D2"
    if st in ("D3", "C3"):
        return "D3"
    if st in ("D7", "C7"):
        return "D7"
    if st in ("D21", "C21"):
        return "D21"
    return st or ""


def repair_velocity(subscriber_id: int | None):
    init_db()
    targets = [s for s in list_subscribers() if subscriber_id is None or s["id"] == subscriber_id]
    for sub in targets:
        sid = sub["id"]
        spreadsheet_id = sub["spreadsheet_id"]
        # 1) Normalize DB stage labels (no scrape required).
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE post_signals
                SET velocity_stage = CASE
                    WHEN UPPER(COALESCE(velocity_stage,'')) IN ('D3','C3') THEN 'D3'
                    WHEN UPPER(COALESCE(velocity_stage,'')) IN ('D7','C7') THEN 'D7'
                    WHEN UPPER(COALESCE(velocity_stage,'')) IN ('D21','C21') THEN 'D21'
                    WHEN posted_at >= NOW() - INTERVAL '24 hours' THEN 'D1'
                    WHEN posted_at >= NOW() - INTERVAL '72 hours' THEN 'D2'
                    ELSE 'D2'
                END,
                velocity_tag = CASE
                    WHEN COALESCE(velocity_percentile,'') ~ '^[0-9]+%%$' THEN
                        CASE
                            WHEN replace(velocity_percentile,'%%','')::int <= 5 THEN '🚀'
                            WHEN replace(velocity_percentile,'%%','')::int <= 15 THEN '🔥'
                            WHEN replace(velocity_percentile,'%%','')::int <= 35 THEN '✅'
                            ELSE '😴'
                        END
                    ELSE ''
                END,
                updated_at = NOW()
                WHERE subscriber_id=%s
                """,
                (sid,),
            )
            # Enforce D1/D2 by age even for already-labeled rows (keeps UX deterministic).
            conn.execute(
                """
                UPDATE post_signals
                SET velocity_stage = CASE
                    WHEN posted_at >= NOW() - INTERVAL '24 hours' THEN 'D1'
                    WHEN posted_at >= NOW() - INTERVAL '72 hours' THEN 'D2'
                    ELSE velocity_stage
                END,
                updated_at = NOW()
                WHERE subscriber_id=%s
                """,
                (sid,),
            )
            conn.commit()

        # 2) Push current DB tag/percentile/stage to sheets.
        handles = [h for h in list_sheet_titles(spreadsheet_id) if h not in IGNORE_SHEETS and h not in ("Feeder", "Billing/Usage")]
        for handle in handles:
            signal_map = get_post_signal_map(sid, handle)
            if not signal_map:
                continue
            rows = get_values(f"{handle}!A3:M10000", spreadsheet_id)
            updates = []
            for i, row in enumerate(rows, start=3):
                post_url = row[0].strip() if len(row) > 0 and row[0] else ""
                if not post_url:
                    continue
                sig = signal_map.get(_shortcode_from_url(post_url))
                if not sig:
                    continue
                raw_k = sig.get("velocity_tag", "") or ""
                raw_l = sig.get("velocity_percentile", "") or ""
                if str(raw_k).strip().lower() == "insufficient_data":
                    k = ""
                    l = ""
                else:
                    k = raw_k
                    l = raw_l
                m = _canonical_stage(sig.get("velocity_stage", ""), k)
                existing_k = row[10].strip() if len(row) > 10 and row[10] else ""
                existing_l = row[11].strip() if len(row) > 11 and row[11] else ""
                existing_m = row[12].strip() if len(row) > 12 and row[12] else ""
                if (existing_k, existing_l, existing_m) != (k, l, m):
                    updates.append({"range": f"{handle}!K{i}:M{i}", "values": [[k, l, m]]})
            if updates:
                batch_update(updates, spreadsheet_id)


def _refresh_followers(subscriber_id: int, spreadsheet_id: str, handles: list[str]):
    timestamp = datetime.now().strftime("%d-%m-%y %I:%M %p")
    for handle in handles:
        clean = handle.lstrip("@")
        try:
            details = run_actor_details(clean)
        except Exception:
            continue

        followers = (
            details.get("followersCount")
            or details.get("ownerFollowersCount")
            or (details.get("owner") or {}).get("followersCount")
            or ((details.get("owner") or {}).get("edge_followed_by") or {}).get("count")
            or ""
        )
        follows_count = (
            details.get("followsCount")
            or details.get("followingsCount")
            or details.get("followingCount")
            or ((details.get("owner") or {}).get("edge_follow") or {}).get("count")
            or ""
        )
        posts_count = details.get("postsCount") or details.get("posts_count") or ""
        business_category = details.get("businessCategoryName") or ""
        full_name = details.get("fullName") or details.get("full_name") or ""
        verified = bool(details.get("verified") or details.get("isVerified"))
        profile_pic_url = details.get("profilePicUrlHD") or details.get("profilePicUrl") or ""
        profile_url = details.get("url") or f"https://www.instagram.com/{clean}/"

        def _to_int(v):
            try:
                return int(v)
            except Exception:
                return None

        upsert_handle_profile_metric(
            subscriber_id=subscriber_id,
            handle=f"@{clean}",
            profile_url=profile_url,
            full_name=full_name,
            business_category=business_category,
            biography=details.get("biography") or "",
            followers_count=_to_int(followers),
            follows_count=_to_int(follows_count),
            posts_count=_to_int(posts_count),
            verified=verified,
            profile_pic_url=profile_pic_url,
        )

        upsert_handle_profile_snapshot(
            spreadsheet_id=spreadsheet_id,
            sheet_name=handle,
            handle=f"@{clean}",
            followers_count=_to_int(followers),
            follows_count=_to_int(follows_count),
            posts_count=_to_int(posts_count),
            business_category=business_category,
            verified=verified,
            sampled_at_label=timestamp,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["schedule", "worker", "embeddings", "alerts", "aggregates", "retention", "repair_velocity"], required=True)
    parser.add_argument("--run_type", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--subscriber_id", type=int, default=None)
    args = parser.parse_args()

    if args.mode == "schedule":
        schedule(args.run_type)
    elif args.mode == "worker":
        worker()
    elif args.mode == "embeddings":
        embeddings_run(args.subscriber_id)
    elif args.mode == "alerts":
        alerts_run(args.subscriber_id)
    elif args.mode == "aggregates":
        aggregates_run(args.subscriber_id)
    elif args.mode == "retention":
        retention_run()
    elif args.mode == "repair_velocity":
        repair_velocity(args.subscriber_id)


if __name__ == "__main__":
    main()
