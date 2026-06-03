import argparse

from .pure_engine import PureEngine, run_fingerprint_once, run_fingerprint_worker, run_once, run_worker


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=[
            "enqueue_daily",
            "enqueue_poll",
            "enqueue_daily_followers",
            "enqueue_weekly_followers",
            "once",
            "worker",
            "fingerprint_reels_once",
            "fingerprint_reels_worker",
            "backfill_d1_media",
            "backfill_fire_day_media",
            "repair_post_visual_media",
            "prepare_feeder_intelligence_media",
            "migrate_stored_supabase_visual_media_to_r2",
            "restore_recent_thumbnails_from_post_pages",
            "refresh_recent_visual_media_sources",
            "migrate_visual_media_to_r2",
            "retire_legacy_post_media",
            "refresh_fire_preview_sources_from_day",
            "purge_preview_assets_before_day",
            "repair_overlong_preview_assets",
            "restore_d7_fire_thumbnails",
            "recompute_fire_rankings",
        ],
        required=True,
    )
    p.add_argument("--limit", type=int, default=300)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--day", type=str, default=None)
    p.add_argument("--post-key", type=str, default=None)
    p.add_argument("--feeder-id", type=int, default=None)
    p.add_argument("--handle", type=str, default=None)
    p.add_argument("--include-failed", action="store_true")
    p.add_argument("--allow-private-refresh", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.mode == "enqueue_daily":
        eng = PureEngine()
        try:
            print(f"enqueued_daily={eng.enqueue_daily()}")
        finally:
            eng.close()
    elif args.mode == "enqueue_poll":
        eng = PureEngine()
        try:
            print(f"enqueued_poll={eng.enqueue_poll()}")
        finally:
            eng.close()
    elif args.mode == "enqueue_daily_followers":
        eng = PureEngine()
        try:
            print(f"enqueued_daily_followers={eng.enqueue_daily_followers()}")
        finally:
            eng.close()
    elif args.mode == "enqueue_weekly_followers":
        eng = PureEngine()
        try:
            print(f"enqueued_daily_followers={eng.enqueue_weekly_followers()}")
        finally:
            eng.close()
    elif args.mode == "once":
        run_once()
    elif args.mode == "worker":
        run_worker()
    elif args.mode == "fingerprint_reels_once":
        result = run_fingerprint_once(limit=args.limit, feeder_id=args.feeder_id, days=args.days)
        print(f"fingerprint_reels_once={result}")
    elif args.mode == "fingerprint_reels_worker":
        run_fingerprint_worker()
    elif args.mode == "backfill_d1_media":
        eng = PureEngine()
        try:
            result = eng.backfill_d1_media(limit=args.limit, days=args.days, batch_size=args.batch_size)
            print(
                f"backfill_d1_media selected={result.get('selected', 0)} "
                f"updated={result.get('updated', 0)} missing={result.get('missing', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "backfill_fire_day_media":
        eng = PureEngine()
        try:
            result = eng.backfill_fire_day_media(day=args.day, limit=args.limit, batch_size=args.batch_size)
            print(
                f"backfill_fire_day_media day={args.day} selected={result.get('selected', 0)} "
                f"updated={result.get('updated', 0)} missing={result.get('missing', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "repair_post_visual_media":
        eng = PureEngine()
        try:
            result = eng.repair_post_visual_media(args.post_key)
            print(
                f"repair_post_visual_media post_key={args.post_key} found={result.get('found', False)} "
                f"staged={result.get('staged', 0)} captured={result.get('captured', 0)} "
                f"failed={result.get('failed', 0)} retired={result.get('retired', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "prepare_feeder_intelligence_media":
        eng = PureEngine()
        try:
            result = eng.prepare_feeder_intelligence_media(
                feeder_id=args.feeder_id,
                handle=args.handle,
                limit=args.limit,
                days=args.days,
                batch_size=args.batch_size,
                include_failed=args.include_failed,
                allow_private_refresh=args.allow_private_refresh,
                dry_run=args.dry_run,
            )
            print(
                f"prepare_feeder_intelligence_media feeder_id={result.get('feeder_id')} "
                f"selected={result.get('selected', 0)} already_ready={result.get('already_ready', 0)} "
                f"needs_refresh={result.get('needs_refresh', 0)} "
                f"needs_existing_source_capture={result.get('needs_existing_source_capture', 0)} "
                f"needs_private_refresh={result.get('needs_private_refresh', 0)} "
                f"existing_source_staged={result.get('existing_source_staged', 0)} "
                f"existing_source_captured={result.get('existing_source_captured', 0)} "
                f"existing_source_failed={result.get('existing_source_failed', 0)} "
                f"refreshed={result.get('refreshed', 0)} "
                f"staged={result.get('staged', 0)} capture_selected={result.get('capture_selected', 0)} "
                f"captured={result.get('captured', 0)} capture_failed={result.get('capture_failed', 0)} "
                f"ready={result.get('ready', 0)} failed={result.get('failed', 0)} "
                f"missing_source={result.get('missing_source', 0)} "
                f"private_refresh_allowed={result.get('private_refresh_allowed', False)} "
                f"private_refresh_skipped={result.get('private_refresh_skipped', 0)} "
                f"dry_run={result.get('dry_run', False)}"
            )
        finally:
            eng.close()
    elif args.mode == "migrate_stored_supabase_visual_media_to_r2":
        eng = PureEngine()
        try:
            result = eng.migrate_stored_supabase_visual_media_to_r2(limit=args.limit)
            print(
                f"migrate_stored_supabase_visual_media_to_r2 selected={result.get('selected', 0)} "
                f"migrated={result.get('migrated', 0)} missing={result.get('missing', 0)} "
                f"failed={result.get('failed', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "restore_recent_thumbnails_from_post_pages":
        eng = PureEngine()
        try:
            result = eng.restore_recent_thumbnails_from_post_pages(limit=args.limit, days=args.days)
            print(
                f"restore_recent_thumbnails_from_post_pages selected={result.get('selected', 0)} "
                f"staged={result.get('staged', 0)} captured={result.get('captured', 0)} "
                f"failed={result.get('failed', 0)} missing={result.get('missing', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "refresh_recent_visual_media_sources":
        eng = PureEngine()
        try:
            result = eng.refresh_recent_visual_media_sources(limit=args.limit, days=args.days, batch_size=args.batch_size)
            print(
                f"refresh_recent_visual_media_sources selected={result.get('selected', 0)} "
                f"staged={result.get('staged', 0)} captured={result.get('captured', 0)} "
                f"failed={result.get('failed', 0)} missing={result.get('missing', 0)} "
                f"retired={result.get('retired', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "migrate_visual_media_to_r2":
        eng = PureEngine()
        try:
            result = eng.migrate_visual_media_to_r2(limit=args.limit, days=args.days)
            print(
                f"migrate_visual_media_to_r2 selected={result.get('selected', 0)} "
                f"copied={result.get('copied', 0)} "
                f"staged={result.get('staged', 0)} captured={result.get('captured', 0)} "
                f"failed={result.get('failed', 0)} missing={result.get('missing', 0)} "
                f"retired={result.get('retired', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "retire_legacy_post_media":
        eng = PureEngine()
        try:
            result = eng._retire_legacy_post_media_rows(limit=args.limit)
            print(
                f"retire_legacy_post_media marked={result.get('marked', 0)} "
                f"purged={result.get('purged', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "refresh_fire_preview_sources_from_day":
        eng = PureEngine()
        try:
            result = eng.refresh_fire_preview_sources_from_day(day=args.day, limit=args.limit, batch_size=args.batch_size)
            print(
                f"refresh_fire_preview_sources_from_day day={args.day} selected={result.get('selected', 0)} "
                f"updated={result.get('updated', 0)} missing={result.get('missing', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "purge_preview_assets_before_day":
        eng = PureEngine()
        try:
            result = eng.purge_preview_assets_before_day(day=args.day, limit=args.limit)
            print(
                f"purge_preview_assets_before_day day={args.day} "
                f"marked={result.get('marked', 0)} purged={result.get('purged', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "repair_overlong_preview_assets":
        eng = PureEngine()
        try:
            result = eng.repair_overlong_preview_assets(limit=args.limit)
            print(
                f"repair_overlong_preview_assets scanned={result.get('scanned', 0)} "
                f"requeued={result.get('requeued', 0)} missing={result.get('missing', 0)} "
                f"valid={result.get('valid', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "restore_d7_fire_thumbnails":
        eng = PureEngine()
        try:
            result = eng.restore_missing_d7_fire_thumbnails(limit=args.limit, days=args.days, batch_size=args.batch_size)
            print(
                f"restore_d7_fire_thumbnails selected={result.get('selected', 0)} "
                f"updated={result.get('updated', 0)} missing={result.get('missing', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "recompute_fire_rankings":
        eng = PureEngine()
        try:
            result = eng.recompute_fire_rankings(limit=args.limit, days=args.days)
            print(
                f"recompute_fire_rankings selected={result.get('selected', 0)} "
                f"processed={result.get('processed', 0)} updated_rows={result.get('updated_rows', 0)} "
                f"lanes={result.get('lanes', 0)}"
            )
        finally:
            eng.close()


if __name__ == "__main__":
    main()
