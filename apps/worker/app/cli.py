import argparse

from .pure_engine import PureEngine, run_intelligence_once, run_intelligence_worker, run_once, run_worker


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
            "intelligence_once",
            "intelligence_worker",
            "backfill_d1_media",
            "backfill_fire_day_media",
            "repair_post_visual_media",
            "migrate_stored_supabase_visual_media_to_r2",
            "restore_recent_thumbnails_from_post_pages",
            "refresh_recent_visual_media_sources",
            "migrate_visual_media_to_r2",
            "retire_legacy_post_media",
            "refresh_fire_preview_sources_from_day",
            "purge_preview_assets_before_day",
            "repair_overlong_preview_assets",
            "restore_d7_fire_thumbnails",
            "resolve_signal_intelligence",
            "compile_feeder_focus",
            "compile_feed_focus",
            "recompute_fire_rankings",
        ],
        required=True,
    )
    p.add_argument("--limit", type=int, default=300)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--day", type=str, default=None)
    p.add_argument("--post-key", type=str, default=None)
    p.add_argument("--signal-id", type=int, default=None)
    p.add_argument("--feeder-id", type=int, default=None)
    p.add_argument("--feed-id", type=int, default=None)
    p.add_argument("--full-rebuild", action="store_true")
    p.add_argument("--warm-start", action="store_true")
    p.add_argument("--post-cap", type=int, default=None)
    p.add_argument("--skip-focus-compile", action="store_true")
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
    elif args.mode == "intelligence_once":
        result = run_intelligence_once(
            signal_limit=args.limit,
            feeder_limit=args.limit,
            feed_limit=args.limit,
            compile_focus=not args.skip_focus_compile,
        )
        print(f"intelligence_once={result}")
    elif args.mode == "intelligence_worker":
        run_intelligence_worker()
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
    elif args.mode == "resolve_signal_intelligence":
        eng = PureEngine()
        try:
            result = eng.resolve_signal_intelligence(signal_id=args.signal_id, limit=args.limit)
            print(
                f"resolve_signal_intelligence selected={result.get('selected', 0)} "
                f"resolved={result.get('resolved', 0)} failed={result.get('failed', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "compile_feeder_focus":
        eng = PureEngine()
        try:
            result = eng.compile_feeder_focus(
                feeder_id=args.feeder_id,
                limit=args.limit,
                full_rebuild=args.full_rebuild,
                warm_start=args.warm_start,
                post_cap=args.post_cap,
            )
            print(
                f"compile_feeder_focus selected={result.get('selected', 0)} "
                f"compiled={result.get('compiled', 0)} skipped={result.get('skipped', 0)} "
                f"failed={result.get('failed', 0)}"
            )
        finally:
            eng.close()
    elif args.mode == "compile_feed_focus":
        eng = PureEngine()
        try:
            result = eng.compile_feed_focus(feed_id=args.feed_id, limit=args.limit, full_rebuild=args.full_rebuild)
            print(
                f"compile_feed_focus selected={result.get('selected', 0)} "
                f"compiled={result.get('compiled', 0)} skipped={result.get('skipped', 0)} "
                f"failed={result.get('failed', 0)}"
            )
        finally:
            eng.close()


if __name__ == "__main__":
    main()
