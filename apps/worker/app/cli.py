import argparse

from .pure_engine import PureEngine, run_once, run_worker


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=["enqueue_daily", "enqueue_poll", "enqueue_weekly_followers", "once", "worker", "backfill_d1_media"],
        required=True,
    )
    p.add_argument("--limit", type=int, default=300)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--batch-size", type=int, default=50)
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
    elif args.mode == "enqueue_weekly_followers":
        eng = PureEngine()
        try:
            print(f"enqueued_weekly_followers={eng.enqueue_weekly_followers()}")
        finally:
            eng.close()
    elif args.mode == "once":
        run_once()
    elif args.mode == "worker":
        run_worker()
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


if __name__ == "__main__":
    main()
