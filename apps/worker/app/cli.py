import argparse

from .pure_engine import PureEngine, run_once, run_worker


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["enqueue_daily", "enqueue_weekly", "once", "worker"], required=True)
    args = p.parse_args()

    if args.mode == "enqueue_daily":
        eng = PureEngine()
        try:
            print(f"enqueued_daily={eng.enqueue_daily()}")
        finally:
            eng.close()
    elif args.mode == "enqueue_weekly":
        eng = PureEngine()
        try:
            print(f"enqueued_weekly={eng.enqueue_weekly()}")
        finally:
            eng.close()
    elif args.mode == "once":
        run_once()
    elif args.mode == "worker":
        run_worker()


if __name__ == "__main__":
    main()
