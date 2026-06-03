"""Read-only: for each feeder with condensations, how many eligible d7-reel-90d
posts already have a fingerprint vs a condensation. Grounds the fill-up costing
(condensation-only floor vs fingerprint+condensation ceiling). SELECT-ONLY."""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_prompts import POST_CONDENSATION_PROMPT_VERSION as PV  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"


def _read_dsn(path: Path) -> str:
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if line.startswith("POSTGRES_DSN="):
            v = line.split("=", 1)[1].strip()
            if (v[:1], v[-1:]) in {('"', '"'), ("'", "'")}:
                v = v[1:-1]
            return v
    raise RuntimeError("POSTGRES_DSN not found")


def _pooler(dsn: str) -> str:
    p = urlsplit(dsn)
    netloc = f"{POOLER_USER}:{p.password or ''}@{POOLER_HOST}:5432"
    path = p.path if p.path and p.path != "/" else "/postgres"
    return urlunsplit((p.scheme, netloc, path, "", ""))


# eligible = distinct d7-reel posts in last 90d, per feeder that has >=1 condensation
SQL = """
with elig as (
  select distinct p.post_key, p.feeder_id
  from public.posts p
  join public.post_metrics pm
    on pm.post_key = p.post_key and lower(pm.checkpoint)='d7'
  where lower(coalesce(p.media_type,''))='reel'
    and p.posted_at >= now() - interval '90 days'
),
cond as (
  select distinct post_key from public.feeder_file_model_calls
  where call_type='post_condensation' and status='complete' and prompt_version=%s
)
select f.id as feeder_id, f.handle,
  count(*)                                              as eligible,
  count(fp.post_key)                                   as with_fp,
  count(c.post_key)                                    as with_cond,
  count(*) filter (where c.post_key is null and fp.post_key is not null) as need_cond_have_fp,
  count(*) filter (where c.post_key is null and fp.post_key is null)     as need_cond_no_fp
from elig e
join public.feeders f on f.id = e.feeder_id
left join public.post_fingerprints fp on fp.post_key = e.post_key
left join cond c on c.post_key = e.post_key
where e.feeder_id in (
  select distinct p.feeder_id
  from public.posts p
  join public.feeder_file_model_calls fmc
    on fmc.post_key = p.post_key and fmc.call_type='post_condensation'
   and fmc.status='complete' and fmc.prompt_version=%s
)
group by f.id, f.handle
order by f.id
"""


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL, (PV, PV))
            rows = cur.fetchall()
    print(f"{'fid':>4}  {'handle':<18} {'elig':>5} {'fp':>5} {'cond':>5} "
          f"{'need_c+fp':>9} {'need_c-nofp':>11}")
    tot = {"elig": 0, "fp": 0, "cond": 0, "ncfp": 0, "ncnofp": 0}
    for r in rows:
        print(f"{r['feeder_id']:>4}  {str(r['handle']):<18} {r['eligible']:>5} "
              f"{r['with_fp']:>5} {r['with_cond']:>5} {r['need_cond_have_fp']:>9} "
              f"{r['need_cond_no_fp']:>11}")
        tot["elig"] += r["eligible"]; tot["fp"] += r["with_fp"]
        tot["cond"] += r["with_cond"]; tot["ncfp"] += r["need_cond_have_fp"]
        tot["ncnofp"] += r["need_cond_no_fp"]
    print(f"{'':>4}  {'TOTAL':<18} {tot['elig']:>5} {tot['fp']:>5} {tot['cond']:>5} "
          f"{tot['ncfp']:>9} {tot['ncnofp']:>11}")


if __name__ == "__main__":
    main()
