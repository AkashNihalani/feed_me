from __future__ import annotations
from typing import List
import re
from googleapiclient.discovery import build
from google.oauth2 import service_account

from .config import SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST, SPREADSHEET_ID

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _service():
    creds = service_account.Credentials.from_service_account_file(
        "/app/credentials/service_account.json", scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_sheet_titles(spreadsheet_id: str | None = None) -> list[str]:
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    sheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(title))",
    ).execute()
    return [s["properties"]["title"] for s in sheet.get("sheets", [])]


def get_values(range_a1: str, spreadsheet_id: str | None = None) -> list[list[str]]:
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
    ).execute()
    return result.get("values", [])


def update_values(range_a1: str, values: list[list[str]], spreadsheet_id: str | None = None):
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def batch_update(ranges: list[dict], spreadsheet_id: str | None = None):
    if not ranges:
        return
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    chunk_size = 200
    for i in range(0, len(ranges), chunk_size):
        chunk = ranges[i : i + chunk_size]
        body = {"valueInputOption": "USER_ENTERED", "data": chunk}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body,
        ).execute()


def append_values(range_a1: str, values: list[list[str]], spreadsheet_id: str | None = None):
    if not values:
        return
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    chunk_size = 200
    for i in range(0, len(values), chunk_size):
        chunk = values[i : i + chunk_size]
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_a1,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": chunk},
        ).execute()


def clear_values(range_a1: str, spreadsheet_id: str | None = None):
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
    ).execute()


def _col_to_a1(col_num_1_based: int) -> str:
    if col_num_1_based < 1:
        col_num_1_based = 1
    letters = ""
    n = col_num_1_based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _sheet_id_by_title(sheet_name: str, spreadsheet_id: str | None = None) -> int | None:
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    sheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(title,sheetId))",
    ).execute()
    for s in sheet.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None


def _sheet_column_count(sheet_name: str, spreadsheet_id: str | None = None) -> int:
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    service = _service()
    sheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(title,gridProperties(columnCount)))",
    ).execute()
    for s in sheet.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return int((props.get("gridProperties") or {}).get("columnCount") or 26)
    return 26


def _ensure_sheet_columns(sheet_name: str, required_col_1_based: int, spreadsheet_id: str | None = None):
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    sheet_id = _sheet_id_by_title(sheet_name, spreadsheet_id)
    if sheet_id is None:
        return
    current = _sheet_column_count(sheet_name, spreadsheet_id)
    if current >= required_col_1_based:
        return
    service = _service()
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "appendDimension": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "length": required_col_1_based - current,
                    }
                }
            ]
        },
    ).execute()


def _apply_formatting(sheet_name: str, header_len: int, spreadsheet_id: str | None = None):
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    sheet_id = _sheet_id_by_title(sheet_name, spreadsheet_id)
    if sheet_id is None:
        return
    def col_index(name: str) -> int:
        try:
            return SHEET_HEADER_LIST.index(name)
        except ValueError:
            return -1

    neon = {"red": 0.8, "green": 1.0, "blue": 0.0}  # #CCFF00
    black = {"red": 0.0, "green": 0.0, "blue": 0.0}  # #000000

    # Basic aesthetic formatting
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 2},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": neon,
                        "textFormat": {"bold": True, "foregroundColor": black},
                        "horizontalAlignment": "CENTER",
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"italic": True, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}},
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat(textFormat,wrapStrategy)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": header_len},
                "properties": {"pixelSize": 140},
                "fields": "pixelSize",
            }
        },
    ]

    # Date/time formatting for posted_at / scanned_at / last_updated_at
    # Use MM for month and mm for minutes (Sheets format tokens).
    date_format = {"numberFormat": {"type": "DATE_TIME", "pattern": "dd-MM-yy hh:mm AM/PM"}}
    for name in ["posted_at", "scanned_at", "last_updated_at"]:
        idx = col_index(name)
        if idx >= 0:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 2,
                            "endRowIndex": 10000,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {"userEnteredFormat": date_format},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )

    # Make velocity emoji stand out
    velocity_idx = col_index("velocity")
    if velocity_idx >= 0:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 2,
                        "endRowIndex": 10000,
                        "startColumnIndex": velocity_idx,
                        "endColumnIndex": velocity_idx + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"fontSize": 36, "bold": True, "italic": False},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment)",
                }
            }
        )

    # Wider columns for readability
    wide_map = {
        "post_url": 260,
        "caption": 360,
        "hashtags": 220,
        "caption_mentions": 220,
        "display_url": 220,
        "video_url": 220,
        "music_info": 220,
    }
    for name, width in wide_map.items():
        idx = col_index(name)
        if idx >= 0:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
            )

    service = _service()
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


def ensure_header(sheet_name: str, spreadsheet_id: str | None = None) -> list[str]:
    existing = get_values(f"{sheet_name}!1:2", spreadsheet_id)
    if not existing:
        clear_values(f"{sheet_name}!A1:AZ2", spreadsheet_id)
        update_values(f"{sheet_name}!1:2", [SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST], spreadsheet_id)
        _apply_formatting(sheet_name, len(SHEET_HEADER_LIST), spreadsheet_id)
        return SHEET_HEADER_LIST
    header = existing[0]
    if header != SHEET_HEADER_LIST:
        # Header changed: migrate existing rows by column-name so data doesn't end up under the wrong headers.
        rows = get_values(f"{sheet_name}!A3:AZ10000", spreadsheet_id)
        old_idx = {name: i for i, name in enumerate(header)}
        migrated: list[list[str]] = []
        for row in rows:
            row_dict = {}
            for name, i in old_idx.items():
                row_dict[name] = row[i] if i < len(row) else ""
            migrated.append([row_dict.get(col, "") for col in SHEET_HEADER_LIST])

        clear_values(f"{sheet_name}!A1:AZ10000", spreadsheet_id)
        update_values(f"{sheet_name}!1:2", [SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST], spreadsheet_id)
        append_values(f"{sheet_name}!A3", migrated, spreadsheet_id)
        _apply_formatting(sheet_name, len(SHEET_HEADER_LIST), spreadsheet_id)
        return SHEET_HEADER_LIST

    # Header matches, but older deployments may have rewritten headers without migrating rows.
    # If the post_url column doesn't look like Instagram URLs, attempt a best-effort repair from known legacy schemas.
    sample = get_values(f"{sheet_name}!A3:AZ60", spreadsheet_id)
    if _needs_repair(sample):
        all_rows = get_values(f"{sheet_name}!A3:AZ10000", spreadsheet_id)
        repaired = _repair_rows_from_legacy(all_rows, SHEET_HEADER_LIST)
        if repaired is not None:
            clear_values(f"{sheet_name}!A3:AZ10000", spreadsheet_id)
            append_values(f"{sheet_name}!A3", repaired, spreadsheet_id)
    # Always enforce both rows and clear stale trailing cells from older schemas.
    clear_values(f"{sheet_name}!A1:AZ2", spreadsheet_id)
    update_values(f"{sheet_name}!1:2", [SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST], spreadsheet_id)
    _apply_formatting(sheet_name, len(SHEET_HEADER_LIST), spreadsheet_id)
    return SHEET_HEADER_LIST


def upsert_handle_profile_snapshot(
    spreadsheet_id: str,
    sheet_name: str,
    *,
    handle: str,
    followers_count: int | None,
    follows_count: int | None,
    posts_count: int | None,
    business_category: str | None,
    verified: bool | None,
    sampled_at_label: str,
):
    followers_val = str(followers_count) if followers_count not in (None, "") else "n/a"
    follows_val = str(follows_count) if follows_count not in (None, "") else "n/a"
    posts_val = str(posts_count) if posts_count not in (None, "") else "n/a"
    category_val = business_category or "n/a"
    trust_val = "Verified" if verified else "Standard"

    labels = [[
        "HANDLE SNAPSHOT",
        "Followers",
        "Following",
        "Posts",
        "Trust / Category",
    ]]
    values = [[
        f"{handle}",
        f"{followers_val} • Audience",
        f"{follows_val} • Network",
        f"{posts_val} • Lifetime Posts",
        f"{trust_val} • {category_val} • {sampled_at_label}",
    ]]
    # Place profile snapshot strictly after schema columns so it never overwrites data columns.
    start_col = len(SHEET_HEADER_LIST) + 1
    end_col = start_col + 4
    _ensure_sheet_columns(sheet_name, end_col, spreadsheet_id)
    start_col_a1 = _col_to_a1(start_col)
    end_col_a1 = _col_to_a1(end_col)

    # Backward-compat cleanup for older writes that used fixed Y:AC and could collide on narrower schemas.
    clear_values(f"{sheet_name}!X1:AC2", spreadsheet_id)

    clear_values(f"{sheet_name}!{start_col_a1}1:{end_col_a1}2", spreadsheet_id)
    update_values(f"{sheet_name}!{start_col_a1}1:{end_col_a1}1", labels, spreadsheet_id)
    update_values(f"{sheet_name}!{start_col_a1}2:{end_col_a1}2", values, spreadsheet_id)


_IG_URL_RE = re.compile(r"^https?://(www\.)?instagram\.com/(p|reel|tv)/", re.I)
_MEDIA_TYPES = {"video", "image", "sidecar", "carousel", "reel"}
_DATE_RE = re.compile(r"\d{2}-\d{2}-\d{2}")


def _needs_repair(sample_rows: list[list[str]]) -> bool:
    if not sample_rows:
        return False
    try:
        url_idx = SHEET_HEADER_LIST.index("post_url")
    except ValueError:
        return False
    media_idx = SHEET_HEADER_LIST.index("media_type") if "media_type" in SHEET_HEADER_LIST else -1
    posted_idx = SHEET_HEADER_LIST.index("posted_at") if "posted_at" in SHEET_HEADER_LIST else -1
    checked = 0
    good = 0
    media_good = 0
    posted_good = 0
    for row in sample_rows:
        if not row:
            continue
        if url_idx < len(row) and row[url_idx]:
            checked += 1
            if _IG_URL_RE.search(str(row[url_idx]).strip()):
                good += 1
        if media_idx >= 0 and media_idx < len(row) and row[media_idx]:
            val = str(row[media_idx]).strip().lower()
            if val in _MEDIA_TYPES:
                media_good += 1
        if posted_idx >= 0 and posted_idx < len(row) and row[posted_idx]:
            val = str(row[posted_idx]).strip()
            if _DATE_RE.search(val):
                posted_good += 1
    # If we have rows but almost none look like IG URLs, assume columns are misaligned.
    if checked >= 3 and good <= max(1, checked // 4):
        return True
    # Secondary heuristic: URLs look fine but media_type and posted_at columns are junk (misaligned data).
    if checked >= 3:
        if media_idx >= 0 and media_good <= max(1, checked // 5):
            return True
        if posted_idx >= 0 and posted_good <= max(1, checked // 5):
            return True
    return False


def _repair_rows_from_legacy(all_rows: list[list[str]], new_header: list[str]) -> list[list[str]] | None:
    # Known legacy schemas we've shipped previously. We try each and pick the one that best matches IG URL patterns.
    legacy_candidates: list[list[str]] = [
        [
            "post_url","posted_at","handle","display_name","views","likes","comments",
            "velocity","velocity_percentile","velocity_trend","caption","hashtags","caption_mentions",
            "media_type","duration_seconds","display_url","video_url","tagged_users","music_info",
            "paid_partnership","sponsors","ai_title","ai_format","ai_intent","scanned_at"
        ],
        [
            "post_id","shortcode","posted_at","caption","likes","comments","views","media_type","url","last_updated",
            "comments_dup","views_dup","is_pinned","display_url","video_url","paid_partnership","sponsors","tagged_users",
            "music_info","ai_title","ai_format","ai_intent","scanned_at"
        ],
    ]

    def score_for(legacy_header: list[str]) -> int:
        idx = {k: i for i, k in enumerate(legacy_header)}
        s = 0
        for row in all_rows[:50]:
            post_url = ""
            if "post_url" in idx and idx["post_url"] < len(row):
                post_url = str(row[idx["post_url"]]).strip()
            elif "url" in idx and idx["url"] < len(row):
                post_url = str(row[idx["url"]]).strip()
            if _IG_URL_RE.search(post_url):
                s += 2
        return s

    best = None
    best_score = -1
    for cand in legacy_candidates:
        s = score_for(cand)
        if s > best_score:
            best_score = s
            best = cand

    if not best or best_score < 4:
        return None

    idx = {k: i for i, k in enumerate(best)}
    repaired: list[list[str]] = []
    for row in all_rows:
        row_dict = {}
        for k, i in idx.items():
            row_dict[k] = row[i] if i < len(row) else ""

        # Normalize legacy key names into our current schema key names.
        if "post_url" not in row_dict and "url" in row_dict:
            row_dict["post_url"] = row_dict.get("url", "")
        # Some legacy feeds stored @handle as the sheet name; if missing, leave blank (sync will fill).
        # Non-present keys will remain blank in the new schema.
        repaired.append([row_dict.get(col, "") for col in new_header])
    return repaired


def sort_by_posted_at(sheet_name: str, spreadsheet_id: str | None = None):
    spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
    sheet_id = _sheet_id_by_title(sheet_name, spreadsheet_id)
    if sheet_id is None:
        return
    try:
        posted_idx = SHEET_HEADER_LIST.index("posted_at")
    except ValueError:
        return
    # Sort all rows starting from row 3 (0-indexed row 2)
    service = _service()
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "sortRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(SHEET_HEADER_LIST),
                        },
                        "sortSpecs": [
                            {
                                "dimensionIndex": posted_idx,
                                "sortOrder": "DESCENDING",
                            }
                        ],
                    }
                }
            ]
        },
    ).execute()


FEEDER_SHEET_TITLE = "Feeder"


def ensure_billing_tab(spreadsheet_id: str, data: list[list[str]]):
    service = _service()
    sheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(title,sheetId))",
    ).execute()
    titles = [s["properties"]["title"] for s in sheet.get("sheets", [])]
    # Rename old sheet if it exists
    if "Billing/Usage" in titles and FEEDER_SHEET_TITLE not in titles:
        old_id = _sheet_id_by_title("Billing/Usage", spreadsheet_id)
        if old_id is not None:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": old_id,
                                    "title": FEEDER_SHEET_TITLE,
                                },
                                "fields": "title",
                            }
                        }
                    ]
                },
            ).execute()
        titles = [t if t != "Billing/Usage" else FEEDER_SHEET_TITLE for t in titles]

    if FEEDER_SHEET_TITLE not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": FEEDER_SHEET_TITLE}}}]},
        ).execute()
    clear_values(f"{FEEDER_SHEET_TITLE}!A1:Z200", spreadsheet_id)
    update_values(f"{FEEDER_SHEET_TITLE}!A1", data, spreadsheet_id)
    _format_billing_tab(spreadsheet_id)


def upsert_feeder_followers(spreadsheet_id: str, rows: list[list[str]]):
    service = _service()
    sheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(title,sheetId))",
    ).execute()
    titles = [s["properties"]["title"] for s in sheet.get("sheets", [])]
    if FEEDER_SHEET_TITLE not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": FEEDER_SHEET_TITLE}}}]},
        ).execute()

    header = [["handle", "followers_live", "updated_at"]]
    clear_values(f"{FEEDER_SHEET_TITLE}!AA1:AC1000", spreadsheet_id)
    update_values(f"{FEEDER_SHEET_TITLE}!AA1:AC1", header, spreadsheet_id)
    if rows:
        update_values(f"{FEEDER_SHEET_TITLE}!AA2", rows, spreadsheet_id)


def _format_billing_tab(spreadsheet_id: str):
    sheet_id = _sheet_id_by_title(FEEDER_SHEET_TITLE, spreadsheet_id)
    if sheet_id is None:
        return
    neon = {"red": 0.8, "green": 1.0, "blue": 0.0}  # #CCFF00
    black = {"red": 0.0, "green": 0.0, "blue": 0.0}  # #000000
    grey = {"red": 0.53, "green": 0.53, "blue": 0.53}  # #878788

    requests = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 3}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 28},
                "fields": "pixelSize",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                        "textFormat": {"bold": True, "foregroundColor": black, "fontSize": 18},
                        "horizontalAlignment": "LEFT",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.97, "green": 0.97, "blue": 0.97},
                        "textFormat": {"italic": True, "foregroundColor": grey},
                        "horizontalAlignment": "LEFT",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
        # Metric label cells (A,C,E,G) - subtle
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"foregroundColor": black, "bold": True}, "wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"foregroundColor": black, "bold": True}, "wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"foregroundColor": black, "bold": True}, "wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"foregroundColor": black, "bold": True}, "wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
            }
        },
        # Metric value cells (B,D,F,H) - neon for key numbers only
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 1, "endColumnIndex": 2},
                "cell": {"userEnteredFormat": {"backgroundColor": neon, "textFormat": {"bold": True, "fontSize": 22, "foregroundColor": black}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"backgroundColor": neon, "textFormat": {"bold": True, "fontSize": 22, "foregroundColor": black}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"backgroundColor": neon, "textFormat": {"bold": True, "fontSize": 22, "foregroundColor": black}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"backgroundColor": neon, "textFormat": {"bold": True, "fontSize": 22, "foregroundColor": black}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        # Section header
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 6, "endRowIndex": 7},
                "cell": {"userEnteredFormat": {"backgroundColor": black, "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 7, "endRowIndex": 8},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"bold": True, "foregroundColor": black}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 8},
                "properties": {"pixelSize": 230},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 2, "endIndex": 5},
                "properties": {"pixelSize": 58},
                "fields": "pixelSize",
            }
        },
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 5, "startColumnIndex": 0, "endColumnIndex": 8},
                "top": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "bottom": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "left": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "right": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "innerHorizontal": {"style": "SOLID", "width": 1, "color": grey},
                "innerVertical": {"style": "SOLID", "width": 1, "color": grey},
            }
        },
        {
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 7, "endRowIndex": 200, "startColumnIndex": 0, "endColumnIndex": 8},
                "top": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "bottom": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "left": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "right": {"style": "SOLID_MEDIUM", "width": 2, "color": black},
                "innerHorizontal": {"style": "SOLID", "width": 1, "color": grey},
                "innerVertical": {"style": "SOLID", "width": 1, "color": grey},
            }
        },
    ]
    service = _service()
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()
