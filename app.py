import json
import math
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from typing import Any, Dict, Tuple, List, Set, Optional

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(page_title="Live Profit Dashboard", layout="wide")

DB_PATH = "profit_dashboard.db"
STORE_TIMEZONE = "Asia/Jerusalem"

# -------------------------
# Config
# -------------------------
AUTO_SYNC_ENABLED_DEFAULT = True

SHOPIFY_AUTO_SYNC_MINUTES = 8
META_AUTO_SYNC_MINUTES = 10
CJ_AUTO_SYNC_MINUTES = 4

STRIPE_FEE_PERCENT = 0.029
STRIPE_FEE_FIXED = 0.30

PAYPAL_FEE_PERCENT = float(st.secrets.get("PAYPAL_FEE_PERCENT", 0.0349))
PAYPAL_FEE_FIXED = float(st.secrets.get("PAYPAL_FEE_FIXED", 0.49))

SHOPIFY_PAYMENTS_FEE_PERCENT = float(st.secrets.get("SHOPIFY_PAYMENTS_FEE_PERCENT", 0.0))
SHOPIFY_PAYMENTS_FEE_FIXED = float(st.secrets.get("SHOPIFY_PAYMENTS_FEE_FIXED", 0.0))

OTHER_GATEWAY_FEE_PERCENT = float(st.secrets.get("OTHER_GATEWAY_FEE_PERCENT", 0.029))
OTHER_GATEWAY_FEE_FIXED = float(st.secrets.get("OTHER_GATEWAY_FEE_FIXED", 0.30))

SHOPIFY_THIRD_PARTY_FEE_PERCENT = 0.01

CJ_PAGE_SIZE = 50
CJ_PAGE_FETCH_WORKERS = 4
CJ_DETAIL_FETCH_WORKERS = 10
CJ_MAX_PAGES_HARD_LIMIT = 260


# -------------------------
# Styling
# -------------------------
st.markdown(
    """
    <style>
    .main {
        background:
            radial-gradient(circle at top left, rgba(40,54,95,0.22), transparent 28%),
            radial-gradient(circle at top right, rgba(29,91,57,0.18), transparent 22%),
            linear-gradient(180deg, #0b0d12 0%, #10131a 100%);
    }

    .block-container {
        padding-top: 0.42rem;
        padding-bottom: 1rem;
        max-width: 1460px;
    }

    h1, h2, h3 {
        letter-spacing: -0.02em;
    }

    .dashboard-title {
        font-size: 1.95rem;
        font-weight: 800;
        line-height: 1.02;
        color: #f5f7fb;
        margin: 0;
        padding: 0;
        letter-spacing: -0.03em;
    }

    .dashboard-subtitle {
        color: #9ba6b5;
        font-size: 0.9rem;
        line-height: 1.28;
        margin-top: 0.18rem;
        margin-bottom: 0.08rem;
    }

    .section-title {
        font-size: 1rem;
        font-weight: 700;
        color: #f5f7fb;
        margin: 0.12rem 0 0.5rem 0;
        line-height: 1.1;
        letter-spacing: -0.01em;
    }

    .top-grid-gap {
        margin-bottom: 0.45rem;
    }

    .status-bar {
        background: linear-gradient(180deg, rgba(19,23,32,0.96) 0%, rgba(14,18,25,0.96) 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 9px 11px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.16);
        margin-bottom: 0.35rem;
    }

    .status-grid {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 8px 10px;
    }

    .status-title {
        color: #edf2f8;
        font-size: 0.88rem;
        font-weight: 700;
        line-height: 1.1;
    }

    .status-sub {
        color: #8d97a5;
        font-size: 0.74rem;
        line-height: 1.1;
    }

    .sync-chip {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 999px;
        padding: 4px 8px;
        color: #c8d0dc;
        font-size: 0.71rem;
        line-height: 1.05;
        white-space: nowrap;
    }

    .metric-card {
        background: linear-gradient(180deg, #171c26 0%, #121722 100%);
        border: 1px solid rgba(255,255,255,0.085);
        border-radius: 18px;
        padding: 15px 15px 12px 15px;
        box-shadow: 0 10px 28px rgba(0,0,0,0.18);
        min-height: 102px;
        margin-bottom: 0.08rem;
    }

    .metric-card.profit {
        background: linear-gradient(180deg, #173326 0%, #11271d 100%);
        border: 1px solid rgba(73,196,125,0.34);
        box-shadow: 0 10px 32px rgba(19,88,50,0.28);
    }

    .metric-card.revenue {
        background: linear-gradient(180deg, #1c2434 0%, #131a26 100%);
        border: 1px solid rgba(102,143,255,0.18);
    }

    .metric-card.soft {
        background: linear-gradient(180deg, #191d27 0%, #11151d 100%);
    }

    .metric-label {
        color: #9aa4b2;
        font-size: 0.84rem;
        margin-bottom: 6px;
        font-weight: 600;
    }

    .metric-value {
        color: #ffffff;
        font-size: 1.68rem;
        font-weight: 800;
        line-height: 1.02;
        margin-bottom: 3px;
        letter-spacing: -0.03em;
    }

    .metric-sub {
        color: #7f8896;
        font-size: 0.78rem;
        margin-top: 2px;
        line-height: 1.18;
    }

    .metric-row-gap {
        margin-top: 0.95rem;
    }

    .compact-note {
        color: #8d97a5;
        font-size: 0.82rem;
        line-height: 1.22;
        margin-bottom: 0.15rem;
    }

    .stRadio > div {
        margin-top: -0.1rem;
    }

    .stDateInput {
        margin-top: -0.05rem;
    }

    .stFileUploader {
        margin-top: 0.1rem;
    }

    .stAlert {
        padding-top: 0.56rem !important;
        padding-bottom: 0.56rem !important;
        border-radius: 14px !important;
    }

    div[data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.08);
    }

    div[data-testid="stHorizontalBlock"] {
        gap: 0.95rem !important;
    }

    .chart-wrap {
        margin-top: 0.05rem;
        margin-bottom: 0.15rem;
    }

    @media (max-width: 768px) {
        .block-container {
            padding-top: 0.26rem;
            padding-left: 0.62rem;
            padding-right: 0.62rem;
            padding-bottom: 0.75rem;
        }

        .dashboard-title {
            font-size: 1.42rem;
            line-height: 1.06;
        }

        .dashboard-subtitle {
            font-size: 0.82rem;
            line-height: 1.18;
            margin-top: 0.16rem;
            margin-bottom: 0.04rem;
        }

        .section-title {
            font-size: 0.93rem;
            margin: 0.08rem 0 0.35rem 0;
        }

        .status-bar {
            padding: 8px 9px;
            border-radius: 14px;
            margin-bottom: 0.28rem;
        }

        .status-title {
            font-size: 0.8rem;
        }

        .status-sub {
            font-size: 0.69rem;
        }

        .sync-chip {
            font-size: 0.66rem;
            padding: 4px 7px;
        }

        .metric-card {
            min-height: 90px;
            padding: 13px 13px 10px 13px;
            border-radius: 16px;
            margin-bottom: 0.16rem;
        }

        .metric-label {
            font-size: 0.8rem;
        }

        .metric-value {
            font-size: 1.34rem;
        }

        .metric-sub {
            font-size: 0.72rem;
        }

        .metric-row-gap {
            margin-top: 0.62rem;
        }

        div[data-testid="stHorizontalBlock"] {
            gap: 0.55rem !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True
)


def metric_card(label: str, value: str, sub: str = "", theme: str = ""):
    theme_class = f" {theme}" if theme else ""
    st.markdown(
        f"""
        <div class="metric-card{theme_class}">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# -------------------------
# Database
# -------------------------
def add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    existing_cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
    if column_name not in existing_cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_orders (
            order_id TEXT PRIMARY KEY,
            order_name TEXT,
            created_at TEXT,
            revenue REAL,
            currency TEXT,
            financial_status TEXT,
            line_items_json TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_order_meta (
            order_id TEXT PRIMARY KEY,
            processed_at TEXT,
            payment_gateways_json TEXT,
            is_test INTEGER DEFAULT 0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta_daily_spend (
            spend_date TEXT PRIMARY KEY,
            spend REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cj_order_costs (
            cj_order_id TEXT PRIMARY KEY,
            order_number TEXT,
            product_cost REAL,
            shipping_cost REAL,
            total_cost REAL,
            raw_json TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS manual_order_costs (
            order_name TEXT PRIMARY KEY,
            total_cost REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            source TEXT PRIMARY KEY,
            last_sync_at TEXT,
            last_status TEXT,
            last_message TEXT
        )
    """)

    add_column_if_missing(conn, "shopify_orders", "original_total_price", "REAL")
    add_column_if_missing(conn, "shopify_orders", "current_total_price", "REAL")
    add_column_if_missing(conn, "shopify_orders", "total_refunded", "REAL")
    add_column_if_missing(conn, "shopify_orders", "cancelled_at", "TEXT")
    add_column_if_missing(conn, "shopify_orders", "refunds_json", "TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_shopify_order_name ON shopify_orders(order_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shopify_created_at ON shopify_orders(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cj_order_number ON cj_order_costs(order_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_manual_order_name ON manual_order_costs(order_name)")

    conn.commit()
    return conn


# -------------------------
# Helpers
# -------------------------
def safe_float(val, default=0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def normalize_order_name(order_name: str) -> str:
    if order_name is None:
        return ""
    return str(order_name).replace("#", "").strip()


def display_with_1_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.index = range(1, len(out) + 1)
    return out


def utcnow_iso() -> str:
    return datetime.utcnow().isoformat()


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def upsert_sync_state(source: str, status: str, message: str = "") -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO sync_state (source, last_sync_at, last_status, last_message)
            VALUES (?, ?, ?, ?)
            """,
            (source, utcnow_iso(), status, message),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_sync_state() -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query("SELECT * FROM sync_state", conn)
    finally:
        conn.close()


def should_sync(source: str, min_interval_minutes: int) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT last_sync_at FROM sync_state WHERE source = ?",
            (source,),
        ).fetchone()

        if not row or not row[0]:
            return True

        last_dt = parse_iso_dt(row[0])
        if last_dt is None:
            return True

        delta = datetime.utcnow() - last_dt.replace(tzinfo=None)
        return delta >= timedelta(minutes=min_interval_minutes)
    finally:
        conn.close()


def detect_column(df: pd.DataFrame, candidates: List[str], required: bool = True) -> Optional[str]:
    normalized_map = {str(col).strip().lower(): col for col in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in normalized_map:
            return normalized_map[key]
    if required:
        raise ValueError(
            f"CSV is missing required columns. Expected one of {candidates}. Found columns: {list(df.columns)}"
        )
    return None


# -------------------------
# Shopify
# -------------------------
def get_shopify_access_token() -> str:
    store = st.secrets["SHOPIFY_STORE"]
    client_id = st.secrets["SHOPIFY_CLIENT_ID"]
    client_secret = st.secrets["SHOPIFY_CLIENT_SECRET"]

    url = f"https://{store}/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    resp = requests.post(url, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError(f"Could not get Shopify access token: {data}")

    return access_token


def shopify_headers() -> Dict[str, str]:
    token = get_shopify_access_token()
    return {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }


def shopify_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    store = st.secrets["SHOPIFY_STORE"]
    url = f"https://{store}/admin/api/2026-01/graphql.json"

    resp = requests.post(
        url,
        headers=shopify_headers(),
        json={"query": query, "variables": variables},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        raise RuntimeError(data["errors"])

    return data["data"]


def sync_shopify_orders(days_back: int = 60) -> int:
    """Sync orders using Shopify REST Admin API.

    Uses current_total_price from REST which reliably reflects all order
    edits including accepted/removed upsells.
    """
    store = st.secrets["SHOPIFY_STORE"]
    api_version = st.secrets.get("SHOPIFY_API_VERSION", "2026-01")
    headers = shopify_headers()

    updated_after = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")

    conn = get_conn()
    try:
        count = 0
        url = f"https://{store}/admin/api/{api_version}/orders.json"
        params = {
            "updated_at_min": updated_after,
            "status": "any",
            "limit": 250,
        }

        while True:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            for order in data.get("orders", []):
                order_id = f"gid://shopify/Order/{order['id']}"
                order_name = order.get("name", "")
                created_at = order.get("created_at", "")
                processed_at = order.get("processed_at", "")
                cancelled_at = order.get("cancelled_at")
                is_test = 1 if order.get("test", False) else 0
                financial_status = (order.get("financial_status") or "").upper()
                payment_gateways = order.get("payment_gateway_names") or []
                currency = order.get("currency", "USD")

                original_total_price = safe_float(order.get("total_price"))
                current_total_price = safe_float(order.get("current_total_price"))

                total_refunded = 0.0
                refunds = []
                for refund in (order.get("refunds") or []):
                    refund_amount = 0.0
                    for txn in (refund.get("transactions") or []):
                        if txn.get("kind") == "refund" and txn.get("status") == "success":
                            refund_amount += safe_float(txn.get("amount"))
                    total_refunded += refund_amount
                    refunds.append({
                        "id": str(refund.get("id", "")),
                        "createdAt": refund.get("created_at"),
                        "processedAt": refund.get("processed_at"),
                        "amount": refund_amount,
                        "currencyCode": currency,
                    })

                # current_total_price from REST API is the source of truth.
                # It reflects all order edits, upsells, refunds, and removals.
                effective_revenue = current_total_price

                conn.execute(
                    """
                    INSERT OR REPLACE INTO shopify_orders
                    (
                        order_id, order_name, created_at, revenue, currency, financial_status, line_items_json,
                        original_total_price, current_total_price, total_refunded, cancelled_at, refunds_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        order_name,
                        created_at,
                        effective_revenue,
                        currency,
                        financial_status,
                        "[]",
                        original_total_price,
                        current_total_price,
                        total_refunded,
                        cancelled_at,
                        json.dumps(refunds),
                    ),
                )

                conn.execute(
                    """
                    INSERT OR REPLACE INTO shopify_order_meta
                    (order_id, processed_at, payment_gateways_json, is_test)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        processed_at,
                        json.dumps(payment_gateways),
                        is_test,
                    ),
                )

                count += 1

            conn.commit()

            # Shopify REST pagination via Link header
            link_header = resp.headers.get("Link", "")
            next_url = None
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
                    break

            if not next_url:
                break
            url = next_url
            params = None

        upsert_sync_state("shopify", "ok", f"Synced {count} orders")
        return count
    except Exception as e:
        upsert_sync_state("shopify", "error", str(e))
        raise
    finally:
        conn.close()


# -------------------------
# Meta
# -------------------------
def sync_meta_spend(days_back: int = 60) -> int:
    account_id = st.secrets["META_AD_ACCOUNT_ID"]
    token = st.secrets["META_ACCESS_TOKEN"]

    now_israel = datetime.now(tz=pd.Timestamp.now(tz=STORE_TIMEZONE).tzinfo)
    since = (now_israel - timedelta(days=days_back)).strftime("%Y-%m-%d")
    until = now_israel.strftime("%Y-%m-%d")

    url = f"https://graph.facebook.com/v25.0/{account_id}/insights"
    params = {
        "access_token": token,
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "fields": "date_start,spend",
        "limit": 100,
        "time_zone": STORE_TIMEZONE,
    }

    conn = get_conn()
    try:
        total = 0

        while True:
            resp = requests.get(url, params=params, timeout=60)

            if resp.status_code != 200:
                raise RuntimeError(f"Meta error {resp.status_code}: {resp.text}")

            data = resp.json()

            for row in data.get("data", []):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO meta_daily_spend (spend_date, spend)
                    VALUES (?, ?)
                    """,
                    (row["date_start"], safe_float(row["spend"])),
                )
                total += 1

            conn.commit()

            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break

            url = next_url
            params = None

        upsert_sync_state("meta", "ok", f"Synced {total} rows")
        return total
    except Exception as e:
        upsert_sync_state("meta", "error", str(e))
        raise
    finally:
        conn.close()


# -------------------------
# CJ
# -------------------------
@st.cache_data(ttl=60 * 60 * 24 * 15, show_spinner=False)
def cj_get_access_token() -> str:
    url = "https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken"
    headers = {"Content-Type": "application/json"}
    payload = {
        "email": st.secrets["CJ_EMAIL"],
        "password": st.secrets["CJ_API_KEY"],
    }

    for attempt in range(4):
        resp = requests.post(url, headers=headers, json=payload, timeout=60)

        if resp.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue

        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 200:
            raise RuntimeError(f"Failed to get CJ token: {data.get('message', data)}")

        token = (data.get("data") or {}).get("accessToken")
        if not token:
            raise RuntimeError(f"CJ token missing in response: {data}")

        return token

    raise RuntimeError("CJ auth failed with rate limit (429) after multiple retries.")


def get_shopify_order_names_in_range(start_date: date, end_date: date) -> List[str]:
    conn = get_conn()
    try:
        orders = pd.read_sql_query("""
            SELECT o.order_name, COALESCE(m.processed_at, o.created_at) AS report_dt
            FROM shopify_orders o
            LEFT JOIN shopify_order_meta m ON o.order_id = m.order_id
        """, conn)

        if orders.empty:
            return []

        orders["report_dt"] = pd.to_datetime(orders["report_dt"], utc=True, errors="coerce")
        orders = orders.dropna(subset=["report_dt"])
        orders["order_date"] = orders["report_dt"].dt.tz_convert(STORE_TIMEZONE).dt.date

        orders = orders[(orders["order_date"] >= start_date) & (orders["order_date"] <= end_date)].copy()
        if orders.empty:
            return []

        names = orders["order_name"].astype(str).apply(normalize_order_name).tolist()

        seen = set()
        out = []
        for name in names:
            if name and name not in seen:
                seen.add(name)
                out.append(name)

        return out
    finally:
        conn.close()


def get_cached_cj_cost_map() -> Dict[str, float]:
    conn = get_conn()
    try:
        cj = pd.read_sql_query("SELECT order_number, total_cost FROM cj_order_costs", conn)
        if cj.empty:
            return {}

        cj["order_number"] = cj["order_number"].astype(str).apply(normalize_order_name)
        cj["total_cost"] = pd.to_numeric(cj["total_cost"], errors="coerce").fillna(0.0)

        out = {}
        for _, row in cj.iterrows():
            order_num = row["order_number"]
            if not order_num:
                continue
            out[order_num] = max(out.get(order_num, 0.0), float(row["total_cost"]))
        return out
    finally:
        conn.close()


def get_manual_cost_order_names() -> Set[str]:
    conn = get_conn()
    try:
        manual = pd.read_sql_query("SELECT order_name FROM manual_order_costs", conn)
        if manual.empty:
            return set()
        return set(manual["order_name"].astype(str).apply(normalize_order_name).tolist())
    finally:
        conn.close()


def get_missing_cj_order_names(target_order_names: List[str]) -> List[str]:
    if not target_order_names:
        return []

    cached_cj = get_cached_cj_cost_map()
    manual_orders = get_manual_cost_order_names()

    missing = []
    for order_name in target_order_names:
        if order_name in manual_orders:
            continue

        cached_total = cached_cj.get(order_name)
        if cached_total is None or safe_float(cached_total) <= 0:
            missing.append(order_name)

    return missing


def cj_extract_order_number(order_row: dict) -> str:
    candidates = [
        order_row.get("orderNum"),
        order_row.get("orderNumber"),
        order_row.get("customerOrderId"),
        order_row.get("referenceNum"),
        order_row.get("merchantOrderNo"),
    ]
    for candidate in candidates:
        normalized = normalize_order_name(candidate)
        if normalized:
            return normalized
    return ""


@st.cache_data(ttl=60 * 10, show_spinner=False)
def get_cj_orders_page(token: str, page_num: int, page_size: int = 50) -> List[dict]:
    url = "https://developers.cjdropshipping.com/api2.0/v1/shopping/order/list"
    headers = {"CJ-Access-Token": token}
    params = {"pageNum": page_num, "pageSize": page_size}

    for attempt in range(3):
        resp = requests.get(url, headers=headers, params=params, timeout=60)

        if resp.status_code == 429:
            time.sleep(1.2 * (attempt + 1))
            continue

        if resp.status_code != 200:
            raise RuntimeError(f"CJ order list HTTP error {resp.status_code}: {resp.text}")

        data = resp.json()

        if data.get("code") != 200:
            raise RuntimeError(f"CJ order list API error: {data.get('message', data)}")

        block = data.get("data") or {}
        return block.get("list") or []

    raise RuntimeError(f"CJ order list rate-limited repeatedly on page {page_num}")


@st.cache_data(ttl=60 * 10, show_spinner=False)
def get_cj_order_detail(token: str, order_id: str) -> dict:
    url = "https://developers.cjdropshipping.com/api2.0/v1/shopping/order/getOrderDetail"
    headers = {
        "CJ-Access-Token": token,
        "Content-Type": "application/json",
    }
    payload = {"orderId": order_id}

    for attempt in range(3):
        resp = requests.post(url, headers=headers, json=payload, timeout=60)

        if resp.status_code == 429:
            time.sleep(1.2 * (attempt + 1))
            continue

        if resp.status_code != 200:
            return {}

        data = resp.json()
        detail = data.get("data")
        if isinstance(detail, dict):
            return detail

        result = data.get("result")
        if isinstance(result, dict):
            return result

        return {}

    return {}


def extract_cj_costs(list_row: dict, detail: dict) -> Tuple[float, float, float]:
    product_cost = safe_float(
        list_row.get("productAmount")
        or list_row.get("productCost")
        or list_row.get("goodsAmount")
        or detail.get("productAmount")
        or detail.get("productCost")
        or detail.get("goodsAmount")
    )

    shipping_cost = safe_float(
        list_row.get("freightAmount")
        or list_row.get("shippingCost")
        or list_row.get("freight")
        or detail.get("freightAmount")
        or detail.get("shippingCost")
        or detail.get("freight")
    )

    total_cost = safe_float(
        list_row.get("orderAmount")
        or list_row.get("totalAmount")
        or detail.get("orderAmount")
        or detail.get("totalAmount")
    )

    if total_cost <= 0:
        total_cost = product_cost + shipping_cost

    return product_cost, shipping_cost, total_cost


def fetch_cj_order_pages_parallel(token: str, page_numbers: List[int], page_size: int = 50, max_workers: int = 4) -> Dict[int, List[dict]]:
    results: Dict[int, List[dict]] = {}

    def worker(page_num: int) -> Tuple[int, List[dict]]:
        return page_num, get_cj_orders_page(token, page_num, page_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, p) for p in page_numbers]
        for fut in as_completed(futures):
            page_num, rows = fut.result()
            results[page_num] = rows

    return results


def fetch_cj_detail_parallel(token: str, rows_needing_detail: List[Tuple[str, dict]], max_workers: int = 10) -> Dict[str, dict]:
    results: Dict[str, dict] = {}

    def worker(order_number: str, row: dict) -> Tuple[str, dict]:
        cj_order_id = str(row.get("orderId") or row.get("id") or "").strip()
        if not cj_order_id:
            return order_number, {}
        return order_number, get_cj_order_detail(token, cj_order_id)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, order_number, row) for order_number, row in rows_needing_detail]
        for fut in as_completed(futures):
            order_number, detail = fut.result()
            results[order_number] = detail

    return results


def cleanup_duplicate_cj_rows() -> None:
    conn = get_conn()
    try:
        cj = pd.read_sql_query("SELECT * FROM cj_order_costs", conn)
        if cj.empty:
            return

        cj["order_number"] = cj["order_number"].astype(str).apply(normalize_order_name)
        cj["total_cost"] = pd.to_numeric(cj["total_cost"], errors="coerce").fillna(0.0)
        cj = cj[cj["order_number"] != ""].copy()

        if cj.empty:
            return

        cj = cj.sort_values(["order_number", "total_cost", "cj_order_id"], ascending=[True, False, False])
        keep = cj.drop_duplicates(subset=["order_number"], keep="first")

        keep_ids = set(keep["cj_order_id"].astype(str).tolist())
        all_ids = set(cj["cj_order_id"].astype(str).tolist())
        delete_ids = list(all_ids - keep_ids)

        for cid in delete_ids:
            conn.execute("DELETE FROM cj_order_costs WHERE cj_order_id = ?", (cid,))
        conn.commit()
    finally:
        conn.close()


def estimate_cj_pages_to_scan(missing_count: int) -> int:
    if missing_count <= 0:
        return 0
    base = max(40, int(math.ceil(missing_count / 12.0) * 8))
    return min(max(base, 60), CJ_MAX_PAGES_HARD_LIMIT)


def sync_cj_costs_for_range(start_date: date, end_date: date, max_pages: Optional[int] = None) -> Tuple[int, int, int]:
    target_order_names = get_shopify_order_names_in_range(start_date, end_date)
    target_count = len(target_order_names)

    if not target_order_names:
        return 0, 0, 0

    missing_order_names = get_missing_cj_order_names(target_order_names)
    already_cached_count = target_count - len(missing_order_names)

    if not missing_order_names:
        cleanup_duplicate_cj_rows()
        upsert_sync_state("cj", "ok", "Nothing missing for selected range")
        return 0, already_cached_count, target_count

    token = cj_get_access_token()
    missing_set: Set[str] = set(missing_order_names)
    found_rows: Dict[str, dict] = {}

    dynamic_max_pages = estimate_cj_pages_to_scan(len(missing_set))
    effective_max_pages = min(max_pages or dynamic_max_pages, CJ_MAX_PAGES_HARD_LIMIT)

    progress_text = st.empty()
    progress_bar = st.progress(0)

    page = 1
    batch_pages = 8

    while page <= effective_max_pages and len(found_rows) < len(missing_set):
        page_numbers = list(range(page, min(page + batch_pages, effective_max_pages + 1)))
        progress_text.info(
            f"Loading missing CJ COGS... scanning pages {page_numbers[0]}-{page_numbers[-1]} / {effective_max_pages} | "
            f"found {len(found_rows)} / {len(missing_set)}"
        )

        page_map = fetch_cj_order_pages_parallel(
            token,
            page_numbers,
            page_size=CJ_PAGE_SIZE,
            max_workers=CJ_PAGE_FETCH_WORKERS
        )

        saw_any_rows = False
        for p in sorted(page_numbers):
            rows = page_map.get(p, [])
            if rows:
                saw_any_rows = True

            for row in rows:
                order_number = cj_extract_order_number(row)
                if order_number and order_number in missing_set and order_number not in found_rows:
                    found_rows[order_number] = row

        progress_bar.progress(min(page_numbers[-1] / effective_max_pages, 1.0))

        if not saw_any_rows:
            break

        page += batch_pages
        time.sleep(0.03)

    rows_needing_detail: List[Tuple[str, dict]] = []
    final_cost_rows: Dict[str, Tuple[dict, dict, float, float, float]] = {}

    for order_number, row in found_rows.items():
        product_cost, shipping_cost, total_cost = extract_cj_costs(row, {})
        if total_cost > 0:
            final_cost_rows[order_number] = (row, {}, product_cost, shipping_cost, total_cost)
        else:
            rows_needing_detail.append((order_number, row))

    if rows_needing_detail:
        progress_text.info(f"Loading CJ detail for {len(rows_needing_detail)} order(s)...")
        detail_map = fetch_cj_detail_parallel(token, rows_needing_detail, max_workers=CJ_DETAIL_FETCH_WORKERS)

        for order_number, row in rows_needing_detail:
            detail = detail_map.get(order_number, {})
            product_cost, shipping_cost, total_cost = extract_cj_costs(row, detail)
            final_cost_rows[order_number] = (row, detail, product_cost, shipping_cost, total_cost)

    conn = get_conn()
    try:
        synced_count = 0

        for order_number, bundle in final_cost_rows.items():
            row, detail, product_cost, shipping_cost, total_cost = bundle
            cj_order_id = str(row.get("orderId") or row.get("id") or "").strip()
            if not cj_order_id:
                continue

            conn.execute("DELETE FROM cj_order_costs WHERE order_number = ?", (order_number,))

            conn.execute(
                """
                INSERT OR REPLACE INTO cj_order_costs
                (cj_order_id, order_number, product_cost, shipping_cost, total_cost, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cj_order_id,
                    order_number,
                    product_cost,
                    shipping_cost,
                    total_cost,
                    json.dumps({"list_row": row, "detail": detail}),
                ),
            )
            synced_count += 1

        conn.commit()
    finally:
        conn.close()
        progress_bar.empty()
        progress_text.empty()

    cleanup_duplicate_cj_rows()

    not_found_count = len(missing_set - set(found_rows.keys()))
    if not_found_count > 0:
        st.warning(
            f"{not_found_count} selected order(s) still were not found in scanned CJ pages. "
            f"If this was a very large date range, run Force CJ Sync once more after Shopify data finishes loading."
        )

    upsert_sync_state("cj", "ok", f"Synced {synced_count} CJ rows")
    return synced_count, already_cached_count, target_count


# -------------------------
# Manual override CSV
# -------------------------
def import_manual_cost_csv(file) -> int:
    name = str(file.name).lower()
    if name.endswith(".xlsx"):
        df = pd.read_excel(file)
    else:
        df = pd.read_csv(file)

    if df.empty:
        raise ValueError("CSV/XLSX is empty")

    order_col = detect_column(
        df,
        ["order_name", "Order", "order", "Order ID", "ShopifyOrderID", "Shopify Order", "Name"]
    )
    cost_col = detect_column(
        df,
        ["total_cost", "Total", "total", "Cost", "Real Cost", "SupplierTotalPriceAdjusted"]
    )

    clean_df = df[[order_col, cost_col]].copy()
    clean_df.columns = ["order_name", "total_cost"]

    clean_df["order_name"] = clean_df["order_name"].astype(str).apply(normalize_order_name)
    clean_df["total_cost"] = pd.to_numeric(clean_df["total_cost"], errors="coerce")

    clean_df = clean_df.dropna(subset=["order_name", "total_cost"])
    clean_df = clean_df[clean_df["order_name"] != ""].copy()

    if clean_df.empty:
        raise ValueError("Could not find valid order/cost rows in the uploaded file")

    clean_df = clean_df.drop_duplicates(subset=["order_name"], keep="last")

    conn = get_conn()
    try:
        for _, row in clean_df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO manual_order_costs (order_name, total_cost)
                VALUES (?, ?)
                """,
                (row["order_name"], float(row["total_cost"])),
            )
        conn.commit()
        return len(clean_df)
    finally:
        conn.close()


# -------------------------
# Fees
# -------------------------
def detect_gateway_name(payment_gateways_json: str) -> str:
    try:
        gateways = json.loads(payment_gateways_json) if payment_gateways_json else []
    except Exception:
        gateways = []

    if not isinstance(gateways, list):
        gateways = [str(gateways)]

    joined = " | ".join([str(x) for x in gateways]).lower()

    if "paypal" in joined:
        return "paypal"
    if "stripe" in joined:
        return "stripe"
    if "shopify payments" in joined or "shopify_payments" in joined:
        return "shopify_payments"
    if joined.strip():
        return "other"
    return "unknown"


def fee_rule_for_gateway(gateway_key: str) -> Tuple[float, float]:
    if gateway_key == "stripe":
        return STRIPE_FEE_PERCENT + SHOPIFY_THIRD_PARTY_FEE_PERCENT, STRIPE_FEE_FIXED
    if gateway_key == "paypal":
        return PAYPAL_FEE_PERCENT + SHOPIFY_THIRD_PARTY_FEE_PERCENT, PAYPAL_FEE_FIXED
    if gateway_key == "shopify_payments":
        return SHOPIFY_PAYMENTS_FEE_PERCENT, SHOPIFY_PAYMENTS_FEE_FIXED
    if gateway_key == "other":
        return OTHER_GATEWAY_FEE_PERCENT + SHOPIFY_THIRD_PARTY_FEE_PERCENT, OTHER_GATEWAY_FEE_FIXED
    return OTHER_GATEWAY_FEE_PERCENT + SHOPIFY_THIRD_PARTY_FEE_PERCENT, OTHER_GATEWAY_FEE_FIXED


# -------------------------
# Reporting
# -------------------------
def dedupe_cj_for_merge(cj: pd.DataFrame) -> pd.DataFrame:
    if cj.empty:
        return cj

    cj = cj.copy()
    cj["order_number"] = cj["order_number"].astype(str).apply(normalize_order_name)
    cj["total_cost"] = pd.to_numeric(cj["total_cost"], errors="coerce").fillna(0.0)
    cj = cj[cj["order_number"] != ""].copy()

    if cj.empty:
        return cj

    cj = cj.sort_values(["order_number", "total_cost", "cj_order_id"], ascending=[True, False, False])
    cj = cj.drop_duplicates(subset=["order_number"], keep="first")
    return cj


def build_refund_rows(orders_df: pd.DataFrame) -> pd.DataFrame:
    refund_rows = []

    for _, row in orders_df.iterrows():
        if safe_float(row.get("is_cancelled_int")) == 1:
            continue

        refunds_raw = row.get("refunds_json")
        if not refunds_raw:
            continue

        try:
            refunds = json.loads(refunds_raw)
        except Exception:
            refunds = []

        if not isinstance(refunds, list):
            continue

        for idx, refund in enumerate(refunds):
            refund_amount = safe_float(refund.get("amount"))
            refund_ts = refund.get("processedAt") or refund.get("createdAt")
            if refund_amount <= 0 or not refund_ts:
                continue

            refund_dt = pd.to_datetime(refund_ts, utc=True, errors="coerce")
            if pd.isna(refund_dt):
                continue

            refund_rows.append(
                {
                    "event_id": f"{row['order_id']}__refund__{idx}",
                    "order_id": row["order_id"],
                    "order_name": row["order_name"],
                    "event_type": "refund",
                    "event_dt": refund_dt,
                    "event_date": refund_dt.tz_convert(STORE_TIMEZONE).strftime("%Y-%m-%d"),
                    "revenue": -refund_amount,
                    "cogs": 0.0,
                    "gateway_key": row["gateway_key"],
                    "payment_fee": 0.0,
                    "cost_source": "refund_adjustment",
                    "financial_status": row["financial_status"],
                    "allocated_ad_spend": 0.0,
                }
            )

    return pd.DataFrame(refund_rows)


def build_cancel_rows(orders_df: pd.DataFrame) -> pd.DataFrame:
    cancel_rows = []

    for _, row in orders_df.iterrows():
        cancelled_at = row.get("cancelled_at")
        if pd.isna(cancelled_at):
            continue

        cancel_dt = pd.to_datetime(cancelled_at, utc=True, errors="coerce")
        if pd.isna(cancel_dt):
            continue

        original_revenue = safe_float(row.get("original_total_price"))
        cogs = safe_float(row.get("cogs_base"))
        fee_pct = safe_float(row.get("fee_pct"))
        fee_fixed = safe_float(row.get("fee_fixed"))
        original_fee = (original_revenue * fee_pct) + fee_fixed if original_revenue > 0 else 0.0

        cancel_rows.append(
            {
                "event_id": f"{row['order_id']}__cancel",
                "order_id": row["order_id"],
                "order_name": row["order_name"],
                "event_type": "cancel",
                "event_dt": cancel_dt,
                "event_date": cancel_dt.tz_convert(STORE_TIMEZONE).strftime("%Y-%m-%d"),
                "revenue": -original_revenue,
                "cogs": -cogs,
                "gateway_key": row["gateway_key"],
                "payment_fee": -original_fee,
                "cost_source": "cancel_adjustment",
                "financial_status": row["financial_status"],
                "allocated_ad_spend": 0.0,
            }
        )

    return pd.DataFrame(cancel_rows)


def load_profit_df() -> pd.DataFrame:
    conn = get_conn()
    try:
        orders = pd.read_sql_query("SELECT * FROM shopify_orders", conn)
        order_meta = pd.read_sql_query("SELECT * FROM shopify_order_meta", conn)
        meta = pd.read_sql_query("SELECT * FROM meta_daily_spend", conn)
        cj = pd.read_sql_query("SELECT * FROM cj_order_costs", conn)
        manual = pd.read_sql_query("SELECT * FROM manual_order_costs", conn)
    finally:
        conn.close()

    if orders.empty:
        return pd.DataFrame()

    if order_meta.empty:
        order_meta = pd.DataFrame(columns=["order_id", "processed_at", "payment_gateways_json", "is_test"])

    orders = orders.merge(order_meta, how="left", on="order_id")

    orders["created_at"] = pd.to_datetime(orders["created_at"], utc=True, errors="coerce")
    orders["processed_at"] = pd.to_datetime(orders["processed_at"], utc=True, errors="coerce")
    orders["cancelled_at"] = pd.to_datetime(orders.get("cancelled_at"), utc=True, errors="coerce")

    orders["report_dt"] = orders["processed_at"].combine_first(orders["created_at"])
    orders = orders.dropna(subset=["report_dt"]).copy()

    orders["sale_date"] = orders["report_dt"].dt.tz_convert(STORE_TIMEZONE).dt.strftime("%Y-%m-%d")
    orders["order_name_normalized"] = orders["order_name"].apply(normalize_order_name)

    if "is_test" in orders.columns:
        orders["is_test"] = orders["is_test"].fillna(0).astype(int)
        orders = orders[orders["is_test"] == 0].copy()

    if not manual.empty:
        manual["order_name"] = manual["order_name"].apply(normalize_order_name)
        manual["total_cost"] = pd.to_numeric(manual["total_cost"], errors="coerce")

    cj = dedupe_cj_for_merge(cj)

    orders = orders.merge(
        manual,
        how="left",
        left_on="order_name_normalized",
        right_on="order_name",
        suffixes=("", "_manual"),
    )

    if not cj.empty:
        orders = orders.merge(
            cj[["order_number", "total_cost"]],
            how="left",
            left_on="order_name_normalized",
            right_on="order_number",
            suffixes=("", "_cj"),
        )
    else:
        orders["total_cost_cj"] = math.nan

    orders["original_total_price"] = pd.to_numeric(orders.get("original_total_price"), errors="coerce").fillna(
        pd.to_numeric(orders.get("revenue"), errors="coerce").fillna(0.0)
    )
    orders["current_total_price"] = pd.to_numeric(orders.get("current_total_price"), errors="coerce").fillna(0.0)
    orders["total_refunded"] = pd.to_numeric(orders.get("total_refunded"), errors="coerce").fillna(0.0)
    orders["cogs_base"] = orders["total_cost"].combine_first(orders["total_cost_cj"]).fillna(0.0)

    orders["payment_gateways_json"] = orders["payment_gateways_json"].fillna("[]")
    orders["gateway_key"] = orders["payment_gateways_json"].apply(detect_gateway_name)

    fee_parts = orders["gateway_key"].apply(fee_rule_for_gateway)
    orders["fee_pct"] = fee_parts.apply(lambda x: x[0])
    orders["fee_fixed"] = fee_parts.apply(lambda x: x[1])
    orders["is_cancelled_int"] = orders["cancelled_at"].notna().astype(int)

    sale_rows = orders[
        [
            "order_id",
            "order_name",
            "report_dt",
            "sale_date",
            "original_total_price",
            "cogs_base",
            "gateway_key",
            "financial_status",
            "fee_pct",
            "fee_fixed",
            "total_cost",
            "total_cost_cj",
        ]
    ].copy()

    sale_rows["event_id"] = sale_rows["order_id"].astype(str) + "__sale"
    sale_rows["event_type"] = "sale"
    sale_rows["event_dt"] = sale_rows["report_dt"]
    sale_rows["event_date"] = sale_rows["sale_date"]
    sale_rows["revenue"] = sale_rows["original_total_price"].fillna(0.0)
    sale_rows["cogs"] = sale_rows["cogs_base"].fillna(0.0)
    sale_rows["payment_fee"] = (sale_rows["revenue"] * sale_rows["fee_pct"]) + sale_rows["fee_fixed"]
    sale_rows["allocated_ad_spend"] = 0.0

    def cost_source_from_row(row) -> str:
        if pd.notna(row.get("total_cost")):
            return "manual"
        if pd.notna(row.get("total_cost_cj")) and safe_float(row.get("total_cost_cj")) > 0:
            return "cj"
        return "missing"

    sale_rows["cost_source"] = sale_rows.apply(cost_source_from_row, axis=1)

    sale_rows = sale_rows[
        [
            "event_id",
            "order_id",
            "order_name",
            "event_type",
            "event_dt",
            "event_date",
            "revenue",
            "cogs",
            "payment_fee",
            "allocated_ad_spend",
            "cost_source",
            "gateway_key",
            "financial_status",
        ]
    ]

    refund_rows = build_refund_rows(orders)
    cancel_rows = build_cancel_rows(orders)

    all_rows = [sale_rows]
    if not refund_rows.empty:
        all_rows.append(refund_rows)
    if not cancel_rows.empty:
        all_rows.append(cancel_rows)

    df = pd.concat(all_rows, ignore_index=True)

    df = df.merge(meta, how="left", left_on="event_date", right_on="spend_date")
    df["spend"] = df["spend"].fillna(0.0)

    sale_mask = df["event_type"] == "sale"
    daily_sale_revenue = df[sale_mask].groupby("event_date")["revenue"].sum().to_dict()

    def alloc_ad_spend(row):
        if row["event_type"] != "sale":
            return 0.0
        day_total = safe_float(daily_sale_revenue.get(row["event_date"], 0.0))
        if day_total <= 0:
            return 0.0
        return (safe_float(row["revenue"]) / day_total) * safe_float(row["spend"])

    df["allocated_ad_spend"] = df.apply(alloc_ad_spend, axis=1)

    df["profit"] = df["revenue"] - df["cogs"] - df["allocated_ad_spend"] - df["payment_fee"]
    df["margin_pct"] = df["profit"] / df["revenue"].replace(0, math.nan) * 100
    df["order_date"] = df["event_date"]

    df = df.drop_duplicates(subset=["event_id"], keep="first")

    return df[
        [
            "event_id",
            "event_type",
            "order_name",
            "order_date",
            "revenue",
            "cogs",
            "payment_fee",
            "allocated_ad_spend",
            "profit",
            "margin_pct",
            "cost_source",
            "gateway_key",
            "financial_status",
        ]
    ].sort_values(["order_date", "order_name", "event_type"], ascending=[False, False, True])


# -------------------------
# Date filters
# -------------------------
def get_date_range_from_preset(preset: str) -> Tuple[date, date]:
    today = pd.Timestamp.now(tz=STORE_TIMEZONE).date()

    if preset == "Today":
        return today, today
    if preset == "Yesterday":
        y = today - timedelta(days=1)
        return y, y
    if preset == "Last 7 Days":
        return today - timedelta(days=6), today
    if preset == "Last 30 Days":
        return today - timedelta(days=29), today
    if preset == "Last 90 Days":
        return today - timedelta(days=89), today
    if preset == "Last 365 Days":
        return today - timedelta(days=364), today

    return today - timedelta(days=29), today


def compute_days_back_for_sync(start_date: date, end_date: date) -> int:
    span = max((end_date - start_date).days + 1, 1)
    return max(span + 7, 21)


# -------------------------
# Sync orchestration
# -------------------------
def run_sync_all(start_date: date, end_date: date, force: bool = False) -> Dict[str, Any]:
    days_back = compute_days_back_for_sync(start_date, end_date)
    result = {
        "shopify_orders": 0,
        "meta_rows": 0,
        "cj_rows": 0,
        "cj_cached_or_manual": 0,
        "cj_targets": 0,
        "did_anything": False,
    }

    errors = []

    if force or should_sync("shopify", SHOPIFY_AUTO_SYNC_MINUTES):
        try:
            n = sync_shopify_orders(days_back)
            result["shopify_orders"] = n
            result["did_anything"] = True
        except Exception as e:
            errors.append(f"Shopify: {e}")

    if force or should_sync("meta", META_AUTO_SYNC_MINUTES):
        try:
            n = sync_meta_spend(days_back)
            result["meta_rows"] = n
            result["did_anything"] = True
        except Exception as e:
            errors.append(f"Meta: {e}")

    if force or should_sync("cj", CJ_AUTO_SYNC_MINUTES):
        try:
            synced_count, already_cached_count, target_count = sync_cj_costs_for_range(start_date, end_date)
            result["cj_rows"] = synced_count
            result["cj_cached_or_manual"] = already_cached_count
            result["cj_targets"] = target_count
            result["did_anything"] = True
        except Exception as e:
            errors.append(f"CJ: {e}")

    result["errors"] = errors
    return result


def maybe_autoload_missing_cogs_for_visible_range(start_date: date, end_date: date) -> bool:
    target_order_names = get_shopify_order_names_in_range(start_date, end_date)
    if not target_order_names:
        return False

    missing = get_missing_cj_order_names(target_order_names)
    if not missing:
        return False

    range_key = f"{start_date}_{end_date}"
    now_ts = time.time()
    last_try_key = f"cj_autoload_last_try_{range_key}"
    last_try = st.session_state.get(last_try_key, 0.0)

    if now_ts - last_try < 8:
        return False

    st.session_state[last_try_key] = now_ts

    with st.spinner(f"Loading missing CJ COGS for {len(missing)} visible order(s)..."):
        synced_count, _, _ = sync_cj_costs_for_range(start_date, end_date)

    return synced_count > 0


# -------------------------
# Chart
# -------------------------
def render_revenue_profit_chart(graph_df: pd.DataFrame, start_date: date, end_date: date):
    full_dates = pd.date_range(start=pd.Timestamp(start_date), end=pd.Timestamp(end_date), freq="D")

    if graph_df.empty:
        graph_df = pd.DataFrame({"order_date": full_dates, "revenue": 0.0, "profit": 0.0})
    else:
        graph_df = graph_df.copy()
        graph_df["order_date"] = pd.to_datetime(graph_df["order_date"]).dt.normalize()
        graph_df = pd.DataFrame({"order_date": full_dates}).merge(graph_df, on="order_date", how="left").fillna(0.0)

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    span_days = max((end_date - start_date).days + 1, 1)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=graph_df["order_date"],
            y=graph_df["revenue"],
            mode="lines+markers",
            name="Revenue",
            line=dict(width=3),
            marker=dict(size=6),
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Revenue: $%{y:,.2f}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=graph_df["order_date"],
            y=graph_df["profit"],
            mode="lines+markers",
            name="Profit",
            line=dict(width=3),
            marker=dict(size=6),
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Profit: $%{y:,.2f}<extra></extra>",
        )
    )

    xaxis_cfg = dict(
        title=None,
        range=[start_ts, end_ts],
        fixedrange=False,
        showgrid=False,
        zeroline=False,
        rangeslider=dict(visible=False),
        tickformat="%b %d",
        minallowed=start_ts,
        maxallowed=end_ts,
    )

    if span_days <= 14:
        xaxis_cfg["tickmode"] = "array"
        xaxis_cfg["tickvals"] = list(full_dates)
        xaxis_cfg["ticktext"] = [d.strftime("%b %d") for d in full_dates]

    fig.update_layout(
        height=300 if span_days <= 14 else 330,
        margin=dict(l=10, r=10, t=8, b=8),
        hovermode="x unified",
        dragmode="pan",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.01)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=xaxis_cfg,
        yaxis=dict(
            title=None,
            showgrid=True,
            gridcolor="rgba(255,255,255,0.08)",
            zeroline=False,
            tickprefix="$",
            separatethousands=True,
        ),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "displaylogo": False,
            "doubleClick": "reset",
            "responsive": True,
        },
    )


# -------------------------
# UI
# -------------------------
st.markdown(
    """
    <div class="top-grid-gap">
        <div class="dashboard-title">Live Profit Dashboard</div>
        <div class="dashboard-subtitle">
            Shopify, Meta, and CJ stay synced automatically when stale. Missing CJ costs for the visible range load automatically too.
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

top_a, top_b = st.columns([1.6, 0.9], vertical_alignment="center")

with top_a:
    st.markdown("<div class='section-title'>Date Range</div>", unsafe_allow_html=True)

    preset = st.radio(
        "Quick Range",
        ["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 365 Days", "Custom"],
        horizontal=True,
        label_visibility="collapsed"
    )

    if preset == "Custom":
        default_start = pd.Timestamp.now(tz=STORE_TIMEZONE).date() - timedelta(days=29)
        default_end = pd.Timestamp.now(tz=STORE_TIMEZONE).date()

        selected_range = st.date_input(
            "Choose date range",
            value=(default_start, default_end),
            key="custom_date_range"
        )

        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
        elif isinstance(selected_range, list) and len(selected_range) == 2:
            start_date, end_date = selected_range[0], selected_range[1]
        else:
            start_date, end_date = default_start, default_end
    else:
        start_date, end_date = get_date_range_from_preset(preset)

with top_b:
    if "auto_sync_enabled" not in st.session_state:
        st.session_state.auto_sync_enabled = AUTO_SYNC_ENABLED_DEFAULT

    st.markdown("<div class='section-title'>Controls</div>", unsafe_allow_html=True)
    ctrl1, ctrl2 = st.columns([1, 1])

    with ctrl1:
        st.session_state.auto_sync_enabled = st.checkbox(
            "Auto-sync enabled",
            value=st.session_state.auto_sync_enabled
        )

    with ctrl2:
        if st.button("Refresh all now", use_container_width=True):
            with st.spinner("Refreshing Shopify, Meta, and CJ..."):
                sync_result = run_sync_all(start_date, end_date, force=True)
            if sync_result.get("errors"):
                st.session_state["sync_errors"] = sync_result["errors"]
            else:
                st.session_state.pop("sync_errors", None)
            st.rerun()

    if st.session_state.get("sync_errors"):
        for err in st.session_state.pop("sync_errors"):
            st.warning(err)

sync_states = get_last_sync_state()
state_map = {r["source"]: r for _, r in sync_states.iterrows()} if not sync_states.empty else {}
shopify_last = state_map.get("shopify", {}).get("last_sync_at", "—")
meta_last = state_map.get("meta", {}).get("last_sync_at", "—")
cj_last = state_map.get("cj", {}).get("last_sync_at", "—")

st.markdown(
    f"""
    <div class="status-bar">
        <div class="status-grid">
            <div>
                <div class="status-title">{start_date} → {end_date}</div>
                <div class="status-sub">Overview, chart, refunds, and cancellations reflect only this window.</div>
            </div>
            <div class="sync-chip">Shopify: {shopify_last}</div>
            <div class="sync-chip">Meta: {meta_last}</div>
            <div class="sync-chip">CJ: {cj_last}</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

with st.expander("Manual COGS override CSV / XLSX", expanded=False):
    st.markdown(
        "<div class='compact-note'>Upload a manual costs file only when you want to override CJ matched COGS for specific orders.</div>",
        unsafe_allow_html=True
    )
    uploaded = st.file_uploader(
        "Upload manual cost CSV/XLSX",
        type=["csv", "xlsx"],
        label_visibility="collapsed"
    )
    if uploaded is not None:
        try:
            n = import_manual_cost_csv(uploaded)
            st.success(f"Imported {n} manual override cost rows")
        except Exception as e:
            st.error(f"CSV import failed: {e}")

if st.session_state.auto_sync_enabled:
    try:
        with st.spinner("Loading latest Shopify, Meta, and CJ data..."):
            run_sync_all(start_date, end_date, force=False)
    except Exception as e:
        st.warning(f"Auto-sync skipped due to error: {e}")

df = load_profit_df()

if st.session_state.auto_sync_enabled and not df.empty:
    try:
        changed = maybe_autoload_missing_cogs_for_visible_range(start_date, end_date)
        if changed:
            df = load_profit_df()
    except Exception as e:
        st.warning(f"Visible-range CJ auto-load failed: {e}")

if df.empty:
    st.info("No data yet. The app will load automatically once the sources are available.")
else:
    df["order_date_dt"] = pd.to_datetime(df["order_date"]).dt.date
    filtered_df = df[(df["order_date_dt"] >= start_date) & (df["order_date_dt"] <= end_date)].copy()

    sale_df = filtered_df[filtered_df["event_type"] == "sale"].copy()
    refund_df = filtered_df[filtered_df["event_type"] == "refund"].copy()
    cancel_df = filtered_df[filtered_df["event_type"] == "cancel"].copy()

    revenue_sum = filtered_df["revenue"].sum()
    cogs_sum = filtered_df["cogs"].sum()
    ad_spend_sum = filtered_df["allocated_ad_spend"].sum()
    fee_sum = filtered_df["payment_fee"].sum()
    profit_sum = filtered_df["profit"].sum()
    orders_count = len(sale_df)

    refunds_sum = -refund_df["revenue"].sum()
    canceled_sum = -cancel_df["revenue"].sum()

    margin = (profit_sum / revenue_sum * 100) if revenue_sum > 0 else 0.0
    roas = (revenue_sum / ad_spend_sum) if ad_spend_sum > 0 else 0.0

    st.markdown("<div class='section-title'>Overview</div>", unsafe_allow_html=True)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        metric_card("Revenue", f"${revenue_sum:,.2f}", f"{start_date} → {end_date}", theme="revenue")
    with m2:
        metric_card("Profit", f"${profit_sum:,.2f}", f"Margin: {margin:.2f}%", theme="profit")
    with m3:
        metric_card("Orders", f"{orders_count:,}", "Original sale orders in range", theme="soft")
    with m4:
        metric_card("ROAS", f"{roas:.2f}x", "Revenue / ad spend", theme="soft")

    st.markdown("<div class='metric-row-gap'></div>", unsafe_allow_html=True)

    m5, m6, m7, m8, m9 = st.columns(5)
    with m5:
        metric_card("COGS", f"${cogs_sum:,.2f}", "Includes cancel reversals", theme="soft")
    with m6:
        metric_card("Ad Spend", f"${ad_spend_sum:,.2f}", "Allocated from Meta daily spend", theme="soft")
    with m7:
        metric_card("Fees", f"${fee_sum:,.2f}", "Gateway fee + 1% external gateway fee", theme="soft")
    with m8:
        metric_card("Refunds", f"${refunds_sum:,.2f}", "Booked on refund date", theme="soft")
    with m9:
        metric_card("Canceled", f"${canceled_sum:,.2f}", "Reversed on cancel date", theme="soft")

    st.markdown("<div class='section-title'>Revenue vs Profit</div>", unsafe_allow_html=True)

    graph_df = filtered_df.groupby("order_date", as_index=False).agg(
        revenue=("revenue", "sum"),
        profit=("profit", "sum"),
    )

    graph_df["order_date"] = pd.to_datetime(graph_df["order_date"])
    graph_df = graph_df.sort_values("order_date")
    graph_df = graph_df[
        (graph_df["order_date"].dt.date >= start_date) &
        (graph_df["order_date"].dt.date <= end_date)
    ].copy()

    render_revenue_profit_chart(graph_df, start_date, end_date)

    left, right = st.columns([1.55, 1], vertical_alignment="top")

    with left:
        st.markdown("<div class='section-title'>Orders & Adjustments</div>", unsafe_allow_html=True)

        display_df = filtered_df[
            [
                "event_type",
                "order_name",
                "order_date",
                "revenue",
                "cogs",
                "payment_fee",
                "allocated_ad_spend",
                "profit",
                "margin_pct",
                "cost_source",
                "gateway_key",
                "financial_status",
            ]
        ].copy()

        display_df = display_df.rename(
            columns={
                "event_type": "Type",
                "order_name": "Order",
                "order_date": "Date",
                "revenue": "Revenue",
                "cogs": "COGS",
                "payment_fee": "Fees",
                "allocated_ad_spend": "Ad Spend",
                "profit": "Profit",
                "margin_pct": "Margin %",
                "cost_source": "Cost Source",
                "gateway_key": "Gateway",
                "financial_status": "Financial Status",
            }
        )

        display_df["Type"] = display_df["Type"].replace(
            {"sale": "Sale", "refund": "Refund", "cancel": "Cancel"}
        )

        st.dataframe(display_with_1_index(display_df), use_container_width=True, height=510)

    with right:
        st.markdown("<div class='section-title'>Daily Summary</div>", unsafe_allow_html=True)

        summary_df = filtered_df.groupby("order_date", as_index=False).agg(
            sales=("event_type", lambda s: int((s == "sale").sum())),
            refunds=("event_type", lambda s: int((s == "refund").sum())),
            cancels=("event_type", lambda s: int((s == "cancel").sum())),
            revenue=("revenue", "sum"),
            cogs=("cogs", "sum"),
            fees=("payment_fee", "sum"),
            ad_spend=("allocated_ad_spend", "sum"),
            profit=("profit", "sum"),
        )

        summary_df = summary_df.rename(
            columns={
                "order_date": "Date",
                "sales": "Sales",
                "refunds": "Refunds",
                "cancels": "Cancels",
                "revenue": "Revenue",
                "cogs": "COGS",
                "fees": "Fees",
                "ad_spend": "Ad Spend",
                "profit": "Profit",
            }
        )

        st.dataframe(display_with_1_index(summary_df), use_container_width=True, height=300)

        missing_count = int((sale_df["cost_source"] == "missing").sum())
        cj_count = int((sale_df["cost_source"] == "cj").sum())
        manual_count = int((sale_df["cost_source"] == "manual").sum())

        st.markdown("<div class='section-title'>Cost Source Mix</div>", unsafe_allow_html=True)
        mix1, mix2, mix3 = st.columns(3)
        with mix1:
            metric_card("Manual", f"{manual_count}", "CSV overrides", theme="soft")
        with mix2:
            metric_card("CJ", f"{cj_count}", "Matched from CJ", theme="soft")
        with mix3:
            metric_card("Missing", f"{missing_count}", "Still no cost", theme="soft")

        with st.expander("Selected range details"):
            st.write(f"Start date: **{start_date}**")
            st.write(f"End date: **{end_date}**")
            st.write(f"Sale orders in range: **{len(sale_df)}**")
            st.write(f"Refund events in range: **{len(refund_df)}**")
            st.write(f"Cancel events in range: **{len(cancel_df)}**")
            st.write(f"Revenue sum: **${revenue_sum:,.2f}**")
            st.write(f"COGS sum: **${cogs_sum:,.2f}**")
            st.write(f"Refunded amount: **${refunds_sum:,.2f}**")
            st.write(f"Canceled amount: **${canceled_sum:,.2f}**")
            st.write(f"Profit sum: **${profit_sum:,.2f}**")

    with st.expander("Advanced tools"):
        c1, c2, c3 = st.columns(3)

        with c1:
            if st.button("Force Shopify Sync", use_container_width=True):
                try:
                    with st.spinner("Syncing Shopify..."):
                        n = sync_shopify_orders(compute_days_back_for_sync(start_date, end_date))
                    st.success(f"Synced {n} Shopify orders")
                except Exception as e:
                    st.error(f"Shopify sync failed: {e}")

        with c2:
            if st.button("Force Meta Sync", use_container_width=True):
                try:
                    with st.spinner("Syncing Meta..."):
                        n = sync_meta_spend(compute_days_back_for_sync(start_date, end_date))
                    st.success(f"Synced {n} Meta rows")
                except Exception as e:
                    st.error(f"Meta sync failed: {e}")

        with c3:
            if st.button("Force CJ Sync", use_container_width=True):
                try:
                    with st.spinner("Syncing CJ..."):
                        synced_count, already_cached_count, target_count = sync_cj_costs_for_range(start_date, end_date)
                    st.success(
                        f"CJ sync done. Selected orders: {target_count} | "
                        f"Cached/manual: {already_cached_count} | Newly synced: {synced_count}"
                    )
                except Exception as e:
                    st.error(f"CJ sync failed: {e}")

    with st.expander("Raw filtered data"):
        st.dataframe(display_with_1_index(filtered_df), use_container_width=True)
