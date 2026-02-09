import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

LENS_ENDPOINT = "https://api.lens.org/patent/search"


def _post_with_retry(
    headers: Dict[str, str],
    payload: Dict,
    timeout: int = 30,
    max_retries: int = 4,
    backoff_sec: float = 2.0,
) -> requests.Response:
    """
    POST with simple retry handling for 429 (rate limit).
    """
    for attempt in range(max_retries + 1):
        r = requests.post(LENS_ENDPOINT, headers=headers, json=payload, timeout=timeout)
        if r.status_code == 429:
            sleep_s = backoff_sec * (attempt + 1)
            time.sleep(sleep_s)
            continue
        return r
    return r


def build_query(
    keyword: str,
    scope: str,
    date_from: Optional[str] = None,
) -> Dict:
    """
    scope: title | abstract | title+abstract | title+abstract+claims
    """
    keyword = keyword.strip()

    if scope == "title":
        must = [{"match": {"title": keyword}}]
    elif scope == "abstract":
        must = [{"match": {"abstract": keyword}}]
    elif scope == "title+abstract":
        must = [{
            "bool": {
                "should": [
                    {"match": {"title": keyword}},
                    {"match": {"abstract": keyword}},
                ],
                "minimum_should_match": 1
            }
        }]
    else:
        must = [{
            "bool": {
                "should": [
                    {"match": {"title": keyword}},
                    {"match": {"abstract": keyword}},
                    {"match": {"claim": keyword}},
                ],
                "minimum_should_match": 1
            }
        }]

    filters = []
    if date_from:
        filters.append({
            "range": {
                "date_published": {
                    "gte": date_from
                }
            }
        })

    q = {"bool": {"must": must}}
    if filters:
        q["bool"]["filter"] = filters

    return q


def extract_rows(data_items: List[Dict]) -> List[Dict]:
    rows = []
    for item in data_items:
        lens_id = item.get("lens_id", "")
        biblio = item.get("biblio", {}) or {}

        pub_ref = biblio.get("publication_reference", {}) or {}
        jurisdiction = pub_ref.get("jurisdiction", "") or pub_ref.get("country", "")
        doc_number = pub_ref.get("doc_number", "") or pub_ref.get("document_number", "")
        kind = pub_ref.get("kind", "")

        inv_title = biblio.get("invention_title")
        title_text = ""
        if isinstance(inv_title, dict):
            title_text = inv_title.get("text", "") or inv_title.get("title", "")
        elif isinstance(inv_title, list) and inv_title:
            t0 = inv_title[0] or {}
            if isinstance(t0, dict):
                title_text = t0.get("text", "") or t0.get("title", "")

        date_published = item.get("date_published", "") or item.get("publication_date", "")

        lens_link = f"https://www.lens.org/lens/patent/{lens_id}"

        google_link = ""
        if jurisdiction and doc_number and kind:
            google_link = f"https://patents.google.com/patent/{jurisdiction}{doc_number}{kind}"
        elif jurisdiction and doc_number:
            google_link = f"https://patents.google.com/patent/{jurisdiction}{doc_number}"

        rows.append({
            "lens_id": lens_id,
            "title": title_text,
            "jurisdiction": jurisdiction,
            "doc_number": doc_number,
            "kind": kind,
            "date_published": date_published,
            "lens_link": lens_link,
            "google_patents_link": google_link,
        })

    return rows


def lens_search_with_scroll(
    token: str,
    keyword: str,
    scope: str,
    date_from: Optional[str] = None,
    max_results: int = 200,
    scroll_ttl: str = "1m",
    delay_sec: float = 0.2,
    timeout: int = 30,
) -> Tuple[pd.DataFrame, Dict]:

    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }

    include = [
        "lens_id",
        "doc_key",
        "biblio.publication_reference",
        "biblio.invention_title",
        "date_published",
    ]

    debug = {
        "pages": 0,
        "scroll_id": None,
        "last_status": None,
        "returned": 0,
    }

    payload = {
        "query": build_query(keyword, scope, date_from),
        "include": include,
        "scroll": scroll_ttl,
        "size": min(100, max_results),
    }

    all_rows: List[Dict] = []
    scroll_id: Optional[str] = None

    while len(all_rows) < max_results:
        r = _post_with_retry(headers=headers, payload=payload, timeout=timeout)
        debug["last_status"] = r.status_code
        debug["pages"] += 1

        if r.status_code == 204:
            break

        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: توکن اشتباه است یا دسترسی API فعال نیست.")
        if r.status_code == 403:
            raise RuntimeError("403 Forbidden: دسترسی API یا پلن اجازه نمی‌دهد.")
        if r.status_code != 200:
            raise RuntimeError(f"API Error {r.status_code}: {r.text}")

        js = r.json()
        data_items = js.get("data") or []
        if not data_items:
            break

        if scroll_id is None:
            scroll_id = js.get("scroll_id")
            debug["scroll_id"] = scroll_id

        rows = extract_rows(data_items)
        remaining = max_results - len(all_rows)
        all_rows.extend(rows[:remaining])
        debug["returned"] = len(all_rows)

        if not scroll_id or len(all_rows) >= max_results:
            break

        payload = {
            "scroll_id": scroll_id,
            "include": include
        }

        time.sleep(max(0.0, delay_sec))

    return pd.DataFrame(all_rows), debug


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Lens Patent Keyword Search", layout="wide")
st.title("جستجوی پتنت با کلیدواژه (Lens Patent API) — با فیلتر تاریخ")

with st.sidebar:
    token = st.text_input("Lens API Token", type="password")
    keyword = st.text_input("کلیدواژه", value="skin stapler")
    scope = st.selectbox("جستجو در", ["title", "abstract", "title+abstract", "title+abstract+claims"], index=2)

    year_from = st.number_input("از سال", min_value=1900, max_value=2100, value=2018, step=1)
    date_from = f"{year_from}-01-01"

    max_results = st.number_input("حداکثر تعداد خروجی", min_value=10, max_value=5000, value=200, step=10)
    scroll_ttl = st.selectbox("Scroll TTL", ["30s", "1m", "2m", "5m"], index=1)
    delay_sec = st.slider("Delay بین درخواست‌ها (ثانیه)", 0.0, 2.0, 0.2, 0.1)
    debug_mode = st.checkbox("Debug", value=False)
    run = st.button("جستجو")

st.caption("فیلتر تاریخ بر اساس date_published (مثلاً ۲۰۱۸ به بعد) اعمال می‌شود.")

if run:
    if not token.strip():
        st.error("توکن Lens را وارد کن.")
        st.stop()

    with st.spinner("در حال جستجو در Lens API..."):
        df, dbg = lens_search_with_scroll(
            token=token,
            keyword=keyword,
            scope=scope,
            date_from=date_from,
            max_results=int(max_results),
            scroll_ttl=scroll_ttl,
            delay_sec=float(delay_sec),
        )

    if debug_mode:
        st.json(dbg)

    if df.empty:
        st.warning("نتیجه‌ای پیدا نشد.")
        st.stop()

    st.subheader(f"تعداد نتایج: {len(df)}")
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "دانلود CSV",
        data=csv,
        file_name=f"lens_patents_{keyword.replace(' ', '_')}.csv",
        mime="text/csv",
    )
