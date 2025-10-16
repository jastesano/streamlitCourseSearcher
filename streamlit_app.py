# --- Course Search v1.4.5 (stable: chips + match-all + smart search + compact UI) ---
import re
from decimal import Decimal

import streamlit as st
from snowflake.snowpark.context import get_active_session

# --- init ---
session = get_active_session()
st.title("Course Search v1.4.5")

# ---------- helpers ----------
def esc_sql(s: str) -> str:
    return s.replace("'", "''") if s is not None else s

def split_terms(s: str):
    if not s:
        return []
    # commas or pipes, trim, drop empties
    return [p.strip() for p in re.split(r"[,|]", s) if p.strip()]

def normalize_term_to_phrase(term: str) -> str:
    """lowercase; keep only alphanumerics; join with single spaces"""
    tokens = re.findall(r"[A-Za-z0-9]+", term.lower())
    return " ".join(tokens)

def to_int_safe(x, default=None):
    if x is None:
        return default
    if isinstance(x, int):
        return x
    if isinstance(x, (float, Decimal)):
        return int(float(x))
    try:
        return int(x)
    except Exception:
        return default

# normalized columns for smart matching
NORM_TITLE = "CONCAT(' ', REGEXP_REPLACE(LOWER(title), '[^a-z0-9]+', ' '), ' ')"
NORM_DESC  = "CONCAT(' ', REGEXP_REPLACE(LOWER(description), '[^a-z0-9]+', ' '), ' ')"

# ---------- load distincts / ranges ----------
levels_uggr = [r[0] for r in session.sql(
    "select distinct career_label from DZ_WB.JASTESANO.COURSES_V "
    "where career_label is not null order by 1"
).collect()]

colleges = [r[0] for r in session.sql(
    "select distinct college from DZ_WB.JASTESANO.COURSES_V "
    "where college is not null order by 1"
).collect()]

subjects = [r[0] for r in session.sql(
    "select distinct subject_code from DZ_WB.JASTESANO.COURSES_V "
    "where subject_code is not null order by 1"
).collect()]

rowset = session.sql("""
    SELECT MIN(TRY_TO_NUMBER(course_number)) AS mn,
           MAX(TRY_TO_NUMBER(course_number)) AS mx
    FROM DZ_WB.JASTESANO.COURSES_V
    WHERE TRY_TO_NUMBER(course_number) IS NOT NULL
""").collect()
raw_min, raw_max = (rowset[0][0], rowset[0][1]) if rowset else (None, None)
num_min = to_int_safe(raw_min, 0)
num_max = to_int_safe(raw_max, 9999)
if num_min is None or num_max is None or num_min >= num_max:
    num_min, num_max = 0, 9999

# ---------- sidebar UI ----------
with st.sidebar:
    # compact spacing + chip styling
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] {
        margin-top: 0rem !important; margin-bottom: .45rem !important;
    }
    .chip-row { display:flex; flex-wrap:wrap; gap:4px; row-gap:4px; }
    .included-terms-section button[kind="secondary"]{
        border:1px solid #cbd5e1 !important; background:#fff !important;
        color:#334155 !important; padding:2px 8px !important;
        border-radius:8px !important; font-size:12px !important; line-height:1.2 !important;
        margin:0 !important;
    }
    .stCheckbox label { white-space: nowrap !important; }
    </style>
    """, unsafe_allow_html=True)

    st.subheader("Search terms")
    st.session_state.setdefault("terms", [])

    # unified input (single or comma/| list) with Enter-to-add
    def _add_from_input():
        raw = st.session_state.get("term_input", "")
        if raw:
            for t in split_terms(raw):
                if t not in st.session_state["terms"]:
                    st.session_state["terms"].append(t)
        # don't clear widget programmatically (Snowflake Streamlit can block it)

    st.text_input(
        "Type a term (or comma/| list) and press Enter",
        key="term_input",
        placeholder="e.g., machine learning, NLP, AI",
        on_change=_add_from_input,  # <-- Enter triggers add
    )

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        if st.button("Add"):       # manual add still works
            _add_from_input()
    with c2:
        if st.button("Clear"):
            st.session_state["terms"] = []
    with c3:
        match_all = st.checkbox("Match ALL", key="match_all", value=False,
                                help="Require every term (AND). Off = ANY (OR).")

    # chips
    st.markdown("<div class='included-terms-section'>", unsafe_allow_html=True)
    if st.session_state["terms"]:
        st.markdown("**Included terms:**")
        st.markdown("<div class='chip-row'>", unsafe_allow_html=True)
        for i, term in enumerate(st.session_state["terms"]):
            if st.button(f"{term} ✕", key=f"chip_{i}", type="secondary", help="Remove"):
                st.session_state["terms"].remove(term)
                st.experimental_rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("**Search in**")
    scope = st.radio("", ["Title", "Description", "Both"], horizontal=True, index=2)

    use_regex = st.checkbox("Use raw regex instead (advanced)", value=False,
                            help="Bypasses smart tokenized matching and uses REGEXP_LIKE.")

    st.markdown("**Filters**")
    sel_career = st.multiselect("Level (UG/GR/LAW)", levels_uggr, default=[])
    sel_college = st.multiselect("College", colleges, default=[])
    sel_subject = st.multiselect("Subject", subjects, default=[])

    st.markdown("**Course # Range**")
    low_high = st.slider(
        "Restrict to course numbers between…",
        min_value=int(num_min), max_value=int(num_max),
        value=(int(num_min), int(num_max)),
        help="Filters by numeric course number (e.g., 3000–5999).",
    )
    low_num, high_num = map(int, low_high)

    st.subheader("Result size")
    limit = st.select_slider("Max rows", options=[50, 100, 200, 500, 1000], value=200)

# ---------- build WHERE ----------
where_clauses = []

def preds_for_term(term: str):
    preds = []
    # Smart tokenized phrase matching (always on unless regex override)
    if not use_regex:
        phrase = normalize_term_to_phrase(term)
        if not phrase:
            return preds
        pattern = f"% {esc_sql(phrase)} %"
        if scope in ("Title", "Both"):
            preds.append(f"{NORM_TITLE} LIKE '{pattern}'")
        if scope in ("Description", "Both"):
            preds.append(f"{NORM_DESC} LIKE '{pattern}'")
        return preds
    else:
        # Raw regex (case-insensitive)
        if scope in ("Title", "Both"):
            preds.append(f"REGEXP_LIKE(title, '{esc_sql(term)}', 'i')")
        if scope in ("Description", "Both"):
            preds.append(f"REGEXP_LIKE(description, '{esc_sql(term)}', 'i')")
        return preds

terms = st.session_state.get("terms", [])
if terms:
    per_term_groups = []
    for t in terms:
        p = preds_for_term(t)
        if p:
            per_term_groups.append("(" + " OR ".join(p) + ")")
    if per_term_groups:
        joiner = " AND " if st.session_state.get("match_all") else " OR "
        where_clauses.append("(" + joiner.join(per_term_groups) + ")")

if sel_career:
    where_clauses.append("career_label IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_career) + ")")
if sel_college:
    where_clauses.append("college IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_college) + ")")
if sel_subject:
    where_clauses.append("subject_code IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_subject) + ")")
where_clauses.append(f"(TRY_TO_NUMBER(course_number) BETWEEN {low_num} AND {high_num})")

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

# ---------- query & show ----------
sql = f"""
SELECT
  subject_code      AS Subject,
  course_number     AS "Course #",
  title             AS Title,
  college           AS College,
  modality          AS Modality,
  description       AS Description
FROM DZ_WB.JASTESANO.COURSES_V
{where_sql}
ORDER BY subject_code, course_number
LIMIT {limit}
"""

df = session.sql(sql).to_pandas()
st.markdown(f"**Results: {len(df)} rows**")
st.dataframe(df, use_container_width=True)

if not df.empty:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, file_name="course_search_results.csv", mime="text/csv")

with st.expander("Notes & Query"):
    st.code(sql, language="sql")
    st.markdown("""
- **Smart phrase matching** (default): tokenizes both sides (lowercase; punctuation → spaces), matching whole phrases.
- **Match ALL** = AND logic across terms; otherwise terms are OR’ed.
- **Regex** is optional for advanced patterns.
- **Course # Range** filters by numeric course number.
""")