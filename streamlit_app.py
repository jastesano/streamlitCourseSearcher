# --- Course Search v1.4.2 (single input, styled chips with ✕, "match all", smart search) ---
import re
from decimal import Decimal

import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.title("Course Search v1.4.2")

# --------------------------- utilities ---------------------------
def esc_sql(s: str) -> str:
    return s.replace("'", "''") if s is not None else s

def split_terms(s: str):
    if not s:
        return []
    # support commas or pipes; trim; drop empties; de-dupe in caller
    parts = [p.strip() for p in s.replace("|", ",").split(",")]
    return [p for p in parts if p]

def normalize_term_to_phrase(term: str) -> str:
    """Lowercase, keep only alphanumerics, join with single spaces."""
    tokens = re.findall(r"[A-Za-z0-9]+", term.lower())
    return " ".join(tokens)

def _to_int_safe(x, default=None):
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

def add_terms_to_state(items):
    terms = st.session_state.setdefault("terms", [])
    for t in items:
        t = t.strip()
        if t and t not in terms:
            terms.append(t)

# tokenized normalized columns (for smart search)
NORM_TITLE = "CONCAT(' ', REGEXP_REPLACE(LOWER(title), '[^a-z0-9]+', ' '), ' ')"
NORM_DESC  = "CONCAT(' ', REGEXP_REPLACE(LOWER(description), '[^a-z0-9]+', ' '), ' ')"

# ---------------------- load filter values once -------------------
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

rowset = session.sql(
    """
    SELECT
      MIN(TRY_TO_NUMBER(course_number)) AS mn,
      MAX(TRY_TO_NUMBER(course_number)) AS mx
    FROM DZ_WB.JASTESANO.COURSES_V
    WHERE TRY_TO_NUMBER(course_number) IS NOT NULL
    """
).collect()
raw_min, raw_max = (rowset[0][0], rowset[0][1]) if rowset else (None, None)
num_min = _to_int_safe(raw_min, 0)
num_max = _to_int_safe(raw_max, 9999)
if num_min is None or num_max is None or num_min >= num_max:
    num_min, num_max = 0, 9999

# --------------------------- sidebar UI ---------------------------
with st.sidebar:
    st.subheader("Search terms")

    # session state for chips
    st.session_state.setdefault("terms", [])

    # One input that supports single or bulk entry
    def _submit_terms():
        raw = st.session_state.get("term_input", "")
        add_terms_to_state(split_terms(raw))
        # we won't clear the widget value programmatically to avoid Snowflake/Streamlit state errors

    st.text_input(
        "Type a term (or comma/| list) and press Enter",
        key="term_input",
        placeholder="e.g., machine learning, NLP, AI, deep learning",
        on_change=_submit_terms,
    )

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        if st.button("Add"):
            _submit_terms()
    with c2:
        if st.button("Clear"):
            st.session_state["terms"] = []
    with c3:
        match_all = st.checkbox("Match ALL", value=False, help="Require every term (AND). Off = ANY (OR).")

    # Chip styling: each chip is a removable button
    chip_css = """
    <style>
      .chip-row { margin: 6px 0; display: flex; flex-wrap: wrap; gap: 6px; }
      .chip-button > button {
        border: 1px solid #cbd5e1 !important;
        background: #fff !important;
        color: #334155 !important;
        padding: 2px 8px !important;
        border-radius: 10px !important;
        font-size: 12px !important;
        line-height: 1.2 !important;
      }
      .chip-button > button:hover {
        background: #f1f5f9 !important;
      }
    </style>
    """
    st.markdown(chip_css, unsafe_allow_html=True)

    if st.session_state["terms"]:
        st.markdown("**Included terms:**")
        st.markdown("<div class='chip-row'>", unsafe_allow_html=True)
        # render each chip as a tiny button "term ✕" — clicking it removes the term
        for i, term in enumerate(st.session_state["terms"]):
            with st.container():
                # put each chip in its own styled button container
                rem = st.button(f"{term}  ✕", key=f"chip_{i}", help="Remove", type="secondary")
                # style the current st.button as chip via class name (best-effort)
                st.write(f"<div class='chip-button'></div>", unsafe_allow_html=True)
                if rem:
                    st.session_state["terms"].remove(term)
                    st.experimental_rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # Search scope & advanced option
    scope = st.radio("Search in", ["Title", "Description", "Both"], horizontal=True)
    use_regex = st.checkbox("Use raw regex instead (advanced)", value=False,
                            help="Bypasses smart tokenized matching and uses REGEXP_LIKE.")

    st.subheader("Filters")
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

    # Optional explicit search trigger
    st.button("Search")

# ----------------------- build WHERE conditions -------------------
terms = st.session_state["terms"][:]
where_clauses = []

def preds_for_term(term: str):
    preds = []
    if not use_regex:
        # Smart tokenized phrase matching (always on)
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

if terms:
    per_term_groups = []
    for t in terms:
        p = preds_for_term(t)
        if p:
            per_term_groups.append("(" + " OR ".join(p) + ")")
    if per_term_groups:
        joiner = " AND " if match_all else " OR "
        where_clauses.append("(" + joiner.join(per_term_groups) + ")")

# Filters
if sel_career:
    where_clauses.append("career_label IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_career) + ")")
if sel_college:
    where_clauses.append("college IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_college) + ")")
if sel_subject:
    where_clauses.append("subject_code IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_subject) + ")")
where_clauses.append(f"(TRY_TO_NUMBER(course_number) BETWEEN {low_num} AND {high_num})")

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

# ------------------------------ query -----------------------------
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

st.markdown(f"**Results:** {len(df)} rows")
st.dataframe(df, use_container_width=True)

if not df.empty:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, file_name="course_search_results.csv", mime="text/csv")

# --------------------- notes & query (collapsed) ------------------
with st.expander("Notes & Query"):
    st.markdown("""
- **Smart search** tokenizes text (lowercase; punctuation → spaces) and matches whole phrases.
- Toggle **Match ALL** to require every term; otherwise ANY term may match.
- Use **raw regex** only for advanced patterns.
- Course # range filters by numeric `course_number`.
""")
    st.caption("Generated SQL")
    st.code(sql, language="sql")