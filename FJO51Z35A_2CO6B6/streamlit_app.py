# --- Course Search v1.3.2 (tokenized-phrase search + course# range) ---
import streamlit as st
from snowflake.snowpark.context import get_active_session
import re

session = get_active_session()
st.title("Course Search v1.3.2")

# -------- pull once: distincts and course# min/max --------
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

minmax_row = session.sql(
    """
    select
      min(try_to_number(course_number)) as mn,
      max(try_to_number(course_number)) as mx
    from DZ_WB.JASTESANO.COURSES_V
    where try_to_number(course_number) is not null
    """
).collect()[0]
num_min = int(minmax_row[0] or 0)
num_max = int(minmax_row[1] or 9999)
if num_min >= num_max:
    num_min, num_max = 0, 9999  # safety

# ------------- Sidebar controls -------------
with st.sidebar:
    st.subheader("Search")
    raw_terms = st.text_area(
        "Terms (comma or | separated)",
        placeholder="artificial intelligence, machine learning, AI, ML",
        height=70,
    )
    # Modes
    smart_token_mode = st.toggle(
        "Smart whole-word/phrase matching (recommended)",
        value=True,
        help="Tokenizes text (lowercase, punctuation -> spaces) and matches phrases as whole tokens."
    )
    use_regex = st.toggle("Use raw regex instead", value=False, help="Advanced. Bypasses smart matching.")
    match_all = st.toggle("Require ALL terms", value=False, help="Off = match ANY term")
    scope = st.radio("Search in", ["Title", "Description", "Both"], horizontal=True)

    st.subheader("Filters")
    sel_career = st.multiselect("Level (UG/GR/LAW)", levels_uggr, default=[])
    sel_college = st.multiselect("College", colleges, default=[])
    sel_subject = st.multiselect("Subject", subjects, default=[])

    # Course number range
    st.markdown("**Course # Range**")
    low_high = st.slider(
        "Restrict to course numbers between…",
        min_value=num_min, max_value=num_max, value=(num_min, num_max),
        help="Filters by the numeric course number (e.g., 3000–5999).",
    )
    low_num, high_num = low_high

    st.subheader("Result size")
    limit = st.select_slider("Max rows", options=[50, 100, 200, 500, 1000], value=200)

# ------------- Helpers -------------
def esc_sql(s: str) -> str:
    return s.replace("'", "''") if s is not None else s

def split_terms(s: str):
    if not s:
        return []
    parts = [p.strip() for p in s.replace("|", ",").split(",")]
    return [p for p in parts if p]

def normalize_term_to_phrase(term: str) -> str:
    """Lowercase, keep only alphanumerics, join with single spaces."""
    tokens = re.findall(r"[A-Za-z0-9]+", term.lower())
    return " ".join(tokens)

# Build normalized expressions (inline in SQL): pad with spaces so LIKE ' %phrase% ' works at edges
NORM_TITLE = "CONCAT(' ', REGEXP_REPLACE(LOWER(title), '[^a-z0-9]+', ' '), ' ')"
NORM_DESC  = "CONCAT(' ', REGEXP_REPLACE(LOWER(description), '[^a-z0-9]+', ' '), ' ')"

# ------------- Build WHERE clause -------------
where_clauses = []
terms = split_terms(raw_terms)

def preds_for_term(term: str):
    preds = []
    if smart_token_mode and not use_regex:
        phrase = normalize_term_to_phrase(term)
        if not phrase:
            return preds
        pattern = f"% {esc_sql(phrase)} %"
        if scope in ("Title", "Both"):
            preds.append(f"{NORM_TITLE} LIKE '{pattern}'")
        if scope in ("Description", "Both"):
            preds.append(f"{NORM_DESC} LIKE '{pattern}'")
        return preds

    # Regex mode (raw)
    if use_regex:
        if scope in ("Title", "Both"):
            preds.append(f"REGEXP_LIKE(title, '{esc_sql(term)}', 'i')")
        if scope in ("Description", "Both"):
            preds.append(f"REGEXP_LIKE(description, '{esc_sql(term)}', 'i')")
        return preds

    # Plain contains (fallback)
    like = f"%{esc_sql(term)}%"
    if scope in ("Title", "Both"):
        preds.append(f"LOWER(title) LIKE LOWER('{like}')")
    if scope in ("Description", "Both"):
        preds.append(f"LOWER(description) LIKE LOWER('{like}')")
    return preds

if terms:
    per_term_groups = []
    for t in terms:
        p = preds_for_term(t)
        if p:
            per_term_groups.append("(" + " OR ".join(p) + ")")
    if per_term_groups:
        where_clauses.append("(" + (" AND ".join(per_term_groups) if match_all else " OR ".join(per_term_groups)) + ")")

# Level / College / Subject filters
if sel_career:
    where_clauses.append("career_label IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_career) + ")")
if sel_college:
    where_clauses.append("college IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_college) + ")")
if sel_subject:
    where_clauses.append("subject_code IN (" + ", ".join(f"'{esc_sql(x)}'" for x in sel_subject) + ")")

# Course number range filter
where_clauses.append(
    f"(try_to_number(course_number) BETWEEN {low_num} AND {high_num})"
)

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

# ------------- Query (clean display) -------------
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

st.caption("Query (read-only):")
st.code(sql, language="sql")

df = session.sql(sql).to_pandas()
st.markdown(f"**Results:** {len(df)} rows")
st.dataframe(df, use_container_width=True)

if not df.empty:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, file_name="course_search_results.csv", mime="text/csv")

with st.expander("Notes"):
    st.markdown("""
- **Smart whole-word/phrase matching** normalizes both sides and matches `' phrase '` against tokenized text.
  - Handles acronyms (AI/ML/NLP) and phrases with spaces/hyphens/punctuation.
- Toggle **Require ALL terms** for AND logic across terms; otherwise terms are ORed.
- **Regex** mode bypasses smart matching and uses `REGEXP_LIKE(..., 'pattern', 'i')`.
- **Course # Range** uses the numeric course number (e.g., 3000–5999).
""")