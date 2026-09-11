"""
chatbot.py — Grounded question-answering via template → Cypher → Neo4j.

Rules:
  1. Match question against a prioritised template list (regex-based).
  2. Build parameterised Cypher from the matched template.
  3. Run Cypher against Neo4j.
  4. If no template matches      → grounded=False, honest refusal.
  5. If Cypher returns no rows   → grounded=False, honest empty-result message.
  6. Otherwise                   → grounded=True, plain-English answer + cypher + result.

The chatbot NEVER answers from general knowledge.
"""
import logging
import re
from typing import Any

from app import neo4j_client
from app.jobs import get_all_dataset_ids

logger = logging.getLogger(__name__)

# ── Response shape ────────────────────────────────────────────────────────────

def _response(answer: str, cypher=None, result=None, grounded=True) -> dict:
    return {
        "answer": answer,
        "cypher": cypher,
        "result": result or [],
        "grounded": grounded,
    }


def _ungrounded(reason: str) -> dict:
    return _response(reason, cypher=None, result=[], grounded=False)


# ── Template definitions ───────────────────────────────────────────────────────
# Each template is a dict:
#   pattern   – compiled regex (applied to lower-cased question)
#   build     – callable(match, question_lower) → (cypher_str, params_dict)
#   phrase    – callable(result, match) → str  (plain-English answer)

def _count_all(m, q):
    cypher = "MATCH (r:Row) RETURN count(r) AS total"
    return cypher, {}

def _phrase_count_all(result, m):
    total = result[0].get("total", 0) if result else 0
    return f"There are {total} rows in the dataset."


def _count_where(m, q):
    col = m.group(1).strip().replace(" ", "_")
    val = m.group(2).strip().strip("\"'")
    cypher = f"MATCH (r:Row) WHERE r.`{col}` = $val RETURN count(r) AS total"
    return cypher, {"val": val}

def _phrase_count_where(result, m):
    total = result[0].get("total", 0) if result else 0
    col = m.group(1).strip()
    val = m.group(2).strip().strip("\"'")
    return f"There are {total} rows where {col} = '{val}'."


def _show_connected(m, q):
    val = m.group(1).strip().strip("\"'")
    cypher = (
        "MATCH (r:Row) WHERE any(k IN keys(r) WHERE r[k] = $val) "
        "RETURN r LIMIT 25"
    )
    return cypher, {"val": val}

def _phrase_show_connected(result, m):
    val = m.group(1).strip().strip("\"'")
    if not result:
        return f"No rows found connected to '{val}'."
    return f"Found {len(result)} row(s) connected to '{val}'."


def _list_datasets(m, q):
    cypher = "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at"
    return cypher, {}

def _phrase_list_datasets(result, m):
    if not result:
        return "No datasets have been loaded yet."
    names = ", ".join(r.get("filename", r.get("id", "?")) for r in result)
    return f"Loaded datasets: {names}."


def _list_columns(m, q):
    cypher = "MATCH (r:Row) RETURN keys(r) AS cols LIMIT 1"
    return cypher, {}

def _phrase_list_columns(result, m):
    if not result:
        return "No rows found — cannot list columns."
    cols = [c for c in (result[0].get("cols") or []) if c not in ("key", "row_index", "dataset_id")]
    return f"The dataset has these columns: {', '.join(cols)}."


def _show_all_rows(m, q):
    cypher = "MATCH (r:Row) RETURN r LIMIT 25"
    return cypher, {}

def _phrase_show_all_rows(result, m):
    return f"Here are {len(result)} rows from the dataset."


def _rows_where(m, q):
    col = m.group(1).strip().replace(" ", "_")
    val = m.group(2).strip().strip("\"'")
    cypher = f"MATCH (r:Row) WHERE r.`{col}` = $val RETURN r LIMIT 25"
    return cypher, {"val": val}

def _phrase_rows_where(result, m):
    col = m.group(1).strip()
    val = m.group(2).strip().strip("\"'")
    return f"Found {len(result)} row(s) where {col} = '{val}'."


def _avg_col(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = f"MATCH (r:Row) RETURN avg(toFloat(r.`{col}`)) AS average"
    return cypher, {}

def _phrase_avg_col(result, m):
    col = m.group(1).strip()
    avg = result[0].get("average") if result else None
    if avg is None:
        return f"Could not compute average for '{col}' (may not be numeric)."
    return f"The average {col} is {avg:.2f}."


def _sum_col(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = f"MATCH (r:Row) RETURN sum(toFloat(r.`{col}`)) AS total"
    return cypher, {}

def _phrase_sum_col(result, m):
    col = m.group(1).strip()
    total = result[0].get("total") if result else None
    if total is None:
        return f"Could not sum '{col}' (may not be numeric)."
    return f"The total {col} is {total:.2f}."


def _max_col(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = f"MATCH (r:Row) RETURN max(r.`{col}`) AS maximum"
    return cypher, {}

def _phrase_max_col(result, m):
    col = m.group(1).strip()
    val = result[0].get("maximum") if result else None
    return f"The maximum {col} is {val}."


def _min_col(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = f"MATCH (r:Row) RETURN min(r.`{col}`) AS minimum"
    return cypher, {}

def _phrase_min_col(result, m):
    col = m.group(1).strip()
    val = result[0].get("minimum") if result else None
    return f"The minimum {col} is {val}."


def _distinct_values(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = f"MATCH (r:Row) RETURN DISTINCT r.`{col}` AS value ORDER BY value LIMIT 50"
    return cypher, {}

def _phrase_distinct_values(result, m):
    col = m.group(1).strip()
    vals = [str(r.get("value", "")) for r in result]
    return f"Distinct values of {col}: {', '.join(vals)}."


def _group_by(m, q):
    col = m.group(1).strip().replace(" ", "_")
    cypher = (
        f"MATCH (r:Row) RETURN r.`{col}` AS group_val, count(r) AS count "
        f"ORDER BY count DESC LIMIT 20"
    )
    return cypher, {}

def _phrase_group_by(result, m):
    col = m.group(1).strip()
    if not result:
        return f"No data found to group by {col}."
    lines = [f"  {r['group_val']}: {r['count']}" for r in result]
    return f"Row counts by {col}:\n" + "\n".join(lines)


# ── Template registry (order = priority) ──────────────────────────────────────

TEMPLATES = [
    # 1. How many rows total
    {
        "pattern": re.compile(r"how many rows", re.I),
        "match_guard": lambda m, q: not re.search(r"\bwhere\b|\bwith\b", q),
        "build": _count_all,
        "phrase": _phrase_count_all,
    },
    # 2. How many rows where col = val
    {
        "pattern": re.compile(
            r"how many rows?\s+(?:where|with)\s+([a-z0-9_\s]+?)\s*(?:=|is|equals?|are)\s*[\"']?([^\"']+?)[\"']?\s*$",
            re.I,
        ),
        "match_guard": None,
        "build": _count_where,
        "phrase": _phrase_count_where,
    },
    # 3. Show everything connected to <value>
    {
        "pattern": re.compile(r"(?:show|find|get)\s+(?:me\s+)?everything\s+connected\s+to\s+[\"']?(.+?)[\"']?\s*$", re.I),
        "match_guard": None,
        "build": _show_connected,
        "phrase": _phrase_show_connected,
    },
    # 4. List / which datasets
    {
        "pattern": re.compile(r"(?:list|which|show)\s+datasets?", re.I),
        "match_guard": None,
        "build": _list_datasets,
        "phrase": _phrase_list_datasets,
    },
    # 5. List columns
    {
        "pattern": re.compile(r"(?:list|what|show)\s+columns?|what\s+(?:are\s+the\s+)?fields?", re.I),
        "match_guard": None,
        "build": _list_columns,
        "phrase": _phrase_list_columns,
    },
    # 6. Average of column
    {
        "pattern": re.compile(r"(?:average|avg|mean)\s+(?:of\s+)?([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _avg_col,
        "phrase": _phrase_avg_col,
    },
    # 7. Sum of column
    {
        "pattern": re.compile(r"(?:total|sum)\s+(?:of\s+)?([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _sum_col,
        "phrase": _phrase_sum_col,
    },
    # 8. Max of column
    {
        "pattern": re.compile(r"(?:max(?:imum)?|highest|largest)\s+(?:of\s+)?([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _max_col,
        "phrase": _phrase_max_col,
    },
    # 9. Min of column
    {
        "pattern": re.compile(r"(?:min(?:imum)?|lowest|smallest)\s+(?:of\s+)?([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _min_col,
        "phrase": _phrase_min_col,
    },
    # 10. Distinct / unique values
    {
        "pattern": re.compile(r"(?:distinct|unique|different)\s+(?:values?\s+(?:of|in|for)\s+)?([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _distinct_values,
        "phrase": _phrase_distinct_values,
    },
    # 11. Group by / breakdown by column
    {
        "pattern": re.compile(r"(?:group\s+by|breakdown\s+by|count\s+by)\s+([a-z0-9_\s]+)", re.I),
        "match_guard": None,
        "build": _group_by,
        "phrase": _phrase_group_by,
    },
    # 12. Rows where col = val (show me)
    {
        "pattern": re.compile(
            r"(?:rows?|records?|show\s+me)?\s*where\s+([a-z0-9_\s]+?)\s*(?:=|is|equals?|are)\s*[\"']?([^\"']+?)[\"']?\s*$",
            re.I,
        ),
        "match_guard": None,
        "build": _rows_where,
        "phrase": _phrase_rows_where,
    },
    # 13. Show all rows (fallback)
    {
        "pattern": re.compile(r"show\s+(?:me\s+)?(?:all\s+)?rows?|list\s+(?:all\s+)?rows?|(?:give\s+me\s+)?(?:some\s+)?(?:sample|example)\s+rows?", re.I),
        "match_guard": None,
        "build": _show_all_rows,
        "phrase": _phrase_show_all_rows,
    },
]


# ── Main entry point ──────────────────────────────────────────────────────────

def answer(question: str) -> dict:
    """
    Answer a plain-English question from the graph only.
    Returns: {answer, cypher, result, grounded}
    """
    q = question.strip()
    if not q:
        return _ungrounded("Please ask a question.")

    # Check if any dataset has been loaded yet
    dataset_ids = get_all_dataset_ids()
    if not dataset_ids:
        return _ungrounded(
            "No dataset has been loaded yet. Please upload a CSV file first."
        )

    q_lower = q.lower()

    # Walk templates in priority order
    for template in TEMPLATES:
        m = template["pattern"].search(q_lower)
        if not m:
            continue
        guard = template.get("match_guard")
        if guard and not guard(m, q_lower):
            continue

        # Build Cypher
        cypher, params = template["build"](m, q_lower)
        logger.info("Matched template | cypher=%s | params=%s", cypher, params)

        # Run against Neo4j
        try:
            result = neo4j_client.run_query(cypher, params)
        except Exception as exc:
            logger.error("Neo4j query failed: %s", exc)
            return _ungrounded(f"The graph query failed: {exc}")

        # Serialise result (convert Neo4j node objects to dicts if needed)
        serialised = _serialise(result)

        # If result is empty / null → ungrounded
        if not serialised or _is_null_result(serialised):
            return _response(
                "I found no matching data in the graph for that question.",
                cypher=cypher,
                result=serialised,
                grounded=False,
            )

        # Phrase a natural-language answer
        answer_text = template["phrase"](serialised, m)
        return _response(answer_text, cypher=cypher, result=serialised, grounded=True)

    # No template matched
    return _ungrounded(
        "I don't have that in the data. I can answer questions like: "
        "'How many rows?', 'Show rows where status = shipped', "
        "'Average price', 'List columns', 'Group by category'."
    )


def _serialise(result: list) -> list:
    """Convert Neo4j node objects / neo4j types to plain Python."""
    out = []
    for record in result:
        row = {}
        for k, v in record.items():
            row[k] = _serialise_value(v)
        out.append(row)
    return out


def _serialise_value(v: Any) -> Any:
    """Recursively serialise Neo4j-specific types."""
    if hasattr(v, "_properties"):  # Node
        return dict(v._properties)
    if isinstance(v, dict):
        return {kk: _serialise_value(vv) for kk, vv in v.items()}
    if isinstance(v, list):
        return [_serialise_value(i) for i in v]
    return v


def _is_null_result(result: list) -> bool:
    """Return True if result is [{key: None}] or [{key: 0}] for count queries."""
    if not result:
        return True
    if len(result) == 1 and len(result[0]) == 1:
        val = next(iter(result[0].values()))
        return val is None
    return False
