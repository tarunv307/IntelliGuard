"""
chatbot.py — Dynamic, schema-aware grounded question-answering via Cypher & Neo4j.

Capabilities:
  1. Dynamically inspects Neo4j schema (columns) and distinct graph values.
  2. Resolves column synonyms (e.g., units -> quantity, dollars/cost -> price).
  3. Maps natural language questions to precise Cypher queries across any CSV structure.
  4. Automatically detects whether values exist in the graph. If absent, provides an honest refusal with available columns/values.
  5. Always returns {answer, cypher, result, grounded}.
"""
import logging
import re
from typing import Any, Optional

from app import neo4j_client
from app.jobs import get_all_dataset_ids

logger = logging.getLogger(__name__)

# ── Response format helper ───────────────────────────────────────────────────

def _response(answer: str, cypher=None, result=None, grounded=True) -> dict:
    return {
        "answer": answer,
        "cypher": cypher,
        "result": result or [],
        "grounded": grounded,
    }


def _ungrounded(reason: str, cypher=None, result=None) -> dict:
    return _response(reason, cypher=cypher, result=result or [], grounded=False)


# ── Common Column Synonyms ───────────────────────────────────────────────────

SYNONYMS = {
    "units": ["quantity", "unit_count", "units", "qty", "count", "items", "amount"],
    "unit": ["quantity", "unit_count", "units", "qty", "count", "items", "amount"],
    "quantity": ["quantity", "qty", "count", "units", "items"],
    "dollars": ["price", "cost", "amount", "total", "rate", "revenue", "dollar", "unit_price"],
    "dollar": ["price", "cost", "amount", "total", "rate", "revenue", "dollars", "unit_price"],
    "price": ["price", "cost", "amount", "rate", "unit_price", "dollars"],
    "cost": ["price", "cost", "amount", "rate", "unit_price"],
    "revenue": ["total", "amount", "price", "revenue"],
    "customer": ["customer_id", "customer", "client_id", "client", "user_id", "user", "buyer"],
    "client": ["customer_id", "customer", "client_id", "client", "user_id", "user"],
    "buyer": ["customer_id", "customer", "client_id", "user_id"],
    "product": ["product", "product_name", "item", "item_name", "goods", "sku", "name"],
    "item": ["product", "product_name", "item", "item_name", "goods", "sku", "name"],
    "category": ["category", "type", "class", "genre", "group", "department"],
    "type": ["category", "type", "class", "genre", "group"],
    "status": ["status", "state", "stage", "condition"],
    "state": ["status", "state", "condition"],
    "order": ["order_id", "order_num", "order_number", "id"],
    "date": ["date", "created_at", "order_date", "timestamp", "time"],
}

METADATA_COLS = {"key", "row_index", "dataset_id"}


# ── Schema & Value Introspection ─────────────────────────────────────────────

def get_graph_schema() -> list[str]:
    """Fetch all user column names from Row nodes in Neo4j."""
    try:
        res = neo4j_client.run_query("MATCH (r:Row) RETURN keys(r) AS cols LIMIT 1", {})
        if res and res[0].get("cols"):
            return [c for c in res[0]["cols"] if c not in METADATA_COLS]
    except Exception as exc:
        logger.warning("Could not fetch schema from graph: %s", exc)
    return []


def resolve_column(term: str, available_cols: list[str]) -> Optional[str]:
    """Resolve a term to an actual column name using exact, snake_case, or synonym match."""
    t = term.lower().strip().replace(" ", "_").replace("-", "_")
    
    # 1. Exact match
    for col in available_cols:
        if col.lower() == t:
            return col
            
    # 2. Case-insensitive substring match
    for col in available_cols:
        if t in col.lower() or col.lower() in t:
            return col
            
    # 3. Synonym match
    if t in SYNONYMS:
        for candidate in SYNONYMS[t]:
            for col in available_cols:
                if col.lower() == candidate:
                    return col
                    
    return None


def find_value_in_graph(val: str, available_cols: list[str]) -> list[tuple[str, str]]:
    """
    Search if `val` matches any property value in the graph.
    Returns list of (column_name, exact_matched_value).
    """
    matches = []
    val_clean = val.strip().strip("\"'").lower()
    if not val_clean:
        return matches

    for col in available_cols:
        try:
            cypher = (
                f"MATCH (r:Row) "
                f"WHERE toLower(toString(r.`{col}`)) = $val "
                f"RETURN DISTINCT r.`{col}` AS val LIMIT 1"
            )
            res = neo4j_client.run_query(cypher, {"val": val_clean})
            if res and res[0].get("val") is not None:
                matches.append((col, str(res[0]["val"])))
        except Exception:
            continue
    return matches


# ── Dynamic Query Builder ───────────────────────────────────────────────────

def dynamic_cypher_engine(question: str, available_cols: list[str]) -> Optional[dict]:
    """
    Intelligently parses the natural language question into a Cypher query
    based on the real columns and values present in the Neo4j graph.
    """
    q = question.strip()
    q_lower = q.lower()

    # 1. Row count (e.g. "how many rows", "total records", "number of rows")
    if re.search(r"^(how many|total|number of|count of)?\s*(rows|records|entries|lines)\b", q_lower) and not re.search(r"\b(where|with|for|in|by)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN count(r) AS total_rows"
        res = neo4j_client.run_query(cypher, {})
        total = res[0].get("total_rows", 0) if res else 0
        return _response(f"There are {total} rows in the dataset.", cypher=cypher, result=res, grounded=True)

    # 2. List columns / fields / attributes
    if re.search(r"\b(columns?|fields?|attributes?|headers?|keys?)\b", q_lower) and re.search(r"\b(list|show|what|get|all)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN keys(r) AS cols LIMIT 1"
        res = neo4j_client.run_query(cypher, {})
        cols = [c for c in (res[0].get("cols") if res else []) if c not in METADATA_COLS]
        return _response(f"The dataset contains these columns: {', '.join(cols)}.", cypher=cypher, result=res, grounded=True)

    # 3. List datasets
    if re.search(r"\b(datasets?|files?)\b", q_lower) and re.search(r"\b(list|show|which|what)\b", q_lower):
        cypher = "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at"
        res = neo4j_client.run_query(cypher, {})
        if not res:
            return _ungrounded("No datasets found in the graph.", cypher=cypher, result=[])
        names = ", ".join(r.get("filename", r.get("id", "?")) for r in res)
        return _response(f"Loaded datasets: {names}.", cypher=cypher, result=res, grounded=True)

    # 4. Show all / sample rows
    if re.search(r"^(show|list|get|give me|view)\s+(me\s+)?(all\s+)?(rows|data|records|sample|table)", q_lower):
        cypher = "MATCH (r:Row) RETURN r LIMIT 25"
        res = neo4j_client.run_query(cypher, {})
        return _response(f"Here are {len(res)} rows from the dataset.", cypher=cypher, result=_serialise(res), grounded=True)

    # 5. Group by / breakdown by / distribution of
    group_match = re.search(r"(?:group\s+by|breakdown\s+by|distribution\s+of|count\s+by|per|by)\s+([a-z0-9_\s]+)", q_lower)
    if group_match:
        raw_col = group_match.group(1).strip()
        col = resolve_column(raw_col, available_cols)
        if col:
            cypher = f"MATCH (r:Row) RETURN r.`{col}` AS `{col}`, count(r) AS count ORDER BY count DESC LIMIT 25"
            res = neo4j_client.run_query(cypher, {})
            if res:
                breakdown = ", ".join(f"{r.get(col, 'Unknown')}: {r.get('count')}" for r in res)
                return _response(f"Breakdown by {col}: {breakdown}.", cypher=cypher, result=res, grounded=True)
            return _ungrounded(f"No data found for column '{col}'.", cypher=cypher, result=[])

    # 6. Aggregations (Average, Sum, Max, Min, Distinct)
    # Check for "average <col>", "avg <col>", "mean <col>"
    avg_match = re.search(r"\b(average|avg|mean)\s+(?:of\s+)?([a-z0-9_\s]+)", q_lower)
    if avg_match:
        col = resolve_column(avg_match.group(2).strip(), available_cols)
        if col:
            cypher = f"MATCH (r:Row) RETURN avg(toFloat(r.`{col}`)) AS average"
            res = neo4j_client.run_query(cypher, {})
            avg_val = res[0].get("average") if res else None
            if avg_val is not None:
                return _response(f"The average {col} is {avg_val:.2f}.", cypher=cypher, result=res, grounded=True)
            return _ungrounded(f"Could not calculate average for '{col}' (column may not be numeric).", cypher=cypher, result=res)

    # Check for "total <col>", "sum <col>"
    sum_match = re.search(r"\b(total|sum)\s+(?:of\s+)?([a-z0-9_\s]+)", q_lower)
    if sum_match:
        target_term = sum_match.group(2).strip()
        # Check if there is a condition e.g. "total units for electronics"
        cond_match = re.search(r"(?:for|in|where|of)\s+([a-z0-9_\s]+)$", target_term)
        filter_clause = ""
        params = {}
        
        if cond_match:
            cand_val = cond_match.group(1).strip()
            val_matches = find_value_in_graph(cand_val, available_cols)
            if val_matches:
                v_col, v_val = val_matches[0]
                filter_clause = f"WHERE toLower(toString(r.`{v_col}`)) = $filter_val "
                params["filter_val"] = v_val.lower()
                target_term = target_term[:cond_match.start()].strip()

        col = resolve_column(target_term, available_cols)
        if col:
            cypher = f"MATCH (r:Row) {filter_clause}RETURN sum(toFloat(r.`{col}`)) AS total"
            res = neo4j_client.run_query(cypher, params)
            tot_val = res[0].get("total") if res else None
            if tot_val is not None:
                msg = f"The total {col} is {tot_val:.2f}."
                if params:
                    msg += f" (filtered by {params.get('filter_val')})"
                return _response(msg, cypher=cypher, result=res, grounded=True)

    # Check for "max / highest <col>", "min / lowest <col>"
    extreme_match = re.search(r"\b(max(?:imum)?|highest|top|min(?:imum)?|lowest|cheapest)\s+(?:of\s+)?([a-z0-9_\s]+)", q_lower)
    if extreme_match:
        op = extreme_match.group(1)
        col = resolve_column(extreme_match.group(2).strip(), available_cols)
        if col:
            func = "max" if op in ["max", "maximum", "highest", "top"] else "min"
            cypher = f"MATCH (r:Row) RETURN {func}(r.`{col}`) AS extreme_val"
            res = neo4j_client.run_query(cypher, {})
            ext_val = res[0].get("extreme_val") if res else None
            return _response(f"The {op} {col} is {ext_val}.", cypher=cypher, result=res, grounded=True)

    # Check for "distinct / unique <col>"
    distinct_match = re.search(r"\b(distinct|unique|different)\s+(?:values?\s+(?:of|in|for)\s+)?([a-z0-9_\s]+)", q_lower)
    if distinct_match:
        col = resolve_column(distinct_match.group(2).strip(), available_cols)
        if col:
            cypher = f"MATCH (r:Row) RETURN DISTINCT r.`{col}` AS val ORDER BY val LIMIT 50"
            res = neo4j_client.run_query(cypher, {})
            vals = [str(r.get("val")) for r in res if r.get("val") is not None]
            return _response(f"Distinct values of {col}: {', '.join(vals)}.", cypher=cypher, result=res, grounded=True)

    # 7. Filtered row searches (e.g. "where status = shipped", "orders for customer C7", "show electronics", "find Widget A")
    # A. Check for explicit `col = val` or `col is val`
    explicit_where = re.search(r"(?:where|with|having)\s+([a-z0-9_\s]+?)\s*(?:=|is|equals?|are|:)\s*[\"']?([^\"']+?)[\"']?\s*$", q_lower)
    if explicit_where:
        raw_col = explicit_where.group(1).strip()
        raw_val = explicit_where.group(2).strip()
        col = resolve_column(raw_col, available_cols)
        if col:
            # Check if count or rows requested
            is_count = bool(re.search(r"\b(how many|count|number of)\b", q_lower))
            if is_count:
                cypher = f"MATCH (r:Row) WHERE toLower(toString(r.`{col}`)) = $val RETURN count(r) AS count"
                res = neo4j_client.run_query(cypher, {"val": raw_val.lower()})
                cnt = res[0].get("count", 0) if res else 0
                return _response(f"There are {cnt} rows where {col} = '{raw_val}'.", cypher=cypher, result=res, grounded=True)
            else:
                cypher = f"MATCH (r:Row) WHERE toLower(toString(r.`{col}`)) = $val RETURN r LIMIT 25"
                res = neo4j_client.run_query(cypher, {"val": raw_val.lower()})
                if res:
                    return _response(f"Found {len(res)} row(s) where {col} = '{raw_val}'.", cypher=cypher, result=_serialise(res), grounded=True)
                return _ungrounded(f"No rows found where {col} = '{raw_val}'.", cypher=cypher, result=[])

    # B. Check for words in question that match actual values in the graph (e.g., "how many Widget A", "find C7", "electronics orders")
    # Extract words / tokens from question preserving single characters like 'A', 'B', '7'
    raw_tokens = [t.strip("\"',.?!;:") for t in q.split()]
    tokens = [t for t in raw_tokens if t]
    
    # Try 3-word, 2-word combinations, then 1-word
    candidate_phrases = []
    for i in range(len(tokens) - 2):
        candidate_phrases.append(f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}")
    for i in range(len(tokens) - 1):
        candidate_phrases.append(f"{tokens[i]} {tokens[i+1]}")
    candidate_phrases.extend(tokens)

    matched_filters = []
    stop_words = {"how", "many", "show", "what", "find", "list", "units", "dollars", "price", "rows", "data", "there", "where", "with", "have", "are", "the", "in", "of", "for", "to"}
    for phrase in candidate_phrases:
        if phrase.lower() in stop_words:
            continue
        v_matches = find_value_in_graph(phrase, available_cols)
        if v_matches:
            matched_filters.append((phrase, v_matches[0][0], v_matches[0][1]))
            break

    if matched_filters:
        phrase, col, matched_val = matched_filters[0]
        is_count = bool(re.search(r"\b(how many|count|number of|total)\b", q_lower))
        if is_count:
            # If asking "how many units of X" and "quantity" exists, sum quantity
            if re.search(r"\b(units?|quantity|items?)\b", q_lower) and resolve_column("quantity", available_cols):
                qty_col = resolve_column("quantity", available_cols)
                cypher = f"MATCH (r:Row) WHERE toLower(toString(r.`{col}`)) = $val RETURN sum(toFloat(r.`{qty_col}`)) AS total_units"
                res = neo4j_client.run_query(cypher, {"val": matched_val.lower()})
                tot_u = res[0].get("total_units", 0) if res else 0
                return _response(f"There are {tot_u} total units for '{matched_val}' ({col}).", cypher=cypher, result=res, grounded=True)
            else:
                cypher = f"MATCH (r:Row) WHERE toLower(toString(r.`{col}`)) = $val RETURN count(r) AS count"
                res = neo4j_client.run_query(cypher, {"val": matched_val.lower()})
                cnt = res[0].get("count", 0) if res else 0
                return _response(f"There are {cnt} rows where {col} is '{matched_val}'.", cypher=cypher, result=res, grounded=True)
        else:
            cypher = f"MATCH (r:Row) WHERE toLower(toString(r.`{col}`)) = $val RETURN r LIMIT 25"
            res = neo4j_client.run_query(cypher, {"val": matched_val.lower()})
            return _response(f"Found {len(res)} row(s) for '{matched_val}' ({col}).", cypher=cypher, result=_serialise(res), grounded=True)

    # 8. Check if user is asking about a concept / value not in the graph (e.g. "how many units are in dollars", "cars in stock")
    # Detect unmatched keywords
    unmatched_words = [t for t in tokens if t.lower() not in ["how", "many", "are", "is", "in", "of", "the", "for", "to", "what", "show", "list", "find"]]
    if unmatched_words:
        unmatched_str = " ".join(unmatched_words)
        return _ungrounded(
            f"The value or entity '{unmatched_str}' was not found in the dataset. "
            f"Available columns are: {', '.join(available_cols)}."
        )

    return None


# ── Main Entry Point ─────────────────────────────────────────────────────────

def answer(question: str) -> dict:
    """
    Answer a plain-English question strictly from the graph data.
    """
    q = question.strip()
    if not q:
        return _ungrounded("Please ask a question.")

    # 1. Verify datasets exist
    dataset_ids = get_all_dataset_ids()
    if not dataset_ids:
        return _ungrounded("No dataset has been loaded yet. Please upload a CSV file first.")

    # 2. Get available columns from graph
    available_cols = get_graph_schema()
    if not available_cols:
        return _ungrounded("No row data found in Neo4j. Please upload a valid CSV file.")

    # 3. Dynamic schema-aware graph engine
    try:
        engine_res = dynamic_cypher_engine(q, available_cols)
        if engine_res:
            return engine_res
    except Exception as exc:
        logger.error("Dynamic engine error: %s", exc)
        return _ungrounded(f"Query error: {exc}")

    # 4. Fallback honest refusal with graph context
    return _ungrounded(
        f"I don't have that in the data. "
        f"The dataset has columns: {', '.join(available_cols)}. "
        f"You can ask questions like: 'How many rows?', 'List columns', 'Average price', 'Total quantity by category', 'Show rows where status is shipped'."
    )


# ── Serialisation Helpers ───────────────────────────────────────────────────

def _serialise(result: list) -> list:
    out = []
    for record in result:
        row = {}
        for k, v in record.items():
            row[k] = _serialise_value(v)
        out.append(row)
    return out


def _serialise_value(v: Any) -> Any:
    if hasattr(v, "_properties"):  # Node
        return dict(v._properties)
    if isinstance(v, dict):
        return {kk: _serialise_value(vv) for kk, vv in v.items()}
    if isinstance(v, list):
        return [_serialise_value(i) for i in v]
    return v
