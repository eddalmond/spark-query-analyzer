"""
F-10 · Natural Language Query Explainer

Produces a concise, plain-English narrative summarising:
  1. What the query does (query profile from plan tree)
  2. The single biggest performance problem (lead finding, template-driven)
  3. The highest-impact fix (single imperative sentence)

Zero external dependencies — pure Python string logic on structured findings.

Optional enhancement: if mlflow.deployments is available and a workspace LLM endpoint
is configured, delegate to that instead of templates.  Fail silently to templates.
"""

from dataclasses import dataclass, field
from typing import Optional


# --------------------------------------------------------------------------------
# Template library
# --------------------------------------------------------------------------------

TEMPLATES = {
    "MISSING_BROADCAST": (
        "The biggest problem is that `{table}` ({size}) is being shuffled across all "
        "executors when it is small enough to broadcast — this is causing an unnecessary "
        "full data exchange before the join."
    ),
    "CARTESIAN_PRODUCT": (
        "The biggest problem is a cartesian (cross) join between `{t1}` and `{t2}` — "
        "every row of each table is being paired with every row of the other, producing "
        "an exploding row count with no filter to constrain it."
    ),
    "FULL_TABLE_SCAN": (
        "The biggest problem is that `{table}` ({rows}M rows) is being read in full "
        "with no partition filter applied.  Every partition is being scanned regardless "
        "of whether its data is relevant to the query."
    ),
    "SORT_MERGE_JOIN": (
        "The biggest problem is a sort-based join — data is being sorted on both sides "
        "before a merge join can proceed.  If either side is large this is expensive; "
        "a broadcast join would avoid the sort entirely."
    ),
    "SKEW_INDICATOR": (
        "The biggest problem is data skew on `{join_key}` — one or more tasks are "
        "processing far more data than the others, causing the entire stage to wait "
        "on the slowest partition."
    ),
    "EXPLODING_JOIN": (
        "The biggest problem is an exploding join — the output row count vastly exceeds "
        "the input, typically caused by joining on a non-unique key that creates duplicate "
        "matches.  The result set is ballooning before any aggregation can shrink it."
    ),
    "REPEATED_SCAN": (
        "The biggest problem is that `{table}` is being scanned {count} times across "
        "this query.  Each scan re-reads the data from storage; materialising it once "
        "in a CTE or cache would eliminate the redundant I/O."
    ),
    "MISSING_PUSHDOWN": (
        "The biggest problem is that a filter is being applied after reading the data "
        "instead of during the scan.  This means more data is being read and processed "
        "than necessary — pushing the filter into the source scan avoids the waste."
    ),
    "WIDE_TRANSFORM": (
        "The biggest problem is a wide transformation with no LIMIT — the query is "
        "materialising a large number of rows after a shuffle, which can cause memory "
        "pressure on the executors.  Consider whether a LIMIT is appropriate."
    ),
    "BROADCAST_USED": None,  # informational only — no "biggest problem" narrative
}

FIX_TEMPLATES = {
    "MISSING_BROADCAST": "Add a BROADCAST hint to the small table in the JOIN clause: "
                         "`JOIN /*+ BROADCAST({table}) */ {table} ON ...`",
    "CARTESIAN_PRODUCT": "Add an explicit JOIN condition that relates the two tables, "
                         "or filter at least one side to a single partition if a cross "
                         "join is intentional.",
    "FULL_TABLE_SCAN": "Add a partition column filter to the WHERE clause — "
                       "e.g. `WHERE partition_col = '{value}'` — to enable partition pruning.",
    "SORT_MERGE_JOIN": "Consider a BROADCAST or SHUFFLE_HASH join hint instead: "
                        "`JOIN /*+ BROADCAST(t) */ t ON ...` or "
                        "`JOIN /*+ SHUFFLE_HASH(t) */ t ON ...`",
    "SKEW_INDICATOR": "Enable AQE skew join handling: "
                      "`spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')`.  "
                      "Alternatively, salt the join key on the skewed side.",
    "EXPLODING_JOIN": "Verify the join key has proper cardinality and that selective "
                      "filters are applied to both join inputs before the join.",
    "REPEATED_SCAN": "Wrap the repeated table in a CTE (WITH clause) or cache it "
                     "with `df = spark.table('{table}').cache()` before reusing it.",
    "MISSING_PUSHDOWN": "Rewrite the query so the filter predicate is applied in the "
                         "inner query or scan operator, not in an outer SELECT.",
    "WIDE_TRANSFORM": "Add a LIMIT clause if the full result set is not needed for "
                       "downstream processing, or review whether the transformation is necessary.",
}


# --------------------------------------------------------------------------------
# Dataclasses
# --------------------------------------------------------------------------------

@dataclass
class QueryProfile:
    """Structured description of what the query does."""
    num_joins: int = 0
    num_aggregations: int = 0
    largest_table: str = ""
    largest_table_rows: str = ""
    has_streaming: bool = False
    query_type: str = ""  # e.g. "SELECT", "INSERT", "CREATE TABLE"

    def summary_sentence(self) -> str:
        parts = []
        if self.num_joins > 0:
            parts.append(f"joins {self.num_joins} table{'s' if self.num_joins > 1 else ''}")
        if self.num_aggregations > 0:
            parts.append(f"with {self.num_aggregations} aggregation{'s' if self.num_aggregations > 1 else ''}")
        if self.largest_table:
            parts.append(f"against `{self.largest_table}` ({self.largest_table_rows} rows)")
        if self.has_streaming:
            parts.append("in a streaming context")

        if not parts:
            return "This query performs a direct table scan."
        return "This query " + ", ".join(parts) + "."


@dataclass
class NarrativeResult:
    profile: QueryProfile
    lead_finding_sentence: str
    fix_sentence: str
    llm_used: bool = False
    is_informational: bool = False  # True when only INFO findings — no problem to flag

    def render(self, detailed: bool = False) -> str:
        """Render as an HTML summary banner."""
        if self.is_informational:
            summary = (
                f"<p><strong>Summary:</strong> {self.profile.summary_sentence()}  "
                f"No performance issues detected.</p>"
            )
        else:
            summary = (
                f"<p><strong>What it does:</strong> {self.profile.summary_sentence()}</p>"
                f"<p><strong>Biggest problem:</strong> {self.lead_finding_sentence}</p>"
                f"<p><strong>Fix:</strong> {self.fix_sentence}</p>"
            )
        if detailed and not self.is_informational:
            # Detailed mode is a longer multi-paragraph version — not implemented in template path
            pass
        return summary


# --------------------------------------------------------------------------------
# NarrativeExplainer
# --------------------------------------------------------------------------------

class NarrativeExplainer:
    """
    Turns structured analysis findings into a plain-English narrative.

    Usage:
        explainer = NarrativeExplainer(findings, plan_text, query)
        result = explainer.explain()
        banner_html = result.render()
    """

    def __init__(
        self,
        findings: list,  # list of Finding from analyzer.AnalysisResult
        plan_text: str = "",
        query: str = "",
    ):
        self.findings = findings
        self.plan_text = plan_text
        self.query = query

    def explain(self, detailed: bool = False) -> NarrativeResult:
        """
        Build a NarrativeResult.

        Attempts optional LLM path first if configured and mlflow.deployments
        is available; falls back to templates silently.
        """
        profile = self._build_profile()

        # Try optional workspace LLM path
        llm_result = self._try_llm_explain(profile, detailed)
        if llm_result is not None:
            return llm_result

        # Template path
        return self._template_explain(profile)

    # --------------------------------------------------------------------------

    def _build_profile(self) -> QueryProfile:
        """Infer query characteristics from plan text and query string."""
        plan = self.plan_text
        q = self.query.upper()

        profile = QueryProfile()

        # Query type
        if q.startswith("INSERT"):
            profile.query_type = "INSERT"
        elif q.startswith("CREATE"):
            profile.query_type = "CREATE"
        elif q.startswith("SELECT"):
            profile.query_type = "SELECT"
        else:
            profile.query_type = "QUERY"

        # Join count (look for Join nodes in plan tree lines)
        profile.num_joins = sum(1 for line in plan.split("\n") if "Join" in line.strip())

        # Aggregation count
        profile.num_aggregations = sum(
            1 for line in plan.split("\n")
            if any(kw in line for kw in ("Aggregate", "HashAggregate", "SortAggregate"))
        )

        # Streaming detection
        profile.has_streaming = "streaming" in plan.lower() or "stream" in q.lower()

        # Largest table by scanning num_rows hints in plan
        import re
        scan_rows = re.findall(r"Scan.*?\[(\d+(?:\.\d+)?[KM]?)", plan, re.IGNORECASE)
        if scan_rows:
            largest = self._parse_row_count(max(scan_rows, key=_row_count_key))
            profile.largest_table_rows = largest

        # Table names from query
        tables = re.findall(
            r"(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)", q, re.IGNORECASE
        )
        tables = [t for t in tables if t.upper() not in ("SELECT", "WHERE", "AND", "OR", "ON")]
        if tables:
            # Heuristic: the largest scanned table (by name match in plan) is the main one
            for table in tables:
                if table in plan and "Scan" in plan:
                    profile.largest_table = table.split(".")[-1]
                    break
            if not profile.largest_table and tables:
                profile.largest_table = tables[0].split(".")[-1]

        return profile

    @staticmethod
    def _parse_row_count(s: str) -> str:
        """Normalise a row count string like '1.5M', '500K' to just the number + unit."""
        return s.upper()

    def _try_llm_explain(self, profile: QueryProfile, detailed: bool) -> Optional[NarrativeResult]:
        """
        Attempt workspace LLM path via mlflow.deployments.
        Returns None on any failure (missing mlflow, no deployment, API error).
        """
        try:
            from mlflow.deployments import get_client
        except Exception:
            return None

        try:
            client = get_client("databricks")
        except Exception:
            return None

        deployment_name = "databricks-meta-llama"  # configurable in future

        findings_json = [
            {
                "severity": f.severity,
                "code": f.code,
                "message": f.message,
                "table": f.table,
                "suggestion": f.suggestion,
            }
            for f in self.findings
        ]

        prompt = (
            f"You are a Spark performance expert. Explain this Databricks query analysis "
            f"in plain English for a data engineer. Include: what the query does, the "
            f"biggest problem, and the single highest-impact fix.\n\n"
            f"Query profile: {profile.summary_sentence()}\n"
            f"Findings: {findings_json}\n\n"
            f"Respond with a concise paragraph. Do not use markdown."
        )

        try:
            response = client.predict(deployment=deployment_name, inputs={"prompt": prompt})
            text = response.get("choices", [{}])[0].get("text", "").strip()
        except Exception:
            return None

        if not text:
            return None

        return NarrativeResult(
            profile=profile,
            lead_finding_sentence=text,  # LLM covers everything in one paragraph
            fix_sentence="",  # LLM already gave the fix in the paragraph
            llm_used=True,
            is_informational=(len(self.findings) == 0 or all(f.severity == "info" for f in self.findings)),
        )

    def _template_explain(self, profile: QueryProfile) -> NarrativeResult:
        """Build narrative purely from templates — zero external dependencies."""

        # Determine if there's anything to report
        if not self.findings or all(f.severity == "info" for f in self.findings):
            return NarrativeResult(
                profile=profile,
                lead_finding_sentence="",
                fix_sentence="",
                is_informational=True,
            )

        # Sort: critical > high > medium > info, then by code presence in TEMPLATES
        priority = {"critical": 0, "high": 1, "medium": 2, "info": 3}
        template_codes = set(TEMPLATES.keys())

        def sort_key(f):
            sev_rank = priority.get(f.severity, 99)
            has_template = 0 if f.code in template_codes else 1
            return (sev_rank, has_template)

        sorted_findings = sorted(self.findings, key=sort_key)
        lead = sorted_findings[0]

        # Build lead finding sentence
        lead_text = self._fill_template(lead)
        if not lead_text:
            # Fallback to raw message if no template
            lead_text = lead.message

        # Build fix sentence
        fix_text = self._fill_fix_template(lead)
        if not fix_text:
            fix_text = lead.suggestion or "Review the findings below for recommended fixes."

        return NarrativeResult(
            profile=profile,
            lead_finding_sentence=lead_text,
            fix_sentence=fix_text,
            llm_used=False,
            is_informational=False,
        )

    def _fill_template(self, finding) -> str:
        """Substitute template placeholders for a Finding."""
        template = TEMPLATES.get(finding.code, "")
        if not template:
            return ""

        table = getattr(finding, "table", None) or "the table"
        detail = getattr(finding, "detail", None) or ""

        # Extract size from detail if present (e.g. "~100MB" in message)
        size = self._extract_size_from_text(finding.message, detail)

        # Extract row count from message/detail
        rows = self._extract_rows_from_text(finding.message)

        # Join keys
        t1, t2 = self._extract_join_tables(finding.message, detail)
        join_key = self._extract_join_key(finding.message, detail)

        substitutions = {
            "{table}": table,
            "{size}": size,
            "{rows}": rows,
            "{t1}": t1,
            "{t2}": t2,
            "{join_key}": join_key,
            "{count}": str(getattr(finding, "_repeat_count", 2)),
        }

        result = template
        for placeholder, value in substitutions.items():
            result = result.replace(placeholder, value)
        return result

    def _fill_fix_template(self, finding) -> str:
        """Substitute fix template for a Finding."""
        template = FIX_TEMPLATES.get(finding.code, "")
        if not template:
            return ""

        table = getattr(finding, "table", None) or "{table}"
        if "{table}" in template:
            template = template.replace("{table}", table)

        return template

    @staticmethod
    def _extract_size_from_text(message: str, detail: str) -> str:
        """Pull a size estimate from finding text, e.g. '~100MB'."""
        import re
        text = f"{message} {detail}"
        m = re.search(r"([\d.]+[KMG]?B)", text, re.IGNORECASE)
        return m.group(1) if m else "unknown size"

    @staticmethod
    def _extract_rows_from_text(message: str) -> str:
        """Pull a row estimate from finding text, e.g. '1.5M'."""
        import re
        m = re.search(r"(\d+(?:\.\d+)?\s*[KM]?\s*rows?|\d+(?:\.\d+)?[KM])", message, re.IGNORECASE)
        return m.group(1) if m else "?"

    @staticmethod
    def _extract_join_tables(message: str, detail: str) -> tuple[str, str]:
        """Extract two table names from a join-related finding."""
        import re
        text = f"{message} {detail}"
        tables = re.findall(r"`?(\w+(?:\.\w+)?)`?", text)
        tables = [t for t in tables if t.upper() not in ("AND", "OR", "ON", "JOIN", "SELECT")]
        t1 = tables[0] if len(tables) > 0 else "table1"
        t2 = tables[1] if len(tables) > 1 else "table2"
        return t1, t2

    @staticmethod
    def _extract_join_key(message: str, detail: str) -> str:
        """Extract a join key column name if mentioned."""
        import re
        text = f"{message} {detail}"
        m = re.search(r"(?:on|join)\s+(?:key|by)?\s*[`\"]?(\w+)[`\"]?", text, re.IGNORECASE)
        return m.group(1) if m else "the join key"


# --------------------------------------------------------------------------------
# HTML rendering
# --------------------------------------------------------------------------------

_SUMMARY_CSS = """
.sqa-narrative {
  background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 100%);
  color: #f1f5f9;
  padding: 14px 16px;
  border-radius: 8px;
  margin-bottom: 8px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 13px;
  line-height: 1.6;
}
.sqa-narrative p {
  margin: 0 0 6px 0;
}
.sqa-narrative p:last-child {
  margin-bottom: 0;
}
.sqa-narrative strong {
  color: #60a5fa;
}
.sqa-narrative-badge {
  display: inline-block;
  background: rgba(96, 165, 250, 0.15);
  border: 1px solid rgba(96, 165, 250, 0.3);
  color: #93c5fd;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: .05em;
  padding: 1px 6px;
  border-radius: 4px;
  margin-bottom: 8px;
}
"""


def format_narrative_banner(result: NarrativeResult, detailed: bool = False) -> str:
    """
    Render a NarrativeResult as an HTML banner.
    Insert this at the very top of the HTML card, above the finding list.
    """
    narrative_html = result.render(detailed=detailed)
    llm_badge = (
        '<div class="sqa-narrative-badge">&#x1F916; AI Summary</div>'
        if result.llm_used else
        '<div class="sqa-narrative-badge">&#x1F4DD; Summary</div>'
    )
    return (
        f"<style>{_SUMMARY_CSS}</style>"
        f"<div class='sqa-narrative'>"
        f"{llm_badge}"
        f"{narrative_html}"
        f"</div>"
    )