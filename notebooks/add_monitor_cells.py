#!/usr/bin/env python3
"""Insert performance monitor cells into the notebook."""
import json

NOTEBOOK = "notebooks/QueryPerformanceAnalyzer.ipynb"
with open(NOTEBOOK) as f:
    nb = json.load(f)

cells = nb["cells"]

# Fix source format
for c in cells:
    if isinstance(c.get("source"), str):
        c["source"] = [c["source"]]

# Find install magics cell
insert_after = 0
for i, c in enumerate(cells):
    if "# Install magics" in "".join(c.get("source", "")):
        insert_after = i
        break

new_cells = [
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 🕐 Performance Monitor — `@monitor_performance` decorator\n",
            "\n",
            "Apply `@monitor_performance` to any Python or Spark function to track it "
            "in the central `_spark_query_analyzer.query_history` Delta table.\n",
            "\n",
            "Works alongside `%analyze` — both write to the same history table, "
            "so platform teams can query performance across all 200 scheduled notebooks "
            "from one Delta table.\n"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "source": [
            "from spark_query_analyzer import monitor_performance\n",
            "\n",
            "# Apply @monitor_performance to any Python or Spark function.\n",
            "# Each call writes a row to _spark_query_analyzer.query_history.\n",
            "\n",
            "@monitor_performance(job_name='example_ingest', spark=spark)\n",
            "def ingest_sales(path):\n",
            "    df = spark.read.format('parquet').load(path)\n",
            "    return df.count()\n",
            "\n",
            "@monitor_performance(job_name='daily_revenue', spark=spark)\n",
            "def daily_revenue():\n",
            "    result = spark.sql('''\n",
            "        SELECT region, SUM(amount) AS total\n",
            "        FROM fact_sales\n",
            "        WHERE date = '2024-01-01'\n",
            "        GROUP BY region\n",
            "    ''').collect()\n",
            "    return result\n",
            "\n",
            "print('Apply @monitor_performance(job_name=..., spark=spark) to any function')\n"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### What gets recorded\n",
            "\n",
            "Each monitored call writes one row to `_spark_query_analyzer.query_history`:\n",
            "\n",
            "| Column | What it captures |\n",
            "|--------|------------------|\n",
            "| `job_name` | Your chosen identifier |\n",
            "| `run_timestamp` | UTC timestamp |\n",
            "| `duration_ms` | Wall-clock execution time |\n",
            "| `estimated_cost_usd` | DBU estimate for the compute tier |\n",
            "| `cluster_id` | Which cluster ran the job |\n",
            "| `stage_id`, `num_tasks` | Most recent completed stage metrics |\n",
            "| `input_bytes`, `shuffle_*_bytes` | Data volumes for the stage |\n",
            "| `max_task_duration_ms` | Longest task — proxy for skew |\n",
            "| `gc_time_ms` | Garbage collection time |\n",
            "\n",
            "Query across all 200 notebooks from one place:\n",
            "```sql\n",
            "SELECT job_name, duration_ms, estimated_cost_usd, max_task_duration_ms,\n",
            "       run_timestamp\n",
            "FROM _spark_query_analyzer.query_history\n",
            "WHERE job_name IN ('daily_revenue', 'sales_ingest', 'dim_customer')\n",
            "ORDER BY run_timestamp DESC\n",
            "LIMIT 100\n",
            "```\n"
        ]
    }
]

cells[insert_after + 1:insert_after + 1] = new_cells
nb["cells"] = cells

with open(NOTEBOOK, "w") as f:
    json.dump(nb, f, indent=1)

# Validate
with open(NOTEBOOK) as f:
    nb2 = json.load(f)
print(f"✅  {len(nb2['cells'])} cells after insert")
for i, c in enumerate(nb2["cells"]):
    src = "".join(c.get("source", ""))[:70].replace("\n", " ")
    print(f"  {i}  [{c['cell_type']}]: {src}")
