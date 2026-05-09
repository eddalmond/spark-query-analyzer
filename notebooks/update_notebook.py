#!/usr/bin/env python3
"""
Convert QueryPerformanceAnalyzer.ipynb to QueryPerformanceAnalyzer.py
(a valid Databricks notebook .py source file).

Databricks .py format uses:
  # COMMAND ----------
  as cell separators, with code/magic cells written as plain Python/SQL.

Markdown cells are rendered as # comment blocks.
The result can be imported into Databricks or used as a standalone reference.
"""
import json, re, sys

NOTEBOOK = "notebooks/QueryPerformanceAnalyzer.ipynb"
OUT = "notebooks/QueryPerformanceAnalyzer.py"


def notebook_to_py(nb_path: str, out_path: str) -> None:
    with open(nb_path) as f:
        nb = json.load(f)

    lines = []
    lines.append("# Databricks notebook source")
    lines.append("# ──────────────────────────────────────────────────────────────────────────────")
    lines.append(f"# Source: {NOTEBOOK}")
    lines.append("# ──────────────────────────────────────────────────────────────────────────────")
    lines.append("#")
    lines.append("# This file is generated — edit QueryPerformanceAnalyzer.ipynb and regenerate.")
    lines.append("")

    for cell in nb["cells"]:
        src = cell.get("source", [])
        if isinstance(src, str):
            src = [src]
        content = "".join(src).rstrip("\n")

        if not content.strip():
            continue

        lines.append("# COMMAND ----------")
        lines.append("")

        if cell.get("cell_type") == "markdown":
            # Render markdown as a comment block.
            # Headings (# ## ###) become ## ## ## in comment form.
            # Blank lines separate paragraphs.
            in_code_block = False
            code_lines = []
            for raw in content.split("\n"):
                # fenced code blocks: pass through as # ```lang
                if raw.strip().startswith("```"):
                    in_code_block = not in_code_block
                    lines.append(f"# {raw.rstrip()}")
                    continue
                if in_code_block:
                    lines.append(f"# {raw.rstrip()}")
                    continue

                # Inline code (`code`) → # `code`
                rendered = raw
                if not raw.startswith("#") and not raw.startswith("|"):
                    rendered = f"# {raw}"
                elif raw.startswith("|"):
                    rendered = f"# {raw}"
                lines.append(rendered)
        else:
            # Code / magic cell — write as-is
            lines.append(content)

        lines.append("")

    with open(out_path, "w") as f:
        f.write("\n".join(lines))

    print(f"✅  Written → {out_path}")
    print(f"     {len(lines)} lines")


if __name__ == "__main__":
    notebook_to_py(NOTEBOOK, OUT)
