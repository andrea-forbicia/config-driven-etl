"""Write one dataset's run as Markdown to the GitHub job summary.

Usage: run_summary.py <log file> <dataset file>. The log holds the JSON
lines a command writes to stderr; GITHUB_STEP_SUMMARY names the file to
append to.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any


def read_events(log: Path) -> list[dict[str, Any]]:
    if not log.exists():
        return []
    events = []
    for line in log.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def summary(events: list[dict[str, Any]], dataset: str) -> list[str]:
    finished = next(
        (e for e in reversed(events) if e.get("event") == "run_finished"),
        None,
    )
    if finished is None:
        # Install or authentication failed before the command started.
        return [f"### {dataset}", "", "❌ The run did not start."]
    icon = "✅" if finished["outcome"] == "success" else "❌"
    lines = [
        f"### {finished['dataset'] or dataset}",
        "",
        f"{icon} **{finished['outcome']}**, exit {finished['exit_code']}, "
        f"{finished['duration_s']}s, run `{finished['run_id']}`",
    ]
    failed = next((e for e in events if e.get("event") == "run_failed"), None)
    if failed is not None:
        lines += [
            "",
            f"Failed in {finished['failed_stage'] or 'setup'} "
            f"(`{finished['category']}`):",
            "",
            "```",
            failed["error"],
            "```",
        ]
    stages = finished["stages"]
    if stages:
        lines += [
            "",
            "| Stage | In | Out | Rejected | Seconds |",
            "|---|---:|---:|---:|---:|",
        ]
        for stage in stages:
            counts = [
                "" if stage[key] is None else str(stage[key])
                for key in ["rows_in", "rows_out", "rows_rejected"]
            ]
            lines.append(
                f"| {stage['stage']} | {' | '.join(counts)} "
                f"| {stage['duration_s']} |"
            )
    for stage in stages:
        if stage["rejections"]:
            reasons = ", ".join(
                f"{r['reason']} ({r['rows']})" for r in stage["rejections"]
            )
            lines += ["", f"Rejected: {reasons}"]
        if stage["dropped_columns"]:
            columns = ", ".join(f"`{c}`" for c in stage["dropped_columns"])
            lines += ["", f"Dropped columns: {columns}"]
        if stage["partitions"]:
            days = stage["partitions"]
            lines += [
                "",
                f"Partitions replaced: {len(days)}, {days[0]}..{days[-1]}",
            ]
    return lines


def main() -> None:
    log, dataset = Path(sys.argv[1]), sys.argv[2]
    lines = summary(read_events(log), dataset)
    target = Path(os.environ["GITHUB_STEP_SUMMARY"])
    with target.open("a", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
