import argparse
import subprocess
import sys
from typing import Optional
from company_config import get_available_companies


def run(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run run_pipeline.py for all configured companies (sequentially)."
    )
    parser.add_argument(
        "--target-date",
        help="Target business date in YYYY-MM-DD format (for all companies). If omitted, each company run defaults to yesterday.",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for range mode in YYYY-MM-DD format (must be used with --to-date).",
    )
    parser.add_argument(
        "--to-date",
        help="End date for range mode in YYYY-MM-DD format (must be used with --from-date).",
    )
    parser.add_argument(
        "--companies",
        nargs="*",
        help="Optional subset of companies to run (space-separated). Defaults to all configured companies.",
    )
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue running remaining companies even if one fails. Default is to stop on first failure.",
    )

    args = parser.parse_args(argv)

    # Validation: --from-date and --to-date must be provided together
    if (args.from_date is None) != (args.to_date is None):
        parser.error("--from-date and --to-date must be provided together")

    all_companies = [c for c in get_available_companies() if not c.endswith("_example")]
    if not all_companies:
        print("No runnable companies found. Exiting.")
        return 1

    # Optional subset filtering
    if args.companies:
        requested = set(args.companies)
        companies = [c for c in all_companies if c in requested]
        missing = sorted(list(requested - set(companies)))
        if missing:
            print(f"[WARN] Ignoring unknown companies: {', '.join(missing)}")
    else:
        companies = all_companies

    if not companies:
        print("No runnable companies selected. Exiting.")
        return 1

    # Build common date args to forward to run_pipeline.py
    forwarded_date_args: list = []
    if args.from_date and args.to_date:
        forwarded_date_args.extend(["--from-date", args.from_date, "--to-date", args.to_date])
    elif args.target_date:
        forwarded_date_args.extend(["--target-date", args.target_date])

    failures: list = []

    for company in companies:
        if args.from_date and args.to_date:
            print(f"\n=== Running pipeline for {company} (range {args.from_date} to {args.to_date}) ===")
        elif args.target_date:
            print(f"\n=== Running pipeline for {company} (target-date {args.target_date}) ===")
        else:
            print(f"\n=== Running pipeline for {company} (yesterday) ===")

        cmd = [sys.executable, "run_pipeline.py", "--company", company] + forwarded_date_args
        result = subprocess.run(cmd)

        if result.returncode != 0:
            msg = f"Pipeline failed for {company} (exit code {result.returncode})."
            print(f"[ERROR] {msg}")
            failures.append(company)
            if not args.continue_on_failure:
                return result.returncode

    if failures:
        print(f"\nCompleted with failures: {', '.join(failures)}")
        return 1

    print("\nAll company pipelines completed successfully ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
