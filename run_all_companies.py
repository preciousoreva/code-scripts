import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple
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
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip EPOS download and use existing split files in uploads/range_raw/ (range mode only).",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of companies to run in parallel (default: 1 = sequential).",
    )
    parser.add_argument(
        "--stagger-seconds",
        type=int,
        default=2,
        help="Seconds to wait between starting parallel jobs (default: 2).",
    )

    args = parser.parse_args(argv)

    # Validation: --from-date and --to-date must be provided together
    if (args.from_date is None) != (args.to_date is None):
        parser.error("--from-date and --to-date must be provided together")
    
    # Validation: --skip-download only works in range mode
    if args.skip_download and (args.from_date is None or args.to_date is None):
        parser.error("--skip-download can only be used with --from-date and --to-date (range mode)")
    if args.parallel < 1:
        parser.error("--parallel must be >= 1")
    if args.stagger_seconds < 0:
        parser.error("--stagger-seconds must be >= 0")

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
    
    # Forward --skip-download if provided
    if args.skip_download:
        forwarded_date_args.append("--skip-download")

    def _run_company(company_key: str) -> Tuple[str, int]:
        if args.from_date and args.to_date:
            print(f"\n=== Running pipeline for {company_key} (range {args.from_date} to {args.to_date}) ===")
        elif args.target_date:
            print(f"\n=== Running pipeline for {company_key} (target-date {args.target_date}) ===")
        else:
            print(f"\n=== Running pipeline for {company_key} (yesterday) ===")

        cmd = [sys.executable, "run_pipeline.py", "--company", company_key] + forwarded_date_args
        result = subprocess.run(cmd)
        return (company_key, result.returncode)

    failures: list = []
    if args.parallel == 1:
        for company in companies:
            company_key, rc = _run_company(company)
            if rc != 0:
                msg = f"Pipeline failed for {company_key} (exit code {rc})."
                print(f"[ERROR] {msg}")
                failures.append(company_key)
                if not args.continue_on_failure:
                    return rc
    else:
        # Small stagger to reduce simultaneous API bursts
        stagger_seconds = args.stagger_seconds
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = []
            for idx, company in enumerate(companies):
                if idx > 0:
                    time.sleep(stagger_seconds)
                futures.append(executor.submit(_run_company, company))
            for fut in as_completed(futures):
                company_key, rc = fut.result()
                if rc != 0:
                    msg = f"Pipeline failed for {company_key} (exit code {rc})."
                    print(f"[ERROR] {msg}")
                    failures.append(company_key)

    if failures:
        print(f"\nCompleted with failures: {', '.join(failures)}")
        return 1

    print("\nAll company pipelines completed successfully ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
