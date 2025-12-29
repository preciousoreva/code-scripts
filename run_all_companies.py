import subprocess
import sys
from company_config import get_available_companies

def run():
    all_companies = get_available_companies()

    # Filter out example/template companies
    companies = [
        c for c in all_companies
        if not c.endswith("_example")
    ]

    if not companies:
        print("No runnable companies found. Exiting.")
        sys.exit(1)

    for company in companies:
        print(f"\n=== Running pipeline for {company} (yesterday) ===")

        result = subprocess.run(
            [
                sys.executable,
                "run_pipeline.py",
                "--company",
                company
            ]
        )

        if result.returncode != 0:
            print(f"[ERROR] Pipeline failed for {company}. Stopping.")
            sys.exit(result.returncode)

    print("\nAll company pipelines completed successfully ✅")

if __name__ == "__main__":
    run()