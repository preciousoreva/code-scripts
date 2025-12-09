import subprocess
import sys

def run(cmd):
    print(">>", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    # 1. Run Playwright automation
    run([sys.executable, "playwright_automation.py"])

    # 2. Run pipeline to transform latest CSV
    run([sys.executable, "run_pipeline.py"])

if __name__ == "__main__":
    main()