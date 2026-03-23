#!/usr/bin/env python3
"""
run_pipeline.py
━━━━━━━━━━━━━━━
One command to run the entire pipeline:
  1. Scrape / generate job data
  2. Clean & validate
  3. Generate analytics insights
  4. Serve the dashboard

Usage:
    python run_pipeline.py               # mock data, open dashboard
    python run_pipeline.py --live        # try live scraping
    python run_pipeline.py --jobs 5000   # more mock data
    python run_pipeline.py --no-serve    # skip dashboard
"""

import argparse
import subprocess
import sys
import os
import shutil
import http.server
import threading
import webbrowser
from pathlib import Path

def run(cmd, label):
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"\n❌ Step failed: {label}")
        sys.exit(1)
    return result

def serve_dashboard(port=8080):
    """Copy data files to dashboard dir and serve."""
    # Use relative paths instead of absolute paths
    dash_dir = Path("dashboard")
    data_dir = Path("data")

    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Copy CSVs into dashboard/data/ so the HTML can fetch them
    dash_data = dash_dir / "data"
    dash_data.mkdir(exist_ok=True)

    for f in ["cleaned_jobs.csv", "skills_exploded.csv"]:
        src = data_dir / f
        if src.exists():
            shutil.copy(src, dash_data / f)
            print(f"  Copied {f} → dashboard/data/")

    print(f"\n🚀 Dashboard running at: http://localhost:{port}")
    print("   (Press Ctrl+C to stop)\n")

    os.chdir("dashboard")
    handler = http.server.SimpleHTTPRequestHandler

    # Suppress request logs
    class QuietHandler(handler):
        def log_message(self, *args): pass

    server = http.server.HTTPServer(("", port), QuietHandler)
    threading.Timer(1.2, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Use live scraping instead of mock")
    parser.add_argument("--jobs", type=int, default=2500, help="Number of mock jobs")
    parser.add_argument("--no-serve", action="store_true", help="Skip dashboard serving")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    print("\n🌐 Tech Job Market Intelligence Pipeline")
    print("   Building end-to-end analytics project...\n")

    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Step 1: Scrape
    # Use relative paths - these work everywhere (local and GitHub Actions)
    mode = "live" if args.live else "mock"
    run(
        f"python3 scraper/job_scraper.py --mode {mode} --jobs {args.jobs} --output data/raw_jobs.csv",
        "STEP 1/3 — Data Collection"
    )

    # Step 2: Clean
    # Use relative paths
    run(
        "python3 cleaning/clean_jobs.py --input data/raw_jobs.csv",
        "STEP 2/3 — Data Cleaning & Validation"
    )

    # Step 3: Analyze
    # Use relative paths
    run(
        "python3 analytics/insights.py --data data/cleaned_jobs.csv --skills data/skills_exploded.csv",
        "STEP 3/3 — Generating Market Insights"
    )

    print("\n" + "="*55)
    print("  ✅ Pipeline complete!")
    print(f"  📁 Data files: ./data/")
    print(f"  📊 Dashboard:  ./dashboard/index.html")
    print("="*55)

    if not args.no_serve:
        serve_dashboard(args.port)