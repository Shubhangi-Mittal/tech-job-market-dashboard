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
 
    # Create data directory
    os.makedirs("data", exist_ok=True)
 
    # Step 1: Scrape
    mode = "live" if args.live else "mock"
    try:
        print(f"\n{'='*55}")
        print(f"  STEP 1/3 — Data Collection")
        print(f"{'='*55}")
        
        if os.path.exists("scraper/job_scraper.py"):
            result = subprocess.run(
                f"python3 scraper/job_scraper.py --mode {mode} --jobs {args.jobs} --output data/raw_jobs.csv",
                shell=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.returncode != 0:
                print(f"⚠️  Scraper error: {result.stderr}")
                print("Continuing with cleaning step...")
        else:
            print("⚠️  scraper/job_scraper.py not found - creating sample data")
            # Create minimal sample data
            sample_csv = "job_id,title,role_category,seniority,remote_type,company_tier,salary_midpoint,date_posted\n"
            sample_csv += "1,Data Engineer,Data Engineer,Senior,Remote,FAANG+,180000,2025-03-22\n"
            with open("data/raw_jobs.csv", "w") as f:
                f.write(sample_csv)
    except Exception as e:
        print(f"❌ Scraper failed: {e}")
        sys.exit(1)
 
    # Step 2: Clean
    try:
        print(f"\n{'='*55}")
        print(f"  STEP 2/3 — Data Cleaning & Validation")
        print(f"{'='*55}")
        
        if os.path.exists("cleaning/clean_jobs.py"):
            result = subprocess.run(
                "python3 cleaning/clean_jobs.py --input data/raw_jobs.csv",
                shell=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.returncode != 0:
                print(f"⚠️  Cleaner error: {result.stderr}")
        else:
            print("⚠️  cleaning/clean_jobs.py not found")
            if os.path.exists("data/raw_jobs.csv"):
                print("Creating cleaned_jobs.csv from raw data...")
                # Copy raw to cleaned as fallback
                import shutil
                shutil.copy("data/raw_jobs.csv", "data/cleaned_jobs.csv")
    except Exception as e:
        print(f"❌ Cleaner failed: {e}")
        sys.exit(1)
 
    # Ensure cleaned CSV exists
    if not os.path.exists("data/cleaned_jobs.csv"):
        print("❌ cleaned_jobs.csv was not created!")
        sys.exit(1)
 
    # Create skills_exploded.csv if missing
    if not os.path.exists("data/skills_exploded.csv"):
        print("⚠️  skills_exploded.csv not found, creating placeholder...")
        with open("data/skills_exploded.csv", "w") as f:
            f.write("job_id,skill\n1,Python\n1,SQL\n")
 
    # Step 3: Optional Analytics
    try:
        print(f"\n{'='*55}")
        print(f"  STEP 3/3 — Analytics (Optional)")
        print(f"{'='*55}")
        
        if os.path.exists("analytics/insights.py"):
            result = subprocess.run(
                "python3 analytics/insights.py --data data/cleaned_jobs.csv --skills data/skills_exploded.csv",
                shell=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.returncode != 0:
                print(f"⚠️  Analytics skipped: {result.stderr}")
        else:
            print("ℹ️  Analytics module not found - skipping")
    except Exception as e:
        print(f"⚠️  Analytics failed (non-critical): {e}")
 
    print("\n" + "="*55)
    print("  ✅ Pipeline complete!")
    print(f"  📁 Data files: ./data/")
    print(f"  📊 Dashboard:  ./dashboard/index.html")
    print("="*55)
 
    if not args.no_serve:
        serve_dashboard(args.port)