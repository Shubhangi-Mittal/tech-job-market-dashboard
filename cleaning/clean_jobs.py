"""
cleaning/clean_jobs.py
━━━━━━━━━━━━━━━━━━━━━━
Cleans raw job postings into an analysis-ready dataset.

Cleaning decisions are documented inline — this is intentional.
In real data work, every cleaning step needs a business justification.

Outputs:
  - data/cleaned_jobs.csv      → main analysis dataset
  - data/cleaning_report.json  → audit log of every transformation
  - data/skills_exploded.csv   → one row per (job, skill) for skill analysis

Usage:
    python cleaning/clean_jobs.py --input data/raw_jobs.csv
"""

import json
import re
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

# ── Skill normalization map ───────────────────────────────────────────────────
# Job postings use inconsistent names for the same technology.
# This map standardizes them for accurate counting.
SKILL_ALIASES = {
    # Python ecosystem
    "py": "Python", "python3": "Python", "python 3": "Python",
    # SQL variants
    "mysql": "SQL", "postgresql": "SQL", "postgres": "SQL",
    "sqlite": "SQL", "t-sql": "SQL", "pl/sql": "SQL",
    # Cloud
    "amazon web services": "AWS", "gcp": "GCP", "google cloud": "GCP",
    "google cloud platform": "GCP", "azure": "Azure",
    "microsoft azure": "Azure",
    # Big data
    "apache spark": "Spark", "pyspark": "Spark",
    "apache kafka": "Kafka",
    "apache airflow": "Airflow",
    # Viz
    "tableau desktop": "Tableau", "tableau server": "Tableau",
    "powerbi": "Power BI", "power bi desktop": "Power BI",
    "looker studio": "Looker",
    # ML
    "pytorch": "PyTorch", "torch": "PyTorch",
    "tensorflow 2": "TensorFlow", "tf": "TensorFlow",
    "scikit learn": "scikit-learn", "sklearn": "scikit-learn",
    # LLM
    "langchain": "LangChain", "openai": "OpenAI API",
    "llm": "LLMs", "large language model": "LLMs",
    "rag": "RAG", "retrieval augmented generation": "RAG",
    # Containers
    "kubernetes": "Kubernetes", "k8s": "Kubernetes",
    # Version control
    "github": "Git", "gitlab": "Git", "bitbucket": "Git",
}

# Skills to always exclude (too generic to be meaningful)
SKILL_BLOCKLIST = {
    "communication", "teamwork", "problem solving", "agile", "scrum",
    "jira", "confluence", "slack", "zoom", "microsoft office",
    "powerpoint", "word", "windows", "linux", "macos",
}

SALARY_OUTLIER_BOUNDS = (30_000, 600_000)  # annual USD
MIN_SKILLS_REQUIRED = 1


class JobDataCleaner:
    def __init__(self, input_path: str, output_dir: str = "data"):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.report = {
            "run_at": datetime.now().isoformat(),
            "input_file": str(input_path),
            "steps": [],
        }

    def _log(self, step: str, before: int, after: int, detail: str = ""):
        removed = before - after
        pct = removed / before * 100 if before > 0 else 0
        entry = {
            "step": step,
            "rows_before": before,
            "rows_after": after,
            "rows_removed": removed,
            "pct_removed": round(pct, 2),
            "detail": detail,
        }
        self.report["steps"].append(entry)
        print(f"  [{step}] {before:,} → {after:,} rows  (removed {removed:,} / {pct:.1f}%)")
        if detail:
            print(f"    ↳ {detail}")

    # ── Step 1: Load ───────────────────────────────────────────────────────────
    def load(self) -> pd.DataFrame:
        print("\n📂 Loading raw data...")
        df = pd.read_csv(self.input_path, low_memory=False)
        print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")
        self.report["raw_shape"] = {"rows": len(df), "cols": len(df.columns)}
        return df

    # ── Step 2: Deduplication ─────────────────────────────────────────────────
    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n🔁 Deduplicating...")
        before = len(df)

        # Primary dedup: exact job_id
        df = df.drop_duplicates(subset=["job_id"], keep="first")

        # Secondary dedup: same title + company + location + date (different scrape IDs)
        subset = ["title", "company", "location", "date_posted"]
        subset = [c for c in subset if c in df.columns]
        df = df.drop_duplicates(subset=subset, keep="first")

        self._log("deduplication", before, len(df), "Removed exact ID dupes + title/company/location/date combos")
        return df

    # ── Step 3: Column standardization ────────────────────────────────────────
    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n🏷️  Standardizing columns...")

        # Strip whitespace from all string columns
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": None, "None": None, "": None})

        # Standardize remote type
        if "remote_type" in df.columns:
            mapping = {
                "remote": "Remote", "fully remote": "Remote", "wfh": "Remote",
                "hybrid": "Hybrid", "partial remote": "Hybrid",
                "on-site": "On-site", "onsite": "On-site", "in office": "On-site",
                "in-person": "On-site",
            }
            df["remote_type"] = (
                df["remote_type"].str.lower().map(mapping).fillna(df["remote_type"])
            )

        # Parse date
        if "date_posted" in df.columns:
            df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
            df["year_month"] = df["date_posted"].dt.to_period("M").astype(str)
            df["week"] = df["date_posted"].dt.isocalendar().week.astype(int)

        print(f"  ✓ Standardized {len(str_cols)} string columns, parsed dates")
        return df

    # ── Step 4: Salary cleaning ────────────────────────────────────────────────
    def clean_salary(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n💰 Cleaning salary data...")
        before_nulls = df["salary_midpoint"].isna().sum() if "salary_midpoint" in df.columns else 0

        if "salary_midpoint" in df.columns:
            df["salary_midpoint"] = pd.to_numeric(df["salary_midpoint"], errors="coerce")

            # Handle hourly → annual conversion (if salary < $300, assume hourly rate)
            hourly_mask = df["salary_midpoint"] < 300
            df.loc[hourly_mask, "salary_midpoint"] *= 2080  # 40 hrs × 52 weeks

            # Cap outliers
            low, high = SALARY_OUTLIER_BOUNDS
            outliers_before = ((df["salary_midpoint"] < low) | (df["salary_midpoint"] > high)).sum()
            df.loc[df["salary_midpoint"] < low, "salary_midpoint"] = None
            df.loc[df["salary_midpoint"] > high, "salary_midpoint"] = None

            # Create salary bands for categorical analysis
            bins = [0, 80_000, 110_000, 140_000, 170_000, 210_000, 280_000, 999_999]
            labels = ["<$80k", "$80-110k", "$110-140k", "$140-170k", "$170-210k", "$210-280k", "$280k+"]
            df["salary_band"] = pd.cut(df["salary_midpoint"], bins=bins, labels=labels)

        print(f"  ✓ {hourly_mask.sum()} hourly rates converted to annual")
        print(f"  ✓ {outliers_before} salary outliers nulled out")
        print(f"  ✓ Salary bands created")
        return df

    # ── Step 5: Skills parsing ────────────────────────────────────────────────
    def parse_skills(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n🔧 Parsing and normalizing skills...")

        if "skills" not in df.columns:
            return df

        def normalize_skill(s: str) -> str:
            s = s.strip().lower()
            return SKILL_ALIASES.get(s, s.title())

        def clean_skills_list(raw: str) -> list[str]:
            if not raw or raw == "None":
                return []
            parts = re.split(r"[|,;/]", str(raw))
            cleaned = []
            for p in parts:
                p = p.strip().lower()
                if not p or p in SKILL_BLOCKLIST:
                    continue
                normalized = normalize_skill(p)
                if normalized and len(normalized) >= 2:
                    cleaned.append(normalized)
            return list(set(cleaned))

        df["skills_list"] = df["skills"].apply(clean_skills_list)
        df["num_skills"] = df["skills_list"].apply(len)
        df["skills_clean"] = df["skills_list"].apply(lambda x: "|".join(sorted(x)))

        # Flag if Python appears
        df["requires_python"] = df["skills_list"].apply(lambda x: "Python" in x)
        df["requires_sql"] = df["skills_list"].apply(lambda x: "SQL" in x)
        df["requires_cloud"] = df["skills_list"].apply(
            lambda x: any(s in x for s in ["AWS", "GCP", "Azure"])
        )
        df["requires_llm"] = df["skills_list"].apply(
            lambda x: any(s in x for s in ["LangChain", "RAG", "OpenAI API", "LLMs", "PyTorch"])
        )

        low_skill = (df["num_skills"] < MIN_SKILLS_REQUIRED).sum()
        print(f"  ✓ Skills normalized, {low_skill} postings with <{MIN_SKILLS_REQUIRED} skills flagged")
        return df

    # ── Step 6: Remove invalid rows ────────────────────────────────────────────
    def remove_invalid(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n🗑️  Removing invalid rows...")
        before = len(df)

        # Remove rows missing critical fields
        df = df.dropna(subset=["title", "company", "role_category"])

        # Remove postings older than 6 months
        if "date_posted" in df.columns:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=180)
            df = df[df["date_posted"] >= cutoff]

        # Remove jobs with 0 skills listed (likely parsing failures in live mode)
        if "num_skills" in df.columns:
            df = df[df["num_skills"] >= MIN_SKILLS_REQUIRED]

        self._log("remove_invalid", before, len(df),
                  "Removed nulls in critical fields + postings >180 days old")
        return df

    # ── Step 7: Feature engineering ───────────────────────────────────────────
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n⚙️  Engineering features...")

        # Seniority normalization
        if "seniority" in df.columns:
            df["seniority_rank"] = df["seniority"].map({
                "Entry Level": 1, "Mid Level": 2, "Senior": 3,
                "Staff/Lead": 4, "Principal": 5,
            }).fillna(2)

        # Salary competitiveness: pct above median for the role
        if "salary_midpoint" in df.columns and "role_category" in df.columns:
            role_medians = df.groupby("role_category")["salary_midpoint"].transform("median")
            df["salary_vs_median_pct"] = (
                (df["salary_midpoint"] - role_medians) / role_medians * 100
            ).round(1)

        # Is "hot" role (based on growth trend)
        hot_trends = {"explosive", "very_high", "high"}
        if "growth_trend" in df.columns:
            df["is_high_growth_role"] = df["growth_trend"].isin(hot_trends)

        # Application urgency proxy: postings < 7 days old
        if "days_since_posted" in df.columns:
            df["is_fresh_posting"] = df["days_since_posted"].astype(float) <= 7

        # Hiring tier label
        tier_map = {
            "FAANG+": "Elite", "Scale-up": "High Growth",
            "Enterprise": "Stable", "Mid-market": "Stable",
            "Startup": "Early Stage",
        }
        if "company_tier" in df.columns:
            df["hiring_tier"] = df["company_tier"].map(tier_map).fillna("Other")

        print(f"  ✓ Added seniority_rank, salary_vs_median_pct, is_high_growth_role, hiring_tier")
        return df

    # ── Step 8: Build skills exploded table ───────────────────────────────────
    def build_skills_table(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\n🔬 Building skills exploded table...")

        records = []
        for _, row in df.iterrows():
            skills = row.get("skills_list", [])
            if isinstance(skills, list):
                for skill in skills:
                    records.append({
                        "job_id": row["job_id"],
                        "skill": skill,
                        "role_category": row.get("role_category"),
                        "seniority": row.get("seniority"),
                        "salary_midpoint": row.get("salary_midpoint"),
                        "remote_type": row.get("remote_type"),
                        "company_tier": row.get("company_tier"),
                        "date_posted": row.get("date_posted"),
                        "is_high_growth_role": row.get("is_high_growth_role"),
                    })

        skills_df = pd.DataFrame(records)
        path = self.output_dir / "skills_exploded.csv"
        skills_df.to_csv(path, index=False)
        print(f"  ✓ Saved {len(skills_df):,} skill-job pairs → {path}")
        return skills_df

    # ── Main run ───────────────────────────────────────────────────────────────
    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        print("=" * 55)
        print("  JOB MARKET DATA CLEANING PIPELINE")
        print("=" * 55)

        df = self.load()
        df = self.deduplicate(df)
        df = self.standardize_columns(df)
        df = self.clean_salary(df)
        df = self.parse_skills(df)
        df = self.remove_invalid(df)
        df = self.engineer_features(df)
        skills_df = self.build_skills_table(df)

        # Drop intermediate columns not needed for analysis
        drop_cols = ["skills", "description_snippet", "scraped_at", "benefits"]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        # Save
        out_path = self.output_dir / "cleaned_jobs.csv"
        df.to_csv(out_path, index=False)
        print(f"\n✅ Cleaned dataset saved → {out_path}")
        print(f"   Final shape: {len(df):,} rows × {len(df.columns)} columns")

        # Save report
        self.report["final_shape"] = {"rows": len(df), "cols": len(df.columns)}
        self.report["null_summary"] = df.isnull().sum()[df.isnull().sum() > 0].to_dict()
        report_path = self.output_dir / "cleaning_report.json"
        with open(report_path, "w") as f:
            json.dump(self.report, f, indent=2, default=str)
        print(f"   Cleaning report → {report_path}")
        print("=" * 55)

        return df, skills_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw_jobs.csv")
    args = parser.parse_args()

    cleaner = JobDataCleaner(input_path=args.input)
    df, skills_df = cleaner.run()
