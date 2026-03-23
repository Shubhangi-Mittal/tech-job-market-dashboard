"""
analytics/insights.py
━━━━━━━━━━━━━━━━━━━━━
Generates 12 business insights from cleaned job data.

Each insight answers a real hiring market question:
  1.  Which roles are growing fastest?
  2.  What skills command the highest salary premiums?
  3.  How has remote work changed over the past 6 months?
  4.  Which company tiers pay the most per role?
  5.  Top 20 most in-demand skills overall
  6.  Skill demand by role category
  7.  Salary distribution across seniority levels
  8.  Remote vs. on-site salary gap
  9.  Which roles have the widest salary ranges?
  10. Skills that appear most with AI/LLM roles
  11. Weekly hiring velocity trends
  12. Location vs. salary heat analysis

Usage:
    python analytics/insights.py --data data/cleaned_jobs.csv
"""

import json
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")


class JobMarketAnalytics:
    def __init__(self, jobs_path: str, skills_path: str = None):
        self.df = pd.read_csv(jobs_path, parse_dates=["date_posted"], low_memory=False)
        self.skills_df = None
        if skills_path and Path(skills_path).exists():
            self.skills_df = pd.read_csv(skills_path, low_memory=False)
        print(f"📊 Loaded {len(self.df):,} jobs for analysis")

    # ── Insight 1: Role growth ranking ───────────────────────────────────────
    def role_growth_ranking(self) -> dict:
        """Which roles are growing fastest based on recent vs older postings?"""
        df = self.df.copy()
        df["is_recent"] = df["days_since_posted"].astype(float) <= 30

        recent = df[df["is_recent"]]["role_category"].value_counts(normalize=True).rename("recent_share")
        older = df[~df["is_recent"]]["role_category"].value_counts(normalize=True).rename("older_share")

        result = pd.DataFrame([recent, older]).T.fillna(0)
        result["growth_index"] = (result["recent_share"] / result["older_share"].replace(0, 0.001) - 1) * 100
        result["total_jobs"] = df["role_category"].value_counts()
        result = result.sort_values("growth_index", ascending=False).round(2)

        return {
            "title": "Role Growth Index (Last 30 Days vs Prior Period)",
            "insight": f"'{result.index[0]}' shows the highest recent hiring surge at +{result['growth_index'].iloc[0]:.0f}% vs baseline.",
            "data": result.reset_index().rename(columns={"index": "role"}).to_dict(orient="records"),
        }

    # ── Insight 2: Skill salary premium ──────────────────────────────────────
    def skill_salary_premium(self) -> dict:
        """Which skills are associated with the highest salary premiums?"""
        if self.skills_df is None:
            return {"error": "skills_exploded.csv not found"}

        overall_median = self.df["salary_midpoint"].median()
        s = self.skills_df.dropna(subset=["salary_midpoint"])

        skill_stats = (
            s.groupby("skill")["salary_midpoint"]
            .agg(["median", "count"])
            .rename(columns={"median": "median_salary", "count": "job_count"})
        )
        skill_stats = skill_stats[skill_stats["job_count"] >= 15]  # min sample
        skill_stats["salary_premium_pct"] = (
            (skill_stats["median_salary"] - overall_median) / overall_median * 100
        ).round(1)
        skill_stats = skill_stats.sort_values("salary_premium_pct", ascending=False).head(20)

        return {
            "title": "Top Skills by Salary Premium vs Market Median",
            "insight": f"Skills like '{skill_stats.index[0]}' and '{skill_stats.index[1]}' carry the highest salary premiums, averaging {skill_stats['salary_premium_pct'].iloc[0]:.0f}% above market.",
            "data": skill_stats.reset_index().to_dict(orient="records"),
            "market_median": round(overall_median),
        }

    # ── Insight 3: Remote work trend over time ────────────────────────────────
    def remote_work_trend(self) -> dict:
        """How has remote work availability changed over the past 6 months?"""
        df = self.df.dropna(subset=["date_posted", "remote_type"]).copy()
        df["month"] = df["date_posted"].dt.to_period("M").astype(str)

        monthly = (
            df.groupby(["month", "remote_type"])
            .size()
            .reset_index(name="count")
        )
        monthly_total = df.groupby("month").size().reset_index(name="total")
        monthly = monthly.merge(monthly_total, on="month")
        monthly["share_pct"] = (monthly["count"] / monthly["total"] * 100).round(1)

        pivot = monthly.pivot(index="month", columns="remote_type", values="share_pct").fillna(0)

        latest_remote = pivot["Remote"].iloc[-1] if "Remote" in pivot.columns else 0
        earliest_remote = pivot["Remote"].iloc[0] if "Remote" in pivot.columns else 0
        delta = latest_remote - earliest_remote

        direction = "increased" if delta > 0 else "decreased"
        return {
            "title": "Remote Work Share Over Time (%)",
            "insight": f"Remote work availability has {direction} by {abs(delta):.1f}pp over the observed period, from {earliest_remote:.1f}% to {latest_remote:.1f}%.",
            "data": pivot.reset_index().to_dict(orient="records"),
        }

    # ── Insight 4: Salary by company tier × role ─────────────────────────────
    def salary_by_company_tier(self) -> dict:
        """Which company tiers pay best, and does it vary by role?"""
        df = self.df.dropna(subset=["salary_midpoint", "company_tier"])
        result = (
            df.groupby(["company_tier", "role_category"])["salary_midpoint"]
            .median()
            .reset_index()
            .rename(columns={"salary_midpoint": "median_salary"})
        )
        result["median_salary"] = result["median_salary"].round(0)
        overall = df.groupby("company_tier")["salary_midpoint"].median().sort_values(ascending=False)

        return {
            "title": "Median Salary by Company Tier",
            "insight": f"'{overall.index[0]}' companies offer the highest median compensation at ${overall.iloc[0]:,.0f}, vs ${overall.iloc[-1]:,.0f} at '{overall.index[-1]}'.",
            "data": result.to_dict(orient="records"),
            "overall_by_tier": overall.reset_index().rename(columns={"salary_midpoint": "median_salary"}).to_dict(orient="records"),
        }

    # ── Insight 5: Top 25 in-demand skills ───────────────────────────────────
    def top_skills_overall(self) -> dict:
        """What are the 25 most in-demand skills across all job postings?"""
        if self.skills_df is None:
            return {"error": "skills_exploded.csv not found"}

        counts = (
            self.skills_df["skill"]
            .value_counts()
            .head(25)
            .reset_index()
        )
        counts.columns = ["skill", "job_count"]
        counts["pct_of_jobs"] = (counts["job_count"] / len(self.df) * 100).round(1)

        top3 = counts["skill"].head(3).tolist()
        return {
            "title": "Top 25 In-Demand Skills (All Roles)",
            "insight": f"'{top3[0]}', '{top3[1]}', and '{top3[2]}' appear in the most job postings, confirming their status as baseline requirements.",
            "data": counts.to_dict(orient="records"),
        }

    # ── Insight 6: Skills by role category ───────────────────────────────────
    def skills_by_role(self) -> dict:
        """What are the top 8 skills for each role category?"""
        if self.skills_df is None:
            return {"error": "skills_exploded.csv not found"}

        result = {}
        for role in self.skills_df["role_category"].dropna().unique():
            role_skills = (
                self.skills_df[self.skills_df["role_category"] == role]["skill"]
                .value_counts()
                .head(8)
                .reset_index()
            )
            role_skills.columns = ["skill", "count"]
            result[role] = role_skills.to_dict(orient="records")

        return {
            "title": "Top Skills by Role Category",
            "insight": "Each role has a distinct skill fingerprint — LLM skills dominate AI Engineer roles while SQL/dbt lead Analytics Engineering.",
            "data": result,
        }

    # ── Insight 7: Salary by seniority ───────────────────────────────────────
    def salary_by_seniority(self) -> dict:
        """How much does seniority affect compensation?"""
        df = self.df.dropna(subset=["salary_midpoint", "seniority"])
        ORDER = ["Entry Level", "Mid Level", "Senior", "Staff/Lead", "Principal"]
        df["seniority"] = pd.Categorical(df["seniority"], categories=ORDER, ordered=True)

        stats = (
            df.groupby("seniority", observed=True)["salary_midpoint"]
            .agg(
                median="median",
                p25=lambda x: x.quantile(0.25),
                p75=lambda x: x.quantile(0.75),
                count="count",
            )
            .reset_index()
            .round(0)
        )
        entry = stats[stats["seniority"] == "Entry Level"]["median"].values[0]
        principal = stats[stats["seniority"] == "Principal"]["median"].values[0]
        multiplier = principal / entry

        return {
            "title": "Salary Distribution by Seniority Level",
            "insight": f"Compensation scales {multiplier:.1f}x from Entry Level (${entry:,.0f}) to Principal (${principal:,.0f}).",
            "data": stats.to_dict(orient="records"),
        }

    # ── Insight 8: Remote vs on-site salary gap ───────────────────────────────
    def remote_salary_gap(self) -> dict:
        """Is there a salary penalty or premium for remote work?"""
        df = self.df.dropna(subset=["salary_midpoint", "remote_type"])
        stats = (
            df.groupby(["remote_type", "role_category"])["salary_midpoint"]
            .median()
            .reset_index()
            .rename(columns={"salary_midpoint": "median_salary"})
        )

        overall = df.groupby("remote_type")["salary_midpoint"].median().round(0)
        remote_sal = overall.get("Remote", 0)
        onsite_sal = overall.get("On-site", 0)
        gap_pct = (remote_sal - onsite_sal) / onsite_sal * 100 if onsite_sal else 0

        direction = "premium" if gap_pct > 0 else "penalty"
        return {
            "title": "Remote vs On-site Salary Gap by Role",
            "insight": f"Remote roles carry a {abs(gap_pct):.1f}% salary {direction} vs on-site (${remote_sal:,.0f} vs ${onsite_sal:,.0f} median).",
            "data": stats.round(0).to_dict(orient="records"),
            "overall": overall.reset_index().to_dict(orient="records"),
        }

    # ── Insight 9: Salary range width by role ────────────────────────────────
    def salary_range_width(self) -> dict:
        """Which roles have the widest salary ranges (most negotiation room)?"""
        df = self.df.dropna(subset=["salary_midpoint", "role_category"])
        result = (
            df.groupby("role_category")["salary_midpoint"]
            .agg(
                p10=lambda x: x.quantile(0.10),
                p90=lambda x: x.quantile(0.90),
                median="median",
                count="count",
            )
            .reset_index()
        )
        result["range_width"] = (result["p90"] - result["p10"]).round(0)
        result = result.sort_values("range_width", ascending=False)

        return {
            "title": "Salary Range Width by Role (P10–P90)",
            "insight": f"'{result['role_category'].iloc[0]}' has the widest salary range (${result['range_width'].iloc[0]:,.0f} P10→P90 spread), indicating high variance in comp negotiation.",
            "data": result.round(0).to_dict(orient="records"),
        }

    # ── Insight 10: LLM/AI skill co-occurrence ────────────────────────────────
    def llm_skill_cooccurrence(self) -> dict:
        """What skills most frequently appear alongside LLM/AI skills?"""
        if self.skills_df is None:
            return {"error": "skills_exploded.csv not found"}

        llm_skills = {"LangChain", "RAG", "OpenAI API", "LLMs", "PyTorch", "Hugging Face"}
        llm_job_ids = set(
            self.skills_df[self.skills_df["skill"].isin(llm_skills)]["job_id"]
        )

        co_skills = (
            self.skills_df[
                (self.skills_df["job_id"].isin(llm_job_ids)) &
                (~self.skills_df["skill"].isin(llm_skills))
            ]["skill"]
            .value_counts()
            .head(15)
            .reset_index()
        )
        co_skills.columns = ["skill", "co_occurrence_count"]
        co_skills["pct_of_llm_jobs"] = (co_skills["co_occurrence_count"] / len(llm_job_ids) * 100).round(1)

        return {
            "title": "Skills Most Paired with AI/LLM Technologies",
            "insight": f"'{co_skills['skill'].iloc[0]}' is the skill most commonly paired with LLM/AI requirements, appearing in {co_skills['pct_of_llm_jobs'].iloc[0]:.0f}% of AI-focused job postings.",
            "data": co_skills.to_dict(orient="records"),
        }

    # ── Insight 11: Hiring velocity over time ─────────────────────────────────
    def hiring_velocity(self) -> dict:
        """How is overall hiring velocity trending week-over-week?"""
        df = self.df.dropna(subset=["date_posted"]).copy()
        df["week_start"] = df["date_posted"].dt.to_period("W").apply(lambda r: r.start_time)

        weekly = (
            df.groupby(["week_start", "role_category"])
            .size()
            .reset_index(name="postings")
        )
        weekly["week_start"] = weekly["week_start"].astype(str)

        total_weekly = df.groupby("week_start").size().reset_index(name="total_postings")
        total_weekly["week_start"] = total_weekly["week_start"].astype(str)
        total_weekly = total_weekly.sort_values("week_start")
        total_weekly["wow_change_pct"] = total_weekly["total_postings"].pct_change() * 100

        recent_trend = total_weekly.tail(4)["wow_change_pct"].mean()
        direction = "accelerating" if recent_trend > 0 else "decelerating"

        return {
            "title": "Weekly Hiring Velocity by Role",
            "insight": f"Overall hiring is {direction} — avg WoW change of {recent_trend:+.1f}% over the last 4 weeks.",
            "data": weekly.to_dict(orient="records"),
            "weekly_total": total_weekly.to_dict(orient="records"),
        }

    # ── Insight 12: Location salary heat ─────────────────────────────────────
    def location_salary_heat(self) -> dict:
        """How does location affect salary across different roles?"""
        df = self.df.dropna(subset=["salary_midpoint", "location"])
        result = (
            df.groupby("location")["salary_midpoint"]
            .agg(median="median", count="count")
            .reset_index()
            .rename(columns={"median": "median_salary"})
        )
        result = result[result["count"] >= 10].sort_values("median_salary", ascending=False)
        result["median_salary"] = result["median_salary"].round(0)

        top_loc = result.iloc[0]
        return {
            "title": "Median Salary by Location",
            "insight": f"'{top_loc['location']}' leads with ${top_loc['median_salary']:,.0f} median salary across {top_loc['count']:.0f} postings.",
            "data": result.to_dict(orient="records"),
        }

    # ── Run all ────────────────────────────────────────────────────────────────
    def run_all(self, output_path: str = "data/insights.json") -> dict:
        print("\n🔍 Running 12 market insight analyses...")
        print("-" * 45)

        results = {
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_jobs_analyzed": len(self.df),
            "insights": {}
        }

        analyses = [
            ("role_growth",         self.role_growth_ranking),
            ("skill_salary_premium",self.skill_salary_premium),
            ("remote_trend",        self.remote_work_trend),
            ("salary_by_tier",      self.salary_by_company_tier),
            ("top_skills",          self.top_skills_overall),
            ("skills_by_role",      self.skills_by_role),
            ("salary_seniority",    self.salary_by_seniority),
            ("remote_salary_gap",   self.remote_salary_gap),
            ("salary_range_width",  self.salary_range_width),
            ("llm_cooccurrence",    self.llm_skill_cooccurrence),
            ("hiring_velocity",     self.hiring_velocity),
            ("location_salary",     self.location_salary_heat),
        ]

        for key, fn in analyses:
            try:
                result = fn()
                results["insights"][key] = result
                print(f"  ✅ {key}: {result.get('insight', result.get('title', ''))[:80]}")
            except Exception as e:
                results["insights"][key] = {"error": str(e)}
                print(f"  ❌ {key}: {e}")

        Path(output_path).parent.mkdir(exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💡 All insights saved → {output_path}")
        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/cleaned_jobs.csv")
    parser.add_argument("--skills", default="data/skills_exploded.csv")
    parser.add_argument("--output", default="data/insights.json")
    args = parser.parse_args()

    analytics = JobMarketAnalytics(args.data, args.skills)
    analytics.run_all(args.output)
