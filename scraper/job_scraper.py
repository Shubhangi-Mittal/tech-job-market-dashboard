"""
scraper/job_scraper.py
━━━━━━━━━━━━━━━━━━━━━━
Scrapes tech job postings from public job boards.

LIVE MODE:   Uses requests + BeautifulSoup against real job boards (when possible)
MOCK MODE:   Generates a rich, statistically realistic dataset (default — no API key needed)

The mock data is calibrated to reflect REAL 2024-2025 hiring market conditions:
  - AI/ML role explosion
  - Remote work normalization (then partial reversal)
  - Salary compression in some roles, premiums in others
  - Layoff-driven over-applications in certain sectors

Usage:
    python scraper/job_scraper.py --mode mock --jobs 2000
    python scraper/job_scraper.py --mode live --query "data engineer" --pages 5
"""

import csv
import json
import random
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ── Market data (calibrated to 2024-2025 reality) ─────────────────────────────

ROLES = {
    "Data Engineer": {
        "weight": 0.18,
        "salary_range": (95_000, 185_000),
        "yoe_required": (2, 6),
        "core_skills": ["Python", "SQL", "Spark", "Airflow", "dbt", "Kafka"],
        "cloud_skills": ["AWS", "GCP", "Azure", "Databricks", "Snowflake"],
        "tools": ["Git", "Docker", "Terraform", "Kubernetes"],
        "remote_prob": 0.62,
        "growth_trend": "high",
    },
    "Data Analyst": {
        "weight": 0.20,
        "salary_range": (65_000, 130_000),
        "yoe_required": (1, 4),
        "core_skills": ["SQL", "Python", "Excel", "Tableau", "Power BI", "Looker"],
        "cloud_skills": ["BigQuery", "Redshift", "Snowflake"],
        "tools": ["Git", "dbt", "Google Sheets"],
        "remote_prob": 0.55,
        "growth_trend": "stable",
    },
    "Machine Learning Engineer": {
        "weight": 0.14,
        "salary_range": (130_000, 280_000),
        "yoe_required": (3, 8),
        "core_skills": ["Python", "PyTorch", "TensorFlow", "scikit-learn", "MLflow", "Kubernetes"],
        "cloud_skills": ["AWS SageMaker", "GCP Vertex AI", "Azure ML", "Databricks"],
        "tools": ["Git", "Docker", "Ray", "Weights & Biases"],
        "remote_prob": 0.68,
        "growth_trend": "very_high",
    },
    "AI/LLM Engineer": {
        "weight": 0.09,
        "salary_range": (150_000, 320_000),
        "yoe_required": (2, 6),
        "core_skills": ["Python", "LangChain", "OpenAI API", "RAG", "Vector Databases", "PyTorch"],
        "cloud_skills": ["AWS", "GCP", "Azure", "Pinecone", "Weaviate"],
        "tools": ["Git", "Docker", "FastAPI", "Hugging Face"],
        "remote_prob": 0.71,
        "growth_trend": "explosive",
    },
    "Data Scientist": {
        "weight": 0.16,
        "salary_range": (100_000, 210_000),
        "yoe_required": (2, 6),
        "core_skills": ["Python", "R", "SQL", "scikit-learn", "Statistics", "A/B Testing"],
        "cloud_skills": ["AWS", "GCP", "Databricks", "Snowflake"],
        "tools": ["Git", "Jupyter", "Docker", "Airflow"],
        "remote_prob": 0.60,
        "growth_trend": "stable",
    },
    "Analytics Engineer": {
        "weight": 0.08,
        "salary_range": (95_000, 170_000),
        "yoe_required": (2, 5),
        "core_skills": ["SQL", "dbt", "Python", "Looker", "Data Modeling"],
        "cloud_skills": ["Snowflake", "BigQuery", "Redshift", "Databricks"],
        "tools": ["Git", "Airflow", "Tableau"],
        "remote_prob": 0.65,
        "growth_trend": "high",
    },
    "Data Platform Engineer": {
        "weight": 0.07,
        "salary_range": (120_000, 220_000),
        "yoe_required": (4, 9),
        "core_skills": ["Python", "Kubernetes", "Terraform", "Kafka", "Spark", "SQL"],
        "cloud_skills": ["AWS", "GCP", "Azure", "Databricks"],
        "tools": ["Git", "Docker", "Helm", "Prometheus"],
        "remote_prob": 0.70,
        "growth_trend": "high",
    },
    "Business Intelligence Developer": {
        "weight": 0.08,
        "salary_range": (75_000, 140_000),
        "yoe_required": (2, 6),
        "core_skills": ["SQL", "Power BI", "Tableau", "Looker", "DAX", "Data Modeling"],
        "cloud_skills": ["Snowflake", "Redshift", "BigQuery"],
        "tools": ["Git", "Excel", "Python"],
        "remote_prob": 0.48,
        "growth_trend": "declining",
    },
}

COMPANIES = {
    "FAANG+": {
        "weight": 0.12,
        "names": ["Google", "Meta", "Amazon", "Apple", "Netflix", "Microsoft", "OpenAI", "Anthropic", "DeepMind"],
        "salary_multiplier": 1.45,
        "remote_strictness": "selective",  # less remote post-2023
        "hiring_velocity": "slow",  # lots of competition
    },
    "Scale-up": {
        "weight": 0.28,
        "names": ["Stripe", "Databricks", "Snowflake", "Confluent", "dbt Labs", "Fivetran",
                  "Monte Carlo", "Airbyte", "Cohere", "Mistral", "Scale AI", "Hugging Face",
                  "Weights & Biases", "Modal", "Anyscale"],
        "salary_multiplier": 1.25,
        "remote_strictness": "flexible",
        "hiring_velocity": "fast",
    },
    "Enterprise": {
        "weight": 0.30,
        "names": ["JPMorgan Chase", "Goldman Sachs", "Citigroup", "Wells Fargo",
                  "McKinsey", "Deloitte", "Accenture", "IBM", "Oracle", "SAP",
                  "Walmart", "Target", "UnitedHealth", "CVS", "Pfizer"],
        "salary_multiplier": 1.0,
        "remote_strictness": "hybrid",
        "hiring_velocity": "normal",
    },
    "Startup": {
        "weight": 0.20,
        "names": ["TechCorp AI", "DataFlow Inc", "InsightHub", "MetricStack",
                  "PipelineOS", "QueryCraft", "VectorSearch Co", "ModelOps",
                  "DataMesh Labs", "StreamBase"],
        "salary_multiplier": 0.92,
        "remote_strictness": "remote-first",
        "hiring_velocity": "very_fast",
    },
    "Mid-market": {
        "weight": 0.10,
        "names": ["HubSpot", "Twilio", "Zendesk", "Cloudflare", "Okta",
                  "HashiCorp", "Elastic", "New Relic", "Splunk", "Grafana Labs"],
        "salary_multiplier": 1.10,
        "remote_strictness": "flexible",
        "hiring_velocity": "normal",
    },
}

LOCATIONS = {
    "San Francisco, CA":    {"weight": 0.14, "salary_adj": 1.20, "remote_prob_boost": 0.0},
    "New York, NY":         {"weight": 0.13, "salary_adj": 1.15, "remote_prob_boost": -0.05},
    "Seattle, WA":          {"weight": 0.10, "salary_adj": 1.12, "remote_prob_boost": 0.05},
    "Austin, TX":           {"weight": 0.08, "salary_adj": 0.95, "remote_prob_boost": 0.0},
    "Chicago, IL":          {"weight": 0.06, "salary_adj": 0.90, "remote_prob_boost": -0.05},
    "Boston, MA":           {"weight": 0.06, "salary_adj": 1.05, "remote_prob_boost": 0.0},
    "Los Angeles, CA":      {"weight": 0.06, "salary_adj": 1.08, "remote_prob_boost": 0.0},
    "Denver, CO":           {"weight": 0.04, "salary_adj": 0.88, "remote_prob_boost": 0.05},
    "Atlanta, GA":          {"weight": 0.04, "salary_adj": 0.85, "remote_prob_boost": -0.03},
    "Miami, FL":            {"weight": 0.03, "salary_adj": 0.82, "remote_prob_boost": 0.02},
    "Washington, DC":       {"weight": 0.04, "salary_adj": 1.00, "remote_prob_boost": -0.05},
    "Remote":               {"weight": 0.22, "salary_adj": 1.0,  "remote_prob_boost": 1.0},
}

SENIORITY_LEVELS = {
    "Entry Level":  {"weight": 0.20, "yoe": (0, 2),  "salary_mult": 0.72},
    "Mid Level":    {"weight": 0.38, "yoe": (2, 5),  "salary_mult": 1.00},
    "Senior":       {"weight": 0.30, "yoe": (5, 9),  "salary_mult": 1.35},
    "Staff/Lead":   {"weight": 0.08, "yoe": (7, 12), "salary_mult": 1.65},
    "Principal":    {"weight": 0.04, "yoe": (10, 20),"salary_mult": 2.10},
}

INDUSTRIES = [
    "FinTech", "Healthcare/BioTech", "E-commerce", "SaaS/Cloud",
    "AI/ML Platform", "AdTech/MarTech", "Cybersecurity", "Gaming",
    "Logistics/Supply Chain", "Media/Streaming", "InsurTech", "EdTech",
]

EDUCATION = {
    "No Degree Required": 0.18,
    "Bachelor's Degree":  0.45,
    "Master's Preferred": 0.25,
    "PhD Preferred":      0.12,
}

VISA_SPONSORSHIP = {"Yes": 0.35, "No": 0.50, "Possible": 0.15}

BENEFITS = [
    "Health Insurance", "401k Match", "Stock Options / RSUs", "Unlimited PTO",
    "Remote Work Stipend", "Learning Budget", "Parental Leave",
    "Mental Health Benefits", "Gym Membership", "Free Meals",
]


def _weighted_choice(options_dict: dict):
    keys = list(options_dict.keys())
    weights = [v["weight"] if isinstance(v, dict) else v for v in options_dict.values()]
    return random.choices(keys, weights=weights, k=1)[0]


def _pick_skills(role_data: dict, count_core: int = None, count_extra: int = None) -> list[str]:
    all_skills = role_data["core_skills"] + role_data["cloud_skills"] + role_data["tools"]
    n_core = count_core or random.randint(3, len(role_data["core_skills"]))
    n_extra = count_extra or random.randint(1, 4)

    core = random.sample(role_data["core_skills"], min(n_core, len(role_data["core_skills"])))
    extra_pool = role_data["cloud_skills"] + role_data["tools"]
    extra = random.sample(extra_pool, min(n_extra, len(extra_pool)))
    return list(set(core + extra))


def _generate_description(role: str, skills: list[str], seniority: str, remote_type: str) -> str:
    skill_str = ", ".join(skills[:6])
    return (
        f"We are looking for a {seniority} {role} to join our growing team. "
        f"You will work closely with cross-functional stakeholders to deliver data-driven solutions. "
        f"Key requirements include proficiency in {skill_str}. "
        f"This is a {remote_type} position. "
        f"We offer competitive compensation and a collaborative work environment."
    )


def generate_mock_jobs(n: int = 2000, seed: int = 42) -> list[dict]:
    """
    Generate n realistic job postings calibrated to 2024-2025 tech hiring market.
    """
    random.seed(seed)
    jobs = []

    # Time distribution: postings from last 180 days, weighted toward recent
    now = datetime.now()

    role_names = list(ROLES.keys())
    role_weights = [ROLES[r]["weight"] for r in role_names]

    company_tiers = list(COMPANIES.keys())
    company_weights = [COMPANIES[t]["weight"] for t in company_tiers]

    location_names = list(LOCATIONS.keys())
    location_weights = [LOCATIONS[loc]["weight"] for loc in location_names]

    seniority_names = list(SENIORITY_LEVELS.keys())
    seniority_weights = [SENIORITY_LEVELS[s]["weight"] for s in seniority_names]

    for _ in range(n):
        # Core selections
        role_name = random.choices(role_names, weights=role_weights, k=1)[0]
        role = ROLES[role_name]

        company_tier = random.choices(company_tiers, weights=company_weights, k=1)[0]
        company_info = COMPANIES[company_tier]
        company_name = random.choice(company_info["names"])

        location = random.choices(location_names, weights=location_weights, k=1)[0]
        loc_info = LOCATIONS[location]

        seniority = random.choices(seniority_names, weights=seniority_weights, k=1)[0]
        sen_info = SENIORITY_LEVELS[seniority]

        # Remote type
        base_remote_prob = role["remote_prob"] + loc_info["remote_prob_boost"]
        if company_tier == "FAANG+" and location != "Remote":
            base_remote_prob -= 0.15  # big tech pulling back on remote
        base_remote_prob = max(0.05, min(0.95, base_remote_prob))

        if location == "Remote":
            remote_type = "Remote"
        elif random.random() < base_remote_prob:
            remote_type = random.choice(["Remote", "Hybrid"])
        else:
            remote_type = random.choice(["Hybrid", "On-site"])

        is_remote = remote_type == "Remote"

        # Salary calculation
        sal_min, sal_max = role["salary_range"]
        base_salary = random.uniform(sal_min, sal_max)
        salary = base_salary * company_info["salary_multiplier"] * loc_info["salary_adj"] * sen_info["salary_mult"]
        salary = round(salary / 5000) * 5000  # round to nearest $5k

        # Some postings don't show salary
        shows_salary = random.random() < 0.58
        salary_min_shown = round(salary * random.uniform(0.85, 0.95) / 1000) * 1000 if shows_salary else None
        salary_max_shown = round(salary * random.uniform(1.05, 1.20) / 1000) * 1000 if shows_salary else None

        # Skills
        skills = _pick_skills(role)
        n_skills = len(skills)

        # YOE
        yoe_min = random.randint(*sen_info["yoe"])
        yoe_max = yoe_min + random.randint(1, 3)

        # Posting date (exponential recency bias)
        days_ago = int(random.expovariate(0.03))  # avg ~33 days
        days_ago = min(days_ago, 180)
        posted_date = now - timedelta(days=days_ago)

        education = random.choices(
            list(EDUCATION.keys()), weights=list(EDUCATION.values()), k=1
        )[0]
        visa = random.choices(
            list(VISA_SPONSORSHIP.keys()), weights=list(VISA_SPONSORSHIP.values()), k=1
        )[0]
        n_benefits = random.randint(3, 7)
        job_benefits = random.sample(BENEFITS, n_benefits)

        industry = random.choice(INDUSTRIES)
        source = random.choice(["LinkedIn", "Indeed", "Glassdoor", "Levels.fyi", "Company Website"])

        jobs.append({
            "job_id":                str(uuid.uuid4())[:12],
            "title":                 f"{seniority} {role_name}" if seniority not in ["Mid Level"] else role_name,
            "role_category":         role_name,
            "seniority":             seniority,
            "company":               company_name,
            "company_tier":          company_tier,
            "industry":              industry,
            "location":              location if location != "Remote" else random.choice(list(LOCATIONS.keys())[:-1]),
            "remote_type":           remote_type,
            "is_remote":             is_remote,
            "salary_min":            salary_min_shown,
            "salary_max":            salary_max_shown,
            "salary_midpoint":       round(salary),
            "skills":                "|".join(skills),
            "num_skills_required":   n_skills,
            "yoe_min":               yoe_min,
            "yoe_max":               yoe_max,
            "education_required":    education,
            "visa_sponsorship":      visa,
            "benefits":              "|".join(job_benefits),
            "description_snippet":   _generate_description(role_name, skills, seniority, remote_type),
            "source":                source,
            "date_posted":           posted_date.strftime("%Y-%m-%d"),
            "days_since_posted":     days_ago,
            "growth_trend":          role["growth_trend"],
            "company_size_tier":     company_tier,
            "scraped_at":            now.strftime("%Y-%m-%dT%H:%M:%S"),
        })

    print(f"✅ Generated {len(jobs):,} mock job postings")
    return jobs


def scrape_live(query: str = "data engineer", location: str = "United States", pages: int = 3) -> list[dict]:
    """
    Attempt live scraping from public job board pages.
    Falls back to mock data if blocked or unavailable.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        print("⚠️  requests/beautifulsoup4 not installed. Using mock data.")
        return generate_mock_jobs(300)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    jobs = []
    base_url = "https://www.indeed.com/jobs"

    for page in range(pages):
        params = {
            "q": query,
            "l": location,
            "start": page * 15,
            "sort": "date",
        }
        try:
            print(f"  Scraping page {page+1}/{pages}...")
            resp = requests.get(base_url, params=params, headers=headers, timeout=15)

            if resp.status_code == 403:
                print("  ⚠️  Blocked by anti-bot. Switching to mock data.")
                return generate_mock_jobs(500)

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|jobCard"))

            if not cards:
                print(f"  No cards found on page {page+1}. Site structure may have changed.")
                break

            for card in cards:
                title_el = card.find(["h2", "a"], class_=re.compile(r"jobTitle|title"))
                company_el = card.find(["span", "div"], class_=re.compile(r"companyName|company"))
                location_el = card.find(["div", "span"], class_=re.compile(r"companyLocation|location"))
                salary_el = card.find(["div", "span"], class_=re.compile(r"salary|compensation"))

                if not title_el:
                    continue

                jobs.append({
                    "job_id": str(uuid.uuid4())[:12],
                    "title": title_el.get_text(strip=True),
                    "company": company_el.get_text(strip=True) if company_el else "Unknown",
                    "location": location_el.get_text(strip=True) if location_el else "Unknown",
                    "salary_raw": salary_el.get_text(strip=True) if salary_el else None,
                    "source": "Indeed (Live)",
                    "scraped_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                })

            time.sleep(random.uniform(2, 4))  # be respectful

        except Exception as e:
            print(f"  ❌ Error on page {page+1}: {e}")
            break

    if not jobs:
        print("⚠️  Live scrape returned 0 results. Using mock data.")
        return generate_mock_jobs(500)

    print(f"✅ Scraped {len(jobs)} live job postings")
    return jobs


def save_raw(jobs: list[dict], output_path: str = "data/raw_jobs.csv") -> str:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    if not jobs:
        return output_path

    fieldnames = list(jobs[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)

    print(f"💾 Saved {len(jobs):,} raw records → {output_path}")
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["mock", "live"], default="mock")
    parser.add_argument("--jobs", type=int, default=2000, help="Number of mock jobs to generate")
    parser.add_argument("--query", default="data engineer", help="Search query for live mode")
    parser.add_argument("--pages", type=int, default=5, help="Pages to scrape in live mode")
    parser.add_argument("--output", default="data/raw_jobs.csv")
    args = parser.parse_args()

    if args.mode == "live":
        jobs = scrape_live(query=args.query, pages=args.pages)
    else:
        jobs = generate_mock_jobs(n=args.jobs)

    save_raw(jobs, args.output)
