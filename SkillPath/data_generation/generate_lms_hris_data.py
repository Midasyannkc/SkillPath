"""
Generates synthetic LMS (Learning Management System) and HRIS (Human Resources
Information System) data for the SkillPath pipeline.

Produces three raw CSVs into data/bronze/, simulating what would normally
arrive via API extracts or vendor file drops from a real LMS (e.g. Cornerstone,
Docebo) and HRIS (e.g. Workday):

  - hris_employees.csv     : one row per employee, role/department/tenure
  - course_catalog.csv     : one row per course, mapped to a target skill
  - lms_completions.csv    : one row per (employee, course) enrollment event

The data includes intentional messiness (nulls, duplicate enrollment events,
inconsistent department casing) to mirror real upstream systems, which the
Silver layer is responsible for cleaning.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "bronze"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEPARTMENTS = ["Engineering", "engineering", "Sales", "Sales ", "Marketing",
               "Customer Success", "Data", "Product", "Finance"]
ROLES = {
    "Engineering": ["Software Engineer", "Senior Software Engineer", "Engineering Manager"],
    "Data": ["Data Analyst", "Data Engineer", "Analytics Manager"],
    "Sales": ["Account Executive", "Sales Development Rep", "Sales Manager"],
    "Marketing": ["Marketing Specialist", "Content Strategist", "Marketing Manager"],
    "Customer Success": ["CS Rep", "CS Manager"],
    "Product": ["Product Manager", "Product Analyst"],
    "Finance": ["Financial Analyst", "Controller"],
}

SKILLS = [
    "python_programming", "sql_fundamentals", "cloud_architecture",
    "data_modeling", "leadership_101", "negotiation", "public_speaking",
    "project_management", "excel_advanced", "security_awareness",
    "customer_empathy", "financial_analysis",
]

COURSE_TITLES = {
    "python_programming": ["Intro to Python", "Python for Data Analysis"],
    "sql_fundamentals": ["SQL Basics", "Advanced SQL Querying"],
    "cloud_architecture": ["AWS Fundamentals", "Cloud Architecture Patterns"],
    "data_modeling": ["Dimensional Modeling 101", "Data Warehousing Concepts"],
    "leadership_101": ["Foundations of Leadership", "Leading Remote Teams"],
    "negotiation": ["Negotiation Essentials"],
    "public_speaking": ["Public Speaking Mastery"],
    "project_management": ["PM Fundamentals", "Agile Project Management"],
    "excel_advanced": ["Excel Power Users"],
    "security_awareness": ["Security Awareness Training", "Phishing Defense 101"],
    "customer_empathy": ["Customer-Centric Communication"],
    "financial_analysis": ["Financial Analysis Fundamentals"],
}


def generate_employees(n=400):
    employees = []
    start_date = datetime(2019, 1, 1)
    for i in range(1, n + 1):
        dept = random.choice(DEPARTMENTS)
        dept_clean = dept.strip().title() if dept.strip().title() != "Engineering" else "Engineering"
        role_pool = ROLES.get(dept_clean, ROLES["Engineering"])
        hire_date = start_date + timedelta(days=random.randint(0, 2100))
        employees.append({
            "employee_id": f"E{i:05d}",
            "department": dept,  # kept messy on purpose
            "role_title": random.choice(role_pool),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "manager_id": f"E{random.randint(1, n):05d}" if random.random() > 0.05 else "",
        })
    return employees


def generate_course_catalog():
    courses = []
    course_id = 1
    for skill, titles in COURSE_TITLES.items():
        for title in titles:
            courses.append({
                "course_id": f"C{course_id:04d}",
                "course_title": title,
                "target_skill": skill,
                "estimated_hours": random.choice([1, 2, 3, 4, 6, 8]),
            })
            course_id += 1
    return courses


def generate_completions(employees, courses, n_events=3500):
    events = []
    event_id = 1
    for _ in range(n_events):
        emp = random.choice(employees)
        course = random.choice(courses)
        enrolled = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))
        # 15% of enrollments are never completed (still valid rows -- NULL completion)
        completed = None
        score = None
        status = "enrolled"
        if random.random() > 0.15:
            completed = enrolled + timedelta(days=random.randint(1, 45))
            score = round(random.uniform(55, 100), 1)
            status = "completed"
        else:
            status = random.choice(["enrolled", "in_progress", "dropped"])

        events.append({
            "event_id": f"EV{event_id:06d}",
            "employee_id": emp["employee_id"],
            "course_id": course["course_id"],
            "enrolled_date": enrolled.strftime("%Y-%m-%d"),
            "completed_date": completed.strftime("%Y-%m-%d") if completed else "",
            "quiz_score": score if score is not None else "",
            "status": status,
        })
        event_id += 1

    # Inject a handful of exact-duplicate events, simulating a real LMS
    # webhook retry / double-fire, which Silver needs to de-duplicate.
    for _ in range(40):
        events.append(dict(random.choice(events)))

    return events


def write_csv(rows, path, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {path}")


if __name__ == "__main__":
    employees = generate_employees()
    courses = generate_course_catalog()
    completions = generate_completions(employees, courses)

    write_csv(employees, OUTPUT_DIR / "hris_employees.csv",
              ["employee_id", "department", "role_title", "hire_date", "manager_id"])
    write_csv(courses, OUTPUT_DIR / "course_catalog.csv",
              ["course_id", "course_title", "target_skill", "estimated_hours"])
    write_csv(completions, OUTPUT_DIR / "lms_completions.csv",
              ["event_id", "employee_id", "course_id", "enrolled_date",
               "completed_date", "quiz_score", "status"])

    print("Bronze layer generation complete.")
