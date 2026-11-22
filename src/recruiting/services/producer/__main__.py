"""CLI: python -m recruiting.services.producer <command>

  seed [--demo-compaction]   publish the sample jobs and resumes
  job  --id J --title T ...  publish one job requisition
  apply --job-id J --name N --email E --resume-file F
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from recruiting.common.models import JobRequisition, JobStatus
from recruiting.services.producer import seed_data
from recruiting.services.producer.producer import EventProducer


def main() -> None:
    p = argparse.ArgumentParser(prog="recruiting-producer")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed")
    j = sub.add_parser("job")
    j.add_argument("--id", required=True)
    j.add_argument("--title", required=True)
    j.add_argument("--department", default="Engineering")
    j.add_argument("--skills", required=True, help="comma separated")
    j.add_argument("--min-years", type=int, default=0)
    j.add_argument("--status", choices=[s.value for s in JobStatus], default="OPEN")
    j.add_argument("--description", default="")
    a = sub.add_parser("apply")
    a.add_argument("--job-id", required=True)
    a.add_argument("--name", required=True)
    a.add_argument("--email", required=True)
    a.add_argument("--resume-file", required=True)
    args, rest = p.parse_known_args()

    if args.cmd == "seed":
        sys.argv = [sys.argv[0], *rest]
        seed_data.main()
    elif args.cmd == "job":
        EventProducer().publish_job(JobRequisition(
            job_id=args.id, title=args.title, department=args.department,
            required_skills=[s.strip() for s in args.skills.split(",") if s.strip()],
            min_experience_years=args.min_years, status=JobStatus(args.status), description=args.description,
        ))
    elif args.cmd == "apply":
        EventProducer().submit_resume(args.job_id, args.name, args.email, Path(args.resume_file).read_text())


if __name__ == "__main__":
    main()
