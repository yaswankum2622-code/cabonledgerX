# 01 Problem Statement

## The Core Problem

Climate commitments are easy to announce and hard to evaluate. Public-facing target statements often hide the operational question that matters:

**Is the current emissions trajectory actually consistent with the stated target, and if not, what is the first credible corrective action?**

Many ESG dashboards stop at static target metadata, broad sector averages, or a single opaque risk score. That leaves several gaps:

- no clear bridge between operational activity and emissions outcomes
- no transparent forecast logic
- no explicit contradiction detection between public claims and modeled trajectory
- no explanation of why one risk view might disagree with another
- no direct connection from risk to an intervention strategy

## What TargetTruth Tries To Solve

TargetTruth is designed as a compact climate commitment intelligence product. It does not try to be a universal carbon accounting platform or a full compliance system. Instead, it focuses on a narrower and more decision-oriented workflow:

1. construct a credible emissions baseline
2. expose a transparent calculator layer
3. reconstruct historical annual behavior
4. forecast forward to the target horizon
5. assess whether the target is likely to be met
6. identify contradictions and credibility issues
7. compare heuristic and probabilistic scoring
8. rank plausible interventions
9. package findings into dashboard, API, and evidence-pack surfaces

## Why This Matters

The project matters because climate-risk communication is often not matched by decision-grade analytical infrastructure. A useful reviewer, investor, operator, or product interviewer does not only want to know the final score. They want to know:

- what data sources were used
- how emissions were calculated
- what assumptions shaped the forecast
- whether risk logic is explainable
- whether recommended actions are operationally grounded

That is the gap this repository addresses.

## Scope Of The Implemented System

The current repository intentionally uses a mix of real public factor sources and synthetic company-level data:

- real public factor workbooks:
  - eGRID
  - DEFRA
  - SBTi exports
- synthetic company portfolio:
  - 500 modeled companies
  - reproducible activity, emissions, target, and intervention profiles

This keeps the project large enough to demonstrate architecture and modeling design, while avoiding false claims of production-grade issuer truth.

## Non-Goals

The implemented system is not yet:

- a fully audited enterprise carbon accounting platform
- a multi-tenant production SaaS system
- a real-world calibrated commitment-failure model trained on observed misses
- a formal regulatory reporting engine

Those are future directions, not claims about the current repository.

## Success Criteria For This Repo

As a public portfolio repository, success means:

- the problem is clear
- the analytical layers are inspectable
- the repo structure is easy to review
- the dashboard and API are demo-ready
- the modeling choices are defensible in interviews
- the limitations are explicit rather than hidden
