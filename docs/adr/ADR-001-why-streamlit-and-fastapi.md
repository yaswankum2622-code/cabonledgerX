# ADR-001: Why Streamlit + FastAPI

## Status

Accepted

## Context

The project needed two delivery surfaces:

- a high-impact demo and screenshot surface
- a structured programmatic access layer

The analytical pipeline was already parquet-backed and local-first, so the serving layer needed to stay thin.

## Decision

Use:

- Streamlit for the primary interactive product surface
- FastAPI for the read-only service layer

## Rationale

- Streamlit is fast for premium demo iteration and product storytelling.
- FastAPI provides typed schemas, clean routing, and local API credibility.
- Both can share the same parquet-backed access model without duplicating business logic.
- This split is strong for a public portfolio repo because it demonstrates both UX thinking and service design.

## Consequences

Positive:

- fast demo iteration
- low serving complexity
- clear separation between analytics and presentation

Negative:

- not yet production-grade for multi-user serving
- still dependent on local parquet artifacts

## Alternatives Considered

- Streamlit only: too weak for API/service presentation
- FastAPI only: too weak for premium visual storytelling
- database-backed app stack: premature for the current stage
