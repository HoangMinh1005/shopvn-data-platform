# Project execution guardrails

Before planning, implementing, or reporting a project stage, read
`docs/architecture/mvp-implementation-plan.md` completely. It is the canonical
source for stage order, scope, approval state, and checklists. Business rules and
the original project requirements remain authoritative for business correctness.

- Stage 2 is SFTP ingestion and marketplace Finance, not Kafka/simulator.
- Stage 1 is complete within its documented scope. Stage 3–7 numbering/order in
  plan version 1.0 is proposed, not approved historical agreement. Confirm before
  implementing a stage that is still proposed.
- Do not confuse preparation steps P1–P3 with MVP stages S1–S7.
- Start work updates with the applicable stage/task ID and scope. A status or
  diagnostic request does not authorize implementing the next stage.
- Do not reorder stages, broaden scope, change KPIs, or mark proposals approved
  without the user's decision. Follow the change-control checklist in the plan.
- Do not invent missing conversation history. State uncertainty and use recorded
  decisions; when sources disagree, surface the conflict rather than silently
  choosing a new roadmap.
- Mark completion only with appropriately scoped verification evidence. Preserve
  historical reports; distinguish current service health from earlier test PASS.
- Do not mutate source systems, mix simulated data into actual publications, or
  reset HMAC identity/go-live policy to make checks pass.

Use `python scripts/check_repository.py` after documentation changes. For code
changes, also run the relevant unit/lint/integration checks described in
`tests/README.md`. Do not push or merge without the user's request.
