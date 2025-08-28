# B2B Recruitment SaaS — Execution Plan

This document captures the end-to-end plan to evolve the current repo into a multi-tenant B2B SaaS with a self-service agency portal, public developer API, and AWS infrastructure.

## Scope & Goals
- Multi-tenant, B2B only (recruitment agencies).
- Web portal: auth, job upload (CSV/form), CV upload (batch), matching previews, outreach (email/SMS), analytics.
- API: full coverage (auth/tenants, ingest, match, outreach, analytics, webhooks).
- Infra: AWS (ECS Fargate, S3, SES, SNS/Twilio, Mongo Atlas), IaC with Terraform, monitoring.
- Integrations: ATS/job boards inbound; webhooks outbound.

## Data model (Mongo)
- tenants, users, api_keys, jobs, candidates, resumes, matches, outreach, events, webhooks.
- All collections include `tenant_id` and audit fields.
- Indexes on email, api_keys, jobs (external_job_id, geo), matches, events.

## API surface (new routers)
- /auth: signup/login/JWT; roles.
- /tenants: CRUD (admin), settings.
- /keys: API key lifecycle.
- /jobs: POST (single), POST /batch (CSV), GET list/get.
- /candidates: POST JSON, POST /upload (files), GET get.
- /matches: GET candidate/job; POST /recompute.
- /outreach: preview/send/schedule templates.
- /confirm: token landing to email agency with subject `#scoreagents# {external_job_id}`.
- /analytics: overview/funnels/events.
- /integrations: ATS webhooks; feeds; outbound webhooks mgmt.

## Matching updates
- Enforce `must_have` requirements; distance-based scoring; remote flag.
- Prioritize the job initially applied for while suggesting others.

## Frontend
- Vite + React routes: /signup, /login, /dashboard, /jobs, /candidates, /matches, /outreach, /settings, public /confirm/apply.
- S3 presigned uploads; charts; template editor.

## Infra (AWS)
- ECS Fargate + ALB + ACM; S3 (uploads); SES email; SNS/Twilio SMS; SQS workers; CloudWatch + Grafana; Secrets Manager; Terraform; GitHub Actions.

## Roadmap (≈14–16 weeks)
1) Auth + tenancy + S3/SES/SNS setup
2) Ingestion pipelines (jobs/CVs) + worker
3) Matching engine refinements
4) Outreach + confirmation flow
5) Analytics dashboards
6) Public API hardening + docs
7) Integrations + beta launch

## Acceptance criteria (MVP)
- Agency can onboard, upload jobs/CVs, see matches, send outreach; candidates confirm → agency email with CV attached and subject.
- Developer can do same via API + keys; basic dashboard live; deployed on AWS with monitoring.
