Title: Greenhouse Rejection Email Redirect – End-to-End Execution Plan (Backend + Frontend + Tests)

Owner: PTX / TalentDB
Status: Approved for implementation
Last updated: 2025-10-22

Objective
- Implement a public redirect endpoint that accepts a Greenhouse job URL and forwards the user to the tenant’s public portal with pre-filled filters for location and skills.
- Extend the public portal page to read these filters, highlight them in the UI, and filter the job list accordingly.
- Ship with unit tests (backend + frontend) and simple E2E smoke to ensure no regressions.

Success Criteria (Definition of Done)
- GET /portal/{tenant_slug}/redirect/{gh_url:path} exists, is unauthenticated, and responds 302 to /portal/{tenant_slug}?location=X&skills=Y,Z when job is found; otherwise 302 to /portal/{tenant_slug}.
- Job lookup uses tenant slug + external_job_id parsed from the Greenhouse URL.
- Frontend route /portal/:slug loads public portal without auth, parses location and skills query params, shows selected filters highlighted, and filters the job cards accordingly.
- All new and existing tests pass locally.

Architectural Notes (current repo)
- Backend FastAPI app in talentdb/scripts/api.py, with existing SPA route fallback for /portal/{slug} and a public data endpoint at GET /tenants/public/portal/{slug} (in routers_vc_portal.py) used by frontend PortalPage.tsx.
- Jobs schema contains tenant_id (string of ObjectId), city (original) and city_canonical, and skill_set (merged from must/nice, generally case-preserving on upload).
- Frontend React (Vite + TS), PortalPage.tsx fetches from /tenants/public/portal/{slug} and already renders a public job list with search/location/company/type filters.

Phases and Tasks

Phase 1 — Backend: Add public redirect route
Files:
- talentdb/scripts/api.py (or a new small router)

Tasks:
1. Add GET handler:
   - Path: /portal/{tenant_slug}/redirect/{gh_url:path}
   - include_in_schema=False, no auth dependencies
   - Steps inside handler:
     a) URL-decode gh_url
     b) Extract external job id after '/jobs/' (strip query/fragment)
     c) Resolve tenant by slug: db['tenants'].find_one({'slug': tenant_slug})
     d) Query job: db['jobs'].find_one({'tenant_id': str(tenant._id), 'external_job_id': ext_id})
     e) If job found: location = job.get('city') or '', skills = job.get('skill_set') or []
        - Construct redirect to /portal/{tenant_slug}?location=<encoded>&skills=<comma-joined-encoded>
     f) If not found (tenant or job): redirect to /portal/{tenant_slug}
     g) Return RedirectResponse(status_code=302)

Notes:
- Avoid require_tenant. This must be public.
- Keep redirect internal (no open redirect). Never forward to external GH URL.
- Prefer city (unmodified) to match Portal data’s location values.

Acceptance tests for Phase 1:
- Hitting a sample encoded URL resolves to the handler (no 404) and logs.
- Unit tests added in Phase 3 validate redirect behavior.

Phase 2 — Backend: Robust parsing + data lookup
Files:
- talentdb/scripts/api.py (same handler)

Tasks:
1. Implement robust parsing:
   - urllib.parse.unquote
   - Use split('/jobs/')[-1] then split on '?' and '#', trim
   - Validate ext_id non-empty; if empty → redirect to base portal
2. Tenant lookup:
   - db['tenants'].find_one({'slug': tenant_slug})
   - If not found → redirect to /portal/{tenant_slug}
3. Job lookup:
   - tenant_key = str(_id)
   - db['jobs'].find_one({'tenant_id': tenant_key, 'external_job_id': ext_id})
4. Extract:
   - location = job.get('city') or ''
   - skills = job.get('skill_set') or []
   - Optionally Title-Case skill display via simple transform if needed
5. Build redirect URL:
   - Base: f"/portal/{tenant_slug}"
   - Append ?location=... when non-empty
   - Append &skills=... when non-empty (comma-joined)

Edge cases:
- Encoded GH URL; GH URLs with query params/fragments; skills containing special chars (C#, C++);
- Missing job/tenant gracefully redirect to base portal.

Phase 3 — Backend Tests: FastAPI TestClient
Files:
- tests/test_portal_redirect.py (new)

Scenarios:
1) happy_path_redirect:
   - Seed tenant with slug 'demo-tenant'
   - Seed job { tenant_id: str(tenant._id), external_job_id: '7390395003', city: 'Tel Aviv', skill_set: ['React','Node.js'] }
   - GET /portal/demo-tenant/redirect/https%3A%2F%2Fjob-boards.greenhouse.io%2Facme%2Fjobs%2F7390395003
   - Expect 302 with Location: /portal/demo-tenant?location=Tel%20Aviv&skills=React,Node.js
2) job_not_found_redirects_base:
   - Tenant exists, no job
   - Expect 302 → /portal/demo-tenant
3) bad_tenant_redirects_base:
   - Missing tenant
   - Expect 302 → /portal/unknown-tenant
4) parsing_variants:
   - GH URL with '?gh_src=...' and with trailing '#section' → still extracts ID
5) special_chars_skills:
   - job.skill_set = ['C#','C++'] → Location header properly URL-encodes skills param

Cleanup:
- Delete inserted tenants/jobs at test end (or use unique slugs + delete by slug and tenant_id).

Phase 4 — Frontend: Parse query params and add filters UI for skills
Files:
- frontend/src/pages/PortalPage.tsx
- Optionally small CSS class additions (co-located)

Tasks:
1. Parse query string on mount:
   - const { search } = useLocation();
   - const qp = new URLSearchParams(search)
   - const locationFilter = qp.get('location') || ''
   - const skillsParam = qp.get('skills') || ''
   - const initialSkills = skillsParam ? skillsParam.split(',').filter(Boolean) : []
   - Initialize state: selectedLocation, selectedSkills
2. Build skills catalog from data.jobs:
   - const allSkills = useMemo(() => unique of job.requirements across jobs, case-preserving)
3. Highlight active filters:
   - Render pills/buttons for each skill, with active class when selected
   - Render location options (derived from jobs or free text) with active style
4. Filter logic:
   - Combine existing filters with AND for skills: every(selectedSkills) in job.requirements (case-insensitive compare)
5. Optional: sync URL with filters (useNavigate + replace) to keep shareable links

Phase 5 — Frontend Tests (Vitest + RTL)
Files:
- frontend/src/__tests__/PortalPage.filters.test.tsx (new)

Scenarios:
1) initial_query_applies_filters:
   - Render with location '?location=Tel%20Aviv&skills=React,Node.js'
   - Assert initial state has selected filters and only matching jobs rendered
2) toggle_skill_updates_results:
   - Simulate clicking a skill pill to add/remove
   - Assert job list changes and pill highlight toggles
3) url_sync_optional:
   - If implemented, assert navigate called with updated query params

Phase 6 — E2E Validation
Manual checklist:
- Backend running (uvicorn) + Frontend dev server or built SPA served by backend
- Visit /portal/{slug}/redirect/{encoded GH URL}; confirm 302 to /portal/{slug}?location=...&skills=...
- Confirm portal loads with highlighted filters and filtered jobs
- Try edge cases: unknown job/tenant; skills with special chars; large skill lists

Implementation Details (copy-ready snippets for Copilot)

Backend: Route skeleton (talentdb/scripts/api.py)
- Place near SPA routes or bottom of file; marked include_in_schema=False and no auth.
- Import: from fastapi.responses import RedirectResponse; import urllib.parse

Pseudocode:
- @app.get('/portal/{tenant_slug}/redirect/{gh_url:path}', include_in_schema=False)
  def portal_redirect(tenant_slug: str, gh_url: str):
    dec = urllib.parse.unquote(gh_url)
    id_part = dec.split('/jobs/')[-1]
    id_part = id_part.split('?')[0].split('#')[0].strip()
    if not id_part: return RedirectResponse(f"/portal/{tenant_slug}", status_code=302)
    tenant = db['tenants'].find_one({'slug': tenant_slug})
    if not tenant: return RedirectResponse(f"/portal/{tenant_slug}", status_code=302)
    tid = str(tenant.get('_id'))
    job = db['jobs'].find_one({'tenant_id': tid, 'external_job_id': id_part})
    if not job: return RedirectResponse(f"/portal/{tenant_slug}", status_code=302)
    location = job.get('city') or ''
    skills = [s for s in (job.get('skill_set') or []) if s]
    params = []
    if location:
      params.append('location=' + urllib.parse.quote(location))
    if skills:
      params.append('skills=' + urllib.parse.quote(','.join(skills)))
    qs = ('?' + '&'.join(params)) if params else ''
    return RedirectResponse(f"/portal/{tenant_slug}{qs}", status_code=302)

Backend Tests: tests/test_portal_redirect.py
- Use fastapi.testclient.TestClient(app)
- Seed: insert tenant {name, slug} → get _id; insert job with tenant_id=str(_id)
- Parametrize cases for GH URL forms and skills sets
- Cleanup: delete seeded docs by slug + tenant_id

Frontend: PortalPage.tsx updates
- Read query params via useLocation()
- Initialize selectedLocation, selectedSkills
- Compute allSkills from data.jobs[].requirements
- UI: simple pill/toggle buttons; className includes 'active' when selected
- Filter logic: matches if each selected skill present in job.requirements (case-insensitive)
- Optional: useNavigate to reflect filters in URL

Quality Gates
- Lint/typecheck pass for frontend
- All backend pytest pass, including new tests
- Manual smoke of redirect URL behavior

Runbook
Backend unit tests:
- pytest -q (or run the VS Code task: pytest-header-importer)

Frontend tests:
- cd frontend
- npm install
- npm run test

Dev servers:
- Backend: python -m talentdb.scripts.api (or uvicorn scripts.api:app --port 8080)
- Frontend: cd frontend && npm run dev (proxy routes already map to backend 8080)

Rollback plan
- Feature is self-contained; revert route and frontend changes. No schema changes.

Security & Privacy
- Endpoint is read-only and public, returns only redirect.
- Prevent open redirects by building relative Location headers only to our app.
- Public jobs API remains limited to non-sensitive fields (already implemented under /tenants/public/portal/{slug}).

Appendix: Acceptance Checklist
- [ ] New route added, unauthenticated, reachable
- [ ] ID parsing handles encoded URLs, queries, fragments
- [ ] Mongo lookup by slug + external_job_id works
- [ ] Redirect includes encoded location and skills
- [ ] Frontend reads and applies filters; highlights active filters
- [ ] Tests cover success and edge cases; CI/local pass
- [ ] Manual E2E verified on sample tenant + job
