# Greenhouse Redirect – Manual Validation Checklist

Use this guide for Phase 6 end-to-end verification after deploying or running the feature locally. It consolidates the expected flow, data setup, and observable outcomes for the Greenhouse rejection email redirect experience.

## 1. Prerequisites

- Backend API running locally (`uvicorn talentdb.scripts.api:app --reload --port 8080`) or on the target environment.
- Frontend portal served via Vite dev server (`npm run dev -- --host 0.0.0.0 --port 5173`) or the compiled SPA served by the backend.
- Access to the MongoDB instance used by the backend. You can reuse the fixture logic from `tests/test_portal_redirect.py` as a reference for the expected document shapes.
- Browser with developer tools available (Chrome/Edge/Firefox) to inspect redirects and network responses.

## 2. Seed Verification Data (once per tenant)

1. Insert/update a tenant document with the slug you plan to test. Example:
   ```javascript
   db.tenants.insertOne({
     name: "Demo Tenant",
     slug: "demo-tenant",
     plan: "free"
   })
   ```
2. Insert at least two jobs for that tenant, ensuring one matches the Greenhouse external job ID you will exercise:
   ```javascript
   db.jobs.insertMany([
     {
       tenant_id: ObjectId("<TENANT_OBJECT_ID>"),
       external_job_id: "7390395003",
       job_title: "Full-Stack Engineer",
       city: "Tel Aviv",
       skill_set: ["React", "Node.js", "TypeScript"],
       is_open: true
     },
     {
       tenant_id: ObjectId("<TENANT_OBJECT_ID>"),
       external_job_id: "1234567",
       job_title: "QA Analyst",
       city: "Tel Aviv",
       skill_set: ["Cypress", "Python"],
       is_open: true
     }
   ])
   ```
3. If testing skills with special characters, add another job containing `C#` or `C++` within `skill_set`.

> **Tip:** Remove the test documents after validation to avoid polluting production collections.

## 3. Happy Path Redirect

1. Open a browser tab and visit:
   ```
   http://localhost:8080/portal/demo-tenant/redirect/https%3A%2F%2Fjob-boards.greenhouse.io%2Facme%2Fjobs%2F7390395003
   ```
2. Confirm the status code is **302** by inspecting the Network tab. The `Location` header should be:
   ```
   /portal/demo-tenant?location=Tel%20Aviv&skills=React,Node.js,TypeScript
   ```
3. Allow the browser to follow the redirect. On the portal page verify:
   - Location pill/button for **Tel Aviv** is highlighted.
   - Skill pills for **React**, **Node.js**, and **TypeScript** are highlighted.
   - Only the "Full-Stack Engineer" job card remains visible.
   - Browser URL matches the redirected query string (order of parameters may differ).
4. Copy the resulting portal URL, paste it into a new tab, and ensure the page loads with the same filters applied (shareable link regression check).

## 4. Edge Case Scenarios

| Scenario | Input URL | Expected Behavior |
| --- | --- | --- |
| Unknown tenant | `/portal/unknown-tenant/redirect/<encoded_url>` | 302 → `/portal/unknown-tenant`; portal shows empty/tenant-missing message. |
| Job missing for tenant | `/portal/demo-tenant/redirect/.../jobs/0000` | 302 → `/portal/demo-tenant` with no query parameters applied. |
| URL with query string | `/portal/demo-tenant/redirect/https%3A%2F%2Fjob-boards.greenhouse.io%2Facme%2Fjobs%2F7390395003%3Fgh_src%3Dreferral` | ID extracted correctly, same redirect as happy path. |
| URL with fragment | `/portal/demo-tenant/redirect/.../jobs/7390395003#details` | Fragment ignored; redirect identical to happy path. |
| Special characters in skills | Job `skill_set`: `["C#", "C++"]` | Redirect encodes to `skills=C%23,C%2B%2B`, portal highlights both skills and filters accordingly. |
| Multiple skills toggle | On portal, deselect one highlighted skill | Job list expands to include roles matching remaining filters; query string updates to reflect selection. |

## 5. Regression Checks

- Navigate to `/portal/demo-tenant` with no query params to ensure the full job catalog renders and existing search filters (keyword, company, job type) still operate as before.
- Open an unrelated route to verify no auth redirects or SPA fallback regressions were introduced.
- Review backend logs for any errors raised during redirect handling.

## 6. Recording Results

Capture the outcome for each scenario in the table below. Retain this record in your test management system or attach it to the deployment ticket.

| Scenario | Date | Tester | Result | Notes |
| --- | --- | --- | --- | --- |
| Happy path redirect |  |  |  |  |
| Unknown tenant |  |  |  |  |
| Missing job |  |  |  |  |
| Query string |  |  |  |  |
| Fragment |  |  |  |  |
| Special character skills |  |  |  |  |
| Skill toggle UX |  |  |  |  |

## 7. Cleanup

- Remove seeded tenants/jobs if they are only needed for QA.
- Shut down dev servers if no longer required.
- File any bugs with reproduction steps and observed vs expected behavior, referencing this checklist.

Once all scenarios pass, mark Phase 6 of the execution plan as complete and proceed with the release checklist.
