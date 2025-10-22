# Dynamic Portal QA Checklist (macOS)

Use this quick script after deploying or making changes to the dynamic portal page.

## Setup

1. `cd frontend`
2. `npm install` (first run only)
3. `npm run dev`
4. Open `http://localhost:5173/portal/dynamic/demo?q=react&location=Tel%20Aviv`

## Smoke flow

- [ ] Page loads with matching tenant data and no console errors.
- [ ] URL query string remains canonicalized (`?q=react&location=Tel+Aviv`).
- [ ] Filters hydrate: "Search" input reads `react`, Location select set to `Tel Aviv`.
- [ ] Jobs list filtered accordingly and count badge updates.

## Filter interactions

- [ ] Typing in the search input updates the URL within 300 ms and filters results.
- [ ] Changing the Location dropdown rewrites the URL immediately.
- [ ] Changing Company preserves existing query params.
- [ ] Toggling a skill chip adds/removes `skills=` in the query string, respects multiple selections.
- [ ] Selecting "Remote" adds `type=remote` and filters remote roles only.
- [ ] Clicking "Clear skills" removes `skills` while leaving other params intact.

## Navigation

- [ ] Refreshing the page keeps the current filters active.
- [ ] Browser back/forward updates both the UI controls and job list.
- [ ] Copy/paste the URL into a new tab reproduces the same filtered view.

## Production sanity (optional)

1. `npm run build`
2. `npm run preview`
3. Repeat the smoke flow and filter checks against the preview server (`http://localhost:4173`).

Document any failures with the exact URL and UI state before filing a bug.
