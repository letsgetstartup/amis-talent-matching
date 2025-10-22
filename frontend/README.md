# TalentDB React Frontend

Simple React (Vite + TS) interface to explore candidates, jobs, and matching explanations from the FastAPI backend.

## Features
- List candidates & pick one to view top job matches
- Show match score, overlapping skills, distance (if available)
- Explain modal with weighted components
- Job search by single skill & city with matched skill counts
- RTL + Hebrew labels, Bootstrap styling

## Getting Started
```
cd frontend
npm install
npm run dev
```
Default backend base: http://127.0.0.1:8001 (override with `VITE_API_BASE` in a `.env` file).

Create `.env`:
```
VITE_API_BASE=http://localhost:8001
```

Open http://localhost:5173

## Dynamic Portal Page

- Route: `/portal/dynamic/:slug`
- Mirrors the existing portfolio UI but keeps filters in sync with the browser URL for deep-linking and shareable searches.
- Unknown query string keys are preserved so the page can coexist with marketing tracking parameters.

### Supported query parameters

| Key | Description | Example |
| --- | --- | --- |
| `q` | Full-text search applied to title, company, description, and requirements. | `q=react%20engineer` |
| `location` | Exact job location; best-effort canonicalization against the portal data. | `location=Tel%20Aviv` |
| `company` | Exact company name filter; canonicalized against available companies. | `company=Acme` |
| `type` | Job modality (`remote` \| `onsite`). Any other value is ignored. | `type=remote` |
| `skills` | Comma-separated normalized skills (underscores and case-insensitive). | `skills=react,node_js` |

Defaults are omitted from the URL. Filters automatically sync back to the UI when navigating via the browser back/forward buttons or when loading a bookmarked URL.

## Production Build
```
npm run build
npm run preview
```

## Next Steps
- Authentication header (X-API-Key) input field
- Upload CV / job text directly from UI
- Pagination & better search filters (ESCO, multiple skills)
- Config sliders for weights & thresholds
