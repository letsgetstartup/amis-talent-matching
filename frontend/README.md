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
