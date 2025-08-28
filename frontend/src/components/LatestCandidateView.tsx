import React, { useEffect, useState } from 'react';
import { fetchCandidateDetail, fetchMatchesForCandidate } from '../api';

// Derive API base similarly to api.ts (avoid direct import.meta usage issues in isolated builds)
const API_BASE = (import.meta as any)?.env?.VITE_API_BASE || 'http://127.0.0.1:8001';

const POLL_INTERVAL_MS = 0; // set >0 to auto-refresh

const LatestCandidateView: React.FC = () => {
  const [candidateId, setCandidateId] = useState<string|undefined>();
  const [candidate, setCandidate] = useState<any|undefined>();
  const [matches, setMatches] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string|null>(null);

  async function loadLatest(){
    setError(null);
    try {
  const r = await fetch(`${API_BASE}/candidates/latest`).then(r=>r.json());
      if(r.candidate){
        setCandidateId(r.candidate);
        setLoading(true);
        const [cand, m] = await Promise.all([
          fetchCandidateDetail(r.candidate),
          fetchMatchesForCandidate(r.candidate, 10)
        ]);
        setCandidate(cand);
        setMatches(m.matches || []);
      } else {
        setCandidateId(undefined);
      }
    } catch(e:any){ setError(e.message); }
    finally { setLoading(false); }
  }

  useEffect(()=>{ loadLatest(); if(POLL_INTERVAL_MS>0){ const t=setInterval(loadLatest, POLL_INTERVAL_MS); return ()=>clearInterval(t);} },[]);

  if(error) return <div className="alert alert-danger small">שגיאה: {error}</div>;
  if(!candidateId) return <div>אין מועמדים</div>;
  return <div style={{direction:'rtl'}} className="small">
    <h4 className="mb-3">מועמד אחרון</h4>
    {loading && <div>טוען...</div>}
    {candidate && <div className="card mb-3">
      <div className="card-header">{candidate.title || 'ללא כותרת'}</div>
      <div className="card-body" style={{maxHeight:160, overflowY:'auto'}}>
        <div className="text-muted">{candidateId}</div>
        <div className="mt-2 d-flex flex-wrap gap-1">
          {candidate.skills.slice(0,40).map((s:string)=> <span key={s} className="badge bg-secondary-subtle text-dark border">{s}</span>)}
        </div>
      </div>
    </div>}
    <div className="card">
      <div className="card-header">התאמות מובילות</div>
      <ul className="list-group list-group-flush">
        {matches.map(m=> <li key={m.job_id} className="list-group-item d-flex justify-content-between align-items-start">
          <div>
            <div><strong>{m.title || m.job_id}</strong></div>
            <div className="small text-muted">חפיפה: {(m.skill_overlap||[]).join(', ')||'—'}</div>
          </div>
          <span className="badge text-bg-primary">{(m.score*100).toFixed(1)}%</span>
        </li>)}
        {matches.length===0 && <li className="list-group-item">אין התאמות</li>}
      </ul>
    </div>
  </div>;
};
export default LatestCandidateView;
