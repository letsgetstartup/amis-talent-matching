import React, { useEffect, useState } from 'react';
import { fetchShareCandidate, ShareResponse } from '../api';

const ShareCandidateView: React.FC<{shareId: string}> = ({ shareId }) => {
  const [data, setData] = useState<ShareResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string|null>(null);

  useEffect(()=>{ (async()=>{
    setLoading(true); setError(null);
    try { const r = await fetchShareCandidate(shareId, 12); setData(r); }
    catch(e:any){ setError(e.message); }
    finally { setLoading(false); }
  })(); }, [shareId]);

  if(loading) return <div>טוען...</div>;
  if(error) return <div className="alert alert-danger">שגיאה: {error}</div>;
  if(!data) return <div>לא נמצא</div>;
  const cand = data.candidate;
  return <div style={{direction:'rtl'}} className="small">
    <h4 className="mb-3">שיתוף מועמד</h4>
    <div className="card mb-3">
      <div className="card-header d-flex justify-content-between align-items-center">
        <span>{cand.title || 'ללא כותרת'}</span>
        <span className="badge text-bg-secondary">{(cand.skills||[]).length} כישורים</span>
      </div>
      <div className="card-body" style={{maxHeight:180, overflowY:'auto'}}>
        <div className="text-muted small">ID: {cand.candidate_id}</div>
        <div className="mt-2 d-flex flex-wrap gap-1">
          {cand.skills.slice(0,60).map(s=> <span key={s} className="badge bg-light text-dark border">{s}</span>)}
        </div>
      </div>
    </div>
    <div className="card">
      <div className="card-header">התאמות</div>
      <ul className="list-group list-group-flush">
        {data.matches.map(m => <li key={m.job_id} className="list-group-item">
          <div className="d-flex justify-content-between align-items-start">
            <div style={{maxWidth:'75%'}}>
              <div><strong>{m.title || m.job_id}</strong></div>
              <div className="small text-muted">חפיפה: {(m.skill_overlap||[]).slice(0,8).join(', ') || '—'}</div>
              {m.reason && <div className="small" style={{color:'#0a58ca'}}>{m.reason}</div>}
              {m.job_only_skills && m.job_only_skills.length>0 && <div className="small text-muted">חסר: {m.job_only_skills.slice(0,5).join(', ')}</div>}
            </div>
            <span className="badge text-bg-primary">{(m.score*100).toFixed(1)}%</span>
          </div>
        </li>)}
        {data.matches.length===0 && <li className="list-group-item">אין התאמות</li>}
      </ul>
    </div>
  </div>;
};
export default ShareCandidateView;
