import React, { useState, useRef } from 'react';
import { ingest, fetchShareCandidate, uploadCandidate } from '../api';

interface Props { onComplete?: (shareId: string)=>void; }

const CandidateUpload: React.FC<Props> = ({ onComplete }) => {
  const [text, setText] = useState('');
  const [fileName, setFileName] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string|null>(null);
  const [shareId, setShareId] = useState<string|null>(null);
  const [jobs, setJobs] = useState<any[]>([]);
  const fileRef = useRef<File|null>(null);

  async function handleFile(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    if(!f) return;
    fileRef.current = f;
    setFileName(f.name);
    try { const txt = await f.text(); setText(txt); } catch(err:any){ setError(err.message); }
  }

  async function submit() {
    if(!text.trim() && !fileRef.current){ setError('הדבק טקסט של קו"ח או בחר קובץ'); return; }
    setLoading(true); setError(null); setJobs([]); setShareId(null);
    try {
      let sid: string | null = null;
      if(fileRef.current){
        const resp = await uploadCandidate(fileRef.current);
        sid = resp.share_id;
      } else {
        const resp = await ingest('candidate', text, fileName || 'cv.txt', true);
        sid = resp.share_id || (resp.candidate && resp.candidate.share_id);
      }
      if(!sid) throw new Error('share_id לא התקבל');
      setShareId(sid);
      if(onComplete) onComplete(sid);
      // poll share endpoint for matches
      let attempts = 0;
      while(attempts < 30){
        attempts++;
        try {
          const shareResp = await fetchShareCandidate(sid, 10);
          if(shareResp && shareResp.matches){
            setJobs(shareResp.matches);
            break;
          }
        } catch {}
        await new Promise(r=>setTimeout(r, 2000));
      }
    } catch(err:any){ setError(err.message); }
    finally { setLoading(false); }
  }

  return <div className="card">
    <div className="card-header">העלה קו"ח וקבל משרות</div>
    <div className="card-body small" style={{direction:'rtl'}}>
      <div className="mb-2">
        <input className="form-control form-control-sm" type="file" onChange={handleFile} />
      </div>
      <div className="mb-2">
        <textarea className="form-control form-control-sm" rows={6} placeholder="הדבק טקסט קורות חיים כאן" value={text} onChange={e=>setText(e.target.value)} />
      </div>
      <div className="d-flex gap-2 mb-2">
        <button disabled={loading} className="btn btn-sm btn-primary" onClick={submit}>{loading? 'מעלה...' : 'עבד והתאם'}</button>
        {shareId && <a className="btn btn-sm btn-outline-secondary" href={`#share/${shareId}`} target="_blank" rel="noreferrer">דף שיתוף ↗</a>}
      </div>
      {error && <div className="alert alert-danger py-1 px-2 mb-2">{error}</div>}
      {shareId && <div className="text-muted mb-2">share_id: <code>{shareId}</code></div>}
      {jobs.length>0 && <div>
        <div className="fw-bold mb-1">משרות מומלצות ({jobs.length}):</div>
        <ul className="list-unstyled small cv-jobs">
          {jobs.map(j=> <li key={j.job_id} className="mb-1">
            <span className="badge bg-primary me-2">{Math.round(j.score)}</span>
            <span>{j.job_id?.slice(-6)}</span>
            {j.reason && <span className="ms-2 text-muted">{j.reason}</span>}
          </li>)}
        </ul>
      </div>}
      {loading && <div className="text-muted">טוען/מעבד... (עד 40 שניות)</div>}
    </div>
  </div>;
};

export default CandidateUpload;
