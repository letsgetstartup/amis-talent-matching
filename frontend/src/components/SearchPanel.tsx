import React, { useState } from 'react';
import { searchJobs } from '../api';

interface Props { onPickJob: (jobId: string)=>void; }

const SearchPanel: React.FC<Props> = ({ onPickJob }) => {
  const [skill, setSkill] = useState('');
  const [city, setCity] = useState('');
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  async function runSearch(){
    setLoading(true);
    try {
      const params: any = { limit:10, sort_by:'matched', city: city || undefined };
      if(skill.includes(',')) params.skills = skill; else params.skill = skill;
      const r = await searchJobs(params); setResults(r.results);
    } finally { setLoading(false);} }

  return <div className="card h-100">
    <div className="card-header">חיפוש משרות</div>
    <div className="card-body" style={{maxHeight:'60vh', overflowY:'auto'}}>
      <div className="mb-2">
  <input className="form-control" placeholder="כישורים (פסיק להפרדה)" value={skill} onChange={e=>setSkill(e.target.value)} />
      </div>
      <div className="mb-2">
  <input className="form-control" placeholder="עיר" value={city} onChange={e=>setCity(e.target.value)} />
      </div>
      <button className="btn btn-primary w-100 mb-3" onClick={runSearch} disabled={loading}>חפש</button>
      {loading && <div>טוען...</div>}
      {results.map(r => <div key={r.job_id} className="border rounded p-2 mb-2">
        <div><strong>{r.title || r.job_id?.slice(-6)}</strong></div>
        <div className="small text-muted">{r.city}</div>
        <div className="small">התאמות: {r.matched_skills.length + r.matched_esco.length}</div>
        <button className="btn btn-sm btn-outline-secondary mt-1" onClick={()=>onPickJob(r.job_id)}>הסבר מול מועמד נבחר</button>
      </div>)}
    </div>
  </div>;
};
export default SearchPanel;
