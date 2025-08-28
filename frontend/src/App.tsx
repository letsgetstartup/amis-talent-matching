import React, { useEffect, useState } from 'react';
import { fetchCandidates, fetchJobs, fetchMatchesForCandidate, explainMatch, fetchCandidateDetail, fetchJobDetail, fetchWeights, fetchLLMStatus } from './api';
import CandidateMatches from './components/CandidateMatches';
import LatestCandidateView from './components/LatestCandidateView';
import ShareCandidateView from './components/ShareCandidateView';
import ErrorBoundary from './components/ErrorBoundary';
import MatchExplainModal from './components/MatchExplainModal';
import SearchPanel from './components/SearchPanel';
import ConfigPanel from './components/ConfigPanel';
import IngestPanel from './components/IngestPanel';
import CandidateUpload from './components/CandidateUpload';
import MatchingPage from './components/MatchingPage';

interface ExplainData { score:number; skill_overlap:string[]; candidate_only_skills:string[]; job_only_skills:string[]; title_similarity:number; weighted_skill_score:number; distance_km?:number|null; distance_score?:number|null; }

const App: React.FC = () => {
  const [candidateIds, setCandidateIds] = useState<string[]>([]);
  const [jobIds, setJobIds] = useState<string[]>([]);
  const [selectedCandidate, setSelectedCandidate] = useState<string | null>(null);
  const [matches, setMatches] = useState<any[]>([]);
  const [candidateDetail, setCandidateDetail] = useState<any|null>(null);
  const [jobDetail, setJobDetail] = useState<any|null>(null); // last explained job
  const [weights, setWeights] = useState<any|null>(null);
  const [llmStatus, setLlmStatus] = useState<any|null>(null);
  const [dark, setDark] = useState<boolean>(false);
  const [explain, setExplain] = useState<ExplainData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(()=>{ (async()=>{
    try {
  const c = await fetchCandidates(0,50); setCandidateIds(c.candidates);
      const j = await fetchJobs(0,50); setJobIds(j.jobs);
  const w = await fetchWeights(); setWeights(w);
  try { const ls = await fetchLLMStatus(); setLlmStatus(ls); } catch{}
    } catch(e:any){ setError(e.message); }
  })(); },[]);

  async function loadMatches(candId: string){
    setLoading(true); setError(null);
    try { const r = await fetchMatchesForCandidate(candId,5); setMatches(r.matches); setSelectedCandidate(candId); const detail = await fetchCandidateDetail(candId); setCandidateDetail(detail);} catch(e:any){ setError(e.message);} finally { setLoading(false);} }

  async function handleExplain(jobId: string){ if(!selectedCandidate) return; try { const data = await explainMatch(selectedCandidate, jobId); setExplain(data); const jd = await fetchJobDetail(jobId); setJobDetail(jd);} catch(e:any){ setError(e.message);} }

  // Apply dark class to body
  if(typeof document !== 'undefined'){ document.body.classList.toggle('dark', dark); }
  const hash = typeof window !== 'undefined' ? window.location.hash : '';
  if(hash === '#matching'){
    return <div className="container py-4" style={{direction:'rtl'}}>
      <div className="mb-3 d-flex gap-2">
        <a href="#" className="btn btn-sm btn-outline-secondary">לראשי</a>
        <a href="#latest" className="btn btn-sm btn-outline-secondary">מועמד אחרון</a>
      </div>
      <MatchingPage />
    </div>;
  }
  if(hash === '#latest'){
    return <div className="container py-4" style={{direction:'rtl'}}>
      <div className="mb-3 d-flex gap-2">
        <a href="#" className="btn btn-sm btn-outline-secondary">לראשי</a>
      </div>
      <ErrorBoundary><LatestCandidateView /></ErrorBoundary>
    </div>;
  }
  if(hash.startsWith('#share/')){
    const sid = hash.split('/')[1];
    return <div className="container py-4" style={{direction:'rtl'}}>
      <div className="mb-3 d-flex gap-2">
        <a href="#" className="btn btn-sm btn-outline-secondary">לראשי</a>
        <a href="#latest" className="btn btn-sm btn-outline-secondary">מועמד אחרון</a>
      </div>
      <ErrorBoundary><ShareCandidateView shareId={sid} /></ErrorBoundary>
    </div>;
  }
  return <div className="container py-4" style={{direction:'rtl'}}>
  <h2 className="mb-4">התאמת משרות (React)</h2>
    <div className="mb-2 d-flex gap-3 small">
      <a href="#latest">⟵ מועמד אחרון</a>
      <span className="text-muted">שיתוף: לאחר קליטת קו"ח יופיע share_id בתגובה /ingest</span>
    </div>
    <div className="mb-3 d-flex gap-2">
      <button className="btn btn-sm btn-outline-secondary" onClick={()=>setDark(d=>!d)}>{dark? 'מצב בהיר' : 'מצב כהה'}</button>
      <button className="btn btn-sm btn-outline-secondary" onClick={async()=>{ try { const ls = await fetchLLMStatus(); setLlmStatus(ls);} catch(e:any){ setError(e.message);} }}>רענן LLM</button>
    </div>
    {error && <div className="alert alert-danger">שגיאה: {error}</div>}
    <div className="row g-3">
      <div className="col-md-3">
        <CandidateUpload onComplete={()=>{ /* refresh candidate list */ fetchCandidates(0,50).then(c=>setCandidateIds(c.candidates)); }} />
        <div className="card mb-3 mt-3">
          <div className="card-header">קורות חיים</div>
          <div className="card-body" style={{maxHeight: '30vh', overflowY:'auto'}}>
            {candidateIds.map(id=> <button key={id} className={`btn btn-sm w-100 mb-2 ${selectedCandidate===id?'btn-primary':'btn-outline-primary'}`} onClick={()=>loadMatches(id)}>{id.slice(-6)}</button>)}
          </div>
        </div>
        {candidateDetail && <div className="card mb-3" style={{maxHeight:'40vh', overflowY:'auto'}}>
          <div className="card-header">פרטי מועמד</div>
          <div className="card-body small">
            <div><strong>{candidateDetail.title}</strong></div>
            <div className="text-muted">עיר: {candidateDetail.city || '—'}</div>
            <div className="mt-2"><strong>כישורים:</strong></div>
            <div style={{display:'flex', flexWrap:'wrap', gap:4}}>
              {candidateDetail.skills.slice(0,40).map((s:string)=><span key={s} className="badge text-bg-light border">{s}</span>)}
            </div>
          </div>
        </div>}
  {weights && <ConfigPanel initial={weights} onUpdate={(w)=>{ setWeights(w); if(selectedCandidate) loadMatches(selectedCandidate); }} />}
  <IngestPanel onDone={()=>{ if(selectedCandidate) loadMatches(selectedCandidate); }} />
      </div>
      <div className="col-md-6">
        <CandidateMatches matches={matches} loading={loading} onExplain={handleExplain} />
      </div>
      <div className="col-md-3">
        <SearchPanel onPickJob={(jobId: string)=>handleExplain(jobId)} />
        {jobDetail && <div className="card mt-3 small" style={{maxHeight:'35vh', overflowY:'auto'}}>
          <div className="card-header">משרה</div>
          <div className="card-body">
            <div><strong>{jobDetail.title}</strong></div>
            <div className="text-muted">עיר: {jobDetail.city || '—'}</div>
            <div className="mt-2"><strong>כישורים:</strong></div>
            <div style={{display:'flex', flexWrap:'wrap', gap:4}}>
              {jobDetail.skills.slice(0,40).map((s:string)=><span key={s} className="badge bg-light text-dark border">{s}</span>)}
            </div>
          </div>
        </div>}
        {llmStatus && <div className="card mt-3 small" style={{maxHeight:'30vh', overflowY:'auto'}}>
          <div className="card-header">LLM סטטוס</div>
          <div className="card-body">
            {Object.entries(llmStatus).map(([k,v])=> <div key={k}>{k}: {typeof v==='object'? JSON.stringify(v): String(v)}</div>)}
          </div>
        </div>}
      </div>
    </div>
    <MatchExplainModal data={explain} onClose={()=>setExplain(null)} />
  </div>;
};
export default App;
