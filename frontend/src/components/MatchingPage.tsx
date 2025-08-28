import React, { useEffect, useMemo, useState } from 'react';
import { fetchCandidates, fetchJobs, fetchMatchesForCandidate, fetchMatchesForJob, explainMatch, saveMatch } from '../api';
import ScoreBar from './matching/ScoreBar';
import DistanceCell from './matching/DistanceCell';
import MatchExplainModal from './MatchExplainModal';

interface Row {
  id: string;
  title?: string;
  city?: string;
  score: number;
  distance_km?: number | null;
}

export default function MatchingPage(){
  const [mode, setMode] = useState<'candidate'|'job'>('candidate');
  const [items, setItems] = useState<any[]>([]);
  const [selectedId, setSelectedId] = useState<string>('');
  const [matches, setMatches] = useState<any[]>([]);
  const [k, setK] = useState<number>(10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [explains, setExplains] = useState<Record<string, any>>({}); // key by pair id
  const [details, setDetails] = useState<any|null>(null);
  const [sortBy, setSortBy] = useState<{key:'score'|'distance_km'|'title'|null; dir:'asc'|'desc'}>({key:'score', dir:'desc'});

  useEffect(()=>{
    const load = async ()=>{
      try{
        setError(null);
        if(mode==='candidate'){
          const r = await fetchCandidates(0, 50);
          setItems(r.candidates || []);
        } else {
          const r = await fetchJobs(0, 50);
          setItems(r.jobs || []);
        }
      }catch(e:any){ setError(e.message || 'Load error'); }
    };
    load();
  }, [mode]);

  const canLoad = useMemo(()=>Boolean(selectedId), [selectedId]);

  async function loadMatches(){
    if(!selectedId) return;
    setLoading(true);
    setError(null);
    try{
      const r = mode==='candidate' ? await fetchMatchesForCandidate(selectedId, k) : await fetchMatchesForJob(selectedId, k);
      setMatches(r.matches || []);
      setExplains({}); // clear cache when new list is loaded
    }catch(e:any){
      const status = e?.response?.status; const detail = e?.response?.data?.detail;
      if(status===404){
        setError(`ID not found (${selectedId}). Choose a valid ${mode==='candidate'?'candidate':'job'} from the list.`);
      } else {
        setError(detail || e.message || 'Failed to load matches');
      }
    }
    finally{ setLoading(false); }
  }

  function pairKey(targetId: string){
    const candId = mode==='candidate' ? selectedId : targetId;
    const jobId = mode==='candidate' ? targetId : selectedId;
    return `${candId}|${jobId}`;
  }

  async function loadExplainFor(targetId: string){
    const candId = mode==='candidate' ? selectedId : targetId;
    const jobId  = mode==='candidate' ? targetId : selectedId;
    const key = `${candId}|${jobId}`;
    if(explains[key]) return explains[key];
    try{
      const r = await explainMatch(candId, jobId);
      setExplains(prev=>({ ...prev, [key]: r }));
      return r;
    }catch(e:any){
      const status = e?.response?.status; const detail = e?.response?.data?.detail;
      setError(status===404 ? 'Explain failed (pair not found).' : (detail || e.message || 'Explain error'));
      throw e;
    }
  }

  return (
    <div style={{ padding: 16 }}>
      <h2>Matching</h2>
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap:'wrap' }}>
        <label>
          Mode:
          <select value={mode} onChange={e=>{ setMode(e.target.value as any); setSelectedId(''); setMatches([]);} }>
            <option value="candidate">Candidate → Jobs</option>
            <option value="job">Job → Candidates</option>
          </select>
        </label>
        <label>
          ID:
          <input value={selectedId} list="id-list" onChange={e=>setSelectedId(e.target.value)} placeholder={mode==='candidate' ? 'candidate_id' : 'job_id'} style={{ width: 320 }}/>
          <datalist id="id-list">
            {items.slice(0,200).map((id)=> <option key={id} value={id}>{id}</option>)}
          </datalist>
        </label>
        <label>
          Top K:
          <input type="number" value={k} min={1} max={50} onChange={e=>setK(parseInt(e.target.value||'10',10))} style={{ width: 80 }}/>
        </label>
        <button disabled={!canLoad || loading} onClick={loadMatches}>Load Matches</button>
        {items.length>0 && !selectedId && <button className="btn btn-sm btn-outline-secondary" onClick={()=>setSelectedId(items[0])}>Use first {mode}</button>}
      </div>

      {error && <div style={{ color: 'red', marginTop: 8 }}>Error: {error}. Verify the {mode==='candidate'?'candidate':'job'} ID exists or pick from the suggestions.</div>}

      <div style={{ marginTop: 16 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', cursor:'pointer' }} onClick={()=>setSortBy(s=>({ key:'title', dir: s.dir==='asc'?'desc':'asc' }))}>{mode==='candidate' ? 'Job' : 'Candidate'}</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', width: 200, cursor:'pointer' }} onClick={()=>setSortBy(s=>({ key:'score', dir: s.dir==='asc'?'desc':'asc' }))}>Score</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', cursor:'pointer' }} onClick={()=>setSortBy(s=>({ key:'distance_km', dir: s.dir==='asc'?'desc':'asc' }))}>Distance</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd' }}>Must-haves</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd' }}>Skills</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {[...matches].sort((a:any,b:any)=>{
              if(!sortBy.key) return 0;
              const dir = sortBy.dir==='asc' ? 1 : -1;
              if(sortBy.key==='title'){
                const av = (a.title||'') as string; const bv = (b.title||'') as string;
                return av.localeCompare(bv) * dir;
              }
              const av = a[sortBy.key] ?? -Infinity; const bv = b[sortBy.key] ?? -Infinity;
              return (av===bv?0: av>bv?1:-1) * dir;
            }).map((m:any)=>{
              const id = mode==='candidate' ? m.job_id : m.candidate_id;
              const key = pairKey(id);
              const ex = explains[key];
              return (
                <tr key={id}>
                  <td style={{ padding: '8px 4px' }}>
                    <div style={{ display:'grid' }}>
                      <div style={{ fontWeight:600 }}>{m.title || id}</div>
                      <div className="text-muted small">{id}</div>
                    </div>
                  </td>
                  <td style={{ padding: '8px 4px' }}>
                    <ScoreBar value={m.score || 0} tooltip={ex ? `title:${ex.title_similarity??'—'} semantic:${ex.semantic_similarity??'—'} skills:${ex.weighted_skill_score??'—'}` : 'Click Details for breakdown'} />
                  </td>
                  <td style={{ padding: '8px 4px' }}>
                    <DistanceCell km={m.distance_km} />
                  </td>
                  <td style={{ padding: '8px 4px' }}>
                    {ex ? (
                      (()=>{
                        const must = (ex?.must_ratio ?? 0);
                        const needed = (ex?.needed_ratio ?? 0);
                        const cls = must>=1 ? 'bg-success' : must>0 ? 'bg-warning text-dark' : 'bg-danger';
                        const title = `must: ${(must*100).toFixed(0)}%, needed: ${(needed*100).toFixed(0)}%`;
                        return <span className={`badge ${cls}`} title={title}>{(must*100).toFixed(0)}%</span>;
                      })()
                    ) : (
                      <button className="btn btn-sm btn-outline-secondary" onClick={async()=>{ const d = await loadExplainFor(id); setDetails(d); }}>Load</button>
                    )}
                  </td>
                  <td style={{ padding: '8px 4px' }}>
                    {ex ? <span className="text-muted small">overlap {ex.skill_overlap?.length||0} • missing {ex.job_only_skills?.length||0}</span>
                        : <span className="text-muted small">—</span>}
                  </td>
                  <td style={{ padding: '8px 4px' }}>
                    <button className="btn btn-sm btn-outline-primary" onClick={async()=>{
                      try{
                        const direction = mode==='candidate' ? 'c2j' : 'j2c';
                        const source_id = selectedId; const target_id = id;
                        await saveMatch(direction as any, source_id, target_id, 'saved');
                      }catch(e:any){ alert('Save failed: '+(e.message||'error')); }
                    }}>Save</button>
                    <button className="btn btn-sm btn-primary" style={{ marginLeft: 6 }} onClick={async()=>{
                      try{
                        const d = await loadExplainFor(id);
                        setDetails(d);
                      }catch(e:any){ /* handled in loadExplainFor */ }
                    }}>Details</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <MatchExplainModal data={details} onClose={()=>setDetails(null)} />
    </div>
  );
}
