import React from 'react';

interface Props { matches: any[]; loading: boolean; onExplain: (jobId: string)=>void; }

const CandidateMatches: React.FC<Props> = ({ matches, loading, onExplain }) => {
  if (loading) return <div className="text-light">טוען התאמות...</div>;
  if (!matches.length) return <div className="text-light">אין תוצאות עדיין</div>;

  return (
    <div>
      <h4 className="text-white mb-3">משרות מתאימות</h4>
      {matches.map(m => {
        const overlap: string[] = (m.skill_overlap || []).slice(0, 20);
        return (
          <div key={m.job_id} className="job-card">
            <div className="d-flex justify-content-between align-items-start mb-2">
              <div style={{maxWidth:'70%'}}>
                <h6 className="mb-1" style={{fontWeight:700}}>{m.title || m.job_id?.slice(-6)}</h6>
                {m.distance_km != null && <div className="small text-muted">מרחק: {m.distance_km} ק"מ</div>}
              </div>
              <span className="match-percentage">{(m.score * 100).toFixed(1)}%</span>
            </div>
            <div className="skills-container">
              {overlap.length ? overlap.map(s => <span key={s} className="skill-badge matched">{s}</span>) : <span className="text-muted small">אין כישורים חופפים</span>}
            </div>
            <div className="mt-3 d-flex gap-2">
              <button className="btn btn-sm btn-primary flex-grow-1" onClick={() => onExplain(m.job_id)}>הסבר</button>
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default CandidateMatches;
