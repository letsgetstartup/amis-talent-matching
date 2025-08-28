import React from 'react';

interface Props { data: any | null; onClose: ()=>void; }

const MatchExplainModal: React.FC<Props> = ({ data, onClose }) => {
  if(!data) return null;
  return <div className="modal d-block" tabIndex={-1} style={{background:'rgba(0,0,0,0.5)'}}>
    <div className="modal-dialog modal-lg" dir="rtl">
      <div className="modal-content">
        <div className="modal-header">
          <h5 className="modal-title">פירוט התאמה</h5>
          <button type="button" className="btn-close" onClick={onClose}></button>
        </div>
        <div className="modal-body">
          <div className="row">
            <div className="col-md-6">
              <h6>ציון כולל: {data.score}</h6>
              <p>כישורים חופפים: {(data.skill_overlap||[]).join(', ')||'—'}</p>
              <p>כישורים חסרים בקו"ח: {(data.job_only_skills||[]).join(', ')||'—'}</p>
              <p>כישורים רק בקו"ח: {(data.candidate_only_skills||[]).join(', ')||'—'}</p>
            </div>
            <div className="col-md-6 small">
              <p>דמיון כותרת: {data.title_similarity}</p>
              <p>מרחק (ק"מ): {data.distance_km ?? 'N/A'} (ציון מרחק: {data.distance_score ?? '—'})</p>
              <p>ציון מיומנות משוקלל: {data.weighted_skill_score}</p>
            </div>
          </div>
        </div>
        <div className="modal-footer">
          <button className="btn btn-secondary" onClick={onClose}>סגור</button>
        </div>
      </div>
    </div>
  </div>;
};
export default MatchExplainModal;
