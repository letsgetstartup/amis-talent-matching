import React, { useState } from 'react';
import { ingest } from '../api';

interface Props { onDone: ()=>void; }

const IngestPanel: React.FC<Props> = ({ onDone }) => {
  const [text, setText] = useState('');
  const [kind, setKind] = useState<'candidate'|'job'>('candidate');
  const [filename, setFilename] = useState('inline.txt');
  const [apiKey, setApiKey] = useState('');
  const [forceLLM, setForceLLM] = useState(true);
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState<string|null>(null);

  async function submit(){
    if(!text.trim()) return;
    setLoading(true); setMsg(null);
    try {
      const res = await ingest(kind, text, filename || `${kind}_inline.txt`, forceLLM, apiKey || undefined);
      setMsg(`הועלה (${res.ingested.length})`);
      setText('');
      onDone();
    } catch(e:any){ setMsg('שגיאה: '+e.message); }
    finally { setLoading(false);} }

  return <div className="card mt-3 small">
    <div className="card-header">העלאה</div>
    <div className="card-body">
      <div className="mb-2 d-flex gap-2">
        <select className="form-select form-select-sm w-auto" value={kind} onChange={e=>setKind(e.target.value as any)}>
          <option value="candidate">קורות חיים</option>
          <option value="job">משרה</option>
        </select>
        <input className="form-control form-control-sm" placeholder="שם קובץ" value={filename} onChange={e=>setFilename(e.target.value)} />
      </div>
      <textarea className="form-control mb-2" rows={5} placeholder="טקסט להעלאה" value={text} onChange={e=>setText(e.target.value)}></textarea>
      <div className="form-check form-switch mb-2">
        <input className="form-check-input" type="checkbox" id="forceLLM" checked={forceLLM} onChange={e=>setForceLLM(e.target.checked)} />
        <label className="form-check-label" htmlFor="forceLLM">Force LLM</label>
      </div>
      <input className="form-control form-control-sm mb-2" placeholder="API Key (אופציונלי)" value={apiKey} onChange={e=>setApiKey(e.target.value)} />
      <button className="btn btn-sm btn-success w-100" disabled={loading} onClick={submit}>{loading? 'מעלה...' : 'העלה'}</button>
      {msg && <div className="mt-2 small">{msg}</div>}
    </div>
  </div>;
};

export default IngestPanel;
