import React, { useState } from 'react';
import { updateAllConfig } from '../api';

interface Props { initial: any; onUpdate: (w:any)=>void; }

const numberField = (v:any)=> typeof v === 'number' ? v : 0;

const ConfigPanel: React.FC<Props> = ({ initial, onUpdate }) => {
  const [skillWeight, setSkillWeight] = useState(numberField(initial.skill_weight));
  const [titleWeight, setTitleWeight] = useState(numberField(initial.title_weight));
  const [semanticWeight, setSemanticWeight] = useState(numberField(initial.semantic_weight));
  const [embedWeight, setEmbedWeight] = useState(numberField(initial.embedding_weight));
  const [mustWeight, setMustWeight] = useState(numberField(initial.must_category_weight));
  const [neededWeight, setNeededWeight] = useState(numberField(initial.needed_category_weight));
  const [distanceWeight, setDistanceWeight] = useState(numberField(initial.distance_weight));
  const [minSkillFloor, setMinSkillFloor] = useState<number>(initial.min_skill_floor || 0);
  const [saving, setSaving] = useState(false);
  const [apiKey, setApiKey] = useState('');
  const total = skillWeight + titleWeight + semanticWeight + embedWeight + distanceWeight;

  async function save(){
    setSaving(true);
    try {
      const weights = await updateAllConfig({
        skill_weight: skillWeight,
        title_weight: titleWeight,
        semantic_weight: semanticWeight,
        embed_weight: embedWeight,
        must_weight: mustWeight,
        needed_weight: neededWeight,
        distance_weight: distanceWeight,
        min_skill_floor: minSkillFloor
      }, apiKey || undefined);
      onUpdate(weights);
    } catch(e){ /* ignore for now*/ }
    finally { setSaving(false);} }

  return <div className="card small mt-3">
    <div className="card-header d-flex justify-content-between align-items-center">
      <span>תצורה</span>
      <span className="badge text-bg-secondary">Σ={total.toFixed(2)}</span>
    </div>
    <div className="card-body" style={{maxHeight:'50vh', overflowY:'auto'}}>
      <div className="mb-2">
        <label className="form-label">API Key</label>
        <input className="form-control form-control-sm" value={apiKey} onChange={e=>setApiKey(e.target.value)} placeholder="(אופציונלי)"/>
      </div>
      {[
        {label:'Skill Weight', val:skillWeight, set:setSkillWeight, min:0, max:1, step:0.01},
        {label:'Title Weight', val:titleWeight, set:setTitleWeight, min:0, max:1, step:0.01},
        {label:'Semantic Weight', val:semanticWeight, set:setSemanticWeight, min:0, max:1, step:0.01},
        {label:'Embed Weight', val:embedWeight, set:setEmbedWeight, min:0, max:1, step:0.01},
        {label:'Distance Weight', val:distanceWeight, set:setDistanceWeight, min:0, max:1, step:0.01},
      ].map(r=> <div key={r.label} className="mb-2">
        <label className="form-label d-flex justify-content-between"><span>{r.label}</span><span>{r.val.toFixed(2)}</span></label>
        <input type="range" className="form-range" min={r.min} max={r.max} step={r.step} value={r.val} onChange={e=>r.set(parseFloat(e.target.value))} />
      </div>)}
      <div className="row g-2 mb-2">
        <div className="col-6">
          <label className="form-label">Must Weight</label>
          <input type="number" className="form-control form-control-sm" value={mustWeight} onChange={e=>setMustWeight(parseFloat(e.target.value)||0)} />
        </div>
        <div className="col-6">
          <label className="form-label">Needed Weight</label>
          <input type="number" className="form-control form-control-sm" value={neededWeight} onChange={e=>setNeededWeight(parseFloat(e.target.value)||0)} />
        </div>
      </div>
      <div className="mb-3">
        <label className="form-label">Min Skill Floor</label>
        <input type="number" className="form-control form-control-sm" value={minSkillFloor} onChange={e=>setMinSkillFloor(parseInt(e.target.value)||0)} />
      </div>
      <button className="btn btn-sm btn-primary w-100" disabled={saving} onClick={save}>{saving? 'שומר...' : 'שמור הגדרות'}</button>
    </div>
  </div>;
};

export default ConfigPanel;
