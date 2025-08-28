import React from 'react';

interface Props {
	overlap?: string[];
	candidateOnly?: string[];
	jobOnly?: string[];
	max?: number; // max chips per list before "+N" indicator
}

const ChipsList: React.FC<{ items: string[]; className: string; label?: string; max?: number }> = ({ items, className, label, max=8 }) => {
	if (!items?.length) return null;
	const shown = items.slice(0, max);
	const rest = items.length - shown.length;
	return (
		<div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
			{label && <span className="text-muted small" style={{ minWidth: 80 }}>{label}</span>}
			{shown.map(s => <span key={s} className={`skill-badge ${className}`}>{s}</span>)}
			{rest > 0 && <span className={`skill-badge ${className}`}>+{rest}</span>}
		</div>
	);
};

const SkillsChips: React.FC<Props> = ({ overlap=[], candidateOnly=[], jobOnly=[], max }) => {
	return (
		<div style={{ display: 'grid', gap: 6 }}>
			<ChipsList items={overlap} className="matched" label="Overlap" max={max} />
			<ChipsList items={candidateOnly} className="" label="Candidate only" max={max} />
			<ChipsList items={jobOnly} className="missing" label="Job only" max={max} />
		</div>
	);
};

export default SkillsChips;

