import React from 'react';

interface Props {
	title?: number;
	semantic?: number;
	embedding?: number;
	skills?: number;
}

const Row: React.FC<{ label: string; value?: number }> = ({ label, value }) => {
	const pct = value == null ? 0 : Math.max(0, Math.min(1, value)) * 100;
	return (
		<div style={{ display: 'grid', gridTemplateColumns: '120px 1fr 44px', alignItems: 'center', gap: 8 }}>
			<div className="text-muted small">{label}</div>
			<div style={{ background: '#e9ecef', height: 8, borderRadius: 6 }}>
				<div style={{ width: `${pct}%`, height: '100%', background: '#667eea', borderRadius: 6 }} />
			</div>
			<div className="small" style={{ textAlign: 'right' }}>{pct ? pct.toFixed(0)+'%' : '—'}</div>
		</div>
	);
};

const ScoreBreakdown: React.FC<Props> = ({ title, semantic, embedding, skills }) => {
	return (
		<div style={{ display: 'grid', gap: 6 }}>
			<Row label="Title" value={title} />
			<Row label="Semantic" value={semantic} />
			<Row label="Embedding" value={embedding} />
			<Row label="Skills" value={skills} />
		</div>
	);
};

export default ScoreBreakdown;

