import React from 'react';

interface Props {
	value: number; // 0..1
	tooltip?: string;
}

// Simple accessible horizontal progress bar for match score
const ScoreBar: React.FC<Props> = ({ value, tooltip }) => {
	const pct = Math.max(0, Math.min(1, value)) * 100;
	const color = pct >= 80 ? '#2e7d32' : pct >= 50 ? '#f39c12' : '#c62828';
	return (
		<div title={tooltip} aria-label={`Score ${pct.toFixed(1)}%`} style={{ minWidth: 140 }}>
			<div style={{
				background: '#e9ecef',
				borderRadius: 6,
				height: 12,
				overflow: 'hidden',
				position: 'relative'
			}}>
				<div style={{
					width: `${pct}%`,
					background: color,
					height: '100%'
				}} />
			</div>
			<div style={{ fontSize: 12, color: '#555', marginTop: 4 }}>{pct.toFixed(1)}%</div>
		</div>
	);
};

export default ScoreBar;

