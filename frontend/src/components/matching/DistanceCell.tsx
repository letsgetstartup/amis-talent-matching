import React from 'react';

interface Props { km?: number | null; remote?: boolean }

const DistanceCell: React.FC<Props> = ({ km, remote }) => {
	if (remote) return <span className="badge bg-primary">Remote</span>;
	if (km == null) return <span className="text-muted">—</span>;
	const rounded = km < 1 ? `${(km * 1000).toFixed(0)} m` : `${km.toFixed(1)} km`;
	const color = km <= 10 ? '#2e7d32' : km <= 30 ? '#f39c12' : '#c62828';
	return <span title={`${km.toFixed(3)} km`} style={{ color }}>{rounded}</span>;
};

export default DistanceCell;

