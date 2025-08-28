import React from 'react';
import ScoreBreakdown from './ScoreBreakdown';
import SkillsChips from './SkillsChips';
import DistanceCell from './DistanceCell';

interface Props {
	data: any;
}

const SummaryStat: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
	<div>
		<div className="text-muted small">{label}</div>
		<div style={{ fontWeight: 600 }}>{value}</div>
	</div>
);

const MatchDetailsPanel: React.FC<Props> = ({ data }) => {
	if (!data) return null;
	const overlap: string[] = data.skill_overlap || [];
	const candidateOnly: string[] = data.candidate_only_skills || [];
	const jobOnly: string[] = data.job_only_skills || [];
	const mustRatio = data.must_ratio ?? 0;
	const neededRatio = data.needed_ratio ?? 0;

	const mustMet = Math.round((mustRatio || 0) * 100);
	const neededMet = Math.round((neededRatio || 0) * 100);

	return (
		<div style={{ display: 'grid', gap: 12 }}>
			<div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 12 }}>
				<SummaryStat label="Score" value={(data.score * 100).toFixed(1) + '%'} />
				<SummaryStat label="Must-haves" value={`${mustMet}%`} />
				<SummaryStat label="Needed" value={`${neededMet}%`} />
				<SummaryStat label="Distance" value={<DistanceCell km={data.distance_km} />} />
			</div>

			<div className="card p-3">
				<h6 className="mb-2">Skills</h6>
				<SkillsChips overlap={overlap} candidateOnly={candidateOnly} jobOnly={jobOnly} max={12} />
			</div>

			<div className="card p-3">
				<h6 className="mb-2">Breakdown</h6>
				<ScoreBreakdown title={data.title_similarity} semantic={data.semantic_similarity} embedding={data.embedding_similarity} skills={data.weighted_skill_score} />
			</div>
		</div>
	);
};

export default MatchDetailsPanel;

