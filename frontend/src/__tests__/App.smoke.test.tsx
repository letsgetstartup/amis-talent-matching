import { render, screen } from '@testing-library/react';
import App from '../App';
import React from 'react';

vi.mock('../../api', () => ({
  fetchCandidates: async () => ({ candidates: [], skip:0, limit:50, total:0 }),
  fetchJobs: async () => ({ jobs: [], skip:0, limit:50, total:0 }),
  fetchWeights: async () => ({ skill_weight:1, title_weight:0, semantic_weight:0, embedding_weight:0, distance_weight:0, must_category_weight:0.7, needed_category_weight:0.3 }),
  fetchLLMStatus: async () => ({ llm_attempted:0 }),
  fetchMatchesForCandidate: async () => ({ matches:[] }),
  explainMatch: async () => ({}),
  fetchCandidateDetail: async () => ({}),
  fetchJobDetail: async () => ({}),
}));

describe('App smoke', () => {
  it('renders header', async () => {
    render(<App />);
    expect(await screen.findByText(/התאמת משרות/i)).toBeInTheDocument();
  });
});
