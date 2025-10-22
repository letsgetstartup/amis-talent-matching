import React from 'react';
import '@testing-library/jest-dom';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import PortalDynamicPage from '../pages/PortalDynamicPage';

const portalFixture = {
  name: 'Demo Tenant',
  slug: 'demo',
  stats: { job_count: 2, company_count: 2, location_count: 2 },
  jobs: [
    {
      job_id: 'job-1',
      title: 'React Engineer',
      company_name: 'Acme',
      description: 'Build UI components.',
      requirements: ['React', 'Node.js'],
      location: 'Tel Aviv',
      remote: true,
      application_url: 'https://example.com/apply/react',
      company_website: 'https://acme.example.com',
    },
    {
      job_id: 'job-2',
      title: 'Python Engineer',
      company_name: 'Beta',
      description: 'Work on backend services.',
      requirements: ['Python'],
      location: 'London',
      remote: false,
      application_url: 'https://example.com/apply/python',
      company_website: 'https://beta.example.com',
    },
  ],
};

const LocationWatcher: React.FC = () => {
  const location = useLocation();
  return <span data-testid="location-search">{location.search}</span>;
};

describe('PortalDynamicPage', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok: true,
      json: async () => portalFixture,
      text: async () => JSON.stringify(portalFixture),
    } as unknown as Response);
  });

  afterEach(() => {
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  const renderWithRouter = (initialEntry: string) =>
    render(
      <MemoryRouter initialEntries={[initialEntry]}>
        <LocationWatcher />
        <Routes>
          <Route path="/portal/dynamic/:slug" element={<PortalDynamicPage />} />
        </Routes>
      </MemoryRouter>
    );

  it('hydrates filters from the URL on initial render', async () => {
    renderWithRouter('/portal/dynamic/demo?location=Tel%20Aviv&skills=react,node_js&type=remote');

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(expect.stringContaining('/tenants/public/portal/demo'), expect.anything());
    });

    expect(await screen.findByDisplayValue('Tel Aviv')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'React' })).toHaveStyle('color: #10b981');
  expect(screen.getByTestId('location-search').textContent).toBe('?location=Tel+Aviv&type=remote&skills=react%2Cnode_js');
  });

  it('synchronizes user interactions with the URL search params', async () => {
    renderWithRouter('/portal/dynamic/demo');

    await waitFor(() => {
      expect(screen.getByText('React Engineer')).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText('Location'), { target: { value: 'Tel Aviv' } });
    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?location=Tel+Aviv');
    });

    const searchInput = screen.getByPlaceholderText('Job title, skills, company…');
    fireEvent.change(searchInput, { target: { value: 'react' } });
    vi.advanceTimersByTime(300);
    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?q=react&location=Tel+Aviv');
    });

    const reactSkill = screen.getByRole('button', { name: 'React' });
    fireEvent.click(reactSkill);
    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?q=react&location=Tel+Aviv&skills=react');
    });

    fireEvent.change(screen.getByLabelText('Type'), { target: { value: 'remote' } });
    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?q=react&location=Tel+Aviv&type=remote&skills=react');
    });
  });
});
