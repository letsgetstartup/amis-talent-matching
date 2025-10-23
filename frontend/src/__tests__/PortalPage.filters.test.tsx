import React from 'react';
import '@testing-library/jest-dom';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import PortalPage from '../pages/PortalPage';

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
      remote: false,
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

describe('PortalPage redirect filters', () => {
  beforeEach(() => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok: true,
      json: async () => portalFixture,
      text: async () => JSON.stringify(portalFixture),
    } as unknown as Response);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  const renderWithRouter = (initialEntry: string) => {
    render(
      <MemoryRouter initialEntries={[initialEntry]}>
        <LocationWatcher />
        <Routes>
          <Route path="/portal/:slug" element={<PortalPage />} />
        </Routes>
      </MemoryRouter>
    );
  };

  it('applies location and skills from query string on load', async () => {
    renderWithRouter('/portal/demo?location=Tel%20Aviv&skills=React,Node.js');

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(expect.stringContaining('/tenants/public/portal/demo'), expect.anything());
    });

    expect(await screen.findByText('React Engineer')).toBeInTheDocument();
    expect(screen.queryByText('Python Engineer')).not.toBeInTheDocument();

    const skillsTrigger = screen.getByRole('button', { name: 'Required Skills' });
    fireEvent.click(skillsTrigger);
  const reactOption = await screen.findByRole('option', { name: /React/ });
  const nodeOption = screen.getByRole('option', { name: /Node.js/ });
  expect(reactOption).toHaveAttribute('aria-selected', 'true');
  expect(nodeOption).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByDisplayValue('Tel Aviv')).toBeInTheDocument();
    expect(screen.getByTestId('location-search').textContent).toBe('?location=Tel%20Aviv&skills=React,Node.js');
  });

  it('normalizes underscored skills from query string', async () => {
    renderWithRouter('/portal/demo?skills=react,node_js');

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(expect.stringContaining('/tenants/public/portal/demo'), expect.anything());
    });

  const skillsTrigger = screen.getByRole('button', { name: 'Required Skills' });
  fireEvent.click(skillsTrigger);
    const reactOption = await screen.findByRole('option', { name: /React/ });
    const nodeOption = screen.getByRole('option', { name: /Node.js/ });
    expect(reactOption).toHaveAttribute('aria-selected', 'true');
    expect(nodeOption).toHaveAttribute('aria-selected', 'true');

    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?skills=react,node_js');
    });

    expect(screen.getByText('React Engineer')).toBeInTheDocument();
    expect(screen.queryByText('Python Engineer')).not.toBeInTheDocument();
  });

  it('applies query, company, and type from query string on load', async () => {
    renderWithRouter('/portal/demo?q=python&company=beta&type=onsite');

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(expect.stringContaining('/tenants/public/portal/demo'), expect.anything());
    });

    expect(await screen.findByDisplayValue('python')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Beta')).toBeInTheDocument();
    const typeSelect = screen.getByDisplayValue('On-site');
    expect(typeSelect).toHaveValue('onsite');

    expect(screen.getByText('Python Engineer')).toBeInTheDocument();
    expect(screen.queryByText('React Engineer')).not.toBeInTheDocument();
    expect(screen.getByTestId('location-search').textContent).toBe('?q=python&company=beta&type=onsite');
  });

  it('toggles skill filters and syncs URL search params', async () => {
    renderWithRouter('/portal/demo');

    await waitFor(() => {
      expect(screen.getByText('React Engineer')).toBeInTheDocument();
    });

    const skillsTrigger = screen.getByRole('button', { name: 'Required Skills' });
    fireEvent.click(skillsTrigger);
    const reactOption = await screen.findByRole('option', { name: /React/ });
    fireEvent.click(reactOption);

    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('?skills=react');
    });
    expect(screen.getByText('React Engineer')).toBeInTheDocument();
    expect(screen.queryByText('Python Engineer')).not.toBeInTheDocument();

    fireEvent.click(reactOption);
    await waitFor(() => {
      expect(screen.getByTestId('location-search').textContent).toBe('');
    });
    expect(screen.getByText('React Engineer')).toBeInTheDocument();
    expect(screen.getByText('Python Engineer')).toBeInTheDocument();
  });
});
