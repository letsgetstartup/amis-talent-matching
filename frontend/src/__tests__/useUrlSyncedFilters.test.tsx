import React from 'react';
import '@testing-library/jest-dom';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation, useNavigate } from 'react-router-dom';
import { describe, it, expect } from 'vitest';
import { useUrlSyncedFilters } from '../hooks/useUrlSyncedFilters';

const FiltersHarness: React.FC = () => {
  const { filters, updateFilters, setFilters, resetFilters } = useUrlSyncedFilters({ debounceMs: 0 });
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <div>
      <span data-testid="query">{filters.query}</span>
      <span data-testid="skills">{filters.skills.join('|')}</span>
      <span data-testid="search">{location.search}</span>
      <button type="button" onClick={() => updateFilters({ query: 'React Engineer' })}>
        set-query
      </button>
      <button type="button" onClick={() => setFilters((prev) => ({ ...prev, company: 'Acme' }), { flush: true })}>
        set-company
      </button>
      <button type="button" onClick={() => updateFilters({ skills: ['React', 'Node.js'] }, { flush: true })}>
        set-skills
      </button>
      <button type="button" onClick={() => resetFilters()}>
        reset
      </button>
      <button type="button" onClick={() => navigate('/portal/dynamic/demo?q=python') }>
        external-nav
      </button>
    </div>
  );
};

describe('useUrlSyncedFilters hook', () => {
  const renderHarness = (initialEntry: string) =>
    render(
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route path="/portal/dynamic/:slug" element={<FiltersHarness />} />
        </Routes>
      </MemoryRouter>
    );

  it('hydrates filters from the URL and canonicalizes values', async () => {
    renderHarness('/portal/dynamic/demo?q=React&skills=react,node_js&utm=1');

    await waitFor(() => {
      expect(screen.getByTestId('query')).toHaveTextContent('React');
      expect(screen.getByTestId('skills')).toHaveTextContent('react|node js');
  expect(screen.getByTestId('search').textContent).toBe('?utm=1&q=React&skills=react%2Cnode_js');
    });
  });

  it('updates the URL when filters change', async () => {
    renderHarness('/portal/dynamic/demo');

    fireEvent.click(screen.getByText('set-query'));
    await waitFor(() => {
      expect(screen.getByTestId('search').textContent).toBe('?q=React+Engineer');
    });

    fireEvent.click(screen.getByText('set-company'));
    await waitFor(() => {
      expect(screen.getByTestId('search').textContent).toBe('?q=React+Engineer&company=Acme');
    });

    fireEvent.click(screen.getByText('set-skills'));
    await waitFor(() => {
  expect(screen.getByTestId('search').textContent).toBe('?q=React+Engineer&company=Acme&skills=react%2Cnode_js');
    });

    fireEvent.click(screen.getByText('reset'));
    await waitFor(() => {
      expect(screen.getByTestId('search').textContent).toBe('');
      expect(screen.getByTestId('query')).toHaveTextContent('');
      expect(screen.getByTestId('skills')).toHaveTextContent('');
    });
  });

  it('reacts to external navigation changes by updating filters', async () => {
    renderHarness('/portal/dynamic/demo?q=react');

    fireEvent.click(screen.getByText('external-nav'));

    await waitFor(() => {
      expect(screen.getByTestId('query')).toHaveTextContent('python');
      expect(screen.getByTestId('search').textContent).toBe('?q=python');
    });
  });
});
