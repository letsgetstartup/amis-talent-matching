import { describe, it, expect } from 'vitest';
import {
  buildPortalSearchString,
  normalizePortalFilters,
  parsePortalFilters,
  arePortalFiltersEqual,
} from '../utils/urlFilters';
import { PORTAL_FILTER_DEFAULTS } from '../types/portalFilters';

describe('urlFilters utilities', () => {
  it('parses and normalizes query params into filter state', () => {
    const result = parsePortalFilters('?q=  React  &location=Tel%20Aviv&company=ACME&type=REMOTE&skills=react,node_js,react');
    expect(result).toEqual({
      query: 'React',
      location: 'Tel Aviv',
      company: 'ACME',
      type: 'remote',
      skills: ['react', 'node js'],
    });
  });

  it('serializes filters into canonical query string while preserving unknown params', () => {
    const filters = normalizePortalFilters({
      query: 'react engineer',
      location: 'Tel Aviv',
      company: 'Acme',
      type: 'remote',
      skills: ['React', 'Node.js'],
    });
    const search = buildPortalSearchString(filters, '?utm_source=newsletter&page=1');
  expect(search).toBe('?utm_source=newsletter&page=1&q=react+engineer&location=Tel+Aviv&company=Acme&type=remote&skills=react%2Cnode_js');
  });

  it('drops default filter values from the serialized query string', () => {
    const search = buildPortalSearchString(PORTAL_FILTER_DEFAULTS, '?foo=bar');
    expect(search).toBe('?foo=bar');
  });

  it('detects equivalent filter states even with different array orderings or casing', () => {
    const a = normalizePortalFilters({ query: 'React', skills: ['React', 'Node JS'] });
    const b = normalizePortalFilters({ query: 'React', skills: ['node js', 'react'] });
    expect(arePortalFiltersEqual(a, b)).toBe(true);
  });
});
