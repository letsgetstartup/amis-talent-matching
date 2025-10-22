import React, { useEffect, useMemo, useState } from 'react';
import { useParams } from 'react-router-dom';
import { API_BASE } from '../api';
import useUrlSyncedFilters from '../hooks/useUrlSyncedFilters';
import { PortalJobType } from '../types/portalFilters';
import {
  canonicalizeFromList,
  normalizeToken,
} from '../utils/urlFilters';

interface PortalJob {
  job_id: string;
  title: string;
  company_name?: string;
  description?: string;
  requirements: string[];
  location?: string;
  remote?: boolean;
  application_url?: string;
  company_website?: string;
}

interface PortalStats {
  job_count: number;
  company_count: number;
  location_count: number;
}

interface PortalData {
  name: string;
  slug: string;
  stats: PortalStats;
  jobs: PortalJob[];
}

const PortalDynamicPage: React.FC = () => {
  const { slug } = useParams<{ slug: string }>();
  const { filters, updateFilters, setFilters } = useUrlSyncedFilters({ debounceMs: 250 });
  const [data, setData] = useState<PortalData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!slug) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/tenants/public/portal/${slug}`, { signal: controller.signal })
      .then(async (res) => {
        if (!res.ok) {
          let raw = '';
          try {
            raw = await res.text();
          } catch {
            raw = '';
          }
          if (raw) {
            try {
              const parsed = JSON.parse(raw);
              const detail = parsed?.detail || parsed?.message;
              if (detail) {
                throw new Error(detail);
              }
            } catch {
              // ignore JSON parse errors and fall through to text
            }
          }
          throw new Error(raw || res.statusText || 'Failed to load portal');
        }
        return res.json();
      })
      .then((payload: PortalData) => {
        setData(payload);
      })
      .catch((err) => {
        if ((err as Error).name === 'AbortError') return;
        const message = (err as Error).message?.includes('portal_not_found')
          ? 'This portal link is invalid or no longer active.'
          : (err as Error).message || 'Failed to load portal';
        setError(message);
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [slug]);

  const filteredJobs = useMemo(() => {
    if (!data) {
      return [] as PortalJob[];
    }
    const q = filters.query.trim().toLowerCase();
    const normalizedLocationFilter = normalizeToken(filters.location);
    return data.jobs.filter((job) => {
      const title = (job.title || '').toLowerCase();
      const companyName = (job.company_name || '').toLowerCase();
      const description = (job.description || '').toLowerCase();
      const skills = (job.requirements || []).join(' ').toLowerCase();
      const matchesQuery =
        !q ||
        title.includes(q) ||
        companyName.includes(q) ||
        description.includes(q) ||
        skills.includes(q);
      const matchesLocation =
        !normalizedLocationFilter || normalizeToken(job.location || '').includes(normalizedLocationFilter);
      const matchesCompany = !filters.company || job.company_name === filters.company;
      const isRemote = Boolean(job.remote);
      const matchesType = !filters.type || (filters.type === 'remote' ? isRemote : !isRemote);
      const activeSkills = (filters.skills || []).map((skill) => normalizeToken(skill)).filter(Boolean);
      const jobSkillSet = new Set(
        (job.requirements || [])
          .map((skill) => normalizeToken(skill))
          .filter(Boolean)
      );
      const matchesSkills = !activeSkills.length || activeSkills.every((skill) => jobSkillSet.has(skill));
      return matchesQuery && matchesLocation && matchesCompany && matchesType && matchesSkills;
    });
  }, [data, filters]);

  const locations = useMemo(() => {
    if (!data) return [] as string[];
    return Array.from(new Set((data.jobs || []).map((job) => job.location).filter(Boolean) as string[])).sort();
  }, [data]);

  const companies = useMemo(() => {
    if (!data) return [] as string[];
    return Array.from(new Set((data.jobs || []).map((job) => job.company_name).filter(Boolean) as string[])).sort();
  }, [data]);

  const allSkills = useMemo(() => {
    if (!data) return [] as string[];
    const set = new Set<string>();
    (data.jobs || []).forEach((job) => {
      (job.requirements || []).forEach((skill) => {
        if (skill) {
          set.add(skill);
        }
      });
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [data]);

  useEffect(() => {
    if (!filters.location || !locations.length) {
      return;
    }
    if (locations.includes(filters.location)) {
      return;
    }
    const canonical = canonicalizeFromList(filters.location, locations);
    if (canonical && canonical !== filters.location) {
      updateFilters({ location: canonical }, { flush: true, replace: true });
    }
  }, [filters.location, locations, updateFilters]);

  useEffect(() => {
    if (!filters.company || !companies.length) {
      return;
    }
    if (companies.includes(filters.company)) {
      return;
    }
    const canonical = canonicalizeFromList(filters.company, companies);
    if (canonical && canonical !== filters.company) {
      updateFilters({ company: canonical }, { flush: true, replace: true });
    }
  }, [filters.company, companies, updateFilters]);

  useEffect(() => {
    if (!filters.skills.length || !allSkills.length) {
      return;
    }
    const canonicalSkills = filters.skills
      .map((skill) => canonicalizeFromList(skill, allSkills))
      .filter((skill): skill is string => Boolean(skill));
    if (!canonicalSkills.length) {
      return;
    }
    const uniqueCanonicalSkills = canonicalSkills.filter(
      (skill, index, self) =>
        self.findIndex((value) => normalizeToken(value) === normalizeToken(skill)) === index
    );
    const currentNormalized = filters.skills.map((skill) => normalizeToken(skill));
    const nextNormalized = uniqueCanonicalSkills.map((skill) => normalizeToken(skill));
    if (
      currentNormalized.length === nextNormalized.length &&
      currentNormalized.every((value, idx) => value === nextNormalized[idx])
    ) {
      return;
    }
    updateFilters({ skills: uniqueCanonicalSkills }, { flush: true, replace: true });
  }, [filters.skills, allSkills, updateFilters]);

  const toggleSkill = (skill: string) => {
    const normalizedSkill = normalizeToken(skill);
    if (!normalizedSkill) {
      return;
    }
    setFilters((prev) => {
      const exists = prev.skills.some((item) => normalizeToken(item) === normalizedSkill);
      if (exists) {
        return { ...prev, skills: prev.skills.filter((item) => normalizeToken(item) !== normalizedSkill) };
      }
      return { ...prev, skills: [...prev.skills, skill] };
    }, { flush: true, replace: true });
  };

  if (loading) {
    return <div style={{ padding: 24, textAlign: 'center', color: '#94a3b8' }}>Loading portal…</div>;
  }

  if (error) {
    return <div style={{ padding: 24, textAlign: 'center', color: '#f87171' }}>{error}</div>;
  }

  if (!data) {
    return <div style={{ padding: 24, textAlign: 'center', color: '#94a3b8' }}>No portal found.</div>;
  }

  return (
    <div style={{ background: '#0f172a', color: '#e5e7eb', minHeight: '100vh', paddingBottom: 40, direction: 'ltr' }}>
      <header style={{ background: 'rgba(17,24,39,0.9)', borderBottom: '1px solid #1f2937', padding: '16px 0' }}>
        <div style={{ maxWidth: 1200, margin: '0 auto', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0 20px' }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <div style={{ width: 36, height: 36, borderRadius: 8, background: 'linear-gradient(135deg,#10b981,#06b6d4)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontWeight: 700 }}>
              {(data.name || '?').slice(0, 2).toUpperCase()}
            </div>
            <div>
              <div style={{ fontWeight: 600 }}>{data.name}</div>
              <div style={{ fontSize: 12, fontWeight: 400, color: '#94a3b8' }}>Portfolio Jobs</div>
            </div>
          </div>
          <span style={{ color: '#94a3b8', fontSize: 14 }}>Apply once. Be seen by the entire portfolio.</span>
        </div>
      </header>

      <main style={{ maxWidth: 1200, margin: '0 auto', padding: '40px 20px' }}>
        <section style={{ textAlign: 'center', marginBottom: 32 }}>
          <h1 style={{ fontSize: 42, fontWeight: 700, background: 'linear-gradient(135deg,#10b981,#06b6d4)', WebkitBackgroundClip: 'text', color: 'transparent', marginBottom: 12 }}>
            Join Our Portfolio Ecosystem
          </h1>
          <p style={{ fontSize: 18, color: '#94a3b8', maxWidth: 600, margin: '0 auto' }}>
            Discover roles across our high-growth startups and scale-ups.
          </p>
          <div style={{ display: 'flex', justifyContent: 'center', gap: 40, marginTop: 24, flexWrap: 'wrap' }}>
            <div style={{ textAlign: 'center' }}>
              <span style={{ fontSize: 28, fontWeight: 700, color: '#10b981', display: 'block' }}>{data.stats.job_count}</span>
              <span style={{ fontSize: 14, color: '#94a3b8' }}>Open Roles</span>
            </div>
            <div style={{ textAlign: 'center' }}>
              <span style={{ fontSize: 28, fontWeight: 700, color: '#10b981', display: 'block' }}>{data.stats.company_count}</span>
              <span style={{ fontSize: 14, color: '#94a3b8' }}>Portfolio Companies</span>
            </div>
            <div style={{ textAlign: 'center' }}>
              <span style={{ fontSize: 28, fontWeight: 700, color: '#10b981', display: 'block' }}>{data.stats.location_count}</span>
              <span style={{ fontSize: 14, color: '#94a3b8' }}>Locations</span>
            </div>
          </div>
        </section>

        <section>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 24 }}>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <label style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Search</label>
              <input
                value={filters.query}
                onChange={(e) => updateFilters({ query: e.target.value })}
                placeholder="Job title, skills, company…"
                aria-label="Search"
                style={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8, color: '#e5e7eb', padding: '12px 16px', minWidth: 260 }}
              />
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <label style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Location</label>
              <select
                value={filters.location}
                onChange={(e) => updateFilters({ location: e.target.value }, { flush: true })}
                aria-label="Location"
                style={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8, color: '#e5e7eb', padding: '12px 16px' }}
              >
                <option value="">All locations</option>
                {locations.map((loc) => (
                  <option key={loc} value={loc}>
                    {loc}
                  </option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <label style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Company</label>
              <select
                value={filters.company}
                onChange={(e) => updateFilters({ company: e.target.value }, { flush: true })}
                aria-label="Company"
                style={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8, color: '#e5e7eb', padding: '12px 16px' }}
              >
                <option value="">All companies</option>
                {companies.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <label style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Type</label>
              <select
                value={filters.type ?? ''}
                onChange={(e) => {
                  const nextType = e.target.value ? (e.target.value as PortalJobType) : null;
                  updateFilters({ type: nextType }, { flush: true });
                }}
                aria-label="Type"
                style={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8, color: '#e5e7eb', padding: '12px 16px' }}
              >
                <option value="">All types</option>
                <option value="remote">Remote</option>
                <option value="onsite">On-site</option>
              </select>
            </div>
          </div>

          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, gap: 12, flexWrap: 'wrap' }}>
            <h2 style={{ fontSize: 24, fontWeight: 700, margin: 0 }}>Open Positions</h2>
            <div style={{ fontSize: 14, color: '#94a3b8', padding: '8px 16px', background: 'rgba(16,185,129,0.1)', borderRadius: 20, border: '1px solid rgba(16,185,129,0.2)' }}>
              {filteredJobs.length} {filteredJobs.length === 1 ? 'position' : 'positions'}
            </div>
          </div>

          {allSkills.length > 0 && (
            <div style={{ marginBottom: 20 }}>
              <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8, color: '#94a3b8' }}>Required Skills</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                {allSkills.map((skill) => {
                  const active = filters.skills.some((item) => normalizeToken(item) === normalizeToken(skill));
                  return (
                    <button
                      key={skill}
                      type="button"
                      onClick={() => toggleSkill(skill)}
                      style={{
                        padding: '6px 12px',
                        borderRadius: 20,
                        border: active ? '1px solid rgba(16,185,129,0.5)' : '1px solid rgba(255,255,255,0.08)',
                        background: active ? 'rgba(16,185,129,0.2)' : 'rgba(255,255,255,0.05)',
                        color: active ? '#10b981' : '#e5e7eb',
                        fontSize: 12,
                        fontWeight: 600,
                        cursor: 'pointer',
                        transition: 'all 0.2s ease',
                      }}
                    >
                      {skill}
                    </button>
                  );
                })}
                {filters.skills.length > 0 && (
                  <button
                    type="button"
                    onClick={() => updateFilters({ skills: [] }, { flush: true, replace: true })}
                    style={{
                      padding: '6px 12px',
                      borderRadius: 20,
                      border: '1px solid rgba(255,255,255,0.1)',
                      background: 'rgba(255,255,255,0.03)',
                      color: '#94a3b8',
                      fontSize: 12,
                      fontWeight: 600,
                      cursor: 'pointer',
                    }}
                  >
                    Clear skills
                  </button>
                )}
              </div>
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(320px,1fr))', gap: 20 }}>
            {filteredJobs.map((job) => (
              <div key={job.job_id} style={{ background: '#111827', border: '1px solid #1f2937', borderRadius: 16, padding: 20, display: 'flex', flexDirection: 'column', gap: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <div style={{ width: 44, height: 44, borderRadius: 10, background: 'linear-gradient(135deg,#10b981,#06b6d4)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontWeight: 700 }}>
                    {(job.company_name || '?').slice(0, 2).toUpperCase()}
                  </div>
                  <div>
                    <h3 style={{ margin: 0, fontSize: 18, color: '#e5e7eb' }}>{job.title}</h3>
                    <p style={{ margin: 0, fontSize: 14, color: '#94a3b8' }}>{job.company_name || 'Portfolio company'}</p>
                  </div>
                </div>
                <div style={{ fontSize: 13, color: '#94a3b8', display: 'flex', gap: 12, flexWrap: 'wrap' }}>
                  <span>📍 {job.location || 'Remote'}</span>
                  {job.remote && (
                    <span style={{ background: 'rgba(16,185,129,0.15)', color: '#10b981', padding: '4px 8px', borderRadius: 6, fontSize: 11, fontWeight: 600 }}>
                      Remote
                    </span>
                  )}
                </div>
                {job.description && (
                  <p style={{ color: '#94a3b8', fontSize: 14, margin: 0 }}>{job.description}</p>
                )}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {(job.requirements || []).map((skill) => (
                    <span key={skill} style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)', color: '#e5e7eb', padding: '4px 8px', borderRadius: 6, fontSize: 12, fontWeight: 500 }}>
                      {skill}
                    </span>
                  ))}
                </div>
                <button
                  type="button"
                  onClick={() => {
                    const url = job.application_url || job.company_website;
                    if (url) {
                      window.open(url, '_blank');
                    }
                  }}
                  style={{ marginTop: 'auto', background: 'linear-gradient(135deg,#10b981,#06b6d4)', color: '#fff', border: 'none', borderRadius: 10, padding: '12px 20px', fontSize: 14, fontWeight: 600, cursor: 'pointer' }}
                >
                  Apply Now →
                </button>
              </div>
            ))}
            {filteredJobs.length === 0 && (
              <div style={{ gridColumn: '1 / -1', textAlign: 'center', padding: 40, color: '#94a3b8', border: '1px dashed #1f2937', borderRadius: 12 }}>
                No positions match your filters right now.
              </div>
            )}
          </div>
        </section>
      </main>

      <footer style={{ borderTop: '1px solid #1f2937', padding: '40px 0', textAlign: 'center', color: '#94a3b8', fontSize: 14 }}>
        Powered by PTX • Portfolio Talent Exchange
      </footer>
    </div>
  );
};

export default PortalDynamicPage;
