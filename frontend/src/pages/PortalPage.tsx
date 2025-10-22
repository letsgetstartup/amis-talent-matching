import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { API_BASE } from '../api';

const normalizeToken = (value: string): string =>
  value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();

const sanitizeQueryValue = (value: string): string =>
  value
    .replace(/[_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const encodeSkillForQuery = (value: string): string =>
  normalizeToken(value)
    .replace(/\s+/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_+|_+$/g, '');

const encodeSkillsForQuery = (skills: string[]): string =>
  skills
    .map((skill) => encodeSkillForQuery(skill))
    .filter(Boolean)
    .join(',');

const canonicalizeFromList = (raw: string, options: string[]): string => {
  const sanitized = sanitizeQueryValue(raw);
  const normalizedRaw = normalizeToken(sanitized);
  if (!normalizedRaw) {
    return '';
  }
  const exact = options.find((option) => normalizeToken(option) === normalizedRaw);
  if (exact) {
    return exact;
  }
  const partial = options.find((option) => {
    const normalizedOption = normalizeToken(option);
    return normalizedOption.includes(normalizedRaw) || normalizedRaw.includes(normalizedOption);
  });
  return partial || sanitized;
};

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

const PortalPage: React.FC = () => {
  const { slug } = useParams<{ slug: string }>();
  const navigate = useNavigate();
  const { search } = useLocation();
  const [data, setData] = useState<PortalData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState<string>('');
  const [location, setLocation] = useState<string>('');
  const [company, setCompany] = useState<string>('');
  const [type, setType] = useState<string>('');
  const [selectedSkills, setSelectedSkills] = useState<string[]>([]);
  const lastSearchAppliedRef = useRef<string | null>(null);
  const skipNextSyncRef = useRef(false);
  const pendingNavigationRef = useRef<number | undefined>(undefined);
  const initialSyncSlugRef = useRef<string | null>(null);

  useEffect(() => {
    const normalizedSearch = search || '';
    if (lastSearchAppliedRef.current === normalizedSearch) {
      return;
    }
    lastSearchAppliedRef.current = normalizedSearch;
    const params = new URLSearchParams(normalizedSearch);
    const locParam = sanitizeQueryValue(params.get('location') || '');
    const skillsParam = params.get('skills') || '';
    const queryParam = (params.get('q') || '').trim();
    const companyParam = sanitizeQueryValue(params.get('company') || '');
    const rawTypeParam = sanitizeQueryValue(params.get('type') || '').toLowerCase();
    const allowedTypes = new Set(['remote', 'onsite']);
    const typeParam = allowedTypes.has(rawTypeParam) ? rawTypeParam : '';
    skipNextSyncRef.current = true;
    setLocation((prev) => (prev === locParam ? prev : locParam));
    setQuery((prev) => (prev === queryParam ? prev : queryParam));
    setCompany((prev) => (prev === companyParam ? prev : companyParam));
    setType((prev) => (prev === typeParam ? prev : typeParam));
    const parsedSkills = skillsParam
      .split(',')
      .map((skill) => sanitizeQueryValue(skill))
      .filter(Boolean);
    setSelectedSkills((prev) => {
      const prevNormalized = prev.map((value) => normalizeToken(value));
      const nextNormalized = parsedSkills.map((value) => normalizeToken(value));
      if (
        prevNormalized.length === nextNormalized.length &&
        prevNormalized.every((value, idx) => value === nextNormalized[idx])
      ) {
        return prev;
      }
      return parsedSkills;
    });
  }, [search]);

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
        if (err.name === 'AbortError') return;
        const message = err.message?.includes('portal_not_found')
          ? 'This portal link is invalid or no longer active.'
          : err.message || 'Failed to load portal';
        setError(message);
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [slug]);

  const filters = useMemo(
    () => ({ query, location, company, type, skills: selectedSkills }),
    [query, location, company, type, selectedSkills]
  );

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
      const matchesQuery = !q || title.includes(q) || companyName.includes(q) || description.includes(q) || skills.includes(q);
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
    if (!location || !locations.length) {
      return;
    }
    if (locations.includes(location)) {
      return;
    }
    const canonical = canonicalizeFromList(location, locations);
    if (canonical && canonical !== location) {
      skipNextSyncRef.current = true;
      setLocation(canonical);
    }
  }, [location, locations]);

  useEffect(() => {
    if (!company || !companies.length) {
      return;
    }
    if (companies.includes(company)) {
      return;
    }
    const canonical = canonicalizeFromList(company, companies);
    if (canonical && canonical !== company) {
      skipNextSyncRef.current = true;
      setCompany(canonical);
    }
  }, [company, companies]);

  useEffect(() => {
    if (!selectedSkills.length || !allSkills.length) {
      return;
    }
    const canonicalSkills = selectedSkills
      .map((skill) => canonicalizeFromList(skill, allSkills))
      .filter((skill): skill is string => Boolean(skill));
    if (!canonicalSkills.length) {
      return;
    }
    const uniqueCanonicalSkills = canonicalSkills.filter(
      (skill, index, self) =>
        self.findIndex((value) => normalizeToken(value) === normalizeToken(skill)) === index
    );
    const currentNormalized = selectedSkills.map((skill) => normalizeToken(skill));
    const nextNormalized = uniqueCanonicalSkills.map((skill) => normalizeToken(skill));
    if (
      currentNormalized.length === nextNormalized.length &&
      currentNormalized.every((value, idx) => value === nextNormalized[idx])
    ) {
      return;
    }
    skipNextSyncRef.current = true;
    setSelectedSkills(uniqueCanonicalSkills);
  }, [selectedSkills, allSkills]);

  useEffect(() => {
    if (!slug) return;

    if (initialSyncSlugRef.current !== slug) {
      initialSyncSlugRef.current = slug;
      skipNextSyncRef.current = false;
      return;
    }

    if (skipNextSyncRef.current) {
      skipNextSyncRef.current = false;
      return;
    }
    const nextParams = new URLSearchParams();
    const trimmedQuery = query.trim();
    if (location) {
      nextParams.set('location', location);
    }
    if (selectedSkills.length) {
      nextParams.set('skills', encodeSkillsForQuery(selectedSkills));
    }
    if (trimmedQuery) {
      nextParams.set('q', trimmedQuery);
    }
    if (company) {
      nextParams.set('company', company);
    }
    if (type) {
      nextParams.set('type', type);
    }
    const nextString = nextParams.toString();
    const normalizedNext = nextString ? `?${nextString}` : '';
    if (normalizedNext === (lastSearchAppliedRef.current || '')) {
      return;
    }
    if (typeof window === 'undefined') {
      lastSearchAppliedRef.current = normalizedNext;
      navigate({ pathname: `/portal/${slug}`, search: normalizedNext }, { replace: true });
      return;
    }
    if (typeof window !== 'undefined' && pendingNavigationRef.current) {
      window.clearTimeout(pendingNavigationRef.current);
      pendingNavigationRef.current = undefined;
    }
    pendingNavigationRef.current = window.setTimeout(() => {
      lastSearchAppliedRef.current = normalizedNext;
      navigate({ pathname: `/portal/${slug}`, search: normalizedNext }, { replace: true });
      if (pendingNavigationRef.current) {
        window.clearTimeout(pendingNavigationRef.current);
        pendingNavigationRef.current = undefined;
      }
    }, 300);
    return () => {
      if (typeof window !== 'undefined' && pendingNavigationRef.current) {
        window.clearTimeout(pendingNavigationRef.current);
        pendingNavigationRef.current = undefined;
      }
    };
    // navigate is intentionally excluded: react-router returns a stable reference and including it
    // would cause unnecessary dependency churn in strict mode.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [slug, location, selectedSkills, query, company, type]);

  const toggleSkill = (skill: string) => {
    const normalizedSkill = normalizeToken(skill);
    if (!normalizedSkill) {
      return;
    }
    setSelectedSkills((prev) => {
      const exists = prev.some((item) => normalizeToken(item) === normalizedSkill);
      if (exists) {
        return prev.filter((item) => normalizeToken(item) !== normalizedSkill);
      }
      return [...prev, skill];
    });
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
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Job title, skills, company…"
                style={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8, color: '#e5e7eb', padding: '12px 16px', minWidth: 260 }}
              />
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <label style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Location</label>
              <select
                value={location}
                onChange={(e) => setLocation(e.target.value)}
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
                value={company}
                onChange={(e) => setCompany(e.target.value)}
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
                value={type}
                onChange={(e) => setType(e.target.value)}
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
                  const active = selectedSkills.some((item) => normalizeToken(item) === normalizeToken(skill));
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
                {selectedSkills.length > 0 && (
                  <button
                    type="button"
                    onClick={() => setSelectedSkills([])}
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

export default PortalPage;
