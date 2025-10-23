import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { API_BASE } from '../api';
import MultiSelectDropdown from '../components/MultiSelectDropdown';
import PortalChatbot from '../components/chat/PortalChatbot';
import type { PortalChatFilterAction } from '../types/chat';

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
  const [highlightedJobIds, setHighlightedJobIds] = useState<string[]>([]);
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

  const chatSeedToken = useMemo(() => {
    const params = new URLSearchParams(search || '');
    return params.get('chat_seed');
  }, [search]);

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

  useEffect(() => {
    if (!highlightedJobIds.length || typeof window === 'undefined') {
      return;
    }
    const target = document.getElementById(`portal-job-card-${highlightedJobIds[0]}`);
    if (target) {
      target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      try {
        (target as HTMLElement).focus({ preventScroll: true });
      } catch {
        (target as HTMLElement).focus();
      }
    }
    const timer = window.setTimeout(() => {
      setHighlightedJobIds([]);
    }, 10000);
    return () => window.clearTimeout(timer);
  }, [highlightedJobIds]);

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

  const handleSkillSelectionChange = useCallback(
    (skills: string[]) => {
      if (!skills.length) {
        setSelectedSkills([]);
        return;
      }
      const canonical = skills
        .map((skill) => canonicalizeFromList(skill, allSkills))
        .filter((skill): skill is string => Boolean(skill));
      if (!canonical.length) {
        setSelectedSkills([]);
        return;
      }
      const deduped = canonical.filter(
        (skill, index, self) =>
          self.findIndex((value) => normalizeToken(value) === normalizeToken(skill)) === index
      );
      setSelectedSkills((prev) => {
        const prevNormalized = prev.map((skill) => normalizeToken(skill));
        const nextNormalized = deduped.map((skill) => normalizeToken(skill));
        if (
          prevNormalized.length === nextNormalized.length &&
          prevNormalized.every((value, idx) => value === nextNormalized[idx])
        ) {
          return prev;
        }
        return deduped;
      });
    },
    [allSkills]
  );

  const handleChatbotFilterActions = useCallback(
    (actions: PortalChatFilterAction[]) => {
      if (!actions?.length) {
        return;
      }

      let mutated = false;

      const ensureString = (value: unknown): string => {
        if (typeof value === 'string') {
          return value;
        }
        if (typeof value === 'number') {
          return String(value);
        }
        return '';
      };

      const parseSkillValues = (value: unknown): string[] => {
        if (Array.isArray(value)) {
          return value.filter((item): item is string => typeof item === 'string');
        }
        const stringValue = ensureString(value);
        return stringValue ? [stringValue] : [];
      };

      const prepareSkills = (values: string[]): string[] => {
        if (!values.length) {
          return [];
        }
        const normalized = new Set<string>();
        const prepared: string[] = [];
        values.forEach((raw) => {
          const trimmed = sanitizeQueryValue(raw);
          if (!trimmed) {
            return;
          }
          const canonicalSource = allSkills.length ? allSkills : values;
          const canonical = canonicalizeFromList(trimmed, canonicalSource) || trimmed;
          const key = normalizeToken(canonical);
          if (!key || normalized.has(key)) {
            return;
          }
          normalized.add(key);
          prepared.push(canonical);
        });
        return prepared;
      };

      actions.forEach((action) => {
        const key = action.filter_key;
        const actionType = action.type ?? 'set';
        switch (key) {
          case 'query': {
            if (actionType === 'clear') {
              setQuery((prev) => {
                if (!prev) {
                  return prev;
                }
                mutated = true;
                return '';
              });
              break;
            }
            const nextValue = sanitizeQueryValue(ensureString(action.value));
            setQuery((prev) => {
              if (prev === nextValue) {
                return prev;
              }
              mutated = true;
              return nextValue;
            });
            break;
          }
          case 'location': {
            if (actionType === 'clear') {
              setLocation((prev) => {
                if (!prev) {
                  return prev;
                }
                mutated = true;
                return '';
              });
              break;
            }
            const rawValue = ensureString(action.value);
            const canonical = (rawValue && locations.length
              ? canonicalizeFromList(rawValue, locations)
              : rawValue) || '';
            setLocation((prev) => {
              if (prev === canonical) {
                return prev;
              }
              mutated = true;
              return canonical;
            });
            break;
          }
          case 'company': {
            if (actionType === 'clear') {
              setCompany((prev) => {
                if (!prev) {
                  return prev;
                }
                mutated = true;
                return '';
              });
              break;
            }
            const rawValue = ensureString(action.value);
            const canonical = (rawValue && companies.length
              ? canonicalizeFromList(rawValue, companies)
              : rawValue) || '';
            setCompany((prev) => {
              if (prev === canonical) {
                return prev;
              }
              mutated = true;
              return canonical;
            });
            break;
          }
          case 'type': {
            if (actionType === 'clear') {
              setType((prev) => {
                if (!prev) {
                  return prev;
                }
                mutated = true;
                return '';
              });
              break;
            }
            let value = ensureString(action.value).toLowerCase();
            if (!value && typeof action.value === 'boolean') {
              value = action.value ? 'remote' : 'onsite';
            }
            if (value === 'on-site') {
              value = 'onsite';
            }
            if (value !== 'remote' && value !== 'onsite') {
              value = '';
            }
            setType((prev) => {
              if (prev === value) {
                return prev;
              }
              mutated = true;
              return value;
            });
            break;
          }
          case 'skills': {
            if (actionType === 'clear') {
              setSelectedSkills((prev) => {
                if (!prev.length) {
                  return prev;
                }
                mutated = true;
                return [];
              });
              break;
            }
            const parsedValues = prepareSkills(parseSkillValues(action.value));
            if (!parsedValues.length) {
              if (actionType === 'set') {
                setSelectedSkills((prev) => {
                  if (!prev.length) {
                    return prev;
                  }
                  mutated = true;
                  return [];
                });
              }
              break;
            }
            if (actionType === 'remove') {
              const removals = new Set(parsedValues.map((skill) => normalizeToken(skill)));
              setSelectedSkills((prev) => {
                if (!prev.length) {
                  return prev;
                }
                const next = prev.filter((skill) => !removals.has(normalizeToken(skill)));
                if (next.length === prev.length) {
                  return prev;
                }
                mutated = true;
                return next;
              });
              break;
            }
            if (actionType === 'add') {
              setSelectedSkills((prev) => {
                const existing = new Set(prev.map((skill) => normalizeToken(skill)));
                let localMutated = false;
                const next = [...prev];
                parsedValues.forEach((skill) => {
                  const key = normalizeToken(skill);
                  if (!key || existing.has(key)) {
                    return;
                  }
                  existing.add(key);
                  next.push(skill);
                  localMutated = true;
                });
                if (!localMutated) {
                  return prev;
                }
                mutated = true;
                return next;
              });
              break;
            }
            setSelectedSkills((prev) => {
              const next = [...parsedValues];
              const prevNormalized = prev.map((skill) => normalizeToken(skill));
              const nextNormalized = next.map((skill) => normalizeToken(skill));
              if (
                prevNormalized.length === nextNormalized.length &&
                prevNormalized.every((value, idx) => value === nextNormalized[idx])
              ) {
                return prev;
              }
              mutated = true;
              return next;
            });
            break;
          }
          default:
            break;
        }
      });

      if (mutated) {
        skipNextSyncRef.current = true;
      }
    },
    [allSkills, companies, locations]
  );

  const handleJobHighlight = useCallback((jobIds: string[]) => {
    if (!jobIds?.length) {
      return;
    }
    const sanitized = Array.from(
      new Set(
        jobIds
          .filter((id): id is string => typeof id === 'string' && id.trim().length > 0)
          .map((id) => id.trim())
      )
    );
    if (!sanitized.length) {
      return;
    }
    setHighlightedJobIds(sanitized);
  }, []);

  const handleChatSeedConsumed = useCallback(() => {
    if (!slug) {
      return;
    }
    const params = new URLSearchParams(search || '');
    if (!params.has('chat_seed')) {
      return;
    }
    params.delete('chat_seed');
    const nextString = params.toString();
    const normalizedNext = nextString ? `?${nextString}` : '';
    skipNextSyncRef.current = true;
    lastSearchAppliedRef.current = normalizedNext;
    navigate({ pathname: `/portal/${slug}`, search: normalizedNext }, { replace: true });
  }, [navigate, search, slug]);

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
    <div style={{ background: '#0f172a', color: '#e5e7eb', minHeight: '100vh', paddingBottom: 220, direction: 'ltr' }}>
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
              <MultiSelectDropdown
                label="Required Skills"
                options={allSkills}
                selected={selectedSkills}
                onChange={handleSkillSelectionChange}
                placeholder="Filter by skills…"
                searchable
                clearable
                selectAllOption
                maxHeight="280px"
              />
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(320px,1fr))', gap: 20 }}>
            {filteredJobs.map((job) => {
              const highlighted = highlightedJobIds.includes(job.job_id);
              const cardStyle: React.CSSProperties = {
                background: highlighted ? 'rgba(17,24,39,0.96)' : '#111827',
                border: highlighted ? '1px solid rgba(16,185,129,0.7)' : '1px solid #1f2937',
                borderRadius: 16,
                padding: 20,
                display: 'flex',
                flexDirection: 'column',
                gap: 16,
                boxShadow: highlighted ? '0 0 0 3px rgba(16,185,129,0.18)' : 'none',
                transition: 'border-color 0.3s ease, box-shadow 0.3s ease, background 0.3s ease',
              };

              return (
                <div
                  key={job.job_id}
                  id={`portal-job-card-${job.job_id}`}
                  className={`portal-job-card${highlighted ? ' portal-job-card--highlighted' : ''}`}
                  style={cardStyle}
                  tabIndex={-1}
                  aria-live={highlighted ? 'polite' : undefined}
                >
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
              );
            })}
            {filteredJobs.length === 0 && (
              <div style={{ gridColumn: '1 / -1', textAlign: 'center', padding: 40, color: '#94a3b8', border: '1px dashed #1f2937', borderRadius: 12 }}>
                No positions match your filters right now.
              </div>
            )}
          </div>
        </section>
      </main>

      {slug ? (
        <PortalChatbot
          portalSlug={slug}
          chatSeedToken={chatSeedToken}
          currentFilters={filters}
          onFiltersApply={handleChatbotFilterActions}
          onJobHighlight={handleJobHighlight}
          onChatSeedConsumed={handleChatSeedConsumed}
          className="portal-chatbot--floating"
        />
      ) : null}

      <footer style={{ borderTop: '1px solid #1f2937', padding: '40px 0', textAlign: 'center', color: '#94a3b8', fontSize: 14 }}>
        Powered by PTX • Portfolio Talent Exchange
      </footer>
    </div>
  );
};

export default PortalPage;
