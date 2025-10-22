import {
  PORTAL_FILTER_DEFAULTS,
  PORTAL_FILTER_PARAM_KEYS,
  PortalFilterState,
  PortalJobType,
  isPortalJobType,
} from '../types/portalFilters';

export const normalizeToken = (value: string): string =>
  value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();

export const sanitizeQueryValue = (value: string): string =>
  value
    .replace(/[_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const encodeSkillForQuery = (value: string): string =>
  normalizeToken(value)
    .replace(/\s+/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_+|_+$/g, '');

export const encodeSkillsForQuery = (skills: string[]): string =>
  skills
    .map((skill) => encodeSkillForQuery(skill))
    .filter(Boolean)
    .join(',');

export const canonicalizeFromList = (raw: string, options: string[]): string => {
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

const dedupeSkills = (skills: string[]): string[] => {
  const seen = new Set<string>();
  const deduped: string[] = [];
  skills.forEach((skill) => {
    const normalized = normalizeToken(skill);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    deduped.push(skill);
  });
  return deduped;
};

const cleanText = (value: string): string => sanitizeQueryValue(value ?? '');

export const normalizePortalFilters = (input: Partial<PortalFilterState>): PortalFilterState => {
  const query = cleanText(input.query ?? PORTAL_FILTER_DEFAULTS.query);
  const location = cleanText(input.location ?? PORTAL_FILTER_DEFAULTS.location);
  const company = cleanText(input.company ?? PORTAL_FILTER_DEFAULTS.company);
  const rawTypeInput = (input.type ?? null) as PortalJobType | null | string;
  const rawType = cleanText(typeof rawTypeInput === 'string' ? rawTypeInput : '').toLowerCase();
  const type: PortalJobType | null = isPortalJobType(rawType) ? rawType : null;
  const skills = dedupeSkills((input.skills ?? PORTAL_FILTER_DEFAULTS.skills).map(cleanText).filter(Boolean));
  return {
    query,
    location,
    company,
    type,
    skills,
  };
};

export const parsePortalFilters = (search: string): PortalFilterState => {
  const params = new URLSearchParams(search || '');
  const query = sanitizeQueryValue(params.get(PORTAL_FILTER_PARAM_KEYS.query) || '');
  const location = sanitizeQueryValue(params.get(PORTAL_FILTER_PARAM_KEYS.location) || '');
  const company = sanitizeQueryValue(params.get(PORTAL_FILTER_PARAM_KEYS.company) || '');
  const rawType = sanitizeQueryValue(params.get(PORTAL_FILTER_PARAM_KEYS.type) || '').toLowerCase();
  const type: PortalJobType | null = isPortalJobType(rawType) ? rawType : null;
  const rawSkills = params.get(PORTAL_FILTER_PARAM_KEYS.skills) || '';
  const skills = dedupeSkills(
    rawSkills
      .split(',')
      .map((skill) => sanitizeQueryValue(skill))
      .filter(Boolean)
  );

  return normalizePortalFilters({ query, location, company, type, skills });
};

const shouldPersistQuery = (filters: PortalFilterState): boolean => Boolean(filters.query);
const shouldPersistLocation = (filters: PortalFilterState): boolean => Boolean(filters.location);
const shouldPersistCompany = (filters: PortalFilterState): boolean => Boolean(filters.company);
const shouldPersistType = (filters: PortalFilterState): boolean => Boolean(filters.type);
const shouldPersistSkills = (filters: PortalFilterState): boolean => filters.skills.length > 0;

const filterParamOrder: Array<[keyof PortalFilterState, string]> = [
  ['query', PORTAL_FILTER_PARAM_KEYS.query],
  ['location', PORTAL_FILTER_PARAM_KEYS.location],
  ['company', PORTAL_FILTER_PARAM_KEYS.company],
  ['type', PORTAL_FILTER_PARAM_KEYS.type],
  ['skills', PORTAL_FILTER_PARAM_KEYS.skills],
];

const buildFilterEntries = (filters: PortalFilterState): Array<[string, string]> => {
  const entries: Array<[string, string]> = [];
  if (shouldPersistQuery(filters)) {
    entries.push([PORTAL_FILTER_PARAM_KEYS.query, filters.query.trim()]);
  }
  if (shouldPersistLocation(filters)) {
    entries.push([PORTAL_FILTER_PARAM_KEYS.location, filters.location]);
  }
  if (shouldPersistCompany(filters)) {
    entries.push([PORTAL_FILTER_PARAM_KEYS.company, filters.company]);
  }
  if (shouldPersistType(filters) && filters.type) {
    entries.push([PORTAL_FILTER_PARAM_KEYS.type, filters.type]);
  }
  if (shouldPersistSkills(filters)) {
    entries.push([PORTAL_FILTER_PARAM_KEYS.skills, encodeSkillsForQuery(filters.skills)]);
  }
  return entries;
};

export const buildPortalSearchString = (
  filters: PortalFilterState,
  existingSearch: string = ''
): string => {
  const normalized = normalizePortalFilters(filters);
  const params = new URLSearchParams(existingSearch || '');
  const managedKeys = new Set(filterParamOrder.map(([, param]) => param));
  managedKeys.forEach((key) => params.delete(key));
  const entries = buildFilterEntries(normalized);
  entries.forEach(([key, value]) => params.append(key, value));
  const serialized = params.toString();
  return serialized ? `?${serialized}` : '';
};

export const arePortalFiltersEqual = (
  a: PortalFilterState,
  b: PortalFilterState
): boolean => {
  const normalizedA = normalizePortalFilters(a);
  const normalizedB = normalizePortalFilters(b);
  if (normalizedA.query !== normalizedB.query) return false;
  if (normalizedA.location !== normalizedB.location) return false;
  if (normalizedA.company !== normalizedB.company) return false;
  if (normalizedA.type !== normalizedB.type) return false;
  if (normalizedA.skills.length !== normalizedB.skills.length) return false;
  const sortedA = [...normalizedA.skills].map((skill) => normalizeToken(skill)).sort();
  const sortedB = [...normalizedB.skills].map((skill) => normalizeToken(skill)).sort();
  for (let i = 0; i < sortedA.length; i += 1) {
    if (sortedA[i] !== sortedB[i]) {
      return false;
    }
  }
  return true;
};

export const hasActivePortalFilters = (filters: PortalFilterState): boolean => {
  const normalized = normalizePortalFilters(filters);
  return (
    shouldPersistQuery(normalized) ||
    shouldPersistLocation(normalized) ||
    shouldPersistCompany(normalized) ||
    shouldPersistType(normalized) ||
    shouldPersistSkills(normalized)
  );
};
