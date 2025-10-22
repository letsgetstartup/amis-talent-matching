export type PortalJobType = 'remote' | 'onsite';

export interface PortalFilterState {
  query: string;
  location: string;
  company: string;
  type: PortalJobType | null;
  skills: string[];
}

export const PORTAL_FILTER_DEFAULTS: PortalFilterState = {
  query: '',
  location: '',
  company: '',
  type: null,
  skills: [],
};

export const PORTAL_FILTER_PARAM_KEYS = {
  query: 'q',
  location: 'location',
  company: 'company',
  type: 'type',
  skills: 'skills',
} as const;

export type PortalFilterParamKey = typeof PORTAL_FILTER_PARAM_KEYS[keyof typeof PORTAL_FILTER_PARAM_KEYS];

export const isPortalJobType = (value: string | null | undefined): value is PortalJobType =>
  value === 'remote' || value === 'onsite';
