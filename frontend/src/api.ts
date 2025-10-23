import axios from 'axios';

import type {
  PortalChatHistoryResponse,
  PortalChatMessagePayload,
  PortalChatMessageResponse,
  PortalChatSeedResponse,
  PortalChatSuggestionsResponse,
} from './types/chat';

// API base selection:
// - In dev with Vite proxy: use relative base '' so requests go through proxy to 8080.
// - If VITE_API_BASE is defined, always use it (overrides proxy/relative).
// - In production without proxy: default to same-host:8000 unless overridden.
// Prefer VITE_API_BASE when set. In dev (Vite on :5173) default to relative base '' so the Vite proxy forwards to 8080. Otherwise fallback to same-host:8000.
const VITE_BASE = (typeof window !== 'undefined' ? ((import.meta as any)?.env?.VITE_API_BASE as string | undefined) : undefined);
export const API_BASE: string = VITE_BASE
  ? VITE_BASE
  : (typeof window !== 'undefined' && /:5173$/.test(window.location.host))
    ? ''
    : (typeof window !== 'undefined' && window.location?.hostname ? `http://${window.location.hostname}:8000` : 'http://127.0.0.1:8000');

export interface CandidateList { candidates: string[]; total: number; skip:number; limit:number; sort?: string|null }
export interface JobList { jobs: string[]; total: number; skip:number; limit:number }
export interface Match { job_id?: string; candidate_id?: string; score: number; title?: string; skill_overlap?: string[]; distance_km?: number|null; reason?: string; job_only_skills?: string[]; candidate_only_skills?: string[] }
export interface CandidateDetail { candidate_id:string; title?:string; city?:string|null; skills:string[]; updated_at?:number; share_id?:string }
export interface ShareResponse { candidate: CandidateDetail; matches: Match[] }

export async function fetchCandidates(skip=0, limit=50) {
  const r = await axios.get(`${API_BASE}/candidates?skip=${skip}&limit=${limit}`);
  return r.data as CandidateList & { skip:number; limit:number };
}
export async function fetchJobs(skip=0, limit=50) {
  const r = await axios.get(`${API_BASE}/jobs?skip=${skip}&limit=${limit}`);
  return r.data as JobList & { skip:number; limit:number };
}
export async function fetchMatchesForCandidate(id: string, k=5) {
  const r = await axios.get(`${API_BASE}/match/candidate/${id}?k=${k}`);
  return r.data;
}
export async function fetchMatchesForJob(id: string, k=5) {
  const r = await axios.get(`${API_BASE}/match/job/${id}?k=${k}`);
  return r.data;
}
export async function explainMatch(candId: string, jobId: string) {
  const r = await axios.get(`${API_BASE}/match/explain/${candId}/${jobId}`);
  return r.data;
}
export async function fetchCandidateDetail(id: string){
  const r = await axios.get(`${API_BASE}/candidate/${id}`);
  return r.data.candidate as CandidateDetail;
}
export async function fetchJobDetail(id: string){
  const r = await axios.get(`${API_BASE}/job/${id}`);
  return r.data.job;
}
export async function saveMatch(direction: 'c2j'|'j2c', source_id: string, target_id: string, status: string='saved', notes?: string){
  const r = await axios.post(`${API_BASE}/match/save`, { direction, source_id, target_id, status, notes });
  return r.data;
}
export async function fetchWeights(){
  const r = await axios.get(`${API_BASE}/config/weights`);
  return r.data.weights;
}
export async function fetchLLMStatus(){
  const r = await axios.get(`${API_BASE}/llm/status`);
  return r.data;
}
export async function searchJobs(params: Record<string,string|number|undefined>) {
  const query = Object.entries(params).filter(([,v])=>v!==undefined && v!=='').map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join('&');
  const r = await axios.get(`${API_BASE}/search/jobs?${query}`);
  return r.data;
}
export async function searchCandidates(params: Record<string,string|number|undefined>) {
  const query = Object.entries(params).filter(([,v])=>v!==undefined && v!=='').map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join('&');
  const r = await axios.get(`${API_BASE}/search/candidates?${query}`);
  return r.data;
}
export async function updateAllConfig(body: Record<string, any>, apiKey?: string){
  const r = await axios.post(`${API_BASE}/config/all`, body, { headers: apiKey ? { 'X-API-Key': apiKey }: {} });
  return r.data.weights;
}
export async function ingest(kind: 'candidate'|'job', text: string, filename: string, force_llm=false, apiKey?: string){
  const r = await axios.post(`${API_BASE}/ingest/${kind}?force_llm=${force_llm}`, { text, filename }, { headers: apiKey ? { 'X-API-Key': apiKey } : {} });
  return r.data;
}
export async function fetchShareCandidate(shareId: string, k=10){
  const r = await axios.get(`${API_BASE}/share/candidate/${shareId}?k=${k}`);
  return r.data as ShareResponse;
}
export async function uploadCandidate(file: File){
  const fd = new FormData();
  fd.append('file', file);
  const r = await axios.post(`${API_BASE}/upload/candidate`, fd, { headers: { 'Content-Type': 'multipart/form-data' } });
  return r.data as { share_id:string; candidate_id:string; status:string; llm_success:boolean };
}

function authHeaders(extra: Record<string, string> = {}) {
  if (typeof window === 'undefined') {
    return extra;
  }
  const token = window.localStorage?.getItem('token');
  if (token) {
    return { Authorization: `Bearer ${token}`, ...extra };
  }
  return extra;
}

export async function apiGet<T = any>(path: string): Promise<T> {
  const response = await axios.get(`${API_BASE}${path}`, { headers: authHeaders() });
  return response.data as T;
}

export async function apiPost<T = any>(path: string, body: unknown): Promise<T> {
  const response = await axios.post(`${API_BASE}${path}`, body, { headers: authHeaders({ 'Content-Type': 'application/json' }) });
  return response.data as T;
}

export async function apiUpload<T = any>(path: string, file: File): Promise<T> {
  const formData = new FormData();
  formData.append('file', file);
  const response = await axios.post(`${API_BASE}${path}`, formData, {
    headers: authHeaders(),
  });
  return response.data as T;
}

export async function apiDelete<T = any>(path: string): Promise<T> {
  const response = await axios.delete(`${API_BASE}${path}`, { headers: authHeaders() });
  return response.data as T;
}

export async function postPortalChatMessage(body: PortalChatMessagePayload): Promise<PortalChatMessageResponse> {
  return apiPost<PortalChatMessageResponse>('/portal/chat/message', body);
}

export async function getPortalChatHistory(conversationId: string): Promise<PortalChatHistoryResponse> {
  return apiGet<PortalChatHistoryResponse>(`/portal/chat/conversation/${conversationId}`);
}

export async function deletePortalChatConversation(conversationId: string): Promise<void> {
  await apiDelete(`/portal/chat/conversation/${conversationId}`);
}

export async function getPortalChatSuggestions(portalSlug: string): Promise<PortalChatSuggestionsResponse> {
  return apiPost<PortalChatSuggestionsResponse>('/portal/chat/suggest', { portal_slug: portalSlug });
}

export async function getPortalChatSeed(token: string): Promise<PortalChatSeedResponse> {
  return apiGet<PortalChatSeedResponse>(`/portal/chat/seed/${token}`);
}
