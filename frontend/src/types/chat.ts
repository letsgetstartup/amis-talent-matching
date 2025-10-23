export type PortalChatRole = 'assistant' | 'user';

export interface PortalChatFilterAction {
  type: 'set' | 'add' | 'remove' | 'clear';
  filter_key: string;
  value: unknown;
}

export interface PortalChatMessagePayload {
  message: string;
  portal_slug: string;
  conversation_id?: string | null;
  current_filters?: Record<string, unknown> | null;
  session_id?: string;
}

export interface PortalChatMessageResponse {
  message: string;
  conversation_id: string;
  filters?: PortalChatFilterAction[] | null;
  job_ids?: string[] | null;
  metadata?: Record<string, unknown> | null;
}

export interface PortalChatHistoryMessage {
  role: PortalChatRole | 'assistant' | 'user';
  content: string;
  timestamp: number;
  filters?: PortalChatFilterAction[] | null;
  job_ids?: string[] | null;
}

export interface PortalChatHistoryResponse {
  conversation_id: string;
  portal_slug: string;
  messages: PortalChatHistoryMessage[];
  metadata?: Record<string, unknown> | null;
}

export interface PortalChatSuggestionsResponse {
  starters: string[];
}

export interface PortalChatSeedSuggestion {
  job_id: string;
  title?: string | null;
  company_name?: string | null;
  location?: string | null;
}

export interface PortalChatSeedResponse {
  portal_slug: string;
  auto_message: string;
  highlighted_job_ids: string[];
  injected_user_message: string;
  job_suggestions: PortalChatSeedSuggestion[];
  metadata?: Record<string, unknown> | null;
}

export interface PortalChatUIMessage {
  id: string;
  role: PortalChatRole;
  text: string;
  timestamp: number;
  status?: 'pending' | 'sent' | 'error';
  filters?: PortalChatFilterAction[] | null;
  jobIds?: string[] | null;
  metadata?: Record<string, unknown> | null;
}
