import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  deletePortalChatConversation,
  getPortalChatHistory,
  getPortalChatSeed,
  getPortalChatSuggestions,
  postPortalChatMessage,
} from '../../api';
import type {
  PortalChatFilterAction,
  PortalChatHistoryMessage,
  PortalChatSeedSuggestion,
  PortalChatUIMessage,
} from '../../types/chat';
import ChatComposer from './ChatComposer';
import ChatLoadingState from './ChatLoadingState';
import ChatMessageList from './ChatMessageList';
import ChatQuickReplies from './ChatQuickReplies';

export interface PortalChatbotProps {
  portalSlug: string;
  currentFilters?: Record<string, unknown> | null;
  onFiltersApply?: (actions: PortalChatFilterAction[]) => void;
  onJobHighlight?: (jobIds: string[]) => void;
  chatSeedToken?: string | null;
  onChatSeedConsumed?: () => void;
  className?: string;
}

const CONVERSATION_STORAGE_PREFIX = 'portal-chat-conversation:';
const SESSION_STORAGE_PREFIX = 'portal-chat-session:';

function ensureClientId(): string {
  if (typeof window === 'undefined' || !window.crypto?.randomUUID) {
    return `client-${Math.random().toString(16).slice(2, 10)}`;
  }
  return window.crypto.randomUUID();
}

function secondsToMs(value: number | undefined | null): number {
  if (!value || Number.isNaN(value)) {
    return Date.now();
  }
  if (value > 1e12) {
    return value;
  }
  return Math.round(value * 1000);
}

const emptyFilters: Record<string, unknown> = {};

export const PortalChatbot: React.FC<PortalChatbotProps> = ({
  portalSlug,
  currentFilters,
  onFiltersApply,
  onJobHighlight,
  chatSeedToken,
  onChatSeedConsumed,
  className,
}) => {
  const [messages, setMessages] = useState<PortalChatUIMessage[]>([]);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [starters, setStarters] = useState<string[]>([]);
  const [composerValue, setComposerValue] = useState('');
  const [loadingHistory, setLoadingHistory] = useState(true);
  const [seedLoading, setSeedLoading] = useState(false);
  const [isSending, setIsSending] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobSuggestions, setJobSuggestions] = useState<PortalChatSeedSuggestion[]>([]);

  const handledSeedTokenRef = useRef<string | null>(null);
  const sessionIdRef = useRef<string>('');

  const storageConversationKey = useMemo(() => `${CONVERSATION_STORAGE_PREFIX}${portalSlug}`, [portalSlug]);
  const storageSessionKey = useMemo(() => `${SESSION_STORAGE_PREFIX}${portalSlug}`, [portalSlug]);

  const activeFilters = currentFilters ?? emptyFilters;

  const getSessionId = useCallback(() => {
    if (sessionIdRef.current) {
      return sessionIdRef.current;
    }
    const generated = ensureClientId();
    if (typeof window !== 'undefined') {
      try {
        const existing = window.localStorage.getItem(storageSessionKey);
        if (existing) {
          sessionIdRef.current = existing;
          return existing;
        }
        window.localStorage.setItem(storageSessionKey, generated);
      } catch (err) {
        console.warn('portal-chatbot: failed to access localStorage session id', err);
      }
    }
    sessionIdRef.current = generated;
    return generated;
  }, [storageSessionKey]);

  useEffect(() => {
    getSessionId();
  }, [getSessionId]);

  useEffect(() => {
    let cancelled = false;
    async function loadConversation(existingId: string | null) {
      if (!existingId) {
        setLoadingHistory(false);
        return;
      }
      try {
        const history = await getPortalChatHistory(existingId);
        if (cancelled) return;
        setConversationId(history.conversation_id);
  const normalizedMessages: PortalChatUIMessage[] = history.messages.map((msg: PortalChatHistoryMessage, index: number) => ({
          id: `history-${index}-${msg.timestamp}`,
          role: msg.role === 'assistant' ? 'assistant' : 'user',
          text: msg.content,
          timestamp: secondsToMs(msg.timestamp),
          status: 'sent',
          filters: msg.filters ?? null,
          jobIds: msg.job_ids ?? null,
        }));
        setMessages(normalizedMessages);
        if (typeof window !== 'undefined') {
          window.localStorage.setItem(storageConversationKey, history.conversation_id);
        }
      } catch (err) {
        console.warn('portal-chatbot: failed to load conversation history', err);
        if (typeof window !== 'undefined') {
          try {
            window.localStorage.removeItem(storageConversationKey);
          } catch (removeErr) {
            console.warn('portal-chatbot: failed to clear conversation storage', removeErr);
          }
        }
        setConversationId(null);
        setMessages([]);
      } finally {
        if (!cancelled) {
          setLoadingHistory(false);
        }
      }
    }

    let storedId: string | null = null;
    if (typeof window !== 'undefined') {
      storedId = window.localStorage.getItem(storageConversationKey);
    }
    loadConversation(storedId);

    return () => {
      cancelled = true;
    };
  }, [storageConversationKey]);

  useEffect(() => {
    let cancelled = false;
    async function loadStarters() {
      try {
        const response = await getPortalChatSuggestions(portalSlug);
        if (!cancelled) {
          setStarters(response.starters || []);
        }
      } catch (err) {
        console.warn('portal-chatbot: failed to load starters', err);
        if (!cancelled) {
          setStarters([]);
        }
      }
    }
    loadStarters();
    return () => {
      cancelled = true;
    };
  }, [portalSlug]);

  const appendAssistantMessage = useCallback((text: string, responseMeta?: { filters?: PortalChatFilterAction[] | null; jobIds?: string[] | null; metadata?: Record<string, unknown> | null; }) => {
    setMessages((prev) => [
      ...prev,
      {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        text,
        timestamp: Date.now(),
        status: 'sent',
        filters: responseMeta?.filters ?? null,
        jobIds: responseMeta?.jobIds ?? null,
        metadata: responseMeta?.metadata ?? null,
      },
    ]);
  }, []);

  const sendMessage = useCallback(async (rawText: string, options?: { silentUser?: boolean }) => {
    const text = rawText.trim();
    if (!text) {
      return;
    }
    const clientId = ensureClientId();
    const userMessage: PortalChatUIMessage = {
      id: `user-${clientId}`,
      role: 'user',
      text,
      timestamp: Date.now(),
      status: 'pending',
    };

    if (!options?.silentUser) {
      setMessages((prev) => [...prev, userMessage]);
    }

    setIsSending(true);
    setError(null);
    try {
      const response = await postPortalChatMessage({
        portal_slug: portalSlug,
        message: text,
        conversation_id: conversationId || undefined,
        current_filters: activeFilters,
        session_id: getSessionId(),
      });
      setConversationId(response.conversation_id);
      if (typeof window !== 'undefined') {
        try {
          window.localStorage.setItem(storageConversationKey, response.conversation_id);
        } catch (err) {
          console.warn('portal-chatbot: failed to persist conversation id', err);
        }
      }
      if (!options?.silentUser) {
        setMessages((prev) => prev.map((msg) => (msg.id === userMessage.id ? { ...msg, status: 'sent', timestamp: Date.now() } : msg)));
        setComposerValue('');
      }
      appendAssistantMessage(response.message, {
        filters: response.filters ?? null,
        jobIds: response.job_ids ?? null,
        metadata: response.metadata ?? null,
      });
      if (response.filters?.length && onFiltersApply) {
        onFiltersApply(response.filters);
      }
      if (response.job_ids?.length && onJobHighlight) {
        onJobHighlight(response.job_ids);
      }
    } catch (err) {
      console.warn('portal-chatbot: failed to send message', err);
      if (!options?.silentUser) {
        setMessages((prev) => prev.map((msg) => (msg.id === userMessage.id ? { ...msg, status: 'error' } : msg)));
      }
      setError('Something went wrong while contacting the assistant. Please try again.');
    } finally {
      setIsSending(false);
    }
  }, [activeFilters, appendAssistantMessage, conversationId, getSessionId, onFiltersApply, onJobHighlight, portalSlug, storageConversationKey]);

  useEffect(() => {
    if (!chatSeedToken || handledSeedTokenRef.current === chatSeedToken) {
      return;
    }
    if (messages.length > 0 || conversationId) {
      handledSeedTokenRef.current = chatSeedToken;
      onChatSeedConsumed?.();
      return;
    }
    let cancelled = false;
    setSeedLoading(true);
    (async () => {
      try {
        const seed = await getPortalChatSeed(chatSeedToken);
        if (cancelled) {
          return;
        }
        handledSeedTokenRef.current = chatSeedToken;
        onChatSeedConsumed?.();
        const assistantMessage: PortalChatUIMessage = {
          id: `seed-${Date.now()}`,
          role: 'assistant',
          text: seed.auto_message,
          timestamp: Date.now(),
          status: 'sent',
          jobIds: seed.highlighted_job_ids || [],
          metadata: seed.metadata || null,
        };
        setMessages([assistantMessage]);
        if (seed.highlighted_job_ids?.length && onJobHighlight) {
          onJobHighlight(seed.highlighted_job_ids);
        }
        setJobSuggestions(seed.job_suggestions || []);
        if (seed.injected_user_message) {
          await sendMessage(seed.injected_user_message);
        }
      } catch (err) {
        console.warn('portal-chatbot: failed to consume chat seed', err);
        handledSeedTokenRef.current = chatSeedToken;
        onChatSeedConsumed?.();
        setError('We had trouble preparing the assistant. Please try again.');
      } finally {
        if (!cancelled) {
          setSeedLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [chatSeedToken, conversationId, messages.length, onChatSeedConsumed, onJobHighlight, sendMessage]);

  const handleQuickReply = useCallback((item: string) => {
    setComposerValue('');
    sendMessage(item);
  }, [sendMessage]);

  const handleResetConversation = useCallback(async () => {
    if (!conversationId) {
      setMessages([]);
      return;
    }
    setResetting(true);
    try {
      await deletePortalChatConversation(conversationId);
    } catch (err) {
      console.warn('portal-chatbot: failed to delete conversation', err);
    } finally {
      if (typeof window !== 'undefined') {
        try {
          window.localStorage.removeItem(storageConversationKey);
        } catch (removeErr) {
          console.warn('portal-chatbot: failed to clear storage', removeErr);
        }
      }
      setConversationId(null);
      setMessages([]);
      setJobSuggestions([]);
      setResetting(false);
      setError(null);
      setComposerValue('');
    }
  }, [conversationId, storageConversationKey]);

  const assistantBusy = isSending || seedLoading || resetting;

  if (loadingHistory) {
    return <ChatLoadingState message="Loading conversation…" />;
  }

  return (
    <section className={`portal-chatbot ${className || ''}`} aria-label="Job assistant chatbot">
      <header className="portal-chatbot__header">
        <div>
          <h2>Talk with the job assistant</h2>
          <p>Ask for recommendations, refine filters, or learn more about the openings.</p>
        </div>
        <button
          type="button"
          className="portal-chatbot__reset"
          onClick={handleResetConversation}
          disabled={assistantBusy}
        >
          Reset conversation
        </button>
      </header>

      {error ? <div className="portal-chatbot__error" role="alert">{error}</div> : null}

      <ChatMessageList messages={messages} isTyping={isSending || seedLoading} />

      {jobSuggestions.length ? (
        <div className="portal-chatbot__suggestions">
          <h3>Suggested roles</h3>
          <ul>
            {jobSuggestions.map((job) => (
              <li key={job.job_id}>
                <div className="portal-chatbot__suggestion-card">
                  <div className="portal-chatbot__suggestion-title">{job.title || 'Open role'}</div>
                  <div className="portal-chatbot__suggestion-meta">
                    {job.company_name ? <span>{job.company_name}</span> : null}
                    {job.location ? <span>{job.location}</span> : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => onJobHighlight?.([job.job_id])}
                    className="portal-chatbot__suggestion-action"
                  >
                    Highlight in list
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <ChatQuickReplies items={starters} onSelect={handleQuickReply} disabled={assistantBusy} />

      <ChatComposer
        value={composerValue}
        onChange={setComposerValue}
        onSubmit={() => sendMessage(composerValue)}
        disabled={assistantBusy}
      />
    </section>
  );
};

export default PortalChatbot;
