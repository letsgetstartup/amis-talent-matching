import React, { useEffect, useMemo, useRef } from 'react';

import type { PortalChatUIMessage } from '../../types/chat';

interface ChatMessageListProps {
  messages: PortalChatUIMessage[];
  isTyping: boolean;
  ariaLabel?: string;
}

const timeFormatter = typeof window !== 'undefined'
  ? new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit' })
  : null;

function formatTimestamp(ts: number): string {
  if (!Number.isFinite(ts)) {
    return '';
  }
  if (timeFormatter) {
    return timeFormatter.format(new Date(ts));
  }
  return new Date(ts).toISOString();
}

export const ChatMessageList: React.FC<ChatMessageListProps> = ({ messages, isTyping, ariaLabel }) => {
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const renderedMessages = useMemo(() => messages, [messages]);

  useEffect(() => {
    if (bottomRef.current) {
      const node = bottomRef.current;
      if (typeof node.scrollIntoView === 'function') {
        node.scrollIntoView({ behavior: 'smooth', block: 'end' });
      } else if (node.parentElement) {
        node.parentElement.scrollTop = node.parentElement.scrollHeight;
      }
    }
  }, [renderedMessages, isTyping]);

  return (
    <div
      className="portal-chatbot__messages"
      role="log"
      aria-live="polite"
      aria-label={ariaLabel || 'Chat conversation'}
    >
      {renderedMessages.map((msg) => (
        <div
          key={msg.id}
          className={`portal-chatbot__message portal-chatbot__message--${msg.role} portal-chatbot__message--${msg.status || 'sent'}`}
        >
          <div className="portal-chatbot__message-body">{msg.text}</div>
          <div className="portal-chatbot__message-meta">
            <span>{msg.role === 'assistant' ? 'Assistant' : 'You'}</span>
            {msg.timestamp ? <span aria-hidden="true">· {formatTimestamp(msg.timestamp)}</span> : null}
            {msg.status === 'pending' ? <span className="portal-chatbot__message-status">Sending…</span> : null}
            {msg.status === 'error' ? <span className="portal-chatbot__message-status portal-chatbot__message-status--error">Failed</span> : null}
          </div>
        </div>
      ))}
      {isTyping ? (
        <div className="portal-chatbot__message portal-chatbot__message--assistant portal-chatbot__message--pending">
          <div className="portal-chatbot__message-body">
            <span className="portal-chatbot__typing-dot" />
            <span className="portal-chatbot__typing-dot" />
            <span className="portal-chatbot__typing-dot" />
          </div>
          <div className="portal-chatbot__message-meta"><span>Assistant is typing…</span></div>
        </div>
      ) : null}
      <div ref={bottomRef} />
    </div>
  );
};

export default ChatMessageList;
