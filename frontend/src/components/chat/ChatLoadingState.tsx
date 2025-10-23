import React from 'react';

interface ChatLoadingStateProps {
  message?: string;
}

export const ChatLoadingState: React.FC<ChatLoadingStateProps> = ({ message }) => (
  <div className="portal-chatbot__loading" role="status" aria-live="polite">
    <span className="portal-chatbot__spinner" aria-hidden="true" />
    <span>{message || 'Starting assistant…'}</span>
  </div>
);

export default ChatLoadingState;
