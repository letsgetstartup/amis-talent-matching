import React, { useCallback, useEffect, useRef } from 'react';

interface ChatComposerProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  disabled?: boolean;
  placeholder?: string;
  onUploadClick?: () => void;
  uploadDisabled?: boolean;
}

export const ChatComposer: React.FC<ChatComposerProps> = ({ value, onChange, onSubmit, disabled, placeholder, onUploadClick, uploadDisabled }) => {
  const textAreaRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    if (textAreaRef.current) {
      textAreaRef.current.style.height = 'auto';
      textAreaRef.current.style.height = `${Math.min(textAreaRef.current.scrollHeight, 160)}px`;
    }
  }, [value]);

  const handleKeyDown = useCallback((event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      if (!disabled) {
        onSubmit();
      }
    }
  }, [disabled, onSubmit]);

  return (
    <div className="portal-chatbot__composer">
      <label className="portal-chatbot__composer-label" htmlFor="portal-chat-input">
        Ask about new roles
      </label>
      <div className="portal-chatbot__composer-row">
        <textarea
          id="portal-chat-input"
          ref={textAreaRef}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          placeholder={placeholder || 'Ask the assistant about open roles…'}
          rows={2}
          maxLength={500}
          aria-disabled={disabled}
        />
        <div className="portal-chatbot__composer-actions">
          {onUploadClick ? (
            <button
              type="button"
              className="portal-chatbot__upload"
              onClick={onUploadClick}
              disabled={disabled || uploadDisabled}
            >
              Upload CV
            </button>
          ) : null}
          <button
            type="button"
            className="portal-chatbot__send"
            onClick={onSubmit}
            disabled={disabled || !value.trim()}
          >
            Send
          </button>
        </div>
      </div>
      <p className="portal-chatbot__composer-hint">Press Enter to send, Shift + Enter for a new line.</p>
    </div>
  );
};

export default ChatComposer;
