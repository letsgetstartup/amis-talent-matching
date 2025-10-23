import React from 'react';

interface ChatQuickRepliesProps {
  items: string[];
  onSelect: (value: string) => void;
  disabled?: boolean;
}

export const ChatQuickReplies: React.FC<ChatQuickRepliesProps> = ({ items, onSelect, disabled }) => {
  if (!items.length) {
    return null;
  }

  return (
    <div className="portal-chatbot__quick-replies" role="group" aria-label="Suggested prompts">
      {items.map((item) => (
        <button
          key={item}
          type="button"
          className="portal-chatbot__quick-reply"
          onClick={() => onSelect(item)}
          disabled={disabled}
        >
          {item}
        </button>
      ))}
    </div>
  );
};

export default ChatQuickReplies;
