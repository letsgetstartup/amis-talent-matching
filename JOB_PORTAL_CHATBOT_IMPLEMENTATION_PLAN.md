# Job Portal AI Chatbot - Comprehensive Implementation Plan

## Executive Summary

This document outlines the complete implementation plan for adding an OpenAI-powered chatbot to the Job Portal page, enabling users to:
1. **Discuss jobs** - Ask questions about job listings, get recommendations, compare positions
2. **Apply intelligent filters** - Use natural language to filter jobs (e.g., "Show me remote React jobs in Tel Aviv")
3. **Get personalized assistance** - Receive job matching advice and application guidance

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Technology Stack](#technology-stack)
3. [Phase 1: Backend Infrastructure](#phase-1-backend-infrastructure)
4. [Phase 2: Frontend UI/UX Components](#phase-2-frontend-uiux-components)
5. [Phase 3: AI Integration](#phase-3-ai-integration)
6. [Phase 4: Advanced Features](#phase-4-advanced-features)
7. [Security & Performance](#security--performance)
8. [Testing Strategy](#testing-strategy)
9. [Deployment Plan](#deployment-plan)
10. [Success Metrics](#success-metrics)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Frontend (React)                        │
│  ┌──────────────┐  ┌─────────────────┐  ┌───────────────┐ │
│  │ PortalPage   │  │  ChatbotWidget  │  │  FilterPanel  │ │
│  │  Component   │──│  (Collapsible)  │──│  (Synced)     │ │
│  └──────────────┘  └─────────────────┘  └───────────────┘ │
└────────────────────────────┬────────────────────────────────┘
                             │ API Calls
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                Backend (FastAPI + Python)                   │
│  ┌────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │ Portal Router  │  │ Chatbot Router  │  │  OpenAI     │ │
│  │  (Existing)    │  │    (New)        │──│  Service    │ │
│  └────────────────┘  └─────────────────┘  └─────────────┘ │
│           │                   │                    │        │
│           └───────────────────┴────────────────────┘        │
│                               │                             │
│                               ▼                             │
│                    ┌─────────────────────┐                 │
│                    │  MongoDB Database   │                 │
│                    │  - Jobs Collection  │                 │
│                    │  - Chat History     │                 │
│                    └─────────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Frontend
- **React 18+** with TypeScript
- **TailwindCSS** for styling
- **React Hooks** for state management
- **Framer Motion** for animations (optional)
- **React Markdown** for rich message rendering

### Backend
- **FastAPI** (existing Python framework)
- **OpenAI API** (GPT-4 or GPT-4o for conversation)
- **MongoDB** (existing database)
- **Pydantic** for request/response validation
- **asyncio** for async operations

### AI/ML
- **OpenAI GPT-4o** - Main conversational model
- **Function Calling** - For filter extraction and job search
- **Embeddings** (optional) - For semantic job matching

---

## Phase 1: Backend Infrastructure

### 1.1 Create Chatbot Router Module

**File**: `/talentdb/scripts/routers_portal_chatbot.py`

**Purpose**: Handle all chatbot-related API endpoints for the portal

```python
"""
Portal Chatbot Router - Handles AI-powered job discussions and filtering
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import time
from datetime import datetime

router = APIRouter(prefix="/portal/chat", tags=["portal-chatbot"])

class ChatMessage(BaseModel):
    """Single chat message"""
    role: str  # 'user' or 'assistant'
    content: str
    timestamp: float = Field(default_factory=time.time)

class ChatRequest(BaseModel):
    """User message + context"""
    message: str
    portal_slug: str
    conversation_id: Optional[str] = None
    current_filters: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None

class FilterAction(BaseModel):
    """Filter action to apply on frontend"""
    type: str  # 'set', 'add', 'remove', 'clear'
    filter_key: str  # 'location', 'skills', 'company', 'type', 'query'
    value: Any

class ChatResponse(BaseModel):
    """Assistant response + actions"""
    message: str
    conversation_id: str
    filters: Optional[List[FilterAction]] = None
    job_ids: Optional[List[str]] = None  # Highlighted jobs
    metadata: Optional[Dict[str, Any]] = None

# Endpoints to implement:
# POST /portal/chat/message - Main chat endpoint
# GET /portal/chat/conversation/{conversation_id} - Retrieve history
# POST /portal/chat/suggest - Get conversation starters
# DELETE /portal/chat/conversation/{conversation_id} - Clear history
```

**Key Features**:
- ✅ Stateless conversation handling (with optional persistence)
- ✅ Filter extraction from natural language
- ✅ Job highlighting based on chat context
- ✅ Conversation history tracking

---

### 1.2 OpenAI Service Integration

**File**: `/talentdb/scripts/services/chatbot_service.py`

**Purpose**: Encapsulate all OpenAI API interactions with function calling

```python
"""
Chatbot Service - OpenAI integration with function calling
"""
import os
import json
from typing import Dict, List, Any, Optional, Tuple
from openai import OpenAI
from .ingest_agent import _openai_client, _OPENAI_AVAILABLE

CHATBOT_MODEL = os.getenv("CHATBOT_MODEL", "gpt-4o")
CHATBOT_TEMPERATURE = float(os.getenv("CHATBOT_TEMPERATURE", "0.7"))
MAX_CONVERSATION_TOKENS = int(os.getenv("MAX_CONVERSATION_TOKENS", "8000"))

# Function definitions for OpenAI function calling
FILTER_JOBS_FUNCTION = {
    "name": "apply_job_filters",
    "description": "Apply filters to the job portal based on user requirements",
    "parameters": {
        "type": "object",
        "properties": {
            "location": {"type": "string", "description": "City or region"},
            "skills": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Required skills (e.g., React, Python)"
            },
            "company": {"type": "string", "description": "Company name"},
            "remote": {"type": "boolean", "description": "Remote work option"},
            "query": {"type": "string", "description": "General search keywords"}
        }
    }
}

HIGHLIGHT_JOBS_FUNCTION = {
    "name": "highlight_specific_jobs",
    "description": "Highlight specific jobs by ID when discussing them",
    "parameters": {
        "type": "object",
        "properties": {
            "job_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of job IDs to highlight"
            }
        },
        "required": ["job_ids"]
    }
}

SYSTEM_PROMPT = """You are a helpful AI assistant for a job portal.

Your role:
- Help users find relevant jobs by understanding their requirements
- Apply filters intelligently based on natural language queries
- Discuss specific job opportunities
- Provide career advice and application tips
- Be conversational, friendly, and professional

Available jobs context: {job_count} positions across {company_count} companies

When users ask to filter jobs:
- Use the apply_job_filters function to set filters
- Confirm what filters were applied
- Suggest refinements if needed

When discussing specific jobs:
- Use highlight_specific_jobs to draw attention to them
- Provide detailed information
- Compare roles when asked

Be concise but helpful. Always ground your responses in the actual available jobs."""

class ChatbotService:
    def __init__(self):
        self.client = _openai_client
        self.available = _OPENAI_AVAILABLE
    
    async def process_message(
        self,
        user_message: str,
        portal_context: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
        current_filters: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Optional[List[Dict]], Optional[List[str]]]:
        """
        Process user message and return:
        - response_text: The assistant's message
        - filter_actions: List of filter changes to apply
        - highlighted_jobs: List of job IDs to highlight
        """
        # Build messages array with system prompt + history + new message
        # Call OpenAI with function calling
        # Parse function calls and execute them
        # Return structured response
        pass

# Implementation details in the actual file...
```

**Key Features**:
- ✅ Function calling for filter extraction
- ✅ Conversation context management
- ✅ Token limit handling
- ✅ Graceful error handling

---

### 1.3 Database Schema Extensions

**Collection**: `portal_conversations` (new)

```javascript
{
  "_id": ObjectId("..."),
  "conversation_id": "uuid-v4",
  "portal_slug": "company-jobs",
  "session_id": "anonymous-session-id", // Optional for analytics
  "messages": [
    {
      "role": "user",
      "content": "Show me React jobs in Tel Aviv",
      "timestamp": ISODate("2025-10-23T10:30:00Z")
    },
    {
      "role": "assistant",
      "content": "I found 5 React positions in Tel Aviv...",
      "timestamp": ISODate("2025-10-23T10:30:02Z"),
      "function_calls": [
        {
          "name": "apply_job_filters",
          "arguments": {"location": "Tel Aviv", "skills": ["React"]}
        }
      ]
    }
  ],
  "created_at": ISODate("2025-10-23T10:30:00Z"),
  "updated_at": ISODate("2025-10-23T10:35:00Z"),
  "metadata": {
    "total_messages": 4,
    "applied_filters_count": 2,
    "jobs_discussed": ["job_1", "job_3"]
  }
}
```

**Indexes**:
```javascript
db.portal_conversations.createIndex({ "conversation_id": 1 }, { unique: true })
db.portal_conversations.createIndex({ "portal_slug": 1, "created_at": -1 })
db.portal_conversations.createIndex({ "updated_at": 1 }, { expireAfterSeconds: 2592000 }) // 30 days TTL
```

---

### 1.4 API Endpoints Implementation

```python
@router.post("/message", response_model=ChatResponse)
async def send_message(req: ChatRequest):
    """
    Main chatbot endpoint
    
    Flow:
    1. Validate portal_slug exists
    2. Load/create conversation
    3. Get portal context (jobs, stats)
    4. Process message through ChatbotService
    5. Save conversation history
    6. Return response with filter actions
    """
    pass

@router.get("/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Retrieve full conversation history"""
    pass

@router.post("/suggest")
async def get_suggestions(portal_slug: str):
    """
    Return conversation starters based on portal data
    Examples:
    - "What are the most in-demand skills?"
    - "Show me remote positions"
    - "Tell me about [CompanyName]"
    """
    pass

@router.delete("/conversation/{conversation_id}")
async def clear_conversation(conversation_id: str):
    """Clear conversation history"""
    pass
```

---

## Phase 2: Frontend UI/UX Components

### 2.1 ChatbotWidget Component

**File**: `/frontend/src/components/ChatbotWidget.tsx`

**Design Specifications**:

#### Visual Design
```
┌─────────────────────────────────────────┐
│ 🤖 Job Assistant              [−] [×]  │ ← Header (draggable)
├─────────────────────────────────────────┤
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ 👋 Hi! I'm your job assistant.  │  │ ← Assistant message
│  │ How can I help you today?        │  │   (left-aligned, light bg)
│  └──────────────────────────────────┘  │
│                                         │
│             ┌─────────────────────────┐│
│             │ Show me React jobs      ││ ← User message
│             └─────────────────────────┘│   (right-aligned, blue bg)
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ Found 5 React positions! I've    │  │
│  │ applied filters for you.         │  │
│  │                                   │  │
│  │ 📍 Tel Aviv (3)                  │  │ ← Rich content
│  │ 📍 Remote (2)                    │  │   (clickable chips)
│  └──────────────────────────────────┘  │
│                                         │ ← Chat area
│                                         │   (scrollable)
├─────────────────────────────────────────┤
│ ┌─────────────────────────────────────┐│
│ │ Type your message...         [📎] │ │ ← Input area
│ └─────────────────────────────────────┘│
│                            [Send] [🎤] │ ← Action buttons
└─────────────────────────────────────────┘
```

#### States & Variants

1. **Minimized State** (Floating Button)
   - Position: Fixed bottom-right
   - Size: 60px circle
   - Icon: 💬 with notification badge
   - Animation: Subtle pulse on new suggestions

2. **Expanded State** (Chat Panel)
   - Width: 400px (desktop), 100% (mobile)
   - Height: 600px max (desktop), 70vh (mobile)
   - Position: Fixed bottom-right with 20px margin
   - Responsive: Full-screen on mobile

3. **Embedded State** (Optional)
   - Inline within portal page
   - Takes full width of container
   - Height: Auto-expand based on content

#### Component Structure

```tsx
import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import ReactMarkdown from 'react-markdown';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
  filters?: FilterAction[];
  highlightedJobs?: string[];
}

interface ChatbotWidgetProps {
  portalSlug: string;
  onFilterChange?: (filters: any) => void;
  onJobHighlight?: (jobIds: string[]) => void;
  position?: 'floating' | 'embedded';
  initialMessage?: string;
}

export const ChatbotWidget: React.FC<ChatbotWidgetProps> = ({
  portalSlug,
  onFilterChange,
  onJobHighlight,
  position = 'floating',
  initialMessage
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Load initial greeting
  useEffect(() => {
    if (isOpen && messages.length === 0) {
      loadInitialGreeting();
    }
  }, [isOpen]);

  const loadInitialGreeting = async () => {
    // Fetch conversation starters from API
    // Add welcome message
  };

  const sendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage: Message = {
      role: 'user',
      content: inputValue,
      timestamp: Date.now()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE}/portal/chat/message`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: inputValue,
          portal_slug: portalSlug,
          conversation_id: conversationId,
          current_filters: {} // Get from parent
        })
      });

      const data = await response.json();
      
      const assistantMessage: Message = {
        role: 'assistant',
        content: data.message,
        timestamp: Date.now(),
        filters: data.filters,
        highlightedJobs: data.job_ids
      };

      setMessages(prev => [...prev, assistantMessage]);
      setConversationId(data.conversation_id);

      // Apply filter changes
      if (data.filters && onFilterChange) {
        applyFilterActions(data.filters);
      }

      // Highlight jobs
      if (data.job_ids && onJobHighlight) {
        onJobHighlight(data.job_ids);
      }

    } catch (error) {
      console.error('Chat error:', error);
      // Show error message
    } finally {
      setIsLoading(false);
    }
  };

  const applyFilterActions = (actions: FilterAction[]) => {
    // Convert filter actions to parent component format
    // Call onFilterChange callback
  };

  // Render methods...
  return (
    <AnimatePresence>
      {position === 'floating' && !isOpen ? (
        <motion.button
          className="fixed bottom-6 right-6 w-16 h-16 bg-blue-600 rounded-full shadow-lg"
          onClick={() => setIsOpen(true)}
          whileHover={{ scale: 1.1 }}
          whileTap={{ scale: 0.9 }}
        >
          💬
        </motion.button>
      ) : (
        <motion.div
          className="chatbot-container"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: 20 }}
        >
          {/* Chat UI implementation */}
        </motion.div>
      )}
    </AnimatePresence>
  );
};
```

---

### 2.2 Message Components

#### AssistantMessage Component
```tsx
interface AssistantMessageProps {
  content: string;
  filters?: FilterAction[];
  highlightedJobs?: string[];
  onFilterClick?: (filter: FilterAction) => void;
}

const AssistantMessage: React.FC<AssistantMessageProps> = ({
  content,
  filters,
  highlightedJobs,
  onFilterClick
}) => {
  return (
    <div className="flex items-start gap-2 mb-4">
      <div className="w-8 h-8 bg-blue-100 rounded-full flex items-center justify-center">
        🤖
      </div>
      <div className="flex-1 bg-gray-100 rounded-lg p-3 max-w-[80%]">
        <ReactMarkdown className="prose prose-sm">
          {content}
        </ReactMarkdown>
        
        {filters && filters.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-2">
            {filters.map((filter, idx) => (
              <FilterChip
                key={idx}
                filter={filter}
                onClick={() => onFilterClick?.(filter)}
              />
            ))}
          </div>
        )}
        
        {highlightedJobs && highlightedJobs.length > 0 && (
          <div className="mt-2 text-xs text-gray-500">
            💡 {highlightedJobs.length} job{highlightedJobs.length > 1 ? 's' : ''} highlighted
          </div>
        )}
      </div>
    </div>
  );
};
```

#### QuickReplies Component
```tsx
const QuickReplies: React.FC<{ suggestions: string[]; onSelect: (text: string) => void }> = ({
  suggestions,
  onSelect
}) => {
  return (
    <div className="flex flex-wrap gap-2 p-2 border-t">
      {suggestions.map((suggestion, idx) => (
        <button
          key={idx}
          className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-sm hover:bg-blue-100"
          onClick={() => onSelect(suggestion)}
        >
          {suggestion}
        </button>
      ))}
    </div>
  );
};
```

---

### 2.3 Integration with PortalPage

**File**: `/frontend/src/pages/PortalPage.tsx` (modifications)

```tsx
// Add at the top
import { ChatbotWidget } from '../components/ChatbotWidget';

// Inside PortalPage component
const [highlightedJobIds, setHighlightedJobIds] = useState<Set<string>>(new Set());

const handleChatFilterChange = useCallback((newFilters: any) => {
  // Apply filters from chatbot
  if (newFilters.location !== undefined) {
    setLocation(newFilters.location);
  }
  if (newFilters.skills !== undefined) {
    setSelectedSkills(newFilters.skills);
  }
  if (newFilters.company !== undefined) {
    setCompany(newFilters.company);
  }
  if (newFilters.type !== undefined) {
    setType(newFilters.type);
  }
  if (newFilters.query !== undefined) {
    setQuery(newFilters.query);
  }
  
  // Update URL to reflect changes
  updateURLFromFilters(newFilters);
}, []);

const handleJobHighlight = useCallback((jobIds: string[]) => {
  setHighlightedJobIds(new Set(jobIds));
  
  // Scroll to first highlighted job
  if (jobIds.length > 0) {
    const firstJob = document.getElementById(`job-${jobIds[0]}`);
    firstJob?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
}, []);

// In render
return (
  <div className="portal-page">
    {/* Existing content */}
    
    <ChatbotWidget
      portalSlug={slug || ''}
      onFilterChange={handleChatFilterChange}
      onJobHighlight={handleJobHighlight}
      position="floating"
    />
    
    {/* Job list with highlight support */}
    <div className="jobs-list">
      {filteredJobs.map(job => (
        <JobCard
          key={job.job_id}
          id={`job-${job.job_id}`}
          job={job}
          isHighlighted={highlightedJobIds.has(job.job_id)}
        />
      ))}
    </div>
  </div>
);
```

---

### 2.4 JobCard Enhancement

Add highlighting support:

```tsx
interface JobCardProps {
  job: PortalJob;
  isHighlighted?: boolean;
}

const JobCard: React.FC<JobCardProps> = ({ job, isHighlighted }) => {
  return (
    <div
      className={`job-card ${isHighlighted ? 'ring-2 ring-blue-500 shadow-lg' : ''}`}
    >
      {isHighlighted && (
        <div className="absolute top-2 right-2 bg-blue-500 text-white px-2 py-1 rounded text-xs">
          💡 Suggested
        </div>
      )}
      {/* Existing job card content */}
    </div>
  );
};
```

---

## Phase 3: AI Integration

### 3.1 OpenAI Function Calling Implementation

**Example Conversation Flow**:

```
User: "Show me remote React jobs"

→ OpenAI receives message with function definitions
→ GPT-4o decides to call apply_job_filters()
→ Returns: {
    "function_call": {
      "name": "apply_job_filters",
      "arguments": {
        "skills": ["React"],
        "remote": true
      }
    }
  }

Backend processes function call:
→ Extracts filter parameters
→ Validates against available jobs
→ Formulates response