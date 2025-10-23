# Job Portal AI Chatbot - Implementation Plan (Part 2)

## Phase 3: AI Integration (Continued)

### 3.1 OpenAI Function Calling Implementation (Continued)

**Complete Implementation**:

```python
# /talentdb/scripts/services/chatbot_service.py

from openai import OpenAI
import json
from typing import Dict, List, Any, Optional, Tuple
import os
import uuid
from datetime import datetime

class ChatbotService:
    def __init__(self, db_client):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.db = db_client
        self.model = os.getenv("CHATBOT_MODEL", "gpt-4o")
        self.temperature = float(os.getenv("CHATBOT_TEMPERATURE", "0.7"))
        
    def _build_system_prompt(self, portal_context: Dict[str, Any]) -> str:
        """Build dynamic system prompt with portal context"""
        return f"""You are a helpful AI assistant for a job portal.

Portal Information:
- Total Jobs: {portal_context.get('job_count', 0)}
- Companies: {portal_context.get('company_count', 0)}
- Locations: {portal_context.get('location_count', 0)}
- Available Skills: {', '.join(portal_context.get('top_skills', [])[:10])}

Your capabilities:
1. Help users find relevant jobs using natural language
2. Apply smart filters based on their requirements
3. Discuss specific job opportunities in detail
4. Provide career advice and application tips
5. Compare different positions

Guidelines:
- Be conversational and friendly
- Ask clarifying questions when needed
- Suggest relevant jobs proactively
- Use the apply_job_filters function to update filters
- Use highlight_specific_jobs to draw attention to relevant positions
- Always base responses on actual available jobs
- If unsure, ask the user to clarify

Current available filters:
- Location: {', '.join(portal_context.get('locations', [])[:5])}
- Skills: Technical and soft skills
- Companies: {', '.join(portal_context.get('companies', [])[:5])}
- Type: Remote or Onsite
- Keywords: General search

Be helpful and accurate!"""

    def _get_function_definitions(self) -> List[Dict]:
        """Return OpenAI function definitions"""
        return [
            {
                "name": "apply_job_filters",
                "description": "Apply filters to narrow down job search results based on user requirements",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {
                            "type": "string",
                            "description": "City, region, or country (e.g., 'Tel Aviv', 'New York', 'Remote')"
                        },
                        "skills": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of required skills (e.g., ['React', 'Python', 'AWS'])"
                        },
                        "company": {
                            "type": "string",
                            "description": "Specific company name"
                        },
                        "remote": {
                            "type": "boolean",
                            "description": "True for remote jobs, false for onsite"
                        },
                        "query": {
                            "type": "string",
                            "description": "General search keywords for job title or description"
                        },
                        "action": {
                            "type": "string",
                            "enum": ["set", "add", "remove", "clear"],
                            "description": "How to apply these filters (set=replace all, add=append, remove=subtract, clear=reset)"
                        }
                    }
                }
            },
            {
                "name": "highlight_specific_jobs",
                "description": "Highlight specific jobs by ID when discussing or recommending them",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of job IDs to highlight in the UI"
                        },
                        "reason": {
                            "type": "string",
                            "description": "Why these jobs are being highlighted"
                        }
                    },
                    "required": ["job_ids"]
                }
            },
            {
                "name": "search_jobs",
                "description": "Search for jobs matching specific criteria and return job details",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string"},
                        "skills": {"type": "array", "items": {"type": "string"}},
                        "company": {"type": "string"},
                        "remote": {"type": "boolean"},
                        "query": {"type": "string"},
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of jobs to return (default: 5)",
                            "default": 5
                        }
                    }
                }
            }
        ]

    async def process_message(
        self,
        user_message: str,
        portal_context: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
        current_filters: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Optional[List[Dict]], Optional[List[str]]]:
        """
        Process user message through OpenAI with function calling
        
        Returns:
            (response_text, filter_actions, highlighted_job_ids)
        """
        
        # Build messages array
        messages = [
            {"role": "system", "content": self._build_system_prompt(portal_context)}
        ]
        
        # Add conversation history (last 10 messages to stay within token limits)
        messages.extend(conversation_history[-10:])
        
        # Add current user message
        messages.append({"role": "user", "content": user_message})
        
        # Add context about current filters if any
        if current_filters:
            filter_context = f"Current active filters: {json.dumps(current_filters)}"
            messages.append({"role": "system", "content": filter_context})
        
        try:
            # Call OpenAI with function calling
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                functions=self._get_function_definitions(),
                function_call="auto",  # Let model decide when to call functions
                temperature=self.temperature,
                max_tokens=1000
            )
            
            message = response.choices[0].message
            filter_actions = []
            highlighted_jobs = []
            
            # Check if function was called
            if message.function_call:
                function_name = message.function_call.name
                function_args = json.loads(message.function_call.arguments)
                
                # Execute function
                if function_name == "apply_job_filters":
                    filter_actions = self._convert_to_filter_actions(function_args)
                    
                    # Generate follow-up response
                    function_result = {
                        "status": "success",
                        "filters_applied": function_args
                    }
                    
                    # Add function result and get final response
                    messages.append({
                        "role": "function",
                        "name": function_name,
                        "content": json.dumps(function_result)
                    })
                    
                    # Get conversational response
                    final_response = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        temperature=self.temperature,
                        max_tokens=500
                    )
                    
                    response_text = final_response.choices[0].message.content
                
                elif function_name == "highlight_specific_jobs":
                    highlighted_jobs = function_args.get("job_ids", [])
                    reason = function_args.get("reason", "")
                    
                    # Generate response about highlighted jobs
                    response_text = self._generate_highlight_response(
                        highlighted_jobs, reason, portal_context
                    )
                
                elif function_name == "search_jobs":
                    # Execute search and format results
                    search_results = await self._search_jobs(function_args, portal_context)
                    highlighted_jobs = [job['job_id'] for job in search_results]
                    
                    # Generate response with search results
                    response_text = self._format_search_results(search_results)
            
            else:
                # Direct response without function call
                response_text = message.content
            
            return (response_text, filter_actions if filter_actions else None, highlighted_jobs if highlighted_jobs else None)
        
        except Exception as e:
            print(f"OpenAI API error: {e}")
            return (
                "I apologize, but I'm having trouble processing your request right now. Please try again.",
                None,
                None
            )
    
    def _convert_to_filter_actions(self, function_args: Dict) -> List[Dict]:
        """Convert function arguments to filter actions"""
        actions = []
        action_type = function_args.get("action", "set")
        
        for key in ["location", "skills", "company", "remote", "query"]:
            if key in function_args and function_args[key] is not None:
                actions.append({
                    "type": action_type,
                    "filter_key": key if key != "remote" else "type",
                    "value": function_args[key] if key != "remote" else ("remote" if function_args[key] else "onsite")
                })
        
        return actions
    
    def _generate_highlight_response(self, job_ids: List[str], reason: str, portal_context: Dict) -> str:
        """Generate natural response about highlighted jobs"""
        job_count = len(job_ids)
        return f"I've highlighted {job_count} position{'s' if job_count != 1 else ''} for you. {reason}"
    
    async def _search_jobs(self, criteria: Dict, portal_context: Dict) -> List[Dict]:
        """Execute job search based on criteria"""
        # This would query the actual jobs from the portal context
        # For now, return mock implementation
        return []
    
    def _format_search_results(self, jobs: List[Dict]) -> str:
        """Format job search results into conversational text"""
        if not jobs:
            return "I couldn't find any jobs matching those criteria. Try broadening your search?"
        
        response = f"I found {len(jobs)} position{'s' if len(jobs) != 1 else ''} for you:\n\n"
        
        for i, job in enumerate(jobs, 1):
            response += f"{i}. **{job.get('title', 'Unknown')}** at {job.get('company_name', 'Unknown Company')}\n"
            response += f"   📍 {job.get('location', 'Location not specified')}\n"
            if job.get('remote'):
                response += "   🏠 Remote\n"
            response += "\n"
        
        return response
    
    async def save_conversation(
        self,
        conversation_id: str,
        portal_slug: str,
        messages: List[Dict],
        session_id: Optional[str] = None
    ):
        """Save or update conversation in database"""
        conversation = {
            "conversation_id": conversation_id,
            "portal_slug": portal_slug,
            "session_id": session_id,
            "messages": messages,
            "updated_at": datetime.utcnow(),
            "metadata": {
                "total_messages": len(messages),
                "applied_filters_count": sum(1 for msg in messages if msg.get("filters")),
                "jobs_discussed": list(set(
                    job_id
                    for msg in messages
                    for job_id in msg.get("highlightedJobs", [])
                ))
            }
        }
        
        await self.db.portal_conversations.update_one(
            {"conversation_id": conversation_id},
            {"$set": conversation},
            upsert=True
        )
    
    async def get_conversation_starters(self, portal_context: Dict) -> List[str]:
        """Generate contextual conversation starters"""
        starters = [
            "What jobs do you have available?",
            "Show me remote positions",
        ]
        
        # Add skill-based starters
        top_skills = portal_context.get("top_skills", [])[:3]
        for skill in top_skills:
            starters.append(f"Find {skill} jobs")
        
        # Add location-based starters
        locations = portal_context.get("locations", [])[:2]
        for loc in locations:
            starters.append(f"Jobs in {loc}")
        
        # Add company-based starters
        companies = portal_context.get("companies", [])[:2]
        for company in companies:
            starters.append(f"Tell me about {company}")
        
        return starters[:6]  # Return max 6 starters
```

---

### 3.2 Context Management

**Portal Context Builder**:

```python
# /talentdb/scripts/routers_portal_chatbot.py

async def get_portal_context(portal_slug: str, db) -> Dict[str, Any]:
    """Build context about portal for AI assistant"""
    
    # Get portal data
    portal = await db.tenants.find_one(
        {"public_portal_slug": portal_slug},
        {"name": 1, "public_portal_slug": 1}
    )
    
    if not portal:
        raise HTTPException(status_code=404, detail="Portal not found")
    
    tenant_id = str(portal["_id"])
    
    # Get all jobs for this tenant
    jobs = list(await db.jobs.find(
        {"tenant_id": tenant_id, "public_portal": True},
        {
            "job_id": 1,
            "title": 1,
            "company_name": 1,
            "city": 1,
            "remote": 1,
            "must_have": 1,
            "nice_to_have": 1,
            "_id": 0
        }
    ).to_list(length=None))
    
    # Extract statistics
    all_skills = set()
    locations = set()
    companies = set()
    
    for job in jobs:
        if job.get("city"):
            locations.add(job["city"])
        if job.get("company_name"):
            companies.add(job["company_name"])
        for skill in job.get("must_have", []) + job.get("nice_to_have", []):
            all_skills.add(skill)
    
    # Get skill frequency
    skill_freq = {}
    for job in jobs:
        for skill in job.get("must_have", []) + job.get("nice_to_have", []):
            skill_freq[skill] = skill_freq.get(skill, 0) + 1
    
    top_skills = sorted(skill_freq.items(), key=lambda x: x[1], reverse=True)[:20]
    top_skills = [skill for skill, _ in top_skills]
    
    return {
        "portal_slug": portal_slug,
        "portal_name": portal.get("name", ""),
        "job_count": len(jobs),
        "company_count": len(companies),
        "location_count": len(locations),
        "jobs": jobs,
        "top_skills": top_skills,
        "all_skills": list(all_skills),
        "locations": sorted(list(locations)),
        "companies": sorted(list(companies))
    }
```

---

## Phase 4: Advanced Features

### 4.1 Conversation Starters (Smart Suggestions)

**Dynamic Starter Generation**:

```tsx
// Frontend component
const ConversationStarters: React.FC<{
  starters: string[];
  onSelect: (starter: string) => void;
}> = ({ starters, onSelect }) => {
  return (
    <div className="p-4 border-b">
      <p className="text-sm text-gray-600 mb-2">Try asking:</p>
      <div className="grid grid-cols-2 gap-2">
        {starters.map((starter, idx) => (
          <button
            key={idx}
            className="text-left px-3 py-2 bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg text-sm hover:from-blue-100 hover:to-indigo-100 transition-all"
            onClick={() => onSelect(starter)}
          >
            <span className="block font-medium text-blue-700">{starter}</span>
          </button>
        ))}
      </div>
    </div>
  );
};
```

---

### 4.2 Rich Message Formatting

**Support for Markdown, Job Cards, and Interactive Elements**:

```tsx
// Message renderer with rich content
const RichMessageContent: React.FC<{ content: string; data?: any }> = ({ content, data }) => {
  // Parse special syntax for job cards
  // Example: [job:job_123] in message content
  
  const renderContent = () => {
    // Check for special patterns
    if (content.includes('[job:')) {
      const parts = content.split(/(\[job:[^\]]+\])/g);
      return parts.map((part, idx) => {
        const match = part.match(/\[job:([^\]]+)\]/);
        if (match) {
          const jobId = match[1];
          return <JobCardPreview key={idx} jobId={jobId} />;
        }
        return <ReactMarkdown key={idx}>{part}</ReactMarkdown>;
      });
    }
    
    return <ReactMarkdown>{content}</ReactMarkdown>;
  };
  
  return <div className="message-content">{renderContent()}</div>;
};
```

---

### 4.3 Filter Visualization

**Show Applied Filters in Chat**:

```tsx
const FilterBadge: React.FC<{ filter: FilterAction; onRemove?: () => void }> = ({
  filter,
  onRemove
}) => {
  const getFilterLabel = () => {
    switch (filter.filter_key) {
      case 'location':
        return `📍 ${filter.value}`;
      case 'skills':
        return `💼 ${Array.isArray(filter.value) ? filter.value.join(', ') : filter.value}`;
      case 'company':
        return `🏢 ${filter.value}`;
      case 'type':
        return filter.value === 'remote' ? '🏠 Remote' : '🏢 Onsite';
      case 'query':
        return `🔍 "${filter.value}"`;
      default:
        return filter.value;
    }
  };
  
  return (
    <span className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-xs">
      {getFilterLabel()}
      {onRemove && (
        <button onClick={onRemove} className="hover:text-blue-900">
          ×
        </button>
      )}
    </span>
  );
};
```

---

### 4.4 Voice Input (Optional Enhancement)

```tsx
const VoiceInput: React.FC<{ onTranscript: (text: string) => void }> = ({ onTranscript }) => {
  const [isListening, setIsListening] = useState(false);
  
  const startListening = () => {
    if (!('webkitSpeechRecognition' in window)) {
      alert('Voice input not supported in this browser');
      return;
    }
    
    const recognition = new (window as any).webkitSpeechRecognition();
    recognition.lang = 'en-US';
    recognition.continuous = false;
    recognition.interimResults = false;
    
    recognition.onstart = () => setIsListening(true);
    recognition.onend = () => setIsListening(false);
    
    recognition.onresult = (event: any) => {
      const transcript = event.results[0][0].transcript;
      onTranscript(transcript);
    };
    
    recognition.start();
  };
  
  return (
    <button
      onClick={startListening}
      className={`p-2 rounded ${isListening ? 'bg-red-500 text-white' : 'bg-gray-200'}`}
      title="Voice input"
    >
      {isListening ? '🎤' : '🎙️'}
    </button>
  );
};
```

---

### 4.5 Analytics & Insights

**Track Chatbot Usage**:

```python
# /talentdb/scripts/analytics/chatbot_analytics.py

async def track_chatbot_event(
    db,
    portal_slug: str,
    event_type: str,
    metadata: Dict[str, Any]
):
    """Track chatbot interaction events"""
    
    event = {
        "portal_slug": portal_slug,
        "event_type": event_type,  # message_sent, filter_applied, job_highlighted, etc.
        "metadata": metadata,
        "timestamp": datetime.utcnow()
    }
    
    await db.chatbot_events.insert_one(event)

# Event types:
# - conversation_started
# - message_sent
# - filter_applied
# - job_highlighted
# - job_viewed
# - conversation_ended
```

**Analytics Dashboard**:

```python
@router.get("/analytics/chatbot")
async def get_chatbot_analytics(
    portal_slug: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    tenant_id: str = Depends(require_tenant)
):
    """Get chatbot usage analytics"""
    
    query = {"portal_slug": portal_slug}
    if start_date or end_date:
        query["timestamp"] = {}
        if start_date:
            query["timestamp"]["$gte"] = start_date
        if end_date:
            query["timestamp"]["$lte"] = end_date
    
    # Aggregate statistics
    pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": "$event_type",
                "count": {"$sum": 1}
            }
        }
    ]
    
    stats = await db.chatbot_events.aggregate(pipeline).to_list(length=None)
    
    return {
        "stats": stats,
        "total_conversations": await db.portal_conversations.count_documents({"portal_slug": portal_slug}),
        "avg_messages_per_conversation": None,  # Calculate from conversations
        "most_common_filters": None  # Calculate from events
    }
```

---

## Phase 5: Security & Performance

### 5.1 Rate Limiting

```python
# /talentdb/scripts/middleware/rate_limit.py

from fastapi import HTTPException, Request
from datetime import datetime, timedelta
import hashlib

# In-memory rate limiter (use Redis in production)
request_counts = {}

async def rate_limit_chatbot(request: Request, max_requests: int = 20, window_minutes: int = 5):
    """
    Rate limit chatbot requests
    - Anonymous users: 20 requests per 5 minutes
    - Authenticated users: 100 requests per 5 minutes
    """
    
    # Get identifier (IP for anonymous, user_id for authenticated)
    identifier = request.client.host
    
    # Check auth header for user_id (implement based on your auth system)
    # if auth_user:
    #     identifier = auth_user.id
    #     max_requests = 100
    
    key = hashlib.md5(f"chatbot:{identifier}".encode()).hexdigest()
    now = datetime.utcnow()
    
    if key in request_counts:
        requests, timestamp = request_counts[key]
        
        # Reset if window expired
        if now - timestamp > timedelta(minutes=window_minutes):
            request_counts[key] = (1, now)
        elif requests >= max_requests:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Try again in {window_minutes} minutes."
            )
        else:
            request_counts[key] = (requests + 1, timestamp)
    else:
        request_counts[key] = (1, now)

# Apply to routes
@router.post("/message", dependencies=[Depends(rate_limit_chatbot)])
async def send_message(req: ChatRequest):
    # ...
```

---

### 5.2 Input Sanitization

```python
from typing import Optional
import re

def sanitize_user_input(text: str, max_length: int = 500) -> str:
    """Sanitize user input to prevent injection attacks"""
    
    # Trim and limit length
    text = text.strip()[:max_length]
    
    # Remove potentially harmful characters
    # (but keep normal punctuation for natural conversation)
    text = re.sub(r'[<>{}]', '', text)
    
    return text

# Apply in endpoint
@router.post("/message")
async def send_message(req: ChatRequest):
    req.message = sanitize_user_input(req.message)
    # ... process
```

---

### 5.3 Response Caching

```python
# Cache common queries to reduce OpenAI API calls

from functools import lru_cache
import hashlib
import json

response_cache = {}

def get_cache_key(message: str, filters: Dict) -> str:
    """Generate cache key from message and context"""
    data = {
        "message": message.lower().strip(),
        "filters": filters
    }
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

async def get_cached_response(message: str, filters: Dict) -> Optional[Dict]:
    """Check if we have a cached response"""
    key = get_cache_key(message, filters)
    return response_cache.get(key)

async def cache_response(message: str, filters: Dict, response: Dict):
    """Cache a response (with TTL)"""
    key = get_cache_key(message, filters)
    response_cache[key] = {
        "data": response,
        "timestamp": datetime.utcnow()
    }
    
    # Implement TTL cleanup in production
```

---

### 5.4 Error Handling

```python
class ChatbotError(Exception):
    """Base chatbot error"""
    pass

class OpenAIServiceError(ChatbotError):
    """OpenAI API error"""
    pass

class ContextError(ChatbotError):
    """Context building error"""
    pass

@router.post("/message")
async def send_message(req: ChatRequest):
    try:
        # Validate portal exists
        portal_context = await get_portal_context(req.portal_slug, db)
        
        # Load conversation
        conversation = await load_or_create_conversation(
            req.conversation_id,
            req.portal_slug,
            db
        )
        
        # Process message
        response = await chatbot_service.process_message(
            user_message=req.message,
            portal_context=portal_context,
            conversation_history=conversation["messages"],
            current_filters=req.current_filters
        )
        
        return ChatResponse(
            message=response[0],
            conversation_id=conversation["conversation_id"],
            filters=response[1],
            job_ids=response[2]
        )
        
    except OpenAIServiceError as e:
        raise HTTPException(
            status_code=503,
            detail="AI service temporarily unavailable. Please try again."
        )
    except ContextError as e:
        raise HTTPException(
            status_code=400,
            detail="Invalid portal or conversation context"
        )
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred"
        )
```

---

## Phase 6: Testing Strategy

### 6.1 Backend Unit Tests

```python
# /tests/test_chatbot_service.py

import pytest
from unittest.mock import Mock, patch
from talentdb.scripts.services.chatbot_service import ChatbotService

@pytest.fixture
def chatbot_service():
    mock_db = Mock()
    return ChatbotService(mock_db)

@pytest.fixture
def portal_context():
    return {
        "portal_slug": "test-portal",
        "job_count": 10,
        "company_count": 5,
        "location_count": 3,
        "top_skills": ["React", "Python", "AWS"],
        "locations": ["Tel Aviv", "New York"],
        "companies": ["TechCorp", "DataCo"]
    }

class TestChatbotService:
    
    @patch('openai.OpenAI')
    async def test_simple_message(self, mock_openai, chatbot_service, portal_context):
        """Test basic message processing without function calls"""
        
        mock_response = Mock()
        mock_response.choices = [
            Mock(message=Mock(content="Hello! How can I help?", function_call=None))
        ]
        mock_openai.return_value.chat.completions.create.return_value = mock_response
        
        response, filters, jobs = await chatbot_service.process_message(
            user_message="Hello",
            portal_context=portal_context,
            conversation_history=[]
        )
        
        assert response == "Hello! How can I help?"
        assert filters is None
        assert jobs is None
    
    @patch('openai.OpenAI')
    async def test_filter_application(self, mock_openai, chatbot_service, portal_context):
        """Test filter extraction from natural language"""
        
        # Mock function call response
        mock_response = Mock()
        mock_response.choices = [
            Mock(message=Mock(
                function_call=Mock(
                    name="apply_job_filters",
                    arguments='{"location": "Tel Aviv", "skills": ["React"]}'
                )
            ))
        ]
        mock_openai.return_value.chat.completions.create.return_value = mock_response
        
        response, filters, jobs = await chatbot_service.process_message(
            user_message="Show me React jobs in Tel Aviv",
            portal_context=portal_context,
            conversation_history=[]
        )
        
        assert filters is not None
        assert len(filters) == 2
        assert any(f["filter_key"] == "location" and f["value"] == "Tel Aviv" for f in filters)
        assert any(f["filter_key"] == "skills" for f in filters)
    
    def test_conversation_starters_generation(self, chatbot_service, portal_context):
        """Test contextual starter generation"""
        
        starters = await chatbot_service.get_conversation_starters(portal_context)
        
        assert len(starters) <= 6
        assert any("React" in s for s in starters)  # Top skill
        assert any("Tel Aviv" in s for s in starters)  # Top location
```

---

### 6.2 Frontend Component Tests

```tsx
// /frontend/src/components/__tests__/ChatbotWidget.test.tsx

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ChatbotWidget } from '../ChatbotWidget';
import { rest } from 'msw';
import { setupServer } from 'msw/node';

const server = setupServer(
  rest.post('/portal/chat/message', (req, res, ctx) => {
    return res(
      ctx.json({
        message: "I've applied those filters!",
        conversation_id: 'test-conv-123',
        filters: [
          { type: 'set', filter_key: 'location', value: 'Tel Aviv' }
        ]
      })
    );
  })
);

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe('ChatbotWidget', () => {
  test('renders minimized button initially', () => {
    render(<ChatbotWidget portalSlug="test-portal" position="floating" />);
    
    const button = screen.getByRole('button');
    expect(button).toBeInTheDocument();
  });
  
  test('opens chat panel on button click', () => {
    render(<ChatbotWidget portalSlug="test-portal" position="floating" />);
    
    const button = screen.getByRole('button');
    fireEvent.click(button);
    
    expect(screen.getByPlaceholderText(/type your message/i)).toBeInTheDocument();
  });
  
  test('sends message and displays response', async () => {
    const onFilterChange = jest.fn();
    render(
      <ChatbotWidget
        portalSlug="test-portal"
        onFilterChange={onFilterChange}
        position="floating"
      />
    );
    
    // Open chat
    fireEvent.click(screen.getByRole('button'));
    
    // Type message
    const input = screen.getByPlaceholderText(/type your message/i);
    fireEvent.change(input, { target: { value: 'Show me Tel Aviv jobs' } });
    
    // Send
    const sendButton = screen.getByText(/send/i);
    fireEvent.click(sendButton);
    
    // Wait for response
    await waitFor(() => {
      expect(screen.getByText(/applied those filters/i)).toBeInTheDocument();
    });
    
    // Check filter callback was called
    expect(onFilterChange).toHaveBeenCalledWith(
      expect.objectContaining({ location: 'Tel Aviv' })
    );
  });
});
```

---

### 6.3 Integration Tests

```python
# /tests/test_chatbot_integration.py

import pytest
from httpx import AsyncClient
from talentdb.scripts.api import app

@pytest.mark.asyncio
async def test_full_conversation_flow():
    """Test complete conversation flow from start to finish"""
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Start conversation
        response1 = await client.post("/portal/chat/message", json={
            "message": "Hello",
            "portal_slug": "test-portal"
        })
        
        assert response1.status_code == 200
        data1 = response1.json()
        conversation_id = data1["conversation_id"]
        
        # Apply filters
        response2 = await client.post("/portal/chat/message", json={
            "message": "Show me React jobs in Tel Aviv",
            "portal_slug": "test-portal",
            "conversation_id": conversation_id
        })
        
        assert response2.status_code == 200
        data2 = response2.json()
        assert data2["filters"] is not None
        
        # Get conversation history
        response3 = await client.get(f"/portal/chat/conversation/{conversation_id}")
        
        assert response3.status_code == 200
        history = response3.json()
        assert len(history["messages"]) >= 2
```

---

## Phase 7: Deployment Plan

### 7.1 Environment Variables

```bash
# Add to /talentdb/.env

# Chatbot Configuration
CHATBOT_MODEL=gpt-4o
CHATBOT_TEMPERATURE=0.7
MAX_CONVERSATION_TOKENS=8000
OPENAI_REQUEST_TIMEOUT=30

# Rate Limiting
CHATBOT_RATE_LIMIT_ANONYMOUS=20
CHATBOT_RATE_LIMIT_WINDOW_MINUTES=5
CHATBOT_RATE_LIMIT_AUTHENTICATED=100

# Features
CHATBOT_VOICE_INPUT_ENABLED=false
CHATBOT_ANALYTICS_ENABLED=true
CHATBOT_CACHING_ENABLED=true
```

---

### 7.2 Database Migration

```python
# /migration_scripts/add_chatbot_collections.py

async def migrate():
    """Create chatbot collections and indexes"""
    
    # Create collections
    await db.create_collection("portal_conversations")
    await db.create_collection("chatbot_events")
    
    # Create indexes
    await db.portal_conversations.create_index("conversation_id", unique=True)
    await db.portal_conversations.create_index([("portal_slug", 1), ("created_at", -1)])
    await db.portal_conversations.create_index("updated_at", expireAfterSeconds=2592000)
    
    await db.chatbot_events.create_index([("portal_slug", 1), ("timestamp", -1)])
    await db.chatbot_events.create_index("timestamp", expireAfterSeconds=7776000)  # 90 days
    
    print("Chatbot collections and indexes created successfully")
```

---

### 7.3 Deployment Checklist

- [ ] Backend deployment
  - [ ] Add chatbot router to main API app
  - [ ] Configure OpenAI API key
  - [ ] Set up rate limiting
  - [ ] Create database indexes
  - [ ] Test endpoints

- [ ] Frontend deployment
  - [ ] Build ChatbotWidget component
  - [ ] Integrate with PortalPage
  - [ ] Test responsive design
  - [ ] Add error boundaries
  - [ ] Build and deploy static assets

- [ ] Monitoring
  - [ ] Set up logging for chatbot events
  - [ ] Configure error tracking (Sentry/similar)
  - [ ] Monitor OpenAI API usage
  - [ ] Track conversation metrics

- [ ] Documentation
  - [ ] API documentation
  - [ ] User guide for portal visitors
  - [ ] Admin guide for monitoring

---

## Phase 8: Success Metrics

### Key Performance Indicators (KPIs)

1. **Engagement Metrics**
   - Conversations started per portal visit: Target 30%+
   - Average messages per conversation: Target 5+
   - Conversation completion rate: Target 60%+

2. **Functionality Metrics**
   - Filter application success rate: Target 90%+
   - Job recommendation accuracy: Target 80%+ relevance
   - Response time: Target <3s average

3. **Business Metrics**
   - Job views increase: Target +25%
   - Application rate improvement: Target +15%
   - Time to application: Target -30%

### Analytics Dashboard

```python
# Admin endpoint for metrics
@router.get("/admin/chatbot/metrics")
async def get_chatbot_metrics(
    portal_slug: Optional[str] = None,
    date_range: int = 30  # days
):
    """Get comprehensive chatbot metrics"""
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=date_range)
    
    query = {"timestamp": {"$gte": start_date, "$lte": end_date}}
    if portal_slug:
        query["portal_slug"] = portal_slug
    
    metrics = {
        "conversations": {
            "total": await db.portal_conversations.count_documents(query),
            "with_filters": await db.portal_conversations.count_documents({
                **query,
                "metadata.applied_filters_count": {"$gt": 0}
            }),
            "avg_messages": None  # Calculate
        },
        "events": {
            "total": await db.chatbot_events.count_documents(query),
            "by_type": await db.chatbot_events.aggregate([
                {"$match": query},
                {"$group": {"_id": "$event_type", "count": {"$sum": 1}}}
            ]).to_list(length=None)
        },
        "performance": {
            "avg_response_time": None,  # Calculate from logs
            "error_rate": None
        }
    }
    
    return metrics
```

---

## Implementation Timeline

### Week 1: Foundation
- ✅ Backend router setup
- ✅ OpenAI service integration
- ✅ Database schema design
- ✅ Basic API endpoints

### Week 2: Core Features
- ✅ Function calling implementation
- ✅ Filter extraction logic
- ✅ Conversation management
- ✅ Context building

### Week 3: Frontend
- ✅ ChatbotWidget component
- ✅ Message rendering
- ✅ Filter synchronization
- ✅ UI/UX polish

### Week 4: Advanced Features
- ✅ Conversation starters
- ✅ Rich message formatting
- ✅ Job highlighting
- ✅ Voice input (optional)

### Week 5: Testing & Optimization
- ✅ Unit tests
- ✅ Integration tests
- ✅ Performance optimization
- ✅ Security audit

### Week 6: Deployment
- ✅ Production deployment
- ✅ Monitoring setup
- ✅ Documentation
- ✅ User training

---

## Appendix

### A. Example Conversations

**Scenario 1: Simple Search**
```
User: "Show me React jobs"
Assistant: "I found 8 React positions! I've filtered the jobs to show only React roles. 
Would you like me to narrow it down by location or experience level?"
[Applies filter: skills=React]
```

**Scenario 2: Complex Query**
```
User: "I'm looking for a senior frontend role in a startup, preferably remote"
Assistant: "I've found 3 senior frontend positions at startups with remote options:

1. **Senior Frontend Engineer** at TechFlow (Remote)
   - React, TypeScript, Next.js required
   
2. **Lead UI Developer** at DataVision (Remote)
   - Vue.js, React, AWS experience
   
3. **Frontend Architect** at InnovateCo (Hybrid - Tel Aviv)
   - React, System Design, Team Leadership

Which one would you like to learn more about?"
[Applies filters: query="senior frontend", type=remote]
[Highlights: job_123, job_456, job_789]
```

**Scenario 3: Comparison**
```
User: "What's the difference between the React jobs at TechFlow and DataVision?"
Assistant: "Great question! Here's a comparison:

**TechFlow - Senior Frontend Engineer**
- Focus: B2B SaaS product
- Stack: React, TypeScript, Next.js, AWS
- Team: 5-person frontend team
- Remote: Fully remote

**DataVision - Lead UI Developer**
- Focus: Data visualization platform
- Stack: Vue.js (primary), React, D3.js
- Team: Leading 3 developers
- Remote: Fully remote

The TechFlow role is more specialized in React/Next.js, while DataVision offers leadership experience and exposure to data visualization. Both are fully remote.

Would you like to apply to either of these?"
[Highlights: job_123, job_456]
```

---

### B. Error Messages

```
{
  "conversation_rate_limit": "You've sent too many messages. Please wait a moment and try again.",
  "openai_error": "I'm having trouble processing your request right now. Please try again in a moment.",
  "portal_not_found": "This job portal link is invalid or no longer active.",
  "invalid_filter": "I couldn't understand that filter. Could you rephrase?",
  "no_jobs_found": "I couldn't find any jobs matching those criteria. Try broadening your search?",
  "network_error": "Connection error. Please check your internet and try again."
}
```

---

### C. API Reference

**POST /portal/chat/message**
```typescript
Request:
{
  message: string;
  portal_slug: string;
  conversation_id?: string;
  current_filters?: {
    location?: string;
    skills?: string[];
    company?: string;
    type?: 'remote' | 'onsite';
    query?: string;
  };
  session_id?: string;
}

Response:
{
  message: string;
  conversation_id: string;
  filters?: Array<{
    type: 'set' | 'add' | 'remove' | 'clear';
    filter_key: 'location' | 'skills' | 'company' | 'type' | 'query';
    value: any;
  }>;
  job_ids?: string[];
  metadata?: {
    suggested_followups?: string[];
    confidence_score?: number;
  };
}
```

---

## Summary

This comprehensive plan provides a complete roadmap for implementing an AI-powered chatbot on the job portal page. The implementation follows best practices for:

✅ **Software Development**
- Modular, testable code architecture
- Clear separation of concerns
- Comprehensive error handling
- Security-first approach

✅ **UI/UX Design**
- Intuitive, conversational interface
- Responsive design (mobile & desktop)
- Accessibility considerations
- Smooth animations and feedback

✅ **AI Integration**
- OpenAI GPT-4o with function calling
- Intelligent filter extraction
- Context-aware responses
- Conversation memory

✅ **Production Ready**
- Rate limiting and security
- Performance optimization
- Monitoring and analytics
- Comprehensive testing

---

**Next Steps:**
1. Review and approve this plan
2. Set up development environment
3. Begin Phase 1 implementation
4. Iterate based on feedback

**Questions for Approval:**
1. Preferred chatbot position (floating vs embedded)?
2. Voice input priority (Phase 4 or later)?
3. Analytics requirements (basic or advanced)?
4. Any specific conversation flows to prioritize?

---

*Document Version: 1.0*  
*Last Updated: October 23, 2025*  
*Author: GitHub Copilot*
