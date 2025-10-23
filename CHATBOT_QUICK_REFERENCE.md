# Job Portal AI Chatbot - Quick Reference Guide

## 📋 Overview

Complete implementation plan for an OpenAI-powered chatbot that enables users to discuss jobs and apply filters using natural language.

---

## 🎯 Core Features

1. **Natural Language Filtering** - "Show me React jobs in Tel Aviv"
2. **Job Recommendations** - Suggest and highlight relevant positions
3. **Conversational Q&A** - Answer questions about jobs, companies, requirements
4. **Smart Context** - Remember conversation history and preferences

---

## 📁 Implementation Checklist

### Phase 1: Backend (Week 1-2)

#### Files to Create:
- [ ] `/talentdb/scripts/routers_portal_chatbot.py` - Main API router
- [ ] `/talentdb/scripts/services/chatbot_service.py` - OpenAI integration
- [ ] `/talentdb/scripts/middleware/rate_limit.py` - Rate limiting

#### Database:
- [ ] Create `portal_conversations` collection
- [ ] Create `chatbot_events` collection
- [ ] Add indexes for performance

#### API Endpoints:
- [ ] `POST /portal/chat/message` - Send message
- [ ] `GET /portal/chat/conversation/{id}` - Get history
- [ ] `POST /portal/chat/suggest` - Get conversation starters
- [ ] `DELETE /portal/chat/conversation/{id}` - Clear history

### Phase 2: Frontend (Week 3-4)

#### Files to Create:
- [ ] `/frontend/src/components/ChatbotWidget.tsx` - Main chatbot component
- [ ] `/frontend/src/components/chat/AssistantMessage.tsx` - Message display
- [ ] `/frontend/src/components/chat/UserMessage.tsx` - User message
- [ ] `/frontend/src/components/chat/QuickReplies.tsx` - Starter buttons
- [ ] `/frontend/src/components/chat/FilterBadge.tsx` - Filter chips

#### Files to Modify:
- [ ] `/frontend/src/pages/PortalPage.tsx` - Add chatbot integration
- [ ] `/frontend/src/api.ts` - Add chatbot API calls

### Phase 3: Testing (Week 5)

#### Backend Tests:
- [ ] Unit tests for chatbot service
- [ ] API endpoint tests
- [ ] Function calling tests
- [ ] Rate limiting tests

#### Frontend Tests:
- [ ] Component unit tests
- [ ] Integration tests
- [ ] E2E conversation tests
- [ ] Responsive design tests

### Phase 4: Deployment (Week 6)

- [ ] Configure environment variables
- [ ] Database migration
- [ ] Deploy backend changes
- [ ] Deploy frontend changes
- [ ] Set up monitoring
- [ ] Configure analytics

---

## 🔑 Key Configuration

### Environment Variables

```bash
# Add to /talentdb/.env

# OpenAI Configuration
OPENAI_API_KEY=sk-...
CHATBOT_MODEL=gpt-4o
CHATBOT_TEMPERATURE=0.7
MAX_CONVERSATION_TOKENS=8000

# Rate Limiting
CHATBOT_RATE_LIMIT_ANONYMOUS=20
CHATBOT_RATE_LIMIT_WINDOW_MINUTES=5
CHATBOT_RATE_LIMIT_AUTHENTICATED=100

# Features
CHATBOT_VOICE_INPUT_ENABLED=false
CHATBOT_ANALYTICS_ENABLED=true
CHATBOT_CACHING_ENABLED=true
```

### Database Indexes

```javascript
// MongoDB indexes to create
db.portal_conversations.createIndex({ "conversation_id": 1 }, { unique: true })
db.portal_conversations.createIndex({ "portal_slug": 1, "created_at": -1 })
db.portal_conversations.createIndex({ "updated_at": 1 }, { expireAfterSeconds: 2592000 })

db.chatbot_events.createIndex({ "portal_slug": 1, "timestamp": -1 })
db.chatbot_events.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 7776000 })
```

---

## 🎨 UI Components

### ChatbotWidget Props

```typescript
interface ChatbotWidgetProps {
  portalSlug: string;                          // Required: Portal identifier
  onFilterChange?: (filters: any) => void;     // Callback when filters applied
  onJobHighlight?: (jobIds: string[]) => void; // Callback when jobs highlighted
  position?: 'floating' | 'embedded';          // Display mode
  initialMessage?: string;                     // Optional welcome message
}
```

### Usage in PortalPage

```tsx
import { ChatbotWidget } from '../components/ChatbotWidget';

// Inside PortalPage component:
<ChatbotWidget
  portalSlug={slug || ''}
  onFilterChange={handleChatFilterChange}
  onJobHighlight={handleJobHighlight}
  position="floating"
/>
```

---

## 🤖 OpenAI Function Definitions

### 1. apply_job_filters
Applies filters based on user requirements

**Parameters:**
- `location` (string) - City or region
- `skills` (array) - Required skills
- `company` (string) - Company name
- `remote` (boolean) - Remote work option
- `query` (string) - General keywords
- `action` (string) - set|add|remove|clear

### 2. highlight_specific_jobs
Highlights jobs in the UI

**Parameters:**
- `job_ids` (array) - Job IDs to highlight
- `reason` (string) - Why highlighting

### 3. search_jobs
Search and return job details

**Parameters:**
- `location`, `skills`, `company`, `remote`, `query`
- `limit` (integer) - Max results (default: 5)

---

## 📊 Example Conversations

### Basic Filter Application
```
User: "Show me React jobs"
Bot: "I found 8 React positions! I've applied the filter. 
     Would you like to narrow down by location?"
[Applies: skills=["React"]]
```

### Complex Query
```
User: "Looking for senior frontend roles at startups, preferably remote"
Bot: "I found 3 senior frontend positions at startups with remote options:
     
     1. Senior Frontend Engineer at TechFlow (Remote)
     2. Lead UI Developer at DataVision (Remote)
     3. Frontend Architect at InnovateCo (Hybrid)
     
     Which one interests you?"
[Applies: query="senior frontend", type="remote"]
[Highlights: job_123, job_456, job_789]
```

### Job Comparison
```
User: "What's the difference between TechFlow and DataVision?"
Bot: "TechFlow focuses on React/Next.js for B2B SaaS, while 
     DataVision offers leadership experience with Vue.js and 
     data visualization. Both are fully remote."
[Highlights: job_123, job_456]
```

---

## 🔒 Security Best Practices

1. **Rate Limiting** - Prevent abuse with IP-based limits
2. **Input Sanitization** - Clean user input (max 500 chars)
3. **API Key Protection** - Never expose OpenAI key to frontend
4. **Conversation Expiry** - Auto-delete after 30 days
5. **Error Handling** - Graceful failures, no data leakage

---

## 📈 Success Metrics

### Engagement (Target)
- 30%+ conversation start rate
- 5+ messages per conversation
- 60%+ completion rate

### Functionality (Target)
- 90%+ filter accuracy
- 80%+ recommendation relevance
- <3s response time

### Business Impact (Target)
- +25% job views
- +15% application rate
- -30% time to apply

---

## 💰 Cost Estimate

### OpenAI API (GPT-4o)
- ~$0.04 per conversation (8 messages avg)
- 100 conversations/day = $120/month
- 500 conversations/day = $600/month

### Optimization Options
- Use GPT-4o-mini: 75% cost reduction
- Response caching: 40% fewer API calls
- Conversation summarization: 30% fewer tokens

---

## 🚀 Quick Start

### 1. Backend Setup (30 minutes)

```bash
# Install dependencies
pip install openai python-dotenv

# Add to .env
echo "OPENAI_API_KEY=sk-..." >> talentdb/.env
echo "CHATBOT_MODEL=gpt-4o" >> talentdb/.env

# Create database collections
python migration_scripts/add_chatbot_collections.py

# Register router in api.py
# from .routers_portal_chatbot import router as chatbot_router
# app.include_router(chatbot_router)
```

### 2. Frontend Setup (30 minutes)

```bash
# Install dependencies
npm install framer-motion react-markdown

# Create component files (see checklist)

# Import in PortalPage.tsx
# import { ChatbotWidget } from '../components/ChatbotWidget';
```

### 3. Test

```bash
# Start backend
python run_server.py

# Start frontend
cd frontend && npm run dev

# Visit portal page
# Click chat button
# Send test message: "Show me React jobs"
```

---

## 📝 API Reference

### POST /portal/chat/message

**Request:**
```json
{
  "message": "Show me React jobs in Tel Aviv",
  "portal_slug": "company-jobs",
  "conversation_id": "uuid-optional",
  "current_filters": {
    "location": "Tel Aviv",
    "skills": ["React"]
  }
}
```

**Response:**
```json
{
  "message": "I found 5 React positions in Tel Aviv...",
  "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
  "filters": [
    {
      "type": "set",
      "filter_key": "location",
      "value": "Tel Aviv"
    },
    {
      "type": "set",
      "filter_key": "skills",
      "value": ["React"]
    }
  ],
  "job_ids": ["job_123", "job_456"]
}
```

---

## 🐛 Troubleshooting

### Common Issues

**1. "OpenAI API error"**
- Check OPENAI_API_KEY is set
- Verify API key is valid
- Check rate limits on OpenAI account

**2. "Filters not applying"**
- Verify onFilterChange callback is connected
- Check filter format matches PortalPage expectations
- Debug console for errors

**3. "Slow responses"**
- Check network latency
- Consider response caching
- Monitor OpenAI API response times

**4. "Conversation not persisting"**
- Verify MongoDB connection
- Check conversation_id is being saved
- Ensure indexes are created

---

## 📚 Additional Resources

### Full Documentation
- `JOB_PORTAL_CHATBOT_IMPLEMENTATION_PLAN.md` - Complete backend/frontend details
- `JOB_PORTAL_CHATBOT_IMPLEMENTATION_PLAN_PART2.md` - Advanced features & testing

### Code Examples
- Backend: See implementation plan for complete service code
- Frontend: See implementation plan for component examples
- Tests: Comprehensive test suites included

### External References
- [OpenAI Function Calling Guide](https://platform.openai.com/docs/guides/function-calling)
- [GPT-4o Documentation](https://platform.openai.com/docs/models/gpt-4o)
- [React TypeScript Best Practices](https://react-typescript-cheatsheet.netlify.app/)

---

## ✅ Pre-Deployment Checklist

- [ ] Environment variables configured
- [ ] Database indexes created
- [ ] API endpoints tested
- [ ] Frontend components built
- [ ] Rate limiting verified
- [ ] Error handling tested
- [ ] Responsive design checked
- [ ] Accessibility reviewed
- [ ] Monitoring configured
- [ ] Analytics set up
- [ ] Documentation complete
- [ ] User guide prepared

---

## 🎯 Next Steps

1. **Review** this plan and the detailed implementation documents
2. **Approve** the architecture and approach
3. **Set up** development environment
4. **Begin** Phase 1 implementation
5. **Iterate** based on testing and feedback

---

**Questions for Approval:**

1. **Position**: Floating button (default) or embedded in page?
2. **Features**: Voice input in Phase 1 or later?
3. **Analytics**: Basic tracking or advanced metrics?
4. **Conversation starters**: Auto-generate or manually curate?
5. **Mobile**: Bottom sheet or full-screen modal?

---

*Version: 1.0*  
*Last Updated: October 23, 2025*  
*Total Implementation Time: 34-51 hours*  
*Estimated Cost: $120-600/month (based on usage)*
