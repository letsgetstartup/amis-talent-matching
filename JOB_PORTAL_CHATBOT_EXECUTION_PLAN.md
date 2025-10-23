# Job Portal Chatbot – End-to-End Execution Plan

_Last updated: October 23, 2025_

## 1. Mission & Success Criteria

### Objectives
- Embed an OpenAI-powered chatbot into the public job portal so visitors can explore jobs conversationally.
- Allow the chatbot to manipulate portal filters via natural language.
- **New requirement:** When visitors arrive through redirect links (e.g. `http://localhost:8000/portal/<slug>/redirect/<externalJobUrl>`), automatically inject a seeded message `find similar jobs for me` tied to the external job URL so the bot greets the user with tailored job recommendations.

### Definition of Done
- Chatbot UI is available, accessible, and responsive on the portal page.
- Users can send free-form questions; assistant responds using real data and applies filters.
- Redirect-triggered journeys pre-populate the chat, show suggested jobs, and display the message _“I found a few jobs for you: … have you found any of them interesting?”_
- All new APIs, hooks, and components are fully tested (unit + integration), lint-clean, and instrumented for observability.

## 2. Guiding Principles
- **Security first:** never expose secrets client-side; sanitize inputs & outputs.
- **Fail safe:** degrade gracefully when OpenAI or backend components are unavailable.
- **State cohesion:** keep URL/query params, job list filters, and chatbot context synchronized.
- **No regressions:** add regression tests for existing portal flows and redirect logic.

## 3. Delivery Phases

### Phase A – Foundations & Environment
1. Confirm `.env` entries (`OPENAI_API_KEY`, `CHATBOT_MODEL`, chatbot rate limits) and document rotation policy.
2. Create feature flag `CHATBOT_PORTAL_ENABLED` for staged rollout.
3. Add migration for Mongo collections:
   - `portal_conversations` (with TTL & indexes).
   - `chatbot_events` for analytics.
4. Provision automated test fixtures (factory to seed tenant, portal, jobs, redirect mapping).

### Phase B – Backend Capabilities
1. **Routing layer**
   - Add `routers_portal_chatbot.py` to FastAPI app with endpoints:
     - `POST /portal/chat/message`
     - `GET /portal/chat/conversation/{id}`
     - `POST /portal/chat/suggest`
     - `DELETE /portal/chat/conversation/{id}`
   - Register router in `talentdb/scripts/api.py` behind feature flag.
2. **Chatbot service**
   - Build `ChatbotService` encapsulating OpenAI calls with function-calling definitions (`apply_job_filters`, `highlight_specific_jobs`, `search_jobs`).
   - Implement retry/backoff, timeout handling, and redaction logging.
3. **Redirect-trigger injector**
   - Extend existing redirect controller (likely in `routers_portal.py` or relevant module):
     - Parse external job URL from `/portal/{slug}/redirect/{encodedUrl}`.
     - Resolve matching job(s) via DB using canonical URL mapping.
     - Generate `chat_seed` payload `{ external_url, inferred_job_ids, injected_prompt: "find similar jobs for me" }`.
     - Persist payload to short-lived store (signed cookie, encrypted query param, or server session) to avoid public tampering.
     - Redirect to `/portal/{slug}?chat_seed=<token>`.
   - Ensure fallback when job not found (seed with external URL only).
4. **Conversation seeding API**
   - New endpoint `POST /portal/chat/seed` (optional) that frontend can call with seed token to obtain structured context & message template.
5. **Testing**
   - Unit tests for chatbot service (mock OpenAI) verifying function call parsing.
   - Integration tests for redirect flow: hitting redirect URL yields seed token, chatbot endpoint auto-produces greeting message.
   - Contract tests for filter application & error modes (rate limiting, invalid slug).

### Phase C – Frontend Integration
1. **API Layer**
   - Add typed clients in `frontend/src/api.ts` for chat endpoints (message, suggest, seed fetch).
   - Centralize error mapping & telemetry.
2. **Chat UI**
   - Create `ChatbotWidget.tsx` (floating by default) with states: closed, open, streaming, error.
   - Subcomponents: `AssistantMessage`, `UserMessage`, `FilterBadge`, `QuickReplies`, `TypingIndicator`.
   - Use `framer-motion` for open/close transitions; `react-markdown` for content.
3. **PortalPage wiring**
   - Introduce React context or prop drilling to share filter setters with chatbot.
   - Maintain `highlightedJobIds` to visually mark suggestions; add `aria-live` updates for accessibility.
   - Ensure keyboard navigation (focus trap when chat open, ESC to close).
4. **Redirect seed handling**
   - On mount, detect `chat_seed` query param.
   - Call backend seed endpoint to retrieve `{ intro_message, inferred_jobs }`.
   - Auto-open widget, push system message _“I found a few jobs for you: … have you found any of them interesting?”_ listing job titles with deep links.
   - Immediately enqueue hidden user message `find similar jobs for me` so backend conversation history reflects correct context.
   - Clear seed token from URL (history replace) to avoid repeat triggers.
5. **Filter synchronization**
   - Implement `applyFilterActions` to update React state + URL slug (use existing debounced sync logic).
   - Support highlight scroll into view (smooth scroll, focus management).
6. **Empty/error states**
   - Show offline banner if chatbot unavailable.
   - Provide manual quick replies when seed data missing.

### Phase D – Quality Gates
1. **Testing matrix**
   - Backend: `pytest` (chatbot, redirect, rate limiting) with fixtures mocking OpenAI.
   - Frontend: `vitest`/`jest` + React Testing Library for widget + redirect seed behaviour.
   - Cypress/Playwright scenarios:
     - Standard portal browse + chat.
     - Redirect entry triggers auto greeting.
     - Mobile viewport interactions.
2. **Lint & formatting**
   - Run `black`, `isort`, `flake8` (or existing pipeline) for backend; `eslint`, `prettier` for frontend.
3. **Accessibility audit**
   - Verify color contrast, focus states, aria labels (`role="log"` for message list, `aria-live`).
4. **Performance**
   - Lazy-load chatbot bundle; preload fonts/icons.
   - Cache conversation starters to minimize OpenAI hits.
5. **Security checks**
   - Validate redirect token signatures.
   - Add rate-limiter tests for abusive patterns.
   - Ensure no sensitive data logged.

### Phase E – Deployment & Observability
1. **Feature flag rollout** (internal -> beta -> general).
2. **Monitoring**
   - Emit structured logs for chat events and redirect seeds.
   - Add metrics: conversation count, filter actions, seed activations, OpenAI failures.
   - Configure alerts on elevated error rates or API cost spikes.
3. **Documentation**
   - Update README + runbook with environment variables and troubleshooting.
   - Provide internal FAQ for customer support.
4. **Post-launch review**
   - Collect user feedback; analyze analytics (conversion lift).
   - Schedule iteration backlog (voice input, streaming, personalization).

## 4. Risk Mitigation
- **OpenAI downtime:** fallback message guiding users to manual filters.
- **Redirect tampering:** sign & validate tokens; default to standard welcome if invalid.
- **High latency:** implement optimistic UI & skeletons, leverage OpenAI streaming later.
- **Cost overrun:** monitor token usage; consider GPT-4o-mini for low-complexity chats.

## 5. Traceability Matrix
| Requirement | Plan Section |
|-------------|--------------|
| Chat assistant on portal | Phase C.2–C.3 |
| Filter control | Phase B.2 + C.5 |
| Redirect-triggered greeting | Phase B.3 & C.4 |
| Auto message "find similar jobs for me" | Phase B.3, C.4 |
| Greeting text with job list | Phase C.4 |
| Testing coverage | Phase D |

---

**Ready for implementation once stakeholders approve.**