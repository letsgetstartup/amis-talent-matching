# PTX Rejected Candidate Portal - GitHub Copilot Execution Plan

**Project**: AI-Powered Rejected Candidate Chatbot Portal  
**Goal**: Transform job rejections into opportunities by recommending similar roles through an intelligent chatbot interface  
**Target**: Multi-tenant SaaS job portal with MongoDB + FastAPI backend + React/TypeScript frontend

---

## 📋 Table of Contents

1. [Prerequisites & Setup](#prerequisites--setup)
2. [Phase 1: Multi-Tenant Data & Security](#phase-1-multi-tenant-data--security)
3. [Phase 2: Job Matching Engine](#phase-2-job-matching-engine)
4. [Phase 3: Public Portal APIs](#phase-3-public-portal-apis)
5. [Phase 4: Candidate Authentication & Profile](#phase-4-candidate-authentication--profile)
6. [Phase 5: Job Application System](#phase-5-job-application-system)
7. [Phase 6: Admin Configuration UI](#phase-6-admin-configuration-ui)
8. [Phase 7: Portal Frontend & Chatbot](#phase-7-portal-frontend--chatbot)
9. [Phase 8: File Upload (CV/Resume)](#phase-8-file-upload-cvresume)
10. [Phase 9: Testing & Validation](#phase-9-testing--validation)
11. [Phase 10: Deployment & Monitoring](#phase-10-deployment--monitoring)

---

## Prerequisites & Setup

### Environment Variables
**File**: `/Users/avirammizrahi/Desktop/amis/talentdb/.env`

Add the following variables:
```bash
# Existing
MONGO_URI=mongodb://localhost:27017
DB_NAME=talent_match
JWT_SECRET=your-secret-key-here
OPENAI_API_KEY=sk-...

# New for PTX Portal
UPLOAD_DIR=/tmp/uploads
MAX_UPLOAD_SIZE_MB=10
AWS_S3_BUCKET=ptx-resumes  # Optional: for production S3 storage
AWS_REGION=us-east-1
SMTP_HOST=smtp.sendgrid.net  # For application notifications
SMTP_PORT=587
SMTP_USER=apikey
SMTP_PASSWORD=your-sendgrid-api-key
FRONTEND_URL=http://localhost:5173  # For CORS and email links
```

### Dependencies
**File**: `/Users/avirammizrahi/Desktop/amis/requirements.txt`

```bash
# Copilot prompt: Add these dependencies if not present
# fastapi-multipart support, bcrypt for passwords, boto3 for S3
python-multipart>=0.0.6
bcrypt>=4.0.1
boto3>=1.28.0  # Optional: for S3 uploads
aiosmtplib>=2.0.0  # For async email sending
```

Install:
```bash
cd /Users/avirammizrahi/Desktop/amis
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Phase 1: Multi-Tenant Data & Security

### 1.1 Update Users Collection Index

**Target File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/db.py`

**Copilot Prompt**:
```python
# In db.py, add a function to create/update indexes for multi-tenant users
# Create unique compound index on users collection: (tenant_id, email)
# Drop old unique index on email if it exists
# Function: ensure_user_indexes()
# Use: db.users.drop_index("email_1") if exists, then db.users.create_index([("tenant_id", 1), ("email", 1)], unique=True)
```

**Implementation**:
```python
def ensure_user_indexes():
    """Ensure users collection has correct multi-tenant indexes."""
    db = get_db()
    users = db.users
    
    # Drop old single-field email index if exists
    try:
        users.drop_index("email_1")
        print("✓ Dropped old email_1 index")
    except Exception:
        pass  # Index may not exist
    
    # Create compound index for tenant_id + email
    try:
        users.create_index([("tenant_id", 1), ("email", 1)], unique=True, name="tenant_email_unique")
        print("✓ Created tenant_email_unique index")
    except Exception as e:
        print(f"Index already exists or error: {e}")
    
    return True
```

**Migration Script**: Create new file `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/migrate_users_tenant.py`

**Copilot Prompt**:
```python
# Create migration script to:
# 1. Check all users have tenant_id (default to "default" if missing)
# 2. Call ensure_user_indexes() from db.py
# 3. Report any duplicate (tenant_id, email) pairs that need manual resolution
# 4. Make idempotent (safe to re-run)
```

### 1.2 Applications Collection Schema

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/models_applications.py`

**Copilot Prompt**:
```python
# Create Pydantic models for job applications:
# ApplicationCreate: tenant_id, job_id, candidate_id, status (default "new"), created_at, resume_url (optional), cover_letter (optional)
# ApplicationOut: inherits ApplicationCreate + _id, updated_at
# ApplicationStatus: Enum ["new", "viewed", "screening", "interview", "rejected", "hired"]
# 
# Add helper functions:
# - create_application(data: ApplicationCreate) -> ApplicationOut
# - get_applications_by_candidate(tenant_id, candidate_id) -> List[ApplicationOut]
# - get_applications_by_job(tenant_id, job_id) -> List[ApplicationOut]
# - update_application_status(application_id, status: ApplicationStatus) -> ApplicationOut
#
# Use db.applications collection with indexes on:
# - (tenant_id, candidate_id, job_id) unique
# - (tenant_id, job_id)
# - (tenant_id, candidate_id)
```

**Implementation Checklist**:
- [ ] Pydantic models defined
- [ ] Database indexes created
- [ ] CRUD helper functions implemented
- [ ] Unit tests created in `/Users/avirammizrahi/Desktop/amis/tests/test_applications.py`

---

## Phase 2: Job Matching Engine

### 2.1 Similarity Scoring Service

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/matching_service.py`

**Copilot Prompt**:
```python
# Implement get_similar_jobs(job_id: str, tenant_id: str, k: int = 5) -> List[dict]
# Algorithm:
# 1. Load base job by _id, extract: tenant_id, must_have_skills (pipe-separated), nice_to_have_skills, title, profession, tags
# 2. Parse skills into set (split by |, strip, lowercase)
# 3. Query all other jobs in same tenant with status="open" or status="active"
# 4. For each candidate job:
#    a. Parse its skills into set
#    b. Compute Jaccard similarity: |A ∩ B| / |A ∪ B|
#    c. Compute title similarity: 1 if same profession, 0.5 if shared word in title (stem/lowercase)
#    d. Final score = 0.85 * skill_jaccard + 0.15 * title_bonus
# 5. Sort by score descending, take top k
# 6. Return list of dicts: {
#      "job_id": str,
#      "title": str,
#      "city": str,
#      "remote": bool,
#      "overlap_skills": List[str],  # intersection
#      "match_score": float  # 0-1
#    }
#
# Edge cases:
# - If base job not found, raise HTTPException 404
# - If base job has no skills, return empty list
# - Exclude the original job_id from results
```

**Unit Test File**: `/Users/avirammizrahi/Desktop/amis/tests/test_matching_service.py`

**Copilot Prompt for Tests**:
```python
# Create pytest tests for matching_service:
# 1. test_exact_skill_match(): job A and B share 100% skills -> score ~0.85
# 2. test_partial_skill_match(): job A and B share 50% skills -> score ~0.425
# 3. test_title_bonus(): same profession adds 0.15
# 4. test_no_overlap(): no shared skills -> score ~0
# 5. test_exclude_original_job(): original job not in results
# 6. test_limit_k(): returns max k results
# 7. test_tenant_isolation(): only returns jobs from same tenant
# 8. test_job_not_found(): raises 404
```

---

## Phase 3: Public Portal APIs

### 3.1 Portal Router

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/routers_portal.py`

**Copilot Prompt**:
```python
# Create FastAPI APIRouter for public portal endpoints (no auth required)
# 
# GET /portal/recommendations/{job_id}
# - Query param: tenant_code (optional, derive from job if missing)
# - Load job by _id, validate exists
# - Call matching_service.get_similar_jobs(job_id, tenant_id, k=5)
# - Return JSON:
#   {
#     "original_job": {
#       "id": str,
#       "title": str,
#       "company": str  # from job.company_name
#     },
#     "recommendations": [
#       {
#         "id": str,
#         "title": str,
#         "location": str,
#         "remote": bool,
#         "match_score": float,
#         "overlap_skills": List[str]
#       }
#     ],
#     "tenant_code": str  # for frontend to use in URLs
#   }
# - Error handling: 404 if job not found, 500 for internal errors
#
# GET /portal/job/{job_id}
# - Return public-safe job details (NO salary, internal notes, contact info)
# - Fields: id, title, description, must_have_skills, nice_to_have_skills, 
#           city, remote, profession, tags, company_name
# - 404 if job not found or status != "open"
#
# GET /portal/tenant/{tenant_code}/jobs
# - Optional: list all open jobs for a tenant (for "Browse all openings" link)
# - Query params: page (default 1), limit (default 20)
# - Return paginated list of public job summaries
```

**Register Router**: Add to `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

```python
from .routers_portal import router as portal_router
app.include_router(portal_router, prefix="/portal", tags=["Portal"])
```

**Test File**: `/Users/avirammizrahi/Desktop/amis/tests/test_portal_api.py`

**Copilot Prompt for Tests**:
```python
# Create pytest tests for portal API:
# 1. test_get_recommendations_success(): valid job_id returns recommendations
# 2. test_get_recommendations_job_not_found(): invalid job_id returns 404
# 3. test_get_job_details_public(): returns only public fields
# 4. test_get_job_details_excludes_sensitive(): verify salary, notes not in response
# 5. test_tenant_jobs_list(): returns paginated jobs for tenant
# 6. test_tenant_isolation(): tenant A cannot see tenant B jobs
# Use TestClient from fastapi.testclient
```

---

## Phase 4: Candidate Authentication & Profile

### 4.1 Candidate Registration

**Target File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/routers_auth.py`

**Copilot Prompt**:
```python
# Add POST /auth/register-candidate endpoint to existing auth router
# Request body (Pydantic model):
# class CandidateRegister(BaseModel):
#     name: str
#     email: EmailStr
#     password: str (min 8 chars)
#     tenant_id: str
#     phone: Optional[str] = None
# 
# Logic:
# 1. Validate email format and password strength
# 2. Check if (tenant_id, email) already exists -> 400 "Email already registered"
# 3. Hash password using bcrypt (import from passlib.hash import bcrypt)
# 4. Create user document: {
#      tenant_id, email, password_hash, role: "candidate", 
#      created_at, is_active: True
#    }
# 5. Create candidate profile document: {
#      tenant_id, user_id, name, email, phone,
#      resume_url: None, skills: [], experience_years: 0,
#      created_at
#    }
# 6. Generate JWT token (use existing auth.create_access_token)
# 7. Return {token, user: {id, email, role, tenant_id}}
#
# Error handling:
# - 400 for validation errors
# - 409 for duplicate email
# - 500 for database errors
```

### 4.2 Candidate Profile Endpoints

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/routers_candidates.py`

**Copilot Prompt**:
```python
# Create candidate profile router (requires auth, role=candidate)
# Use existing auth dependency: Depends(require_role("candidate"))
#
# GET /candidates/me
# - Return current candidate profile:
#   {id, user_id, name, email, phone, resume_url, skills, 
#    experience_years, bio, location, created_at}
# - 404 if profile not found
#
# PUT /candidates/me
# - Update profile fields: name, phone, skills (List[str]), 
#   experience_years, bio, location
# - Validate skills are non-empty strings
# - Return updated profile
# - 400 for validation errors
#
# POST /candidates/me/upload-cv
# - Accept multipart file upload
# - Validate file type: .pdf, .doc, .docx (use python-magic or extension check)
# - Validate file size < MAX_UPLOAD_SIZE_MB (from env)
# - Save to UPLOAD_DIR/{tenant_id}/resumes/{candidate_id}_{timestamp}.{ext}
# - OR upload to S3 if AWS_S3_BUCKET is set
# - Update candidate profile with resume_url
# - Return {resume_url, uploaded_at}
# - Error handling: 413 for file too large, 415 for invalid type
#
# DELETE /candidates/me/cv
# - Delete resume file from storage
# - Set resume_url = None in profile
# - Return 204 No Content
```

**Register Router**: Add to `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

```python
from .routers_candidates import router as candidates_router
app.include_router(candidates_router, prefix="/candidates", tags=["Candidates"])
```

**Test File**: `/Users/avirammizrahi/Desktop/amis/tests/test_candidate_profile.py`

---

## Phase 5: Job Application System

### 5.1 Application Router

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/routers_applications.py`

**Copilot Prompt**:
```python
# Create application router (requires candidate auth)
#
# POST /jobs/{job_id}/apply
# - Requires auth, role=candidate
# - Request body (optional): {cover_letter: str}
# - Logic:
#   1. Load job by job_id, verify exists and status="open"
#   2. Verify job.tenant_id == user.tenant_id (tenant isolation)
#   3. Load candidate profile to get resume_url
#   4. Check if already applied: query applications by (tenant_id, job_id, candidate_id)
#      - If exists and status != "rejected", return 409 "Already applied"
#      - If exists and status == "rejected", allow re-apply (update status to "new")
#   5. Create/update application: {
#        tenant_id, job_id, candidate_id, resume_url,
#        cover_letter, status: "new", created_at, updated_at
#      }
#   6. [OPTIONAL] Send email notification to recruiter (async task)
#   7. Return application object
# - Error handling: 404 job not found, 403 tenant mismatch, 409 duplicate
#
# GET /applications/me
# - Return all applications for current candidate
# - Include job details (title, company) via join/lookup
# - Sort by created_at descending
# - Pagination: ?page=1&limit=20
#
# GET /applications/{application_id}
# - Return single application with full job details
# - Verify candidate owns the application (candidate_id == current_user.id)
# - 403 if not owner, 404 if not found
```

**Register Router**: Add to `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

```python
from .routers_applications import router as applications_router
app.include_router(applications_router, prefix="/applications", tags=["Applications"])
# Also mount under /jobs for POST /jobs/{job_id}/apply
```

### 5.2 Email Notification Service

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/email_service.py`

**Copilot Prompt**:
```python
# Create async email notification service
# 
# async def send_application_notification(
#     recruiter_email: str,
#     job_title: str,
#     candidate_name: str,
#     candidate_email: str,
#     application_url: str
# ):
#     """Send email to recruiter when candidate applies."""
#     # Use aiosmtplib for async SMTP
#     # Email template:
#     # Subject: New Application: {job_title}
#     # Body:
#     #   Hi,
#     #   
#     #   {candidate_name} ({candidate_email}) has applied for {job_title}.
#     #   
#     #   View application: {application_url}
#     #   
#     #   Best regards,
#     #   PTX Talent Team
#     
#     # Use env vars: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
#     # Handle errors gracefully (log but don't fail application creation)
#     pass
```

**Test File**: `/Users/avirammizrahi/Desktop/amis/tests/test_applications.py`

---

## Phase 6: Admin Configuration UI

### 6.1 Backend: Tenant Settings Endpoint

**Target File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/routers_tenant_jobs.py` (or create new tenant router)

**Copilot Prompt**:
```python
# Add GET /admin/tenant/settings endpoint (requires admin role)
# Return tenant configuration including:
# - tenant_id, tenant_code (slug), tenant_name
# - portal_url_template: "https://{FRONTEND_URL}/portal/{tenant_code}?job={JOB_ID}"
# - Features enabled: {portal_enabled: bool, chat_enabled: bool}
# 
# Add PUT /admin/tenant/settings
# - Update tenant settings (name, features)
# - Validate tenant_code is URL-safe (alphanumeric + hyphens)
# - Return updated settings
```

### 6.2 Frontend: Settings Modal Component

**Target File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/components/SettingsModal.tsx` (create if not exists)

**Copilot Prompt (TypeScript/React)**:
```typescript
// Create SettingsModal component for admin users
// Props: isOpen: boolean, onClose: () => void
// 
// Features:
// 1. Fetch tenant settings from GET /admin/tenant/settings
// 2. Display sections:
//    a. General Settings: tenant name, code
//    b. Portal Link Template:
//       - Read-only input with URL template
//       - "Copy Link Template" button using navigator.clipboard.writeText()
//       - Helper text: "Paste this URL in your ATS rejection emails. 
//         Replace {JOB_ID} with the actual job ID for each position."
//    c. Feature Toggles: portal_enabled, chat_enabled (checkboxes)
// 3. Save button calls PUT /admin/tenant/settings
// 4. Show success/error toast notifications
// 
// Use existing UI components from your design system
// Style with Tailwind CSS or styled-components (match existing app style)
```

**Integration**: Add settings button to admin dashboard

**Target File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/pages/AdminTenantPage.tsx`

```typescript
// Add "Settings" or "⚙️" button in header that opens <SettingsModal />
```

---

## Phase 7: Portal Frontend & Chatbot

### 7.1 Portal Page Route

**Target File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/App.tsx`

**Copilot Prompt**:
```typescript
// Add public route (no auth required):
// <Route path="/portal/:tenantCode" element={<PortalPage />} />
// 
// This route should be accessible without authentication
// Place it before authenticated routes in the router config
```

### 7.2 Portal Page Component

**New File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/pages/PortalPage.tsx`

**Copilot Prompt**:
```typescript
// Create PortalPage component
// 
// Features:
// 1. Read URL params: tenantCode (from route), job (from query ?job=JOB_ID)
// 2. useEffect to fetch recommendations: GET /portal/recommendations/{job_id}
// 3. State management:
//    - recommendations: Array<JobRecommendation>
//    - originalJob: {id, title, company}
//    - loading: boolean
//    - error: string | null
// 4. Layout:
//    - Header: "Sorry, we couldn't move forward with your application for {originalJob.title}"
//    - Subheader: "But we think these roles might be a great fit for you:"
//    - Main content: <Chatbot /> component with recommendations
//    - Footer: link to "View all open positions" -> /portal/{tenantCode}/jobs
// 5. Error handling:
//    - If job_id missing: show friendly message "No job specified"
//    - If 404: show "Job not found or no longer available"
//    - If network error: show retry button
// 6. Styling: Professional, empathetic tone; mobile-responsive
// 
// TypeScript interfaces:
// interface JobRecommendation {
//   id: string;
//   title: string;
//   location: string;
//   remote: boolean;
//   match_score: number;
//   overlap_skills: string[];
// }
```

### 7.3 Chatbot Component

**New File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/components/chat/Chatbot.tsx`

**Copilot Prompt**:
```typescript
// Create conversational Chatbot component (rule-based, no LLM needed for MVP)
// 
// Props:
// - recommendations: JobRecommendation[]
// - tenantCode: string
// - originalJobId: string
// - onViewDetails: (jobId: string) => void
// - onApply: (jobId: string) => void
// 
// State:
// - messages: Array<{id: string, sender: 'bot' | 'user', text: string, timestamp: Date}>
// - inputValue: string
// - selectedJobId: string | null
// 
// Initial bot message (on mount):
// "Hi! I'm here to help you find your next opportunity. Based on your application, 
// I found {recommendations.length} roles that match your skills:
// 1. {job1.title} at {location} ({match_score}% match)
// 2. {job2.title}...
// 
// What would you like to do?
// - Type a number to learn more (e.g., "Tell me about #1")
// - Ask questions (e.g., "Are any of these remote?")
// - Apply to a role"
// 
// Message handling logic:
// - Number pattern (e.g., "1", "#2", "job 3"): 
//   -> Call onViewDetails(recommendations[index-1].id), show job details card
// - "remote" keyword: 
//   -> Filter and show only remote roles
// - "apply" + number/selected job:
//   -> Check if user is logged in (call API /auth/me or check localStorage token)
//   -> If not logged in: prompt "Please log in or register to apply" + show auth buttons
//   -> If logged in: call onApply(jobId)
// - "more details", "tell me about":
//   -> If selectedJobId: show full job description
//   -> Else: "Which role would you like to know more about? Type a number."
// - Default fallback:
//   -> "I can help you explore these jobs. Try typing a number (1-{count}) 
//      to see details, or ask about remote positions."
// 
// UI:
// - Chat bubble design (bot messages left-aligned, user right-aligned)
// - Auto-scroll to bottom on new message
// - Input field with Send button (and Enter key support)
// - Quick reply buttons for common actions:
//   ["Show Remote Only", "Apply to #1", "Not Interested"]
// - Show typing indicator when processing
// 
// Accessibility:
// - ARIA labels for screen readers
// - Keyboard navigation support
// - Focus management
```

### 7.4 Job Details Card Component

**New File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/components/chat/JobDetailCard.tsx`

**Copilot Prompt**:
```typescript
// Create JobDetailCard component to display full job details
// 
// Props:
// - jobId: string
// - onApply: (jobId: string) => void
// - onClose: () => void
// 
// Features:
// 1. Fetch job details: GET /portal/job/{jobId}
// 2. Display:
//    - Title + company
//    - Location + remote badge
//    - Must-have skills (pills/tags)
//    - Nice-to-have skills
//    - Full description (with formatted paragraphs)
//    - Match highlights: "Your skills match: {overlap_skills.join(', ')}"
// 3. Actions:
//    - Primary button: "Apply Now" -> calls onApply(jobId)
//    - Secondary: "Back to Chat" -> calls onClose()
// 4. Loading state while fetching
// 5. Mobile-responsive card design
```

### 7.5 Registration & Login Flow

**New File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/pages/RegistrationPage.tsx`

**Copilot Prompt**:
```typescript
// Create candidate registration page
// 
// Features:
// 1. Form fields: name, email, password, phone (optional)
// 2. Read from URL query params:
//    - ?tenant={tenantCode} -> convert to tenant_id (may need API call)
//    - ?next={returnUrl} -> where to redirect after signup
// 3. Form validation:
//    - Email format check
//    - Password strength: min 8 chars, 1 uppercase, 1 number
//    - Confirm password field (must match)
// 4. Submit: POST /auth/register-candidate
// 5. On success:
//    - Save JWT to localStorage
//    - Update auth context (if using React Context)
//    - Redirect to ?next URL or /candidate/jobs
// 6. Error handling:
//    - Show validation errors inline
//    - 409 duplicate email: "Email already registered. Would you like to log in instead?"
// 7. Link to login page: "Already have an account? Log in"
// 
// Use form library: react-hook-form + zod for validation
```

**Update Login Page**: `/Users/avirammizrahi/Desktop/amis/frontend/src/pages/LoginPage.tsx` (assuming it exists)

**Copilot Prompt**:
```typescript
// Update LoginPage to support ?next= redirect parameter
// After successful login:
// const searchParams = new URLSearchParams(location.search);
// const next = searchParams.get('next');
// if (next) {
//   navigate(next);
// } else {
//   // Role-based default redirect
//   navigate(user.role === 'admin' ? '/admin' : '/candidate/jobs');
// }
```

### 7.6 Apply Flow Integration

**Target File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/pages/JobDetailsPage.tsx` (if exists, or create new)

**Copilot Prompt**:
```typescript
// Update or create JobDetailsPage for candidates
// 
// Auth-aware Apply button:
// const handleApply = async () => {
//   // Check if user is logged in
//   const token = localStorage.getItem('token');
//   if (!token) {
//     // Redirect to login with return URL
//     const returnUrl = `/jobs/${jobId}`;
//     navigate(`/login?next=${encodeURIComponent(returnUrl)}`);
//     return;
//   }
//   
//   // Check if resume uploaded
//   const profile = await api.get('/candidates/me');
//   if (!profile.resume_url) {
//     // Show modal: "Please upload your resume first"
//     setShowResumeUploadModal(true);
//     return;
//   }
//   
//   // Submit application
//   try {
//     await api.post(`/jobs/${jobId}/apply`, {
//       cover_letter: coverLetterText
//     });
//     toast.success('Application submitted successfully!');
//     navigate('/applications/me');
//   } catch (error) {
//     if (error.status === 409) {
//       toast.info('You have already applied to this job.');
//     } else {
//       toast.error('Failed to submit application. Please try again.');
//     }
//   }
// };
```

---

## Phase 8: File Upload (CV/Resume)

### 8.1 Local Storage Implementation (MVP)

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/storage_service.py`

**Copilot Prompt**:
```python
# Create file storage service with local and S3 support
# 
# import os
# import boto3
# from pathlib import Path
# from typing import BinaryIO
# 
# class StorageService:
#     def __init__(self):
#         self.upload_dir = Path(os.getenv("UPLOAD_DIR", "/tmp/uploads"))
#         self.use_s3 = bool(os.getenv("AWS_S3_BUCKET"))
#         if self.use_s3:
#             self.s3_client = boto3.client('s3',
#                 region_name=os.getenv("AWS_REGION"),
#                 aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#                 aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
#             )
#             self.bucket = os.getenv("AWS_S3_BUCKET")
#     
#     async def save_resume(
#         self,
#         file: BinaryIO,
#         tenant_id: str,
#         candidate_id: str,
#         filename: str
#     ) -> str:
#         """Save resume and return accessible URL."""
#         # Generate unique filename: {tenant_id}/resumes/{candidate_id}_{timestamp}_{filename}
#         # If S3: upload to bucket and return s3:// URL or presigned URL
#         # If local: save to UPLOAD_DIR and return /uploads/{path}
#         pass
#     
#     async def delete_resume(self, file_url: str):
#         """Delete resume from storage."""
#         pass
#     
#     def get_public_url(self, file_url: str) -> str:
#         """Convert storage URL to publicly accessible URL."""
#         # For S3: generate presigned URL (expires in 1 hour)
#         # For local: return /uploads/{path} (served by FastAPI StaticFiles)
#         pass
# 
# storage = StorageService()
```

### 8.2 Static Files Route

**Target File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

**Copilot Prompt**:
```python
# Add static file serving for local uploads (if not using S3)
# 
# from fastapi.staticfiles import StaticFiles
# 
# if not os.getenv("AWS_S3_BUCKET"):
#     upload_dir = os.getenv("UPLOAD_DIR", "/tmp/uploads")
#     os.makedirs(upload_dir, exist_ok=True)
#     app.mount("/uploads", StaticFiles(directory=upload_dir), name="uploads")
```

### 8.3 Frontend Upload Component

**New File**: `/Users/avirammizrahi/Desktop/amis/frontend/src/components/ResumeUpload.tsx`

**Copilot Prompt**:
```typescript
// Create ResumeUpload component
// 
// Features:
// 1. File input (accept=".pdf,.doc,.docx")
// 2. Drag & drop zone
// 3. File validation:
//    - Max size: 10MB (from env or API response)
//    - Allowed types: pdf, doc, docx
// 4. Upload progress bar
// 5. Preview: show filename and size after selection
// 6. Submit: POST /candidates/me/upload-cv (multipart/form-data)
// 7. Success: show "Resume uploaded successfully" + download link
// 8. Error handling: show validation errors or upload failures
// 9. Delete option: DELETE /candidates/me/cv (with confirmation)
// 
// Use react-dropzone library for drag & drop
// Use axios for upload with onUploadProgress callback
```

---

## Phase 9: Testing & Validation

### 9.1 Backend Unit Tests

**Test Files to Create**:

1. `/Users/avirammizrahi/Desktop/amis/tests/test_matching_service.py`
2. `/Users/avirammizrahi/Desktop/amis/tests/test_portal_api.py`
3. `/Users/avirammizrahi/Desktop/amis/tests/test_applications.py`
4. `/Users/avirammizrahi/Desktop/amis/tests/test_candidate_profile.py`
5. `/Users/avirammizrahi/Desktop/amis/tests/test_storage_service.py`

**Copilot Prompt** (for each test file):
```python
# Create comprehensive pytest tests covering:
# 1. Happy path scenarios
# 2. Edge cases (empty data, invalid input)
# 3. Error conditions (404, 403, 409, 500)
# 4. Multi-tenant isolation (ensure tenant A can't access tenant B data)
# 5. Authentication & authorization (correct roles required)
# 
# Use fixtures for:
# - Test database setup/teardown
# - Mock user authentication
# - Sample data (jobs, candidates, applications)
# 
# Use pytest-asyncio for async tests
# Use httpx.AsyncClient for API testing
# Mock external services (S3, SMTP)
```

**Run Tests**:
```bash
cd /Users/avirammizrahi/Desktop/amis
pytest tests/test_*.py -v --cov=talentdb/scripts --cov-report=html
```

### 9.2 Frontend Tests

**Target Files**: `/Users/avirammizrahi/Desktop/amis/frontend/src/**/__tests__/`

**Copilot Prompt**:
```typescript
// Create Vitest tests for each component:
// 1. PortalPage.test.tsx
//    - Renders recommendations correctly
//    - Handles loading state
//    - Shows error message on API failure
// 2. Chatbot.test.tsx
//    - Sends and receives messages
//    - Handles job selection by number
//    - Shows auth prompt when not logged in
// 3. JobDetailCard.test.tsx
//    - Fetches and displays job details
//    - Apply button triggers correct callback
// 4. ResumeUpload.test.tsx
//    - Validates file type and size
//    - Uploads file successfully
//    - Shows error for invalid files
// 
// Use @testing-library/react for component testing
// Mock API calls with msw (Mock Service Worker)
// Test accessibility with @testing-library/jest-dom
```

**Run Tests**:
```bash
cd /Users/avirammizrahi/Desktop/amis/frontend
npm run test
```

### 9.3 Integration Tests

**New File**: `/Users/avirammizrahi/Desktop/amis/tests/test_portal_e2e.py`

**Copilot Prompt**:
```python
# Create end-to-end test for portal workflow:
# 
# Test scenario:
# 1. Setup: Create tenant, recruiter, 5 jobs (3 backend, 2 frontend)
# 2. Simulate rejection: candidate gets rejected from job #1
# 3. GET /portal/recommendations/{job1_id}
#    - Verify returns 2-3 similar backend jobs
#    - Check match scores are reasonable (> 0.3)
# 4. Candidate registers: POST /auth/register-candidate
#    - Verify JWT token returned
#    - Verify candidate profile created
# 5. Upload resume: POST /candidates/me/upload-cv
#    - Verify file saved and URL returned
# 6. Apply to recommended job: POST /jobs/{job2_id}/apply
#    - Verify application created
#    - Check application status is "new"
# 7. Verify tenant isolation:
#    - Create second tenant with different jobs
#    - Ensure candidate from tenant A can't see/apply to tenant B jobs
# 8. Cleanup: delete test data
# 
# Use pytest fixtures for setup/teardown
# Use TestClient from fastapi.testclient for API calls
```

### 9.4 Security Audit

**Run Script**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/security_audit.py`

**Copilot Prompt** (update existing script):
```python
# Add security checks for portal feature:
# 1. Verify tenant isolation in all portal APIs
# 2. Check authentication on protected endpoints
# 3. Validate file upload restrictions (type, size)
# 4. Test for SQL/NoSQL injection in search queries
# 5. Verify JWT expiration and refresh logic
# 6. Check CORS configuration (only allow FRONTEND_URL)
# 7. Validate no sensitive data in public APIs (salary, internal notes)
# 8. Test rate limiting on registration/login endpoints
# 
# Output: security_audit_report.json with findings
```

---

## Phase 10: Deployment & Monitoring

### 10.1 Environment Configuration

**Production Env File**: Create `/Users/avirammizrahi/Desktop/amis/talentdb/.env.production`

```bash
# Database
MONGO_URI=mongodb+srv://<user>:<password>@cluster.mongodb.net/
DB_NAME=talent_match_prod

# Auth
JWT_SECRET=<generate-strong-secret>
JWT_EXPIRY_HOURS=24

# File Storage (use S3 in production)
AWS_S3_BUCKET=ptx-prod-resumes
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>

# Email
SMTP_HOST=smtp.sendgrid.net
SMTP_PORT=587
SMTP_USER=apikey
SMTP_PASSWORD=<sendgrid-api-key>
SENDGRID_FROM_EMAIL=noreply@ptx.ai
SENDGRID_FROM_NAME=PTX Talent Team

# Frontend
FRONTEND_URL=https://app.ptx.ai
CORS_ORIGINS=["https://app.ptx.ai", "https://portal.ptx.ai"]

# Monitoring
SENTRY_DSN=<your-sentry-dsn>
LOG_LEVEL=INFO
```

### 10.2 Docker Configuration

**Update**: `/Users/avirammizrahi/Desktop/amis/talentdb/Dockerfile`

**Copilot Prompt**:
```dockerfile
# Ensure Dockerfile includes:
# 1. Install system dependencies: libmagic (for file type detection)
# 2. Copy requirements.txt and install Python packages
# 3. Create /app/uploads directory with correct permissions
# 4. Set environment variables (from .env.production)
# 5. Expose port 8000
# 6. Health check: CMD curl --fail http://localhost:8000/health || exit 1
# 7. Run with uvicorn: CMD ["uvicorn", "talentdb.scripts.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 10.3 Frontend Build & Deploy

**Update**: `/Users/avirammizrahi/Desktop/amis/frontend/vite.config.ts`

**Copilot Prompt**:
```typescript
// Update Vite config for production build:
// 1. Set base URL (if deploying to subdirectory)
// 2. Configure API proxy for /api/* -> backend URL
// 3. Enable build optimizations:
//    - minify: true
//    - sourcemap: false (or 'hidden' for debugging)
//    - chunk size warnings: increase to 1000kb
// 4. Environment variables: use import.meta.env.VITE_API_URL
```

**Build Script**: `/Users/avirammizrahi/Desktop/amis/frontend/package.json`

```json
{
  "scripts": {
    "build": "tsc && vite build",
    "build:prod": "tsc && vite build --mode production",
    "preview": "vite preview"
  }
}
```

**Deploy**:
```bash
cd /Users/avirammizrahi/Desktop/amis/frontend
npm run build:prod
# Output: dist/ folder
# Deploy dist/ to CDN or static host (Vercel, Netlify, S3+CloudFront)
```

### 10.4 Database Migrations

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/migrate_production.py`

**Copilot Prompt**:
```python
# Create production migration script:
# 1. Connect to production MongoDB (use MONGO_URI from env)
# 2. Backup existing data: mongodump (optional but recommended)
# 3. Run migrations in order:
#    a. ensure_user_indexes() from db.py
#    b. Create applications collection indexes
#    c. Add tenant_code field to tenants if missing
#    d. Verify no data corruption (count documents before/after)
# 4. Log all changes to migration.log
# 5. Dry-run mode: --dry-run flag to preview changes
# 
# Usage:
# python migrate_production.py --env production [--dry-run]
```

### 10.5 Monitoring & Logging

**Update**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

**Copilot Prompt**:
```python
# Add monitoring and logging:
# 1. Integrate Sentry for error tracking:
#    import sentry_sdk
#    sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"), environment="production")
# 
# 2. Add request logging middleware:
#    @app.middleware("http")
#    async def log_requests(request: Request, call_next):
#        start = time.time()
#        response = await call_next(request)
#        duration = time.time() - start
#        logger.info(f"{request.method} {request.url.path} - {response.status_code} - {duration:.2f}s")
#        return response
# 
# 3. Add structured logging for key events:
#    - Candidate registration
#    - Job application submission
#    - Portal recommendations fetched
#    - File uploads
#    - Errors and exceptions
# 
# 4. Add metrics endpoint: GET /metrics (for Prometheus/Grafana)
#    - Total applications
#    - Applications per tenant
#    - Recommendation API call count
#    - Average recommendation match score
```

**New File**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/monitoring.py`

**Copilot Prompt**:
```python
# Create monitoring utilities:
# 
# class Metrics:
#     def __init__(self):
#         self.counters = defaultdict(int)
#         self.gauges = {}
#     
#     def increment(self, metric: str, value: int = 1, labels: dict = None):
#         """Increment counter metric."""
#         pass
#     
#     def set_gauge(self, metric: str, value: float, labels: dict = None):
#         """Set gauge metric."""
#         pass
#     
#     def histogram(self, metric: str, value: float, labels: dict = None):
#         """Record histogram value."""
#         pass
#     
#     def get_all(self) -> dict:
#         """Return all metrics for /metrics endpoint."""
#         pass
# 
# metrics = Metrics()
# 
# # Usage in routers:
# metrics.increment("applications.created", labels={"tenant": tenant_id})
# metrics.histogram("recommendations.match_score", score)
```

### 10.6 Health Checks

**Update**: `/Users/avirammizrahi/Desktop/amis/talentdb/scripts/api.py`

**Copilot Prompt**:
```python
# Enhance health check endpoints:
# 
# GET /health (liveness probe)
# - Return 200 OK if process is running
# - No external dependencies checked
# 
# GET /ready (readiness probe)
# - Check MongoDB connection: db.command("ping")
# - Check required indexes exist
# - Check S3 connectivity (if enabled)
# - Return 200 if all checks pass, 503 otherwise
# - Response body: {
#     "status": "healthy" | "unhealthy",
#     "checks": {
#       "mongodb": "ok" | "error: ...",
#       "indexes": "ok",
#       "s3": "ok" | "disabled"
#     },
#     "timestamp": "ISO-8601"
#   }
# 
# GET /db/status (detailed diagnostics for admin)
# - Require admin auth
# - Return MongoDB stats: collections, document counts, index sizes
# - Response: {
#     "database": DB_NAME,
#     "collections": {
#       "users": {"count": 123, "indexes": 3},
#       "jobs": {"count": 456, "indexes": 5},
#       ...
#     }
#   }
```

---

## Implementation Checklist

### Phase 1: Multi-Tenant Data & Security ✅
- [ ] Update users unique index to (tenant_id, email)
- [ ] Create migration script for existing data
- [ ] Define applications collection schema
- [ ] Create database indexes for applications
- [ ] Unit tests for data models

### Phase 2: Job Matching Engine ✅
- [ ] Implement similarity scoring algorithm
- [ ] Create matching_service.py
- [ ] Unit tests for matching logic
- [ ] Validate tenant isolation

### Phase 3: Public Portal APIs ✅
- [ ] Create routers_portal.py
- [ ] Implement GET /portal/recommendations/{job_id}
- [ ] Implement GET /portal/job/{job_id}
- [ ] Register portal router in api.py
- [ ] Integration tests for portal APIs

### Phase 4: Candidate Authentication & Profile ✅
- [ ] Add candidate registration endpoint
- [ ] Create routers_candidates.py
- [ ] Implement profile CRUD endpoints
- [ ] CV upload endpoint
- [ ] Auth tests

### Phase 5: Job Application System ✅
- [ ] Create routers_applications.py
- [ ] Implement apply endpoint
- [ ] Email notification service
- [ ] Application status tracking
- [ ] Integration tests

### Phase 6: Admin Configuration UI ✅
- [ ] Tenant settings backend endpoint
- [ ] SettingsModal React component
- [ ] Copy link template functionality
- [ ] Feature toggle UI

### Phase 7: Portal Frontend & Chatbot ✅
- [ ] Add /portal/:tenantCode route
- [ ] Create PortalPage component
- [ ] Build Chatbot component
- [ ] JobDetailCard component
- [ ] Registration page
- [ ] Update login flow with ?next= support
- [ ] Component tests

### Phase 8: File Upload ✅
- [ ] StorageService implementation
- [ ] Local file storage
- [ ] S3 integration (optional)
- [ ] Static file serving
- [ ] ResumeUpload React component
- [ ] Upload validation

### Phase 9: Testing & Validation ✅
- [ ] Backend unit tests (90%+ coverage)
- [ ] Frontend component tests
- [ ] End-to-end integration test
- [ ] Security audit
- [ ] Performance testing (load test)

### Phase 10: Deployment & Monitoring ✅
- [ ] Production environment config
- [ ] Dockerfile updates
- [ ] Frontend build configuration
- [ ] Database migration script
- [ ] Monitoring integration (Sentry)
- [ ] Enhanced health checks
- [ ] Metrics endpoint
- [ ] Documentation update

---

## Testing Commands Reference

### Backend Tests
```bash
# Run all tests
cd /Users/avirammizrahi/Desktop/amis
pytest -v

# Run specific test file
pytest tests/test_portal_api.py -v

# Run with coverage
pytest --cov=talentdb/scripts --cov-report=html

# Run only matching tests
pytest tests/test_matching_service.py::test_exact_skill_match -v
```

### Frontend Tests
```bash
cd /Users/avirammizrahi/Desktop/amis/frontend

# Run all tests
npm run test

# Run with coverage
npm run test -- --coverage

# Run specific test file
npm run test -- Chatbot.test.tsx

# Run in watch mode
npm run test -- --watch
```

### Manual Testing Checklist
1. **Portal Recommendations**:
   - Visit: `http://localhost:5173/portal/default?job=<JOB_ID>`
   - Verify recommendations load
   - Check match scores displayed correctly

2. **Chatbot Interaction**:
   - Type "1" -> should show details of first job
   - Type "remote" -> should filter remote jobs
   - Type "apply" without login -> should prompt to register

3. **Registration & Login**:
   - Register new candidate
   - Verify JWT token saved
   - Upload resume (PDF)
   - Apply to a job

4. **Tenant Isolation**:
   - Create two tenants
   - Verify tenant A candidate can't see tenant B jobs

5. **Admin Settings**:
   - Log in as admin
   - Open settings modal
   - Copy portal link template
   - Verify {JOB_ID} placeholder present

---

## Deployment Steps

### Local Development
```bash
# Terminal 1: Backend
cd /Users/avirammizrahi/Desktop/amis/talentdb
source .venv/bin/activate
uvicorn scripts.api:app --reload --port 8000

# Terminal 2: Frontend
cd /Users/avirammizrahi/Desktop/amis/frontend
npm run dev
```

### Production Build
```bash
# Backend (Docker)
cd /Users/avirammizrahi/Desktop/amis/talentdb
docker build -t ptx-backend:latest .
docker run -p 8000:8000 --env-file .env.production ptx-backend:latest

# Frontend
cd /Users/avirammizrahi/Desktop/amis/frontend
npm run build:prod
# Deploy dist/ folder to hosting service
```

### Database Migration
```bash
cd /Users/avirammizrahi/Desktop/amis/talentdb
python scripts/migrate_production.py --env production --dry-run  # Preview
python scripts/migrate_production.py --env production  # Execute
```

---

## Success Criteria

### Functional Requirements ✅
- [ ] Candidate receives personalized job recommendations after rejection
- [ ] Chatbot responds intelligently to common queries
- [ ] Candidate can register, upload CV, and apply to jobs
- [ ] Admin can copy portal link template for ATS integration
- [ ] Multi-tenant data isolation enforced

### Non-Functional Requirements ✅
- [ ] API response time < 500ms (P95)
- [ ] Portal page loads in < 2 seconds
- [ ] Match algorithm accuracy > 80% (user feedback)
- [ ] Mobile-responsive design (375px - 1920px)
- [ ] WCAG 2.1 Level AA accessibility compliance
- [ ] Zero security vulnerabilities (automated scan)

### Business Metrics 📊
- [ ] Track: Rejection-to-application conversion rate
- [ ] Track: Chatbot engagement (messages per session)
- [ ] Track: Portal visit-to-registration conversion
- [ ] Goal: 15%+ of rejected candidates apply to recommended jobs

---

## Troubleshooting

### Common Issues

**Issue**: `pymongo.errors.ServerSelectionTimeoutError`
- **Solution**: Check MongoDB is running, verify MONGO_URI in .env

**Issue**: Frontend can't connect to backend (CORS error)
- **Solution**: Add frontend URL to CORS_ORIGINS in backend .env

**Issue**: File upload fails with 413 error
- **Solution**: Increase MAX_UPLOAD_SIZE_MB in .env, check nginx/proxy limits

**Issue**: Chatbot doesn't show recommendations
- **Solution**: Verify job_id in URL, check browser console for API errors

**Issue**: JWT token expired after login
- **Solution**: Check JWT_EXPIRY_HOURS in .env, implement token refresh

---

## Next Steps After MVP

### Phase 11: Advanced Features (Future)
1. **LLM-Powered Chatbot**: Replace rule-based logic with GPT-4 for natural conversation
2. **Video Introduction**: Allow candidates to upload video pitch
3. **Interview Scheduling**: Integrate calendar for interview booking
4. **SMS Notifications**: Send portal link via SMS (Twilio)
5. **Analytics Dashboard**: Recruiter view of portal metrics
6. **A/B Testing**: Test different recommendation algorithms
7. **Multi-Language Support**: i18n for portal and chatbot
8. **Skill Assessment**: In-portal coding challenges or quizzes

---

## References

- **Project Spec**: `/Users/avirammizrahi/Desktop/amis/PTX.txt`
- **API Docs**: Auto-generated at `http://localhost:8000/docs` (FastAPI Swagger)
- **MongoDB MCP Docs**: https://github.com/mongodb/mongodb-mcp
- **React Best Practices**: https://react.dev/learn
- **FastAPI Docs**: https://fastapi.tiangolo.com/

---

## Contact & Support

For questions or issues during implementation:
1. Check this plan first
2. Review API documentation at `/docs` endpoint
3. Run tests to isolate the problem
4. Check logs: `tail -f server.log` (backend) or browser console (frontend)

---

**Document Version**: 1.0  
**Last Updated**: October 22, 2025  
**Status**: Ready for Implementation 🚀
