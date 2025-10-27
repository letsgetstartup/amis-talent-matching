# CV Upload Feature - Comprehensive Execution Plan

## Executive Summary

This document outlines a complete execution plan for adding CV/resume upload functionality to the Talent Matching Portal. Based on extensive project research, this plan follows industry best practices for UX/UI design and authentication flows.

---

## 1. Research Findings

### Current System Architecture

**Frontend:**
- React-based application with TypeScript
- Existing components: `PortalPage.tsx`, `PortalChatbot.tsx`, `RegistrationPage.tsx`
- Portal accessible via `/portal/:slug` route
- Chat interface integrated within portal pages
- Multi-tenant architecture with slug-based tenant identification

**Backend:**
- FastAPI with MongoDB
- Authentication: JWT-based with role system (admin, vc_admin, recruiter, candidate)
- Existing upload infrastructure in `routers_candidates.py` (supports PDF, DOCX, TXT, CSV)
- File upload size limit: 12MB (configurable via `MAX_UPLOAD_MB`)
- Tenant-scoped data isolation

**Authentication System:**
- Existing endpoints: `/auth/signup`, `/auth/login`, `/auth/me`
- Role-based access control with `require_role()` dependencies
- JWT token storage in localStorage
- Support for candidate registration (mentioned in plans but needs verification)

### Existing CV Upload Capabilities

The system **already has** CV upload functionality:
- `POST /candidates/upload` - Multi-file upload endpoint
- Supports: PDF, DOCX, TXT, CSV formats
- Automatic text extraction and candidate profile creation
- Skills extraction via AI/NLP
- Generates `share_id` for candidate profiles
- Job matching after CV processing

**Key Files:**
- `/talentdb/scripts/routers_candidates.py` - Upload endpoint
- `/frontend/src/components/CandidateUpload.tsx` - React upload component
- `/frontend/public/recommend.html` - Public CV upload page

---

## 2. UX/UI Best Practices Research

### Industry Standards for CV Upload

#### Option A: In-Chat Upload (Conversational Flow) ⭐ RECOMMENDED
**Pros:**
- Natural conversation flow
- Contextual - user sees why CV is needed
- Lower barrier to entry
- Progressive disclosure
- Modern, engaging experience

**Cons:**
- May feel less formal
- Requires more development for chat integration
- File preview in chat can be challenging

**When to Use:**
- Casual, friendly employer brands
- Tech-savvy audience
- Job boards focusing on user experience
- Platforms emphasizing AI/chatbot interaction

**Examples:** 
- LinkedIn Easy Apply uses inline flows
- Indeed's guided application process
- Modern recruitment chatbots (Paradox, Olivia)

#### Option B: Dedicated Profile Page
**Pros:**
- Clear, structured interface
- Easy to show upload progress
- Better for multiple documents
- Familiar pattern for users
- Easier validation and error handling

**Cons:**
- Requires navigation away from main flow
- May feel like extra step
- Less conversational

**When to Use:**
- Enterprise/corporate brands
- Complex application processes
- When collecting multiple documents
- Professional/formal contexts

#### Option C: Persistent Button in Chat UI
**Pros:**
- Always accessible
- Doesn't interrupt conversation
- Clear affordance
- Good for "upload anytime" model

**Cons:**
- Can clutter UI
- Less contextual prompting
- May be overlooked

---

## 3. ✅ Approved Solution: Chat-First with Post-Upload Registration

### Strategy: Frictionless Upload → Value Demonstration → Conversion

**Primary Flow (Chat-Initiated - APPROVED):**
1. User browses jobs via portal and interacts with chatbot
2. Chatbot contextually suggests: "Upload your CV for personalized matches!"
3. User clicks upload button → Modal opens **WITHOUT requiring login**
4. User uploads CV (anonymous upload)
5. **Immediate processing and analysis**
6. Chatbot shows **initial results** (top 3-5 matches with key insights)
7. **Then prompts for registration:** "Create account to see all matches and apply"
8. User registers → CV automatically linked to new account
9. Full match list revealed + ability to apply

**Secondary Flow (Profile Management - Post-Login):**
1. After registration, user can access profile page
2. View/update/replace CV
3. Edit profile information
4. View application history

**Authentication Strategy (APPROVED):**
- ✅ **No login required for upload**
- ✅ **Show value first** (preview matches)
- ✅ **Then convert** (register to see more)
- ✅ **Seamless linking** (uploaded CV auto-attached to new account)

---

## 4. Detailed Implementation Plan

### Phase 1: Backend Enhancement ✅ (Mostly Complete)

#### 1.1 Enhance Candidate Registration Endpoint
**File:** `/talentdb/scripts/routers_auth.py`

**Tasks:**
- [ ] Verify `POST /auth/register-candidate` exists
- [ ] **Add support for linking temporary CV uploads**
- [ ] Update endpoint with new schema:

```python
class CandidateRegister(BaseModel):
    name: str
    email: EmailStr
    password: str  # Min 8 chars
    tenant_id: str
    phone: Optional[str] = None
    temp_candidate_id: Optional[str] = None  # ← NEW: Link anonymous upload

@router.post("/auth/register-candidate")
def register_candidate(payload: CandidateRegister):
    """
    Register new candidate account.
    If temp_candidate_id provided, link existing CV upload to account.
    """
    from gridfs import GridFS
    
    # Check (tenant_id, email) uniqueness
    existing = db.users.find_one({
        "tenant_id": ObjectId(payload.tenant_id),
        "email": payload.email.lower()
    })
    if existing:
        raise HTTPException(409, "Email already registered")
    
    # Hash password (bcrypt)
    password_hash = hash_password(payload.password)
    
    # Create user document (role="candidate")
    user_doc = {
        "tenant_id": ObjectId(payload.tenant_id),
        "email": payload.email.lower(),
        "password_hash": password_hash,
        "name": payload.name,
        "role": "candidate",
        "created_at": datetime.utcnow(),
        "is_active": True
    }
    user_result = db.users.insert_one(user_doc)
    user_id = str(user_result.inserted_id)
    
    # Handle CV linking
    if payload.temp_candidate_id:
        # Find temporary candidate profile
        temp_candidate = db.candidates.find_one({
            "temp_candidate_id": payload.temp_candidate_id,
            "tenant_id": ObjectId(payload.tenant_id),
            "is_claimed": False
        })
        
        if temp_candidate:
            # Claim the profile - link to user account
            db.candidates.update_one(
                {"_id": temp_candidate["_id"]},
                {
                    "$set": {
                        "user_id": ObjectId(user_id),
                        "is_claimed": True,
                        "email": payload.email.lower(),
                        "full_name": payload.name,
                        "phone": payload.phone,
                        "updated_at": datetime.utcnow()
                    },
                    "$unset": {"expires_at": ""}  # Remove expiration
                }
            )
            candidate_id = str(temp_candidate["_id"])
        else:
            # Temp candidate not found, create new profile
            candidate_id = create_empty_profile(user_id, payload)
    else:
        # No CV upload, create empty profile
        candidate_id = create_empty_profile(user_id, payload)
    
    # Generate JWT token
    token = create_jwt_token({
        "sub": user_id,
        "tenant_id": payload.tenant_id,
        "role": "candidate"
    })
    
    return {
        "token": token,
        "user": {
            "id": user_id,
            "email": payload.email,
            "name": payload.name,
            "role": "candidate",
            "tenant_id": payload.tenant_id,
            "candidate_id": candidate_id
        }
    }

def create_empty_profile(user_id: str, payload: CandidateRegister) -> str:
    """Create empty candidate profile for users without CV upload."""
    profile_doc = {
        "tenant_id": ObjectId(payload.tenant_id),
        "user_id": ObjectId(user_id),
        "full_name": payload.name,
        "email": payload.email.lower(),
        "phone": payload.phone,
        "is_claimed": True,
        "resume_file_id": None,
        "skills": [],
        "created_at": datetime.utcnow()
    }
    result = db.candidates.insert_one(profile_doc)
    return str(result.inserted_id)
```

#### 1.2 Add Candidate Profile Endpoints
**File:** `/talentdb/scripts/routers_candidates.py`

**Tasks:**
- [ ] Add `POST /candidates/upload-anonymous` - **NEW: Anonymous CV upload**
- [ ] Add `GET /candidates/me` - Returns current candidate's profile
- [ ] Add `PUT /candidates/me` - Update profile (name, phone, skills, bio, location)
- [ ] Add `POST /candidates/me/upload-cv` - Upload/replace CV (authenticated)
- [ ] Add `GET /candidates/me/cv` - Download own CV
- [ ] Add `DELETE /candidates/me/cv` - Remove CV
- [ ] Add GridFS integration for file storage

**Anonymous Upload Endpoint Spec:**
```python
from gridfs import GridFS

fs = GridFS(db)

@router.post("/candidates/upload-anonymous")
async def upload_cv_anonymous(
    file: UploadFile = File(...),
    tenant_id: str = Form(...)  # From portal context
):
    """
    Anonymous CV upload - no login required.
    Returns temp_candidate_id and initial match preview.
    """
    # Validate file type (.pdf, .doc, .docx)
    if not file.content_type in ['application/pdf', 'application/msword', ...]:
        raise HTTPException(400, "Invalid file type")
    
    # Validate file size (< 10MB)
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "File too large")
    
    # Store in GridFS
    file_id = fs.put(
        content,
        filename=file.filename,
        content_type=file.content_type,
        tenant_id=tenant_id,
        uploaded_by="anonymous"
    )
    
    # Extract text from CV
    text = extract_text_from_file(content, file.content_type)
    
    # Generate temp candidate ID
    temp_id = f"temp_{uuid.uuid4().hex[:12]}"
    
    # Create temporary candidate document
    candidate_doc = {
        "temp_candidate_id": temp_id,
        "tenant_id": ObjectId(tenant_id),
        "user_id": None,
        "is_claimed": False,
        "resume_file_id": file_id,
        "resume_filename": file.filename,
        "resume_content_type": file.content_type,
        "resume_size_bytes": len(content),
        "resume_uploaded_at": datetime.utcnow(),
        "text_blob": text,
        "created_at": datetime.utcnow(),
        "expires_at": datetime.utcnow() + timedelta(days=7)
    }
    
    # Process with AI to extract skills, experience, etc.
    extracted_info = await extract_candidate_info(text)
    candidate_doc.update(extracted_info)
    
    # Insert into DB
    result = db.candidates.insert_one(candidate_doc)
    candidate_id = str(result.inserted_id)
    
    # Generate matches
    matches = await match_candidate_to_jobs(candidate_id, tenant_id)
    
    return {
        "temp_candidate_id": temp_id,
        "candidate_id": candidate_id,
        "share_id": temp_id,  # For compatibility
        "extracted_info": {
            "title": extracted_info.get("title"),
            "experience_years": extracted_info.get("experience_years"),
            "top_skills": extracted_info.get("skills", [])[:5],
            "location": extracted_info.get("city_canonical")
        },
        "top_matches": matches[:5],  # Preview only
        "total_count": len(matches)
    }

@router.post("/candidates/me/upload-cv")
async def upload_cv_authenticated(
    file: UploadFile = File(...),
    current_user: dict = Depends(require_role("candidate"))
):
    """Authenticated CV upload - replaces existing CV if any."""
    tenant_id = current_user["tenant_id"]
    user_id = current_user["sub"]
    
    # Similar to anonymous but links to user immediately
    # Delete old CV from GridFS if exists
    # ... (similar logic but sets user_id and is_claimed=True)
    
    return {
        "resume_file_id": str(file_id),
        "filename": file.filename,
        "uploaded_at": datetime.utcnow()
    }
```

#### 1.3 Enhanced Public Upload with Auth Prompt
**File:** `/talentdb/scripts/routers_candidates.py`

**Tasks:**
- [ ] Modify `POST /candidates/upload` to support anonymous uploads
- [ ] Return `temp_candidate_id` for anonymous users
- [ ] Add `POST /candidates/claim` - Convert anonymous upload to authenticated profile

---

### Phase 2: Frontend - Chat Integration 🎯

#### 2.1 Upload Modal Component
**New File:** `/frontend/src/components/CVUploadModal.tsx`

**Features:**
```typescript
interface CVUploadModalProps {
  isOpen: boolean;
  onClose: () => void;
  onUploadSuccess: (shareId: string, jobs: Job[]) => void;
  tenantSlug: string;
  requireAuth?: boolean;
}

// Features:
// - Drag & drop zone
// - File input (.pdf, .doc, .docx)
// - File preview (name, size, type)
// - Upload progress bar
// - Validation messages
// - Success state with job recommendations
// - "Create account to save" prompt for anonymous users
```

**UI/UX Specifications:**
- Modal overlay with backdrop blur
- Clean, minimal design matching portal theme
- Clear instructions: "Upload your CV to get matched with jobs"
- Accepted formats shown: PDF, DOC, DOCX (max 10MB)
- Drag-and-drop zone with visual feedback
- Progress indicator during upload
- Error handling with helpful messages
- Success state shows: "✓ CV uploaded! Found X matching jobs"

#### 2.2 Integrate with Chat
**File:** `/frontend/src/components/chat/PortalChatbot.tsx`

**Tasks:**
- [ ] Add `uploadModalOpen` state
- [ ] Add upload trigger in bot messages (e.g., "Upload your CV for personalized matches")
- [ ] Render `<CVUploadModal>` in chatbot component
- [ ] Handle upload success: show job matches in chat
- [ ] Add quick action button: "📎 Upload CV" in chat composer

****✅ Approved Bot Conversation Flow:**
```
User: "Show me developer jobs"
Bot: "I found 15 developer jobs in Tel Aviv. 
     💡 Want personalized recommendations based on your experience?"
     [📄 Upload Your CV] button

User: *clicks button*
Bot: *opens upload modal - NO LOGIN REQUIRED*

User: *selects and uploads CV file (e.g., John_Doe_CV.pdf)*
Modal: "Uploading..." → "Processing..." 
       [Progress bar animation]

Bot: "✓ CV uploaded successfully! Let me analyze it..."
     [Typing indicator for 2-3 seconds]

Bot: "Excellent! Here's what I found:
     
     📊 Your Profile Summary:
     • 5 years of experience in software development
     • Top skills: React, TypeScript, Node.js, MongoDB, AWS
     • Current role: Senior Frontend Developer
     • Location: Tel Aviv
     
     🎯 Top Matching Jobs:
     
     1. **Senior Frontend Developer** at TechCorp
        ⭐ 95% match
        📍 Tel Aviv (Hybrid)
        💰 Salary range available
        ✓ React, TypeScript required
        
     2. **React Team Lead** at StartupCo  
        ⭐ 92% match
        📍 Remote
        ✓ 5+ years React experience required
        
     3. **Full Stack Engineer** at DevShop
        ⭐ 88% match
        📍 Tel Aviv
        ✓ React + Node.js required
     
     📈 I found 12 total matching jobs for you!
     
     💡 Create a free account to:
     • View all 12 matching jobs
     • Apply to positions
     • Save your profile
     • Get job alerts
     
     [Create Free Account] [Already have account? Login]"

User: *Clicks "Create Free Account"*
Bot: *Shows inline registration form:*
     "Quick Sign Up - 30 seconds"
     [Name: _______]
     [Email: _______]
     [Password: _______]
     [Phone (optional): _______]
     [Create Account Button]
     
User: *Fills form and submits*
Bot: "🎉 Welcome aboard, John!
     
     Your CV is now saved to your profile.
     
     Here are all 12 matching jobs:"
     
     [Shows full list with enhanced details + Apply buttons]
     
     1. Senior Frontend Developer at TechCorp (95% match)
        [View Details] [Apply Now]
     2. React Team Lead at StartupCo (92% match)
        [View Details] [Apply Now]
     ...
     
     "You can also:
     • View and edit your profile: [My Profile]
     • Update your CV: [Update CV]
     • Set job alerts: [Alert Settings]"
```

**Alternative Flow - User Chooses Login:**
```
User: *Clicks "Already have account? Login"*
Bot: *Shows login form:*
     "Welcome back!"
     [Email: _______]
     [Password: _______]
     [Forgot password?]
     [Login Button]

User: *Logs in successfully*
Bot: "Welcome back, Sarah! 
     
     I've linked your newly uploaded CV to your account.
     You previously had X applications in progress.
     
     Based on your updated CV, here are 15 matching jobs..."
```**
```
User: "Show me developer jobs"
Bot: "I found 15 developer jobs in Tel Aviv. 
     💡 Upload your CV to get personalized matches!"
     [Upload CV Button]

User: *clicks button*
Bot: *opens modal*

User: *uploads CV*
Bot: "Great! I analyzed your CV. Based on your 5 years of React experience, 
     here are your top 3 matches:
     1. Senior Frontend Developer at Company A (95% match)
     2. React Team Lead at Company B (92% match)
     ..."
```

#### 2.3 Portal Header Upload Button
**File:** `/frontend/src/pages/PortalPage.tsx`

**Tasks:**
- [ ] Add header section above job listings
- [ ] Add "Upload CV" button (visible when not uploaded)
- [ ] Add CV status indicator (when uploaded): "✓ CV on file | Last updated: 2 days ago"
- [ ] Add "Update CV" option in dropdown menu
- [ ] Show upload modal on button click

**Layout:**
```tsx
<header className="portal-header">
  <div className="portal-branding">
    <h1>{portalData.name} - Job Portal</h1>
  </div>
  <div className="portal-actions">
    {!user ? (
      <>
        <button onClick={() => setShowUploadModal(true)}>
          📄 Upload CV for Matches
        </button>
        <button onClick={() => navigate('/login')}>
          Login
        </button>
      </>
    ) : (
      <>
        {!hasCVUploaded ? (
          <button onClick={() => setShowUploadModal(true)} className="primary">
            📄 Upload Your CV
          </button>
        ) : (
          <div className="cv-status">
            <span>✓ CV on file</span>
            <button onClick={() => setShowUploadModal(true)}>Update</button>
          </div>
        )}
        <UserMenu />
      </>
    )}
  </div>
</header>
```

---

### Phase 3: Authentication Integration 🔐

#### 3.1 ✅ Anonymous Upload Flow (APPROVED)
**Flow:**
1. User uploads CV **without login required**
2. CV stored in GridFS with temporary status
3. Backend processes CV (text extraction, skill detection)
4. Generate matches (all jobs, not limited)
5. **Chatbot shows initial insights:**
   - Extracted key info (years of experience, top skills, title)
   - Top 3-5 job matches with scores
   - Teaser: "12 total matches found!"
6. **Chatbot prompts:** "Create free account to see all matches and apply"
7. Show inline registration form in chat
8. User signs up → CV automatically linked via `temp_candidate_id`
9. Chatbot reveals full match list with apply buttons

**Implementation:**
```typescript
// In CVUploadModal.tsx
const handleAnonymousUpload = async (file: File) => {
  setUploading(true);
  
  // Upload without auth
  const response = await uploadCandidateAnonymous(file, tenantSlug);
  // Returns: { temp_candidate_id, share_id, extracted_info, top_matches, total_count }
  
  setTempCandidateId(response.temp_candidate_id);
  
  // Close modal, return to chat
  onClose();
  
  // Chatbot shows results
  onUploadSuccess({
    extractedInfo: response.extracted_info,
    topMatches: response.top_matches.slice(0, 5), // Preview only
    totalCount: response.total_count,
    tempCandidateId: response.temp_candidate_id
  });
};

// In PortalChatbot.tsx
const handleCVUploadSuccess = (data) => {
  // Add bot message with extracted info
  addMessage({
    role: 'assistant',
    text: `Great! I analyzed your CV:\n\n` +
          `✓ ${data.extractedInfo.experience_years} years of experience\n` +
          `✓ Top skills: ${data.extractedInfo.top_skills.join(', ')}\n` +
          `✓ Current/desired role: ${data.extractedInfo.title}\n\n` +
          `Here are your top matches:\n\n` +
          data.topMatches.map((job, i) => 
            `${i+1}. ${job.title} at ${job.company} (${job.score}% match)`
          ).join('\n') +
          `\n\n🎯 I found ${data.totalCount} total matching jobs!\n\n` +
          `Create a free account to see all matches and start applying.`
  });
  
  // Show registration prompt
  setShowRegistrationPrompt(true);
  setTempCandidateId(data.tempCandidateId);
};

const handleRegistration = async (name, email, password) => {
  // Register user
  const user = await registerCandidate({
    name, email, password, 
    tenant_id: tenantId,
    temp_candidate_id: tempCandidateId  // Link uploaded CV
  });
  
  // Store auth token
  localStorage.setItem('token', user.token);
  
  // Fetch full matches
  const allMatches = await getMatchesForCandidate(user.candidate_id);
  
  // Show success message
  addMessage({
    role: 'assistant',
    text: `Welcome, ${name}! 🎉\n\n` +
          `Your CV is now saved to your profile.\n\n` +
          `Here are all ${allMatches.length} matching jobs:`
  });
  
  // Display full job list
  setFullMatches(allMatches);
  setShowRegistrationPrompt(false);
};
```

#### 3.2 Authenticated Upload Flow
**Flow:**
1. User must be logged in
2. Click "Upload CV" → Opens modal
3. Upload replaces existing CV (if any)
4. Immediate matching feedback
5. CV saved to profile permanently

#### 3.3 Registration Page Enhancement
**File:** `/frontend/src/pages/RegistrationPage.tsx`

**Tasks:**
- [ ] Add optional CV upload step in registration wizard
- [ ] Allow "Skip for now" option
- [ ] On completion, redirect to portal with uploaded CV processed

---

### Phase 4: Profile Management Page 📋

#### 4.1 Create Candidate Profile Page
**New File:** `/frontend/src/pages/CandidateProfilePage.tsx`

**Sections:**
1. **Header:** Name, Email, Location
2. **CV Section:**
   - Upload/Update button
   - Current CV info: filename, upload date, file size
   - Download button
   - Delete button (with confirmation)
3. **Extracted Info (read-only):**
   - Skills detected from CV
   - Years of experience
   - Job title
4. **Editable Info:**
   - Phone number
   - Location/City
   - Bio/Summary
   - Additional skills (manual entry)
5. **Application History:**
   - List of jobs applied to
   - Application status

**Routes:**
- Add to `/frontend/src/App.tsx`: `<Route path="/profile" element={<CandidateProfilePage />} />`
- Protected route (require auth)

---

### Phase 5: UI/UX Polish ✨

#### 5.1 Visual Design

**✅ Match Existing Portal Design**

The CV upload components will follow the existing portal design system:

**Color Scheme (from existing styles):**
- Use current portal primary colors (already established)
- Chat message background: `rgba(8, 145, 178, 0.15)` (cyan tint)
- Buttons: Follow existing button styles from portal
- Success states: Match existing success indicators
- Error states: Match existing error styling

**Typography:**
- Follow existing font stack from portal
- Maintain consistent heading hierarchy
- Use same font sizes and weights as chat interface

**Spacing & Layout:**
- Use existing spacing tokens from portal CSS
- Modal dimensions: Match existing modal patterns
- Button sizes: Consistent with portal buttons (44px height maintained)
- Upload zone: Match card/panel patterns from portal

**Components:**
- Upload modal inherits from portal modal design
- File preview cards match job card styling
- Progress indicators match existing loading states
- Registration form matches existing form styling

#### 5.2 Animations

**Upload Modal:**
- Fade in backdrop (0.2s ease)
- Scale in modal (0.3s spring)
- Drag hover state: scale(1.02) + border pulse

**Upload Progress:**
- Linear progress bar with indeterminate state
- Success checkmark animation (bounce)

**File Preview:**
- Slide in from bottom (0.2s ease-out)

#### 5.3 Accessibility

- [ ] ARIA labels for all interactive elements
- [ ] Keyboard navigation (Tab, Enter, Esc)
- [ ] Focus management (trap focus in modal)
- [ ] Screen reader announcements for upload progress
- [ ] Error messages linked to form fields (aria-describedby)
- [ ] High contrast support

#### 5.4 Responsive Design

**Mobile (<768px):**
- Full-screen modal
- Larger touch targets (min 48px)
- Stack buttons vertically
- Hide drag-drop text, show file input only

**Tablet (768px-1024px):**
- Modal width: 90vw, max 600px
- Side-by-side buttons where appropriate

**Desktop (>1024px):**
- Modal width: 560px
- Hover states for all interactive elements
- Drag-and-drop emphasized

---

### Phase 6: Error Handling & Edge Cases 🛡️

#### 6.1 Validation

**Client-Side:**
- File type: Must be .pdf, .doc, .docx
- File size: Max 10MB (configurable)
- Required fields in signup: name, email, password (min 8 chars)

**Server-Side:**
- MIME type validation
- Virus scanning (future: ClamAV integration)
- Rate limiting: Max 5 uploads per hour per IP
- Duplicate detection (same email + tenant)

#### 6.2 Error Scenarios

| Error | User Message | Action |
|-------|-------------|--------|
| File too large | "File exceeds 10MB limit. Try a smaller file." | Show file size, suggest compression |
| Invalid format | "Please upload a PDF, DOC, or DOCX file." | Highlight accepted formats |
| Network failure | "Upload failed. Check your connection and try again." | Retry button |
| Server error | "Something went wrong. Please try again later." | Contact support link |
| Email exists | "This email is already registered. Try logging in." | Link to login page |
| Extraction failed | "We couldn't read your CV. Try a different format." | Offer manual profile entry |

#### 6.3 Edge Cases

- **No text extracted:** Prompt user to manually enter key info
- **Multiple CVs:** Only keep latest, show "Replace existing CV?" confirmation
- **Expired temp upload:** After 24 hours, require re-upload
- **Concurrent uploads:** Lock UI, show "Processing previous upload..."
- **Session expired:** Prompt re-login, preserve uploaded file

---

## 5. Implementation Sequence & Timeline

### Week 1: Backend Foundation
**Days 1-2:**
- [ ] Verify/implement candidate registration endpoint
- [ ] Add candidate profile CRUD endpoints
- [ ] Test authentication flows

**Days 3-5:**
- [ ] Implement CV upload endpoint with file storage
- [ ] Add anonymous upload + claim flow
- [ ] Write API tests
- [ ] Test file validation and error handling

### Week 2: Frontend Components
**Days 1-3:**
- [ ] Build CVUploadModal component
- [ ] Implement drag-and-drop functionality
- [ ] Add progress indicators and animations
- [ ] Test across browsers

**Days 4-5:**
- [ ] Integrate modal with PortalChatbot
- [ ] Add upload button to portal header
- [ ] Implement authentication prompts
- [ ] Test complete flow

### Week 3: Profile & Polish
**Days 1-2:**
- [ ] Build CandidateProfilePage
- [ ] Implement profile editing
- [ ] Add CV management features

**Days 3-4:**
- [ ] UI/UX polish (animations, responsive design)
- [ ] Accessibility audit and fixes
- [ ] Error handling refinement

**Day 5:**
- [ ] End-to-end testing
- [ ] Bug fixes
- [ ] Documentation

### Week 4: Testing & Deployment
**Days 1-2:**
- [ ] User acceptance testing
- [ ] Performance testing (file upload speeds)
- [ ] Security audit

**Days 3-4:**
- [ ] Deploy to staging
- [ ] Monitor for issues
- [ ] Final adjustments

**Day 5:**
- [ ] Production deployment
- [ ] Monitor analytics
- [ ] Gather user feedback

---

## 6. Technical Specifications

### File Storage Strategy

**✅ APPROVED: MongoDB GridFS Storage**

MongoDB GridFS will be used for all CV storage - no external file system or S3 needed.

**Why GridFS:**
- Native MongoDB integration (no additional infrastructure)
- Handles files >16MB (MongoDB document limit)
- Automatic chunking and metadata
- Built-in replication and backup with MongoDB
- Simplified architecture (single database system)

**Implementation:**
```python
from gridfs import GridFS
from pymongo import MongoClient

# Initialize GridFS
db = MongoClient()[database_name]
fs = GridFS(db)

# Store CV
file_id = fs.put(
    file_data,
    filename=filename,
    content_type=content_type,
    tenant_id=tenant_id,
    candidate_id=candidate_id,
    uploaded_at=datetime.utcnow()
)

# Retrieve CV
cv_file = fs.get(file_id)
content = cv_file.read()

# Store file_id in candidate document
db.candidates.update_one(
    {"_id": candidate_id},
    {"$set": {"resume_file_id": file_id}}
)
```

**Benefits:**
- ✅ No S3 costs
- ✅ Simplified deployment
- ✅ Atomic operations with candidate data
- ✅ Consistent backup strategy
- ✅ Works offline/air-gapped deployments

### API Endpoints Summary

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/candidates/upload-anonymous` | None | **NEW: Anonymous CV upload with instant matching** |
| POST | `/auth/register-candidate` | None | Create candidate account (accepts `temp_candidate_id` to link CV) |
| GET | `/candidates/me` | Candidate | Get own profile |
| PUT | `/candidates/me` | Candidate | Update profile |
| POST | `/candidates/me/upload-cv` | Candidate | Upload/replace CV (authenticated) |
| GET | `/candidates/me/cv` | Candidate | Download own CV from GridFS |
| DELETE | `/candidates/me/cv` | Candidate | Remove CV (deletes from GridFS) |
| GET | `/candidates/temp/{temp_id}/matches` | None | Get matches for temporary upload |

### Database Schema Changes

**candidates collection:**
```javascript
{
  _id: ObjectId,
  tenant_id: ObjectId,
  user_id: ObjectId,  // Link to users collection (null for temp uploads)
  temp_candidate_id: string,  // ← NEW: Temporary ID for anonymous uploads
  is_claimed: boolean,  // ← NEW: false until user registers
  full_name: string,
  email: string,
  phone: string,
  resume_file_id: ObjectId,  // ← NEW: Reference to GridFS file
  resume_filename: string,  // ← NEW FIELD
  resume_content_type: string,  // ← NEW: MIME type (application/pdf, etc.)
  resume_uploaded_at: ISODate,  // ← NEW FIELD
  resume_size_bytes: number,  // ← NEW: File size
  skills: [string],
  experience_years: number,
  city_canonical: string,
  bio: string,
  created_at: ISODate,
  updated_at: ISODate,
  expires_at: ISODate  // ← NEW: Auto-delete unclaimed uploads after 7 days
}
```

**GridFS files (fs.files collection):**
```javascript
{
  _id: ObjectId,  // This is the resume_file_id stored in candidates
  filename: string,
  content_type: string,
  length: number,  // File size in bytes
  chunkSize: number,  // Default 255KB
  uploadDate: ISODate,
  metadata: {
    tenant_id: ObjectId,
    candidate_id: ObjectId,
    original_filename: string,
    uploaded_by: string  // "anonymous" or user_id
  }
}
```

**GridFS chunks (fs.chunks collection):**
```javascript
{
  _id: ObjectId,
  files_id: ObjectId,  // Reference to fs.files
  n: number,  // Chunk sequence number
  data: BinData  // Binary chunk data (max 255KB)
}
```

---

## 7. Security Considerations

### Authentication & Authorization
- [ ] JWT tokens expire after 7 days
- [ ] Refresh token mechanism
- [ ] Role-based access control enforced
- [ ] Candidate can only access own data

### File Upload Security
- [ ] MIME type validation (not just extension)
- [ ] Content scanning for malicious code
- [ ] Separate storage from code execution paths
- [ ] Signed URLs for S3 access (time-limited)
- [ ] Rate limiting on upload endpoints

### Data Privacy
- [ ] CVs contain PII - ensure GDPR compliance
- [ ] Allow users to delete their data
- [ ] Audit log for CV access
- [ ] Encrypt at rest (S3 server-side encryption)
- [ ] Encrypt in transit (HTTPS only)

---

## 8. Analytics & Success Metrics

### Key Metrics to Track

**Engagement:**
- CV upload rate (% of portal visitors)
- Upload completion rate
- Time to complete upload
- Upload source (chat vs. header button)

**Quality:**
- Successful extractions vs. failures
- Average match score for uploaded CVs
- Re-upload frequency

**Conversion:**
- Anonymous → Registered user conversion
- CV upload → Job application conversion
- Jobs applied after CV upload

**Technical:**
- Average upload duration
- Error rate by type
- File format distribution

### Analytics Implementation

```typescript
// Track upload start
trackEvent('cv_upload_started', {
  source: 'chat' | 'header' | 'profile',
  isAuthenticated: boolean
});

// Track upload success
trackEvent('cv_upload_success', {
  fileSize: number,
  fileType: string,
  duration: number,
  matchesFound: number
});

// Track conversion
trackEvent('anonymous_user_registered', {
  hadUploadedCV: boolean,
  timeFromUploadToSignup: number
});
```

---

## 9. User Testing Plan

### Usability Testing Scenarios

**Scenario 1: Anonymous User**
1. User visits portal
2. Browses jobs
3. Chatbot suggests CV upload
4. User uploads CV
5. Sees preview matches
6. Prompted to register
7. Completes registration
8. Views all matches

**Scenario 2: Registered User**
1. User logs in
2. Clicks "Upload CV" in header
3. Uploads file
4. Sees immediate matches
5. Updates profile info
6. Applies to job

**Scenario 3: Error Recovery**
1. User selects wrong file type
2. Sees clear error message
3. Selects correct file
4. Upload fails due to network
5. Retries successfully

### Testing Checklist

- [ ] Test on Chrome, Firefox, Safari, Edge
- [ ] Test on iOS Safari, Android Chrome
- [ ] Test with 100KB, 5MB, 12MB files
- [ ] Test with various CV formats (PDF, DOC, DOCX)
- [ ] Test drag-and-drop vs. file input
- [ ] Test with slow network (3G simulation)
- [ ] Test error messages and recovery
- [ ] Test accessibility with screen reader
- [ ] Test keyboard-only navigation

---

## 10. Future Enhancements (Post-MVP)

### Phase 2 Features
- [ ] CV parsing improvements (AI/ML)
- [ ] Support for LinkedIn import
- [ ] Video introduction upload
- [ ] Portfolio link integration
- [ ] Multiple CV versions (different languages)

### Phase 3 Features
- [ ] CV builder (guided form)
- [ ] CV templates and formatting
- [ ] AI-powered CV improvement suggestions
- [ ] Cover letter generation
- [ ] Application tracking dashboard

### Integration Opportunities
- [ ] ATS integration (Greenhouse, Lever)
- [ ] Background check services
- [ ] Skills assessment platforms
- [ ] Reference checking tools

---

## 11. Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Poor CV extraction quality | High | Implement fallback manual entry; use multiple parsing libraries |
| Storage costs (S3) | Medium | Start with local storage; optimize file compression |
| Slow upload speeds | High | Implement chunked uploads; show clear progress |
| Privacy concerns | High | Clear privacy policy; give users control over data |
| Spam uploads | Medium | Rate limiting; CAPTCHA for anonymous uploads |
| File storage full | High | Monitor storage; implement cleanup of old CVs |

---

## 12. Documentation Requirements

### Developer Documentation
- [ ] API endpoint documentation (OpenAPI/Swagger)
- [ ] Component usage guide (Storybook)
- [ ] Database schema documentation
- [ ] Deployment guide
- [ ] Environment variables reference

### User Documentation
- [ ] CV upload guide (with screenshots)
- [ ] Supported file formats FAQ
- [ ] Privacy policy updates
- [ ] Troubleshooting common issues

---

## 13. Launch Checklist

### Pre-Launch
- [ ] All tests passing (unit, integration, E2E)
- [ ] Security audit complete
- [ ] Performance benchmarks met
- [ ] Accessibility audit passed
- [ ] Documentation complete
- [ ] Staging environment tested
- [ ] Rollback plan prepared

### Launch
- [ ] Feature flag enabled for 10% of users
- [ ] Monitor error rates and performance
- [ ] Collect user feedback
- [ ] Address critical issues
- [ ] Gradual rollout to 50%, then 100%

### Post-Launch
- [ ] Review analytics (week 1)
- [ ] User feedback survey
- [ ] Bug triage and fixes
- [ ] Performance optimization
- [ ] Plan next iteration

---

## 14. Recommendation Summary

### ✅ RECOMMENDED APPROACH: Hybrid with Chat-First Flow

**Why This Works Best:**

1. **User-Centric:** Upload happens in context when it's most valuable
2. **Conversion Optimized:** Progressive engagement (browse → upload → register)
3. **Flexible:** Multiple entry points for different user preferences
4. **Modern:** Aligns with current UX trends (conversational UI)
5. **Scalable:** Easy to add more features to profile page later

**Primary Flow:**
- Chat-initiated upload with modal
- Anonymous upload allowed with registration prompt
- Immediate job matching feedback

**Secondary Flow:**
- Header button for quick access
- Profile page for management
- Clear CV status indicators

**Authentication Strategy:**
- Soft gate: Allow exploration without account
- Hard gate: Require registration for applications
- Smooth conversion: One-click registration after anonymous upload

---

## 15. Decisions Made & Plan Updates

### ✅ Confirmed Decisions (October 27, 2025):

1. **UX Flow:** ✅ **Chat-Initiated** (Primary entry point)
2. **Authentication Strategy:** ✅ **Post-Upload Registration** (Upload first, register after seeing initial results)
3. **File Storage:** ✅ **MongoDB GridFS** (No S3 - pure MongoDB solution)
4. **Timeline:** ✅ **4-week implementation plan**
5. **MVP Scope:** ✅ **As specified in this document**
6. **Design:** ✅ **Match existing portal design language**

### 🔄 Updated Implementation Strategy

Based on your feedback, the flow is now:

**Anonymous Upload → Chat Response → Registration Prompt**

```
User: *Opens portal, starts chatting*
User: "Show me developer jobs"
Bot: "I found 15 developer jobs. Want personalized recommendations?"
     [📄 Upload Your CV] button

User: *Clicks button, uploads CV without logging in*
Bot: "Analyzing your CV..." 
     *Processing...*
     "Great! Based on your CV, you have:
     • 5 years of React experience
     • Strong background in TypeScript
     • Experience with Node.js
     
     Here are your top 3 matches:
     1. Senior Frontend Developer at TechCorp (95% match)
     2. React Team Lead at StartupCo (92% match)
     3. Full Stack Engineer at DevShop (88% match)
     
     🎯 Want to see all 12 matches and apply?
     [Create Account] [Login]"

User: *Clicks Create Account*
     *Quick registration form appears*
     
User: *Completes registration*
Bot: "Welcome aboard! Your CV is now saved to your profile.
     Here are all 12 matching jobs..."
     [Shows complete list with apply buttons]
```

---

**Document Version:** 2.0  
**Date:** October 27, 2025  
**Status:** ✅ APPROVED - READY FOR IMPLEMENTATION  
**Author:** GitHub Copilot

