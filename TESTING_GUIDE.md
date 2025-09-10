# Manual UI Testing Guide for New Features

## Prerequisites
- Server running at http://localhost:8000
- Browser with JavaScript enabled
- Clear browser cache/cookies for clean testing

## Test 1: Homepage Join Button
**Objective:** Verify Join button appears and opens registration modal

**Steps:**
1. Navigate to http://localhost:8000/agency-portal.html
2. Verify page loads with homepage layout (no sidebar, full-screen)
3. Look for "✨ Join" button in the hero section
4. Click the Join button
5. Verify registration modal opens with step indicators

**Expected Results:**
- ✅ Join button visible and clickable
- ✅ Modal opens with "Create your account" title
- ✅ Step indicators show "1 · Account", "2 · Collaborators", "3 · Done"

## Test 2: Step 1 - Account Creation
**Objective:** Test form validation and account creation

**Steps:**
1. In Step 1 of modal:
   - Fill Company: "Test Company"
   - Fill Full name: "Test User"
   - Fill Work email: "test@example.com"
   - Fill Password: "testpass123"
   - Check "I agree to the Terms..." checkbox
2. Click "Create account" button
3. Verify progress to Step 2

**Validation Tests:**
- Try submitting with empty fields → Should show error
- Try submitting without terms checkbox → Should show error
- Try submitting with invalid email → Should show error

**Expected Results:**
- ✅ Form validates all required fields
- ✅ Terms checkbox required
- ✅ Progresses to Step 2 on successful creation

## Test 3: Step 2 - Collaborator Invitations
**Objective:** Test email invitation functionality

**Steps:**
1. In Step 2:
   - Type "hr@company.com" in input field
   - Press Enter or comma
   - Add "talent@company.com" similarly
   - Verify email chips appear
2. Click "Finish" button
3. Verify progress to Step 3

**Edge Cases:**
- Try adding invalid email → Should not create chip
- Try adding duplicate email → Should not create duplicate chip
- Click "×" on chip → Should remove chip

**Expected Results:**
- ✅ Email chips created correctly
- ✅ Invalid emails rejected
- ✅ Chips removable
- ✅ Progresses to Step 3

## Test 4: Step 3 - Success and Redirect
**Objective:** Test completion and dashboard redirect

**Steps:**
1. In Step 3:
   - Verify success message
   - Click "Go to dashboard" button
2. Verify modal closes
3. Verify dashboard loads
4. Verify Portfolio Admin panel opens automatically

**Expected Results:**
- ✅ Modal closes on finish
- ✅ Redirects to dashboard
- ✅ Portfolio Admin panel visible

## Test 5: Admin Button in Dashboard
**Objective:** Test Admin button functionality

**Steps:**
1. In dashboard header, look for "Admin" button
2. Click the Admin button
3. Verify Portfolio Admin panel opens

**Expected Results:**
- ✅ Admin button visible in dashboard header
- ✅ Clicking opens Portfolio Admin panel
- ✅ Panel shows company management features

## Test 6: Terms and Privacy Links
**Objective:** Test legal page links

**Steps:**
1. In registration modal Step 1, click "Terms of Service" link
2. Verify terms page opens in new tab/window
3. Click "Privacy Policy" link
4. Verify privacy page opens

**Expected Results:**
- ✅ Terms link opens http://localhost:8000/terms.html
- ✅ Privacy link opens http://localhost:8000/privacy.html
- ✅ Both pages load correctly

## Test 7: Authentication State Management
**Objective:** Test login/logout and state persistence

**Steps:**
1. Complete registration flow
2. Verify user logged in (dashboard shows)
3. Refresh page
4. Verify still logged in
5. Click logout (if available)
6. Verify returns to homepage

**Expected Results:**
- ✅ Authentication state persists across page refresh
- ✅ Logout clears state and returns to homepage

## Test 8: Mobile Responsiveness
**Objective:** Test on mobile viewport

**Steps:**
1. Resize browser to mobile width (< 768px)
2. Repeat Tests 1-7
3. Verify modal fits screen
4. Verify form fields accessible

**Expected Results:**
- ✅ Modal responsive on mobile
- ✅ All interactions work on touch devices

## Test 9: Error Handling
**Objective:** Test error scenarios

**Steps:**
1. Try registering with existing email
2. Try inviting with malformed emails
3. Test network disconnection scenarios

**Expected Results:**
- ✅ Clear error messages for validation failures
- ✅ Graceful handling of network errors

## Test 10: Accessibility
**Objective:** Test keyboard navigation and screen reader compatibility

**Steps:**
1. Use Tab key to navigate through modal
2. Use Enter/Space to activate buttons
3. Verify ARIA labels present
4. Test with screen reader if available

**Expected Results:**
- ✅ Full keyboard navigation support
- ✅ Proper ARIA attributes
- ✅ Screen reader compatible

## Performance Tests
**Objective:** Test loading and interaction performance

**Steps:**
1. Time modal open/close
2. Time form submission
3. Monitor network requests
4. Test on slow connection

**Expected Results:**
- ✅ Modal opens within 100ms
- ✅ Form submission completes within 2s
- ✅ No unnecessary network requests

## Browser Compatibility
**Test on multiple browsers:**
- Chrome/Chromium
- Firefox
- Safari
- Edge

**Expected Results:**
- ✅ Consistent behavior across browsers
- ✅ No JavaScript errors in console

## Summary
After completing all tests, document:
- ✅ Tests passed
- ❌ Tests failed with details
- 🔄 Tests requiring fixes
- 📝 Additional observations

**Overall Status:** [PASS/FAIL/PARTIAL]</content>
<parameter name="filePath">/Users/avirammizrahi/Desktop/amis/TESTING_GUIDE.md
