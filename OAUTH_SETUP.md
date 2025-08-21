# Google OAuth2 Setup Guide for Greyamp Demand Planning App

## Overview
This guide explains how to configure Google OAuth2 authentication for the Greyamp Demand Planning application, which restricts access to greyamp.com domain users only.

## Prerequisites
- Google Cloud Console account
- Access to Replit Secrets management
- Admin privileges for greyamp.com Google Workspace (for domain restriction)

## Step 1: Google Cloud Console Setup

### 1.1 Create a New Project (if needed)
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on project selector at the top
3. Click "NEW PROJECT"
4. Name: "Greyamp Demand Planning"
5. Click "CREATE"

### 1.2 Enable Google+ API and Google OAuth2 API
1. In the Google Cloud Console, navigate to "APIs & Services" > "Library"
2. Search for and enable:
   - Google+ API
   - Google OAuth2 API
   - Google Identity API

### 1.3 Configure OAuth Consent Screen
1. Go to "APIs & Services" > "OAuth consent screen"
2. Select "Internal" (for greyamp.com users only)
3. Fill out the application information:
   - **Application name**: Greyamp Demand Planning
   - **User support email**: Your greyamp.com email
   - **Application logo**: Optional
   - **Authorized domains**: Add `greyamp.com`
   - **Developer contact information**: Your greyamp.com email
4. Add scopes:
   - `../auth/userinfo.email`
   - `../auth/userinfo.profile`
   - `openid`
5. Save and continue

### 1.4 Create OAuth2 Credentials
1. Go to "APIs & Services" > "Credentials"
2. Click "CREATE CREDENTIALS" > "OAuth client ID"
3. Application type: "Web application"
4. Name: "Greyamp Demand Planning Web Client"
5. **Authorized redirect URIs**: Add your Replit app URL:
   - Format: `https://[repl-name].[username].replit.app`
   - Example: `https://greyamp-demand-planning.myusername.replit.app`
6. Click "CREATE"
7. **IMPORTANT**: Copy the Client ID and Client Secret

## Step 2: Replit Configuration

### 2.1 Add Secrets in Replit
1. In your Replit project, click on "Secrets" (lock icon) in the left sidebar
2. Add the following secrets:

```
GOOGLE_CLIENT_ID = [Your Google OAuth2 Client ID]
GOOGLE_CLIENT_SECRET = [Your Google OAuth2 Client Secret]
```

### 2.2 Update Redirect URI if Needed
If your Replit URL changes, update the authorized redirect URI in Google Cloud Console:
1. Go back to Google Cloud Console > Credentials
2. Edit your OAuth2 client
3. Update the authorized redirect URI
4. Save changes

## Step 3: Domain Restriction Configuration

### 3.1 Workspace Admin Settings (Optional but Recommended)
If you're a Google Workspace admin for greyamp.com:
1. Go to [Google Admin Console](https://admin.google.com)
2. Navigate to Security > API Controls
3. Add the Google Cloud project to trusted applications
4. Configure OAuth app access controls

### 3.2 Application-Level Domain Restriction
The application already includes domain restriction code that:
- Uses the `hd` parameter in OAuth requests to hint domain restriction
- Validates email domain after authentication
- Blocks access for non-greyamp.com users

## Step 4: Testing the Setup

### 4.1 Test Authentication Flow
1. Start your Replit application
2. The app should now show the Google OAuth2 login option
3. Click "Sign in with Google"
4. You should be redirected to Google's authentication page
5. Only greyamp.com accounts should be allowed

### 4.2 Fallback Authentication
If OAuth2 is not configured, the app automatically falls back to simple authentication with these test accounts:
- `admin@greyamp.com` / `admin123`
- `demo@greyamp.com` / `demo123`
- `test@greyamp.com` / `test123`

## Security Features

### 4.3 Domain Restriction
- **OAuth Parameter**: `hd=greyamp.com` restricts Google account picker
- **Server-side Validation**: Email domain verification after authentication
- **Error Handling**: Clear error messages for unauthorized domains

### 4.4 State Protection
- CSRF protection using secure random state tokens
- State validation on OAuth callback
- Session security with proper token storage

## Troubleshooting

### Common Issues:

**1. "redirect_uri_mismatch" error**
- Solution: Ensure the redirect URI in Google Cloud Console exactly matches your Replit app URL

**2. "Access denied" for greyamp.com users**
- Solution: Check OAuth consent screen is set to "Internal" and greyamp.com is in authorized domains

**3. "OAuth2 credentials not configured" warning**
- Solution: Verify GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET are set in Replit Secrets

**4. Authentication works but user gets "Access denied"**
- Solution: The user's email might not end with @greyamp.com - check domain validation logic

### Debug Mode:
The application includes comprehensive error logging to help diagnose authentication issues.

## Production Considerations

### Security Recommendations:
1. **Regular Secret Rotation**: Update OAuth2 credentials periodically
2. **Monitoring**: Monitor authentication logs for suspicious activity
3. **Backup Access**: Maintain fallback authentication for emergencies
4. **Domain Verification**: Regularly verify domain restrictions are working

### Scaling:
- The OAuth2 setup supports unlimited greyamp.com users
- No additional configuration needed for new team members
- Google handles rate limiting and security

## Support

For issues with this setup:
1. Check Replit console logs for error details
2. Verify Google Cloud Console configuration
3. Test with fallback authentication first
4. Contact system administrator for Workspace-level issues

---
**Note**: This setup ensures only greyamp.com email addresses can access the demand planning application while providing a seamless OAuth2 experience for authorized users.