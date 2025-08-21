# Setup Guide After Downloading Your Workforce Intelligence Platform

## Installed Packages (Dependencies You Need)

When you download your codebase from Replit, you'll need to reinstall these packages locally. Here's exactly what your project currently uses:

### Core Framework
- `streamlit>=1.46.1` - The main web application framework
- `streamlit-searchbox>=0.1.22` - Enhanced search functionality

### Data Processing & Analytics  
- `pandas>=2.3.0` - Data manipulation and analysis
- `numpy>=2.3.1` - Numerical computing
- `scikit-learn>=1.7.0` - Machine learning library
- `statsmodels>=0.14.4` - Statistical modeling

### Visualization
- `plotly>=6.2.0` - Interactive charts and graphs

### Database
- `psycopg2-binary>=2.9.10` - PostgreSQL adapter
- `sqlalchemy>=2.0.41` - Database ORM

### Google Services Integration
- `google-api-python-client>=2.177.0` - Google APIs client
- `google-auth>=2.40.3` - Google authentication
- `google-auth-httplib2>=0.2.0` - HTTP library for Google Auth
- `google-auth-oauthlib>=1.2.2` - OAuth2 flow for Google
- `gspread>=6.2.1` - Google Sheets integration

### Authentication & Security
- `authlib>=1.6.0` - OAuth and authentication library
- `python-jose>=3.5.0` - JSON Web Token implementation

### File Processing
- `openpyxl>=3.1.5` - Excel file handling

### Utilities
- `requests>=2.32.4` - HTTP requests
- `schedule>=1.2.2` - Task scheduling
- `pytz>=2025.2` - Timezone handling

### Testing
- `pytest>=8.4.1` - Testing framework
- `pytest-html>=3.1.0` - HTML test reports
- `pytest-cov>=4.0.0` - Coverage reporting

## Quick Setup Commands

After downloading your project:

```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# 3. Create requirements.txt with these packages
# Copy the package list above into a requirements.txt file

# 4. Install all packages
pip install streamlit>=1.46.1 streamlit-searchbox>=0.1.22 pandas>=2.3.0 numpy>=2.3.1 scikit-learn>=1.7.0 statsmodels>=0.14.4 plotly>=6.2.0 psycopg2-binary>=2.9.10 sqlalchemy>=2.0.41 google-api-python-client>=2.177.0 google-auth>=2.40.3 google-auth-httplib2>=0.2.0 google-auth-oauthlib>=1.2.2 gspread>=6.2.1 authlib>=1.6.0 python-jose>=3.5.0 openpyxl>=3.1.5 requests>=2.32.4 schedule>=1.2.2 pytz>=2025.2 pytest>=8.4.1

# 5. Set up PostgreSQL database
# Install PostgreSQL locally and create a database

# 6. Set environment variables
# You'll need to recreate your DATABASE_URL and OAuth credentials
```

## What You'll Need to Configure Separately:

1. **Database**: Set up PostgreSQL locally and create your database
2. **Environment Variables**: Recreate your DATABASE_URL and any API keys
3. **Google OAuth**: Reconfigure your Google OAuth credentials for local development
4. **Streamlit Config**: The `.streamlit/config.toml` is included in the download

## Files Included in Your Download:
- All Python files (200+ files including your main app.py)
- Complete utils/ directory with all managers
- Documentation and markdown files
- Configuration files
- Test files and scripts  
- All uploaded assets and images

## Files NOT Included:
- Database data (export separately if needed)
- Environment variables/secrets
- Python packages (need to reinstall as shown above)