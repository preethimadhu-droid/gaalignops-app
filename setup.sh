#!/bin/bash

# GA AlignOps Environment Setup Script
# This script helps set up the development and production environments

echo "🚀 GA AlignOps Environment Setup"
echo "================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.11+ first."
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not installed. Please install pip3 first."
    exit 1
fi

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "⚠️  PostgreSQL is not installed. You'll need to install it for database functionality."
    echo "   On macOS: brew install postgresql"
    echo "   On Ubuntu: sudo apt-get install postgresql postgresql-contrib"
    echo "   On Windows: Download from https://www.postgresql.org/download/windows/"
fi

echo ""
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

echo ""
echo "🔧 Setting up environment configuration..."

# Create config directory if it doesn't exist
mkdir -p config

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file from development template..."
    cp config/development.env .env
    echo "✅ Created .env file. Please edit it with your database credentials."
else
    echo "✅ .env file already exists."
fi

echo ""
echo "🗄️  Database Setup Instructions:"
echo "1. Create a PostgreSQL database for development"
echo "2. Update the DATABASE_URL in your .env file"
echo "3. The application will automatically create dev_ tables on startup"
echo ""
echo "🔐 Authentication Setup:"
echo "- Development: Uses fallback authentication (no OAuth setup needed)"
echo "- Production: Requires Google OAuth credentials in .env file"
echo ""
echo "📊 Google Sheets Integration:"
echo "- Development: Uses development sheets with basic validation"
echo "- Production: Uses production sheets with full validation"
echo ""
echo "🚀 To start the application:"
echo "streamlit run app.py"
echo ""
echo "📚 For detailed setup instructions, see:"
echo "SETUP_ENVIRONMENT_SEGREGATION.md"
echo ""
echo "✅ Setup completed! Please configure your .env file and database."
