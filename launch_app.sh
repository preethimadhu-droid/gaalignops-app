#!/bin/bash

# GA AlignOps Launch Script
echo "🚀 Launching GA AlignOps..."

# Navigate to the correct directory
cd "$(dirname "$0")"

# Check if app.py exists
if [ ! -f "app.py" ]; then
    echo "❌ Error: app.py not found in current directory"
    echo "Current directory: $(pwd)"
    exit 1
fi

echo "✅ Found app.py in: $(pwd)"
echo "🌐 Starting Streamlit app on http://localhost:8501"
echo "📱 Press Ctrl+C to stop the app"

# Launch the app
python3 -m streamlit run app.py --server.port 8501 --server.address 0.0.0.0
