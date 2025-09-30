#!/bin/bash
# fix_installation.sh - Fix pandas/numpy installation issues on macOS

set -e

echo "🔧 Fixing installation issues on macOS..."

# Ensure we're in virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Activating virtual environment..."
    source venv/bin/activate
fi

# Step 1: Update pip and essential tools
echo "📦 Updating pip and build tools..."
pip install --upgrade pip setuptools wheel

# Step 2: Install Xcode command line tools if needed
echo "🔨 Checking Xcode Command Line Tools..."
if ! xcode-select -p &> /dev/null; then
    echo "Installing Xcode Command Line Tools..."
    xcode-select --install
    echo "Please complete the Xcode tools installation, then re-run this script."
    exit 1
else
    echo "✅ Xcode Command Line Tools found"
fi

# Step 3: Clean any failed installations
echo "🧹 Cleaning cache..."
pip cache purge

# Step 4: Try installing with pre-built wheels (no compilation)
echo "📦 Installing pandas with pre-built wheels..."

# For Apple Silicon Macs (M1/M2/M3)
if [[ $(uname -m) == 'arm64' ]]; then
    echo "Detected Apple Silicon Mac..."
    
    # Install dependencies in specific order with no-deps first
    pip install --no-cache-dir --only-binary :all: numpy==1.26.2
    pip install --no-cache-dir --only-binary :all: pandas==2.1.4
    pip install --no-cache-dir --only-binary :all: plotly==5.18.0
    pip install --no-cache-dir --only-binary :all: streamlit==1.38.0
    pip install --no-cache-dir requests==2.31.0
    
# For Intel Macs
else
    echo "Detected Intel Mac..."
    
    # Install with pre-built wheels
    pip install --no-cache-dir --only-binary :all: numpy==1.26.2
    pip install --no-cache-dir --only-binary :all: pandas==2.1.4
    pip install --no-cache-dir --only-binary :all: plotly==5.18.0
    pip install --no-cache-dir --only-binary :all: streamlit==1.38.0
    pip install --no-cache-dir requests==2.31.0
fi

# Step 5: Install optional components (allow failure)
echo "📦 Installing optional components..."
pip install streamlit-aggrid==0.3.5 || echo "⚠️  streamlit-aggrid failed (optional)"
pip install streamlit-js-eval==0.1.7 || echo "⚠️  streamlit-js-eval failed (optional)"

# Step 6: Verify installation
echo ""
echo "🔍 Verifying installation..."

python -c "import pandas; print('✅ pandas installed successfully')" || exit 1
python -c "import numpy; print('✅ numpy installed successfully')" || exit 1
python -c "import streamlit; print('✅ streamlit installed successfully')" || exit 1
python -c "import plotly; print('✅ plotly installed successfully')" || exit 1
python -c "import requests; print('✅ requests installed successfully')" || exit 1

# Test sam_utils import
python -c "from sam_utils import get_system; print('✅ sam_utils module working!')" || exit 1

echo ""
echo "🎉 Installation fixed successfully!"
echo ""
echo "You can now continue with Step 14 of the implementation:"
echo "  python download_and_update.py --lookback-days 30"