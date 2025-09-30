#!/bin/bash
# setup.sh - Automated setup script for SAM.gov Africa Dashboard

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_message "$BLUE" "=========================================="
print_message "$BLUE" "SAM.gov Africa Dashboard - Setup Script"
print_message "$BLUE" "=========================================="

# Check Python version
print_message "$YELLOW" "\n📋 Checking prerequisites..."

if ! command -v python3 &> /dev/null; then
    print_message "$RED" "❌ Python 3 is not installed"
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    print_message "$RED" "❌ Python $REQUIRED_VERSION or higher required (found $PYTHON_VERSION)"
    exit 1
fi

print_message "$GREEN" "✅ Python $PYTHON_VERSION found"

# Check Git
if ! command -v git &> /dev/null; then
    print_message "$RED" "❌ Git is not installed"
    exit 1
fi

print_message "$GREEN" "✅ Git found"

# Create virtual environment
print_message "$YELLOW" "\n📦 Setting up Python environment..."

if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_message "$GREEN" "✅ Virtual environment created"
else
    print_message "$GREEN" "✅ Virtual environment already exists"
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip --quiet

# Install requirements
print_message "$YELLOW" "\n📚 Installing dependencies..."
pip install -r requirements.txt --quiet
print_message "$GREEN" "✅ Dependencies installed"

# Create necessary directories
print_message "$YELLOW" "\n📁 Creating directories..."
mkdir -p data
mkdir -p .streamlit
mkdir -p logs
print_message "$GREEN" "✅ Directories created"

# Run system verification
print_message "$YELLOW" "\n🔍 Running system verification..."
python test_system.py

if [ $? -eq 0 ]; then
    print_message "$GREEN" "✅ System verification passed"
else
    print_message "$RED" "❌ System verification failed"
    exit 1
fi

# Check for existing database
if [ -f "data/opportunities.db" ]; then
    print_message "$YELLOW" "\n📊 Existing database found"
    
    # Run migration
    print_message "$YELLOW" "🔄 Migrating database to optimized structure..."
    python migrate_database.py
    
    if [ $? -eq 0 ]; then
        print_message "$GREEN" "✅ Database migration complete"
    else
        print_message "$RED" "❌ Database migration failed"
        exit 1
    fi
else
    print_message "$YELLOW" "\n📊 No existing database found"
    read -p "Do you want to initialize with recent data? (y/n): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_message "$YELLOW" "📥 Downloading recent data (this may take a few minutes)..."
        python download_and_update.py --lookback-days 30
        
        if [ $? -eq 0 ]; then
            print_message "$GREEN" "✅ Database initialized with recent data"
        else
            print_message "$RED" "❌ Failed to initialize database"
            exit 1
        fi
    fi
fi

# Create Streamlit secrets file if it doesn't exist
if [ ! -f ".streamlit/secrets.toml" ]; then
    print_message "$YELLOW" "\n🔐 Creating Streamlit secrets file..."
    
    cat > .streamlit/secrets.toml << 'EOF'
# GitHub configuration for triggering updates
github_owner = "JWKSOA"
github_repo = "SAM.gov-Africa-Dashboard"
github_workflow = "update-sam-db.yml"
github_token = ""  # Add your GitHub personal access token here
EOF
    
    print_message "$GREEN" "✅ Secrets file created (add your GitHub token)"
else
    print_message "$GREEN" "✅ Secrets file already exists"
fi

# Create convenience scripts
print_message "$YELLOW" "\n📝 Creating convenience scripts..."

# Create run script
cat > run_dashboard.sh << 'EOF'
#!/bin/bash
source venv/bin/activate
streamlit run streamlit_dashboard.py
EOF
chmod +x run_dashboard.sh

# Create update script
cat > run_update.sh << 'EOF'
#!/bin/bash
source venv/bin/activate
python download_and_update.py
EOF
chmod +x run_update.sh

# Create health check script
cat > check_health.sh << 'EOF'
#!/bin/bash
source venv/bin/activate
python -c "
from sam_utils import get_system
system = get_system()
stats = system.db_manager.get_statistics()
print(f'✅ Database healthy')
print(f'   Records: {stats[\"total_records\"]:,}')
print(f'   Size: {stats[\"size_mb\"]:.1f} MB')
print(f'   Recent (30d): {stats[\"recent_records\"]:,}')
"
EOF
chmod +x check_health.sh

print_message "$GREEN" "✅ Convenience scripts created"

# Final summary
print_message "$BLUE" "\n=========================================="
print_message "$BLUE" "Setup Complete!"
print_message "$BLUE" "=========================================="

print_message "$GREEN" "\n✨ Everything is ready! Here's how to get started:\n"
echo "1. Run the dashboard:"
echo "   ./run_dashboard.sh"
echo ""
echo "2. Update data manually:"
echo "   ./run_update.sh"
echo ""
echo "3. Check system health:"
echo "   ./check_health.sh"
echo ""
echo "4. Configure GitHub token in .streamlit/secrets.toml"
echo "   for automatic updates"
echo ""
echo "5. Visit http://localhost:8501 when dashboard is running"

print_message "$YELLOW" "\n📚 Documentation: README.md"
print_message "$YELLOW" "🐛 Issues? Check logs/ directory or run test_system.py"