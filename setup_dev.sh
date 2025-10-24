#!/bin/bash
# 🚀 Local Development Setup Script

set -e  # Exit on error

echo "🚀 Setting up Weather ELT Pipeline development environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
print_status "Found Python $PYTHON_VERSION"

if [[ "$(python3 -c "import sys; print(sys.version_info >= (3, 8))")" != "True" ]]; then
    print_error "Python 3.8+ is required. Found Python $PYTHON_VERSION"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    print_status "Creating virtual environment..."
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_status "Virtual environment already exists"
fi

# Activate virtual environment
print_status "Activating virtual environment..."
source venv/bin/activate || {
    # For Windows users
    print_status "Trying Windows activation..."
    source venv/Scripts/activate
}

# Upgrade pip
print_status "Upgrading pip..."
python -m pip install --upgrade pip

# Install dependencies
print_status "Installing dependencies..."
pip install -r requirements.txt

# Install pre-commit hooks (optional)
print_status "Setting up pre-commit hooks..."
pip install pre-commit
pre-commit install || print_warning "Pre-commit setup failed (optional)"

# Run initial data quality tests
print_status "Running initial data quality tests..."
cd data_quality_tests
python test_bronze_simple.py
if [ $? -eq 0 ]; then
    print_success "Bronze layer tests passed"
else
    print_error "Bronze layer tests failed"
    exit 1
fi

python test_silver_local.py
if [ $? -eq 0 ]; then
    print_success "Silver layer tests passed"
else
    print_error "Silver layer tests failed"
    exit 1
fi

cd ..

# Run code quality checks
print_status "Running code quality checks..."

# Black formatting check
print_status "Checking code formatting (Black)..."
black --check --diff . || {
    print_warning "Code formatting issues found. Run 'black .' to fix them."
}

# Import sorting check
print_status "Checking import sorting (isort)..."
isort --check-only --diff . || {
    print_warning "Import sorting issues found. Run 'isort .' to fix them."
}

# Linting
print_status "Running linting (flake8)..."
flake8 . || {
    print_warning "Linting issues found. Check the output above."
}

# Type checking
print_status "Running type checking (mypy)..."
mypy --ignore-missing-imports src/ || {
    print_warning "Type checking issues found (non-critical)."
}

# Great Expectations validation
print_status "Validating Great Expectations setup..."
cd great_expectations
python -c "import great_expectations as ge; context = ge.get_context(); print('✅ Great Expectations context loaded successfully')"
cd ..

print_success "🎉 Development environment setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Activate the virtual environment: source venv/bin/activate (or venv\\Scripts\\activate on Windows)"
echo "2. Run tests: cd data_quality_tests && python run_all_tests.py"
echo "3. Start developing: Follow the README.md for detailed instructions"
echo "4. Before committing: Run 'black .' and 'isort .' to format code"
echo ""
echo "🔗 Useful commands:"
echo "  - Format code: black ."
echo "  - Sort imports: isort ."
echo "  - Run linting: flake8 ."
echo "  - Run all tests: cd data_quality_tests && python run_all_tests.py"
echo "  - Check dependencies: pip list --outdated"
echo ""
print_success "Happy coding! 🚀"