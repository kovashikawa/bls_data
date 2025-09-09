#!/bin/bash
# Convenience scripts for common uv operations in the BLS Data Repository

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check if uv is installed
check_uv() {
    if ! command -v uv &> /dev/null; then
        print_error "uv is not installed. Please install it first:"
        echo "  pip install uv"
        echo "  # or"
        echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
        exit 1
    fi
}

# Install dependencies
install() {
    print_header "Installing Dependencies"
    check_uv
    uv sync
    print_status "Dependencies installed successfully!"
}

# Install development dependencies
install_dev() {
    print_header "Installing Development Dependencies"
    check_uv
    uv sync --extra dev
    print_status "Development dependencies installed successfully!"
}

# Install all dependencies (including dev and test)
install_all() {
    print_header "Installing All Dependencies"
    check_uv
    uv sync --all-extras
    print_status "All dependencies installed successfully!"
}

# Run a command in the uv environment
run() {
    if [ $# -eq 0 ]; then
        print_error "Please provide a command to run"
        echo "Usage: $0 run <command>"
        exit 1
    fi
    
    print_header "Running Command: $*"
    check_uv
    uv run "$@"
}

# Run tests
test() {
    print_header "Running Tests"
    check_uv
    uv run pytest
}

# Run tests with coverage
test_coverage() {
    print_header "Running Tests with Coverage"
    check_uv
    uv run pytest --cov=bls_data --cov-report=html --cov-report=term
}

# Format code
format() {
    print_header "Formatting Code"
    check_uv
    uv run black .
    uv run isort .
    print_status "Code formatted successfully!"
}

# Lint code
lint() {
    print_header "Linting Code"
    check_uv
    uv run flake8 .
    uv run mypy .
    print_status "Linting completed!"
}

# Clean up
clean() {
    print_header "Cleaning Up"
    rm -rf .venv/
    rm -rf build/
    rm -rf dist/
    rm -rf *.egg-info/
    rm -rf .pytest_cache/
    rm -rf .coverage
    rm -rf htmlcov/
    rm -rf logs/*.log
    print_status "Cleanup completed!"
}

# Show project info
info() {
    print_header "Project Information"
    check_uv
    echo "Project: bls-data"
    echo "Version: 0.1.0"
    echo "Python: $(uv run python --version)"
    echo "Virtual Environment: $(uv run python -c "import sys; print(sys.prefix)")"
    echo ""
    print_status "Installed packages:"
    uv pip list
}

# Update dependencies
update() {
    print_header "Updating Dependencies"
    check_uv
    uv sync --upgrade
    print_status "Dependencies updated successfully!"
}

# Add a new dependency
add() {
    if [ $# -eq 0 ]; then
        print_error "Please provide a package name"
        echo "Usage: $0 add <package-name>"
        exit 1
    fi
    
    print_header "Adding Dependency: $1"
    check_uv
    uv add "$@"
    print_status "Dependency added successfully!"
}

# Add a development dependency
add_dev() {
    if [ $# -eq 0 ]; then
        print_error "Please provide a package name"
        echo "Usage: $0 add-dev <package-name>"
        exit 1
    fi
    
    print_header "Adding Development Dependency: $1"
    check_uv
    uv add --dev "$@"
    print_status "Development dependency added successfully!"
}

# Remove a dependency
remove() {
    if [ $# -eq 0 ]; then
        print_error "Please provide a package name"
        echo "Usage: $0 remove <package-name>"
        exit 1
    fi
    
    print_header "Removing Dependency: $1"
    check_uv
    uv remove "$@"
    print_status "Dependency removed successfully!"
}

# Show help
show_help() {
    echo "BLS Data Repository - UV Commands"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  install          Install production dependencies"
    echo "  install-dev      Install development dependencies"
    echo "  install-all      Install all dependencies (including dev and test)"
    echo "  run <command>    Run a command in the uv environment"
    echo "  test             Run tests"
    echo "  test-coverage    Run tests with coverage report"
    echo "  format           Format code with black and isort"
    echo "  lint             Lint code with flake8 and mypy"
    echo "  clean            Clean up build artifacts and cache"
    echo "  info             Show project information"
    echo "  update           Update all dependencies"
    echo "  add <package>    Add a new dependency"
    echo "  add-dev <package> Add a new development dependency"
    echo "  remove <package> Remove a dependency"
    echo "  help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 install"
    echo "  $0 run python scripts/test_cpi_extraction.py"
    echo "  $0 add requests"
    echo "  $0 test-coverage"
}

# Main script logic
case "${1:-help}" in
    install)
        install
        ;;
    install-dev)
        install_dev
        ;;
    install-all)
        install_all
        ;;
    run)
        shift
        run "$@"
        ;;
    test)
        test
        ;;
    test-coverage)
        test_coverage
        ;;
    format)
        format
        ;;
    lint)
        lint
        ;;
    clean)
        clean
        ;;
    info)
        info
        ;;
    update)
        update
        ;;
    add)
        shift
        add "$@"
        ;;
    add-dev)
        shift
        add_dev "$@"
        ;;
    remove)
        shift
        remove "$@"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
