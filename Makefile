# BLS Data Repository - Makefile for UV Commands
# This Makefile provides convenient shortcuts for common uv operations

.PHONY: help install install-dev install-all run test test-coverage format lint clean info update add add-dev remove

# Default target
help:
	@echo "BLS Data Repository - Available Commands"
	@echo ""
	@echo "Dependencies:"
	@echo "  install          Install production dependencies"
	@echo "  install-dev      Install development dependencies"
	@echo "  install-all      Install all dependencies (including dev and test)"
	@echo "  update           Update all dependencies"
	@echo ""
	@echo "Development:"
	@echo "  run <command>    Run a command in the uv environment"
	@echo "  test             Run tests"
	@echo "  test-coverage    Run tests with coverage report"
	@echo "  format           Format code with ruff, black and isort"
	@echo "  lint             Lint code with ruff, flake8 and mypy"
	@echo "  ruff-check       Run ruff linting only"
	@echo "  ruff-format      Run ruff formatting only"
	@echo "  ruff-fix         Run ruff auto-fix"
	@echo ""
	@echo "Utilities:"
	@echo "  clean            Clean up build artifacts and cache"
	@echo "  info             Show project information"
	@echo ""
	@echo "Package Management:"
	@echo "  add <package>    Add a new dependency"
	@echo "  add-dev <package> Add a new development dependency"
	@echo "  remove <package> Remove a dependency"
	@echo ""
	@echo "Examples:"
	@echo "  make install"
	@echo "  make run python scripts/test_cpi_extraction.py"
	@echo "  make add requests"
	@echo "  make test-coverage"

# Install production dependencies
install:
	@./scripts/uv-commands.sh install

# Install development dependencies
install-dev:
	@./scripts/uv-commands.sh install-dev

# Install all dependencies
install-all:
	@./scripts/uv-commands.sh install-all

# Run a command in the uv environment
run:
	@./scripts/uv-commands.sh run $(ARGS)

# Run tests
test:
	@./scripts/uv-commands.sh test

# Run tests with coverage
test-coverage:
	@./scripts/uv-commands.sh test-coverage

# Format code
format:
	@./scripts/uv-commands.sh format

# Lint code
lint:
	@./scripts/uv-commands.sh lint

# Ruff-specific commands
ruff-check:
	@./scripts/uv-commands.sh ruff-check

ruff-format:
	@./scripts/uv-commands.sh ruff-format

ruff-fix:
	@./scripts/uv-commands.sh ruff-fix

# Clean up
clean:
	@./scripts/uv-commands.sh clean

# Show project info
info:
	@./scripts/uv-commands.sh info

# Update dependencies
update:
	@./scripts/uv-commands.sh update

# Add a dependency
add:
	@./scripts/uv-commands.sh add $(ARGS)

# Add a development dependency
add-dev:
	@./scripts/uv-commands.sh add-dev $(ARGS)

# Remove a dependency
remove:
	@./scripts/uv-commands.sh remove $(ARGS)

# Quick setup for new developers
setup: install-dev
	@echo "Setting up development environment..."
	@make format
	@echo "Development environment ready!"

# Full CI pipeline
ci: format lint test
	@echo "CI pipeline completed successfully!"

# Database setup
db-setup:
	@./scripts/uv-commands.sh run python setup_database.py

# Extract CPI data
extract-cpi:
	@./scripts/uv-commands.sh run python scripts/extract_all_cpi_us_city_avg.py

# Test CPI extraction
test-cpi:
	@./scripts/uv-commands.sh run python scripts/test_cpi_extraction.py
