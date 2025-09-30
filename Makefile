# Makefile for SAM.gov Africa Dashboard
# Usage: make [command]

.PHONY: help setup install test run update bootstrap clean migrate docker-build docker-run docker-stop

# Default command
.DEFAULT_GOAL := help

# Python interpreter
PYTHON := python3
PIP := $(PYTHON) -m pip
VENV := venv
ACTIVATE := source $(VENV)/bin/activate

# Directories
DATA_DIR := data
CACHE_DIR := $(DATA_DIR)/.cache
LOG_DIR := logs

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m

help: ## Show this help message
	@echo "$(BLUE)SAM.gov Africa Dashboard - Available Commands$(NC)"
	@echo "================================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Complete setup (virtual environment, dependencies, directories)
	@echo "$(YELLOW)Setting up environment...$(NC)"
	@$(PYTHON) -m venv $(VENV)
	@$(ACTIVATE) && $(PIP) install --upgrade pip
	@$(ACTIVATE) && $(PIP) install -r requirements.txt
	@mkdir -p $(DATA_DIR) $(CACHE_DIR) $(LOG_DIR) .streamlit
	@echo "$(GREEN)✅ Setup complete$(NC)"

install: ## Install/update dependencies
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	@$(ACTIVATE) && $(PIP) install -r requirements.txt
	@echo "$(GREEN)✅ Dependencies installed$(NC)"

test: ## Run system verification tests
	@echo "$(YELLOW)Running tests...$(NC)"
	@$(ACTIVATE) && $(PYTHON) test_system.py

run: ## Start the Streamlit dashboard
	@echo "$(BLUE)Starting dashboard...$(NC)"
	@echo "$(YELLOW)Visit http://localhost:8501$(NC)"
	@$(ACTIVATE) && streamlit run streamlit_dashboard.py

update: ## Run daily update (incremental)
	@echo "$(YELLOW)Running incremental update...$(NC)"
	@$(ACTIVATE) && $(PYTHON) download_and_update.py

update-full: ## Run daily update with cleanup
	@echo "$(YELLOW)Running full update with cleanup...$(NC)"
	@$(ACTIVATE) && $(PYTHON) download_and_update.py --cleanup

bootstrap: ## Bootstrap historical data (WARNING: takes time)
	@echo "$(RED)WARNING: This will download years of historical data$(NC)"
	@echo "$(YELLOW)Starting bootstrap...$(NC)"
	@$(ACTIVATE) && $(PYTHON) bootstrap_historical.py

bootstrap-recent: ## Bootstrap only recent data (faster)
	@echo "$(YELLOW)Bootstrapping recent data (2023-present)...$(NC)"
	@$(ACTIVATE) && $(PYTHON) bootstrap_historical.py --start-year 2023

migrate: ## Migrate existing database to optimized structure
	@echo "$(YELLOW)Migrating database...$(NC)"
	@$(ACTIVATE) && $(PYTHON) migrate_database.py

health: ## Check system health
	@$(ACTIVATE) && $(PYTHON) -c "\
	from sam_utils import get_system; \
	system = get_system(); \
	stats = system.db_manager.get_statistics(); \
	print('$(GREEN)✅ Database healthy$(NC)'); \
	print(f'   Records: {stats[\"total_records\"]:,}'); \
	print(f'   Size: {stats[\"size_mb\"]:.1f} MB'); \
	print(f'   Recent: {stats[\"recent_records\"]:,}')"

clean: ## Clean temporary files and caches
	@echo "$(YELLOW)Cleaning temporary files...$(NC)"
	@rm -rf __pycache__ *.pyc .pytest_cache
	@rm -rf $(CACHE_DIR)/*
	@rm -f $(LOG_DIR)/*.log
	@find . -type f -name "*.csv" -path "*/data/*" -delete
	@find . -type f -name "*.zip" -path "*/data/*" -delete
	@echo "$(GREEN)✅ Cleanup complete$(NC)"

clean-all: clean ## Clean everything including database (CAUTION!)
	@echo "$(RED)WARNING: This will delete the database!$(NC)"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@rm -f $(DATA_DIR)/opportunities.db
	@rm -f $(DATA_DIR)/.bootstrap_progress.txt
	@rm -f $(DATA_DIR)/.last_ids.json
	@echo "$(GREEN)✅ All data removed$(NC)"

docker-build: ## Build Docker image
	@echo "$(YELLOW)Building Docker image...$(NC)"
	@docker build -t sam-dashboard:latest .
	@echo "$(GREEN)✅ Docker image built$(NC)"

docker-run: docker-build ## Run with Docker
	@echo "$(BLUE)Starting Docker container...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✅ Container running at http://localhost:8501$(NC)"

docker-stop: ## Stop Docker container
	@echo "$(YELLOW)Stopping Docker container...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✅ Container stopped$(NC)"

docker-logs: ## View Docker logs
	@docker-compose logs -f

backup: ## Backup database
	@echo "$(YELLOW)Creating backup...$(NC)"
	@mkdir -p backups
	@cp $(DATA_DIR)/opportunities.db backups/opportunities_$$(date +%Y%m%d_%H%M%S).db
	@echo "$(GREEN)✅ Backup created in backups/$(NC)"

git-setup: ## Configure Git for this project
	@echo "$(YELLOW)Configuring Git...$(NC)"
	@git config user.name "$${GIT_USER_NAME:-Your Name}"
	@git config user.email "$${GIT_USER_EMAIL:-your.email@example.com}"
	@git remote add origin https://github.com/JWKSOA/SAM.gov-Africa-Dashboard.git 2>/dev/null || true
	@echo "$(GREEN)✅ Git configured$(NC)"

push: ## Commit and push changes to GitHub
	@echo "$(YELLOW)Pushing to GitHub...$(NC)"
	@git add -A
	@git commit -m "Update: $$(date +%Y-%m-%d\ %H:%M:%S)" || true
	@git push origin main
	@echo "$(GREEN)✅ Pushed to GitHub$(NC)"

# Development targets
dev-install: ## Install development dependencies
	@$(ACTIVATE) && $(PIP) install pytest black pylint jupyter

format: ## Format code with black
	@$(ACTIVATE) && black *.py

lint: ## Lint code with pylint
	@$(ACTIVATE) && pylint *.py

jupyter: ## Start Jupyter notebook for data exploration
	@$(ACTIVATE) && jupyter notebook

# Statistics and reporting
stats: ## Show database statistics
	@$(ACTIVATE) && $(PYTHON) -c "\
	from sam_utils import get_system; \
	import json; \
	system = get_system(); \
	stats = system.db_manager.get_statistics(); \
	print('$(BLUE)Database Statistics$(NC)'); \
	print('='*40); \
	print(f'Total Records: {stats[\"total_records\"]:,}'); \
	print(f'Recent (30d): {stats[\"recent_records\"]:,}'); \
	print(f'Database Size: {stats[\"size_mb\"]:.1f} MB'); \
	print('\n$(BLUE)Top 10 Countries:$(NC)'); \
	for country, count in list(stats[\"by_country\"].items())[:10]: \
		print(f'  {country}: {count:,}')"

report: ## Generate summary report
	@$(ACTIVATE) && $(PYTHON) -c "\
	from sam_utils import get_system; \
	from datetime import datetime; \
	system = get_system(); \
	stats = system.db_manager.get_statistics(); \
	print('SAM.GOV AFRICA DASHBOARD REPORT'); \
	print('='*40); \
	print(f'Generated: {datetime.now()}'); \
	print(f'Total Opportunities: {stats[\"total_records\"]:,}'); \
	print(f'Countries Covered: {len(stats[\"by_country\"])}'); \
	print(f'Database Size: {stats[\"size_mb\"]:.1f} MB'); \
	" > report_$$(date +%Y%m%d).txt
	@echo "$(GREEN)✅ Report saved to report_$$(date +%Y%m%d).txt$(NC)"

# Shortcuts
i: install
r: run
u: update
t: test
c: clean
h: health

.PHONY: i r u t c h