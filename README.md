# 🌍 SAM.gov Africa Dashboard

An automated system for tracking U.S. government contract opportunities in African countries, powered by SAM.gov data.

## 🚀 Features

- **Automated Data Collection**: Daily updates from SAM.gov with historical archives back to 1998
- **Smart Filtering**: Identifies contracts for all 54 African countries with intelligent country name matching
- **Interactive Dashboard**: Real-time visualization with maps, charts, and searchable tables
- **Performance Optimized**: Incremental updates, database indexing, and caching for fast queries
- **GitHub Actions Integration**: Fully automated daily updates with error recovery
- **Data Integrity**: Automatic deduplication and validation

## 📊 Dashboard Features

- **Geographic visualization** of opportunities across Africa
- **Temporal trends** and historical analysis
- **Agency breakdown** and statistics  
- **Searchable data tables** with export functionality
- **Real-time filters** by country, agency, and date range
- **Direct links** to opportunities on SAM.gov

## 🏗️ Architecture

```
SAM.gov Data → Python ETL → SQLite DB → Streamlit Dashboard
                    ↑
            GitHub Actions (Daily)
```

### Core Components

- **`sam_utils.py`**: Shared utilities and data processing engine
- **`bootstrap_historical.py`**: Initial historical data loader
- **`download_and_update.py`**: Daily incremental updater
- **`streamlit_dashboard.py`**: Interactive web dashboard
- **`.github/workflows/update-sam-db.yml`**: Automated daily updates

## 🚦 Quick Start

### Prerequisites

- Python 3.8+
- Git
- 2GB RAM minimum
- 500MB disk space

### Installation

```bash
# Clone the repository
git clone https://github.com/JWKSOA/SAM.gov-Africa-Dashboard.git
cd SAM.gov-Africa-Dashboard

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt

# Initialize database with recent data (quick start)
python download_and_update.py --lookback-days 30

# Or full historical data (takes longer)
python bootstrap_historical.py --start-year 2020

# Run the dashboard
streamlit run streamlit_dashboard.py
```

## 🔧 Configuration

### Streamlit Secrets

Create `.streamlit/secrets.toml`:

```toml
github_owner = "JWKSOA"
github_repo = "SAM.gov-Africa-Dashboard"
github_workflow = "update-sam-db.yml"
github_token = "your_github_token_here"
```

### Environment Variables

- `SAM_DATA_DIR`: Override data directory location
- `FORCE_UPDATE`: Force daily update even if already run

## 📈 Performance

### Optimized Metrics

- **Daily Updates**: <2 minutes (vs 10+ minutes original)
- **Dashboard Load**: <2 seconds (vs 15+ seconds original)
- **Memory Usage**: <500MB (vs 2GB+ original)
- **Code Reduction**: 40% less code through modularization

### Database Statistics

- **Coverage**: All 54 African countries
- **Historical Data**: 1998 to present
- **Update Frequency**: Daily at 04:30 UTC
- **Typical Size**: 50-100MB
- **Record Count**: 10,000+ opportunities

## 🛠️ Development

### Project Structure

```
.
├── sam_utils.py              # Shared utilities module
├── bootstrap_historical.py   # Historical data loader
├── download_and_update.py    # Daily updater
├── streamlit_dashboard.py    # Web dashboard
├── requirements.txt          # Python dependencies
├── .github/
│   └── workflows/
│       └── update-sam-db.yml # GitHub Actions workflow
├── data/
│   ├── opportunities.db      # SQLite database
│   └── .cache/              # Cache directory
└── .streamlit/
    └── secrets.toml         # Configuration (gitignored)
```

### Running Tests

```bash
# Health check
python -c "from sam_utils import get_system; s = get_system(); print(s.db_manager.get_statistics())"

# Manual update test
python download_and_update.py --lookback-days 7

# Dashboard test
streamlit run streamlit_dashboard.py --logger.level=debug
```

### Manual Bootstrap

```bash
# Full historical bootstrap
python bootstrap_historical.py

# Specific year range
python bootstrap_historical.py --start-year 2020 --end-year 2023

# Skip current data
python bootstrap_historical.py --skip-current
```

## 🔍 Data Sources

- **Current Data**: [SAM.gov Contract Opportunities CSV](https://sam.gov/data-services/Contract%20Opportunities/datagov)
- **Historical Archives**: SAM.gov FY archives (1998-present)
- **Update Frequency**: Daily extracts published by SAM.gov

## 📝 Country Coverage

All 54 recognized African countries including:
- North Africa: Algeria, Egypt, Libya, Morocco, Tunisia, Sudan
- West Africa: Nigeria, Ghana, Senegal, Mali, Burkina Faso, etc.
- East Africa: Kenya, Ethiopia, Tanzania, Uganda, Rwanda, etc.
- Central Africa: DRC, Cameroon, Chad, CAR, Gabon, etc.
- Southern Africa: South Africa, Zimbabwe, Zambia, Botswana, etc.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is open source and available under the MIT License.

## 🙏 Acknowledgments

- Data provided by [SAM.gov](https://sam.gov)
- Built with [Streamlit](https://streamlit.io)
- Automated with [GitHub Actions](https://github.com/features/actions)

## 📞 Support

For issues or questions:
1. Check the [Issues](https://github.com/JWKSOA/SAM.gov-Africa-Dashboard/issues) page
2. Review the implementation instructions
3. Create a new issue with details

---

**Last Updated**: 2024  
**Version**: 2.0.0 (Optimized)  
**Status**: 🟢 Active