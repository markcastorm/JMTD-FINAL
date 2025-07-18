# JPX Data Processing System

A comprehensive automated system for downloading and processing Japan Exchange Group (JPX) ETF and REIT monthly trading data.

## 📋 Table of Contents

- [Overview](#overview)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Output Structure](#output-structure)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)
- [Maintenance](#maintenance)

## 🎯 Overview

This system consists of three main components:

1. **Download Script** (`final.py`) - Automated web scraping of JPX website for monthly reports
2. **Parser Script** (`jtmd_scraper5.py`) - Intelligent extraction of data from Excel files into standardized CSV format
3. **Master Orchestrator** (`jpx_master_processor.py`) - Coordinates the entire pipeline with robust error handling

### Key Features

- ✅ **Automated Data Collection**: Downloads latest monthly ETF/REIT reports from JPX
- ✅ **Intelligent Parsing**: Adaptive extraction of 84 data points per month
- ✅ **Robust Error Handling**: Retry mechanisms and recovery options
- ✅ **Multi-Month Processing**: Process multiple months in sequence
- ✅ **State Tracking**: Resume processing from where it left off
- ✅ **Windows Compatible**: Handles Unicode and encoding issues
- ✅ **Production Ready**: Comprehensive logging and validation

## 🔧 System Requirements

### Software Dependencies
- **Python 3.8+** (tested with Python 3.12)
- **Google Chrome** (for web scraping)
- **Windows 10/11** (primary testing platform)

### Python Packages
```bash
pip install pandas numpy undetected-chromedriver selenium beautifulsoup4 lxml requests openpyxl
```

### Hardware Requirements
- **RAM**: 4GB minimum, 8GB recommended
- **Storage**: 1GB free space for data and logs
- **Network**: Stable internet connection for JPX website access

## 📦 Installation

1. **Clone or Download** the project files to a directory:
   ```
   JPX_Data_System/
   ├── jpx_master_processor.py
   ├── final.py
   ├── jtmd_scraper5.py
   └── README.md
   ```

2. **Install Python Dependencies**:
   ```bash
   pip install pandas numpy undetected-chromedriver selenium beautifulsoup4 lxml requests openpyxl
   ```

3. **Verify Chrome Installation**: Ensure Google Chrome is installed and up-to-date

4. **Test Installation**:
   ```bash
   python jpx_master_processor.py --help
   ```

## 🚀 Quick Start

### First Time Setup

1. **Initialize Configuration**:
   ```bash
   python jpx_master_processor.py
   ```
   This creates a default `jpx_config.ini` file.

2. **Edit Configuration**:
   Open `jpx_config.ini` and set your target months:
   ```ini
   [PROCESSING]
   target_months = 2025-05,2025-06,2025-07
   ```

3. **Test Single Month**:
   ```bash
   python jpx_master_processor.py --month 2025-05
   ```

4. **Process All Configured Months**:
   ```bash
   python jpx_master_processor.py
   ```

### Expected Processing Time
- **Download Phase**: 1-3 minutes per month
- **Parsing Phase**: 30-60 seconds per month
- **Total per Month**: ~2-5 minutes

## ⚙️ Configuration

### Main Configuration File (`jpx_config.ini`)

```ini
[PROCESSING]
# Target months to process (comma-separated, format: YYYY-MM)
target_months = 2025-05,2025-06,2025-07

# Retry settings for robustness
download_retries = 3
parse_retries = 2

# File management options
keep_raw_files = true
validation_enabled = false

[PATHS]
# Output directory for processed data
base_output_dir = JPX_Processing_Output

# Script paths (usually don't need to change)
download_script = final.py
parser_script = jtmd_scraper5.py

[SCRIPTS]
# Timeout settings (seconds)
download_timeout = 1800  # 30 minutes
parse_timeout = 600      # 10 minutes
```

### Configuration Options Explained

| Setting | Description | Default | Notes |
|---------|-------------|---------|--------|
| `target_months` | Months to process | `2025-06,2025-07` | Format: YYYY-MM |
| `download_retries` | Download retry attempts | `3` | Handles network issues |
| `parse_retries` | Parse retry attempts | `2` | Handles parsing errors |
| `keep_raw_files` | Keep downloaded Excel files | `true` | For debugging/archive |
| `validation_enabled` | Enable output validation | `false` | Extra quality checks |

## 📚 Usage Examples

### Basic Operations

```bash
# Process single month (testing)
python jpx_master_processor.py --month 2025-06

# Process all configured months
python jpx_master_processor.py

# Check processing status
python jpx_master_processor.py --status

# Enable verbose logging
python jpx_master_processor.py --verbose
```

### Advanced Operations

```bash
# Use custom configuration file
python jpx_master_processor.py --config custom_config.ini

# Process with verbose output and save log
python jpx_master_processor.py --verbose > processing_log.txt 2>&1

# Process single month with maximum detail
python jpx_master_processor.py --month 2025-06 --verbose
```

### Batch Processing Examples

```bash
# Process last 6 months (edit config first)
# Set: target_months = 2025-01,2025-02,2025-03,2025-04,2025-05,2025-06
python jpx_master_processor.py

# Process current year (Jan-Dec)
# Set: target_months = 2025-01,2025-02,...,2025-12
python jpx_master_processor.py
```

## 📁 Output Structure

```
JPX_Processing_Output/
├── 2025-05/
│   ├── parser_input/              # Organized input files
│   │   ├── etf_m2505.xls
│   │   └── reit_m2505.xls
│   └── parser_output/             # Final processed data
│       ├── JTMD_DATA_20250718.csv # Main data file (85 columns)
│       ├── JTMD_META_20250718.csv # Metadata file
│       └── JTMD_20250718.zip      # Archive for delivery
├── 2025-06/
│   ├── parser_input/
│   └── parser_output/
├── 2025-07/
│   ├── parser_input/
│   └── parser_output/
├── processing_state.json          # Processing status tracking
└── jpx_master_processor.log       # Detailed operation logs

JPX_Monthly_Reports/               # Raw downloaded files
├── 2025/
│   ├── 05_May/
│   │   ├── etf_m2505.xls
│   │   └── reit_m2505.xls
│   ├── 06_June/
│   │   ├── etf_m2506.xls
│   │   └── reit_m2506.xls
│   └── 07_July/
       ├── etf_m2507.xls
       └── reit_m2507.xls
```

### Key Output Files

| File | Description | Format |
|------|-------------|--------|
| `JTMD_DATA_*.csv` | Main dataset with 84 data columns | CSV (85 cols total) |
| `JTMD_META_*.csv` | Metadata definitions | CSV |
| `JTMD_*.zip` | Archive package | ZIP |
| `processing_state.json` | Processing status | JSON |
| `jpx_master_processor.log` | Detailed logs | Text |

### Data Columns (84 total)

The system extracts 84 data points per month:

- **ETF Data (42 columns)**:
  - Balance (14 columns): Proprietary, Brokerage, Total, Institutions, Individuals, Foreigners, etc.
  - Sales (14 columns): Same categories as Balance
  - Purchases (14 columns): Same categories as Balance

- **REIT Data (42 columns)**:
  - Same structure as ETF data
  - Balance, Sales, Purchases for all investor categories

## 🔍 Troubleshooting

### Common Issues and Solutions

#### 1. Download Failures
**Error**: `Data download failed` or `UnicodeEncodeError`
```bash
# Solution: Use the fixed final.py with Windows Unicode support
# Check internet connection and JPX website availability
python jpx_master_processor.py --month 2025-06 --verbose
```

#### 2. Parsing Failures
**Error**: `No input files prepared fo
