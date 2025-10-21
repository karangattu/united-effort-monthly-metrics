# United Effort Organization - Monthly Metrics Dashboard

## Overview

This system automatically fetches, filters, and consolidates monthly metrics from multiple sources every first Monday of the month. The data is presented through an interactive dashboard deployed to GitHub Pages.

## Quick Start

### 1. Local Setup

Clone the repository and install dependencies:

```bash
git clone <repository-url>
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Environment

Create `.env` file in the project root:

```bash
USERNAME=your_apricot_username
PASSWORD=your_apricot_password
AIRTABLE_TOKEN=your_airtable_api_token
```

**⚠️ IMPORTANT:** Never commit `.env` to version control (it's in `.gitignore`)

### 3. Run Locally

```bash
# Fetch Apricot reports
python apricot_reports.py

# Fetch Airtable data and append to summary
python airtable_fetch.py

# Start local server to view dashboard
python -m http.server 8000
```

Then open: `http://localhost:8000/dashboard.html`
