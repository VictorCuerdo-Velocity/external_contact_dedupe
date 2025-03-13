# External Contact Dedupe

A Python-based tool to deduplicate external contacts in the DevRev ticketing system. This project analyzes a pivot CSV file of contacts, applies a set of deduplication strategies, and—if running in production—merges duplicate contacts via the DevRev API. It also backs up full contact details prior to merging to ensure data integrity.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Dry Run Mode](#dry-run-mode)
  - [Production Mode](#production-mode)
- [Project Structure](#project-structure)
- [API Integration](#api-integration)
- [Backup & Data Integrity](#backup--data-integrity)
- [Logging & Reporting](#logging--reporting)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The **External Contact Dedupe** script automates the deduplication of external contacts. It reads a CSV file containing a pivot table of contacts, groups them by email, and applies several deduplication strategies such as:

1. **Primary Selection by CXP UID:**  
   If contacts share the same email and belong to the same DevRev account, merge into the contact that has a CXP UID (i.e. an external reference starting with `user_`).

2. **DevRev Bot Amendment Check:**  
   When all contacts have a CXP UID but one has been amended by the DevRev Bot, merge that contact into the primary.

3. **BI Service & Ticket Count:**  
   If contacts share the same external reference, select the primary based on the update status from the BI service or the higher ticket count.

4. **Upwork Specifics:**  
   For contacts with similar emails where one is linked to a generic Upwork account and the other to a specific one, merge them appropriately.

5. **Velocity Global Handling:**  
   For contacts associated with "Velocity Global - OTHER", merge under the real account. If more than two contacts exist, choose the one where the external reference equals the user ID.

Before performing any merge in production, the script makes backups of each contact using the DevRev API to ensure that no data is lost.

---

## Features

- **Deduplication Strategies:**  
  Implements multiple strategies based on email, external reference, BI service updates, and ticket counts.

- **Dry-Run Mode:**  
  Simulate merge operations without affecting the production environment.

- **Production Merge:**  
  Uses DevRev's merge API (`/rev-users/merge`) to perform the actual merge when not in dry-run mode.

- **Backup & Data Integrity:**  
  Retrieves full contact details via the DevRev API (`/rev-users/get`) and saves backups before merging.

- **Structured Reporting:**  
  Generates a JSON report summarizing all merge actions performed or simulated.

- **Logging:**  
  Comprehensive logging to track each step of the process.

---

## Requirements

- **Python:** 3.9 or higher
- **Libraries:**  
  - `requests`
  - `python-dotenv`
  - Other standard libraries (`csv`, `argparse`, `logging`, etc.)

---

## Installation

1. **Clone the Repository:**

   ```bash
   git clone <repository-url>
   cd EXTERNAL_CONTACT_DEDUPE
   ```

2. **Create and Activate a Virtual Environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Unix/macOS
   # For Windows:
   # .\venv\Scripts\activate
   ```

3. **Install Dependencies:**

   ```bash
   # Populate your requirements.txt with the required packages and then run:
   pip install -r requirements.txt
   ```

---

## Configuration

Create a `.env` file in the project root with the following content (modify as needed):

```
# DevRev API Configuration
ENVIRONMENT=production
DEVREV_API_TOKEN=your_actual_api_token_here
DEVREV_BASE_URL=https://api.devrev.ai

# CSV Configuration
CSV_INPUT_PATH=sample_data/contacts_pivot.csv
CSV_OUTPUT_PATH=sample_data/sample_output.csv

# Additional configuration for processing, rate limits, etc. can be added here.
```

This file securely stores sensitive credentials and API endpoints and is loaded by the script using python-dotenv.

---

## Usage

### Dry Run Mode

Dry run mode simulates the merge process without making any changes in production. It also creates a structured JSON report of the actions that would have been performed and saves backups of the contact details.

Run the script with:

```bash
python dedupe_external_contacts.py --csv sample_data/contacts_pivot.csv --dry-run
```

### Production Mode

When you're ready to perform actual merges, run the script without the `--dry-run` flag. Ensure you have thoroughly tested in dry run mode and have valid backups.

```bash
python dedupe_external_contacts.py --csv sample_data/contacts_pivot.csv
```

**Note:** In production mode, the script will:
- Back up full contact details via the `/rev-users/get` endpoint.
- Perform merge actions via the `/rev-users/merge` endpoint.

---

## Project Structure

```
EXTERNAL_CONTACT_DEDUPE/
├── sample_data/                   # CSV file(s) with contact data (e.g., contacts_pivot.csv)
├── backups/                       # Directory where backup JSON files are stored (created automatically)
├── dedupe_external_contacts.py    # Main deduplication script
├── requirements.txt               # Python dependencies list
├── .env                           # Environment configuration file
└── venv/                          # Virtual environment folder (do not commit to version control)
```

---

## API Integration

This tool integrates with the DevRev API:

**Merge Endpoint (`/rev-users/merge`):**  
Performs the merge by sending a POST request with a JSON payload:

```json
{
  "primary_user": "<primary_contact_user_id>",
  "secondary_user": "<duplicate_contact_user_id>"
}
```

**Get Contact Endpoint (`/rev-users/get`):**  
Backs up contact details by sending a POST request with:

```json
{
  "id": "<contact_user_id>"
}
```

The response, which contains complete contact details (including conversations, tickets, internal discussions, etc.), is saved locally.

---

## Backup & Data Integrity

Before executing any merge, the script:

- Calls the `rev-users.get` API endpoint for both the primary and duplicate contacts.
- Saves the full JSON response in the `backups/` directory with a timestamped filename.
- If backup fails for any contact, the merge for that pair is skipped, ensuring no data is lost.

---

## Logging & Reporting

**Logging:**  
The script logs all key actions to the terminal (with timestamps and log levels).

**Structured Report:**  
At the end of execution, a JSON report (e.g., `dedupe_report_YYYYMMDD_HHMMSS.json`) is generated summarizing:

- Total merge actions identified.
- Detailed primary and duplicate contact information for each merge action.

This report helps you audit the deduplication process.

---

## Troubleshooting

**Backup Failures:**  
If a contact backup fails, the script logs an error and skips the merge for that pair. Check the logs to diagnose API connectivity or authorization issues.

**API Errors:**  
HTTP errors during merge operations are logged with the status code and response message. Verify your API token and endpoint configuration.

**Logging:**  
Review the console logs and the JSON report for detailed insights into the deduplication process.

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a feature branch.
3. Implement your changes and add tests.
4. Run the full test suite.
5. Submit a pull request with a detailed description of your changes.

---


