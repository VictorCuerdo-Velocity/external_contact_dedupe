#!/usr/bin/env python3
"""
Dedupe External Contacts Script with Backup

This script reads a CSV file containing a pivot table of external contacts.
It groups contacts by email and applies deduplication strategies. Before performing a merge in production,
it backs up each contact's full details via the DevRev API (rev-users.get) to ensure data integrity.
The script supports a dry-run mode for simulation and outputs a JSON report.
"""

import csv
import argparse
import logging
import json
import os
import requests
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
DEVREV_BASE_URL = os.getenv("DEVREV_BASE_URL")
DEVREV_API_TOKEN = os.getenv("DEVREV_API_TOKEN")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dedupe")

class ExternalContact:
    def __init__(self, row: Dict[str, str]):
        self.email = row.get("Email", "").strip().lower()
        self.external_ref = row.get("External Ref", "").strip()
        self.modified_by = row.get("Modified By Name", "").strip()
        self.user_id = row.get("User ID", "").strip()
        self.type = row.get("Type", "").strip()
        self.display_id = row.get("Display ID", "").strip()
        self.devrev_account_id = row.get("Devrev Account ID", "").strip()
        self.devrev_account_name = row.get("Devrev Account Name", "").strip()
        self.updated_at = row.get("Updated At", "").strip()
        self.cxp_user_id = row.get("CXP User id", "").strip().upper()
        self.updated_by_bi = row.get("Updated by BI service", "").strip().upper()
        self.linked_to_acc = row.get("Linked to acc", "").strip()
        self.tickets = int(row.get("Tickets", "0").strip())
        self.action = row.get("Action", "").strip()
        self.strategy = row.get("Strategy", "").strip()
    
    def has_cxp_uid(self) -> bool:
        return self.external_ref.startswith("user_")
    
    def updated_by_bi_service(self) -> bool:
        return self.updated_by_bi == "TRUE"
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "email": self.email,
            "external_ref": self.external_ref,
            "user_id": self.user_id,
            "devrev_account_id": self.devrev_account_id,
            "devrev_account_name": self.devrev_account_name,
            "tickets": self.tickets,
            "modified_by": self.modified_by,
            "updated_at": self.updated_at,
            "type": self.type,
            "linked_to_acc": self.linked_to_acc
        }
    
    def __repr__(self):
        return f"<Contact {self.email} | ext_ref: {self.external_ref} | acc: {self.devrev_account_id}>"

def load_contacts(csv_path: str) -> List[ExternalContact]:
    contacts = []
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            email = row.get("Email", "").strip().lower()
            if "test@" in email or "vg@" in email:
                continue
            try:
                contact = ExternalContact(row)
                contacts.append(contact)
            except Exception as e:
                logger.error(f"Error parsing row: {row}. Exception: {e}")
    logger.info(f"Loaded {len(contacts)} contacts from CSV.")
    return contacts

def group_contacts_by_email(contacts: List[ExternalContact]) -> Dict[str, List[ExternalContact]]:
    groups = defaultdict(list)
    for contact in contacts:
        groups[contact.email].append(contact)
    return groups

def choose_primary_contact(group: List[ExternalContact]) -> Tuple[Optional[ExternalContact], List[ExternalContact]]:
    if not group or len(group) < 2:
        return None, []
    
    primary = None
    account_groups = defaultdict(list)
    for contact in group:
        account_groups[contact.devrev_account_id].append(contact)
    
    merge_candidates = []
    for acc, contacts_in_acc in account_groups.items():
        if len(contacts_in_acc) < 2:
            continue
        
        cxp_contacts = [c for c in contacts_in_acc if c.has_cxp_uid()]
        if cxp_contacts and len(cxp_contacts) == 1:
            primary = cxp_contacts[0]
        elif cxp_contacts and len(cxp_contacts) > 1:
            candidates = sorted(cxp_contacts, key=lambda c: (c.updated_by_bi_service(), c.tickets), reverse=True)
            primary = candidates[0]
        else:
            primary = max(contacts_in_acc, key=lambda c: c.tickets)
        
        if any("upwork" in c.type.lower() for c in contacts_in_acc):
            specific = [c for c in contacts_in_acc if "upwork" in c.external_ref.lower()]
            if specific:
                primary = specific[0]
        
        if any("velocity global - other" in c.devrev_account_name.lower() for c in contacts_in_acc):
            real_accounts = [c for c in contacts_in_acc if c.external_ref == c.user_id]
            if real_accounts:
                primary = real_accounts[0]
        
        duplicates = [c for c in contacts_in_acc if c != primary]
        merge_candidates.extend([(primary, dup) for dup in duplicates])
    
    return primary, merge_candidates

def backup_contact(contact: ExternalContact) -> bool:
    """
    Backup the full contact details using the DevRev API (rev-users.get) and save it as a JSON file.
    """
    backup_endpoint = "/rev-users/get"
    url = f"{DEVREV_BASE_URL.rstrip('/')}{backup_endpoint}"
    payload = {"id": contact.user_id}
    headers = {
        "Authorization": f"Bearer {DEVREV_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        backup_data = response.json()
        
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_filename = os.path.join(
            backup_dir,
            f"backup_{contact.user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(backup_filename, "w", encoding="utf-8") as f:
            json.dump(backup_data, f, indent=2)
        logger.info(f"Backup saved for contact {contact.user_id} at {backup_filename}")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during backup for {contact.user_id}: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during backup for {contact.user_id}: {str(e)}")
    return False

def perform_merge(primary: ExternalContact, duplicate: ExternalContact) -> bool:
    """
    Call the DevRev merge API endpoint to merge the duplicate into the primary contact.
    """
    merge_endpoint = "/rev-users/merge"
    url = f"{DEVREV_BASE_URL.rstrip('/')}{merge_endpoint}"
    payload = {
        "primary_user": primary.user_id,
        "secondary_user": duplicate.user_id
    }
    headers = {
        "Authorization": f"Bearer {DEVREV_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Merge successful: {primary.external_ref} <== {duplicate.external_ref}")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during merge: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during merge: {str(e)}")
    return False

def save_merge_report(merge_actions: List[Tuple[ExternalContact, ExternalContact]]) -> None:
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_merge_actions": len(merge_actions),
        "merge_actions": []
    }
    for primary, duplicate in merge_actions:
        report["merge_actions"].append({
            "primary": primary.to_dict(),
            "duplicate": duplicate.to_dict()
        })
    report_filename = f"dedupe_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Structured report saved to {report_filename}")

def dedupe_contacts(contacts: List[ExternalContact], dry_run: bool) -> None:
    groups = group_contacts_by_email(contacts)
    merge_actions = []  # List of tuples (primary, duplicate)
    for email, group in groups.items():
        if len(group) < 2:
            continue
        _, merges = choose_primary_contact(group)
        merge_actions.extend(merges)
    
    logger.info(f"Identified {len(merge_actions)} merge actions.")
    
    for primary, duplicate in merge_actions:
        logger.info(f"Merge Action: Merge duplicate {duplicate.external_ref} (Account: {duplicate.devrev_account_id}) into primary {primary.external_ref}")
        if not dry_run:
            # Backup primary and duplicate before merging
            if not backup_contact(primary):
                logger.error(f"Backup failed for primary {primary.user_id}. Skipping merge for this pair.")
                continue
            if not backup_contact(duplicate):
                logger.error(f"Backup failed for duplicate {duplicate.user_id}. Skipping merge for this pair.")
                continue
            
            success = perform_merge(primary, duplicate)
            if not success:
                logger.error("Merge failed for this pair.")
        else:
            logger.info("Dry run mode: No merge performed.")
    
    save_merge_report(merge_actions)

def main():
    parser = argparse.ArgumentParser(
        description="Dedupe external contacts based on a pivot CSV file."
    )
    parser.add_argument("--csv", required=True, help="Path to the CSV file containing contact pivot data")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry run mode (do not perform actual merges)")
    args = parser.parse_args()

    contacts = load_contacts(args.csv)
    dedupe_contacts(contacts, args.dry_run)
    logger.info("Dedupe process completed.")

if __name__ == "__main__":
    main()
