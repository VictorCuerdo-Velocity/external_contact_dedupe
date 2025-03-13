#!/usr/bin/env python3
"""
Dedupe External Contacts Script

This script reads a CSV file containing a pivot table of external contacts.
It then groups contacts by email and applies a series of deduplication strategies:
  1. If contacts have the same email and belong to the same DevRev account,
     merge into the contact that has a CXP UID (external reference in the form "user_*").
  2. If all contacts have a CXP UID external reference, and one was amended by the DevRev Bot,
     merge that contact into the primary.
  3. If contacts have the same external reference, select the primary based on either:
     - Updated by BI service flag or
     - Having a higher number of tickets.
  4. If one contact is linked to a generic Upwork account and the other to a specific Upwork account,
     mark them for merging.
  5. If one contact is linked to "Velocity Global - OTHER", merge under the real account;
     if more than two remain, choose the one where the external ref equals the user id.
     
The script supports a dry-run mode to simulate the merges without affecting production.
"""

import csv
import argparse
import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("dedupe")

# Define a dataclass-like structure for contacts (for simplicity, using dicts here)
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
        self.cxp_user_id = row.get("CXP User id", "").strip().upper()  # standardize boolean later if needed
        self.updated_by_bi = row.get("Updated by BI service", "").strip().upper()  # "TRUE" or "FALSE"
        self.linked_to_acc = row.get("Linked to acc", "").strip()
        self.tickets = int(row.get("Tickets", "0").strip())
        self.action = row.get("Action", "").strip()
        self.strategy = row.get("Strategy", "").strip()
    
    def has_cxp_uid(self) -> bool:
        # Assume a CXP UID starts with "user_" (as seen in sample rows)
        return self.external_ref.startswith("user_")
    
    def updated_by_bi_service(self) -> bool:
        return self.updated_by_bi == "TRUE"
    
    def __repr__(self):
        return f"<Contact {self.email} | ext_ref: {self.external_ref} | acc: {self.devrev_account_id}>"

def load_contacts(csv_path: str) -> List[ExternalContact]:
    contacts = []
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Exclude test emails or Velocity Global (vg) emails if needed
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
    """
    Given a group of contacts with the same email, apply the dedupe strategies to pick a primary
    and return the duplicates (to be merged into the primary).
    Strategies (simplified):
      - Strategy 1: If at least one contact has a CXP UID and others don’t, choose the one with CXP UID.
      - Strategy 2: If all have CXP UIDs but one was amended by DevRev Bot, choose the one not modified by Bot.
      - Strategy 3: If external_ref values are identical, choose the one updated by BI service or with more tickets.
      - Strategy 4 & 5: Check for special types ("Upwork", "Velocity Global - OTHER") and adjust primary accordingly.
    """
    if not group or len(group) < 2:
        return None, []
    
    primary = None

    # First, if contacts belong to different DevRev accounts, only consider those linked to the same account.
    account_groups = defaultdict(list)
    for contact in group:
        account_groups[contact.devrev_account_id].append(contact)
    # Process each account group separately.
    merge_candidates = []
    for acc, contacts_in_acc in account_groups.items():
        if len(contacts_in_acc) < 2:
            continue  # nothing to merge in this account group
        
        # Strategy 1: Choose contact with CXP UID if others do not have it.
        cxp_contacts = [c for c in contacts_in_acc if c.has_cxp_uid()]
        if cxp_contacts and len(cxp_contacts) == 1:
            primary = cxp_contacts[0]
        elif cxp_contacts and len(cxp_contacts) > 1:
            # Strategy 3: If more than one contact has CXP UID, pick the one updated by BI service or with more tickets.
            candidates = sorted(cxp_contacts, key=lambda c: (c.updated_by_bi_service(), c.tickets), reverse=True)
            primary = candidates[0]
        else:
            # Fallback: choose the one with the highest ticket count.
            primary = max(contacts_in_acc, key=lambda c: c.tickets)
        
        # Special strategy for Upwork
        if any("upwork" in c.type.lower() for c in contacts_in_acc):
            # If one contact is generic and one is specific, choose the specific one.
            specific = [c for c in contacts_in_acc if "upwork" in c.external_ref.lower()]
            if specific:
                primary = specific[0]
        
        # Special strategy for Velocity Global - OTHER:
        if any("velocity global - other" in c.devrev_account_name.lower() for c in contacts_in_acc):
            real_accounts = [c for c in contacts_in_acc if c.external_ref == c.user_id]
            if real_accounts:
                primary = real_accounts[0]
        
        # Determine duplicates (all other contacts in this account group)
        duplicates = [c for c in contacts_in_acc if c != primary]
        merge_candidates.extend([(primary, dup) for dup in duplicates])
    
    return primary, merge_candidates

def dedupe_contacts(contacts: List[ExternalContact], dry_run: bool) -> None:
    groups = group_contacts_by_email(contacts)
    merge_actions = []  # List of tuples (primary, duplicate)

    for email, group in groups.items():
        if len(group) < 2:
            continue
        _, merges = choose_primary_contact(group)
        merge_actions.extend(merges)
    
    logger.info(f"Identified {len(merge_actions)} merge actions.")

    # Process merge actions
    for primary, duplicate in merge_actions:
        logger.info(f"Merge Action: Merge duplicate {duplicate.external_ref} (Account: {duplicate.devrev_account_id}) into primary {primary.external_ref}")
        if not dry_run:
            # Here, you would call the DevRev API merge endpoint.
            # For example:
            # success = devrev_api.merge_contacts(primary.rev_user_id, duplicate.rev_user_id)
            # if success:
            #     logger.info("Merge succeeded.")
            # else:
            #     logger.error("Merge failed.")
            logger.info("Performing merge (this would call the production API).")
        else:
            logger.info("Dry run mode: No merge performed.")

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
