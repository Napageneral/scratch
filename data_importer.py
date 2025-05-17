import os
from datetime import datetime
from typing import Optional
from time import time

from etl_contacts import etl_contacts, etl_live_contacts
from etl_chats import etl_chats
from etl_conversations import (
    etl_conversations,                      # Incremental or "from-scratch" forward logic
    etl_conversations_fresh_split_compare   # Our "backup import" approach
)
from etl_messages import etl_messages
from etl_attachments import etl_attachments
from iphone_backup import get_sms_db_path, get_address_book_db_path

def get_live_chat_db_path() -> str:
    home = os.path.expanduser('~')
    return os.path.join(home, 'Library', 'Messages', 'chat.db')

def import_backup_data(backup_path: str):
    """
    Imports from a backup that may include older/interleaved messages.
    We then do a 'fresh split & compare' to keep existing conversation IDs 
    whenever intervals match exactly, and only replace intervals that changed.
    """
    t0 = time()
    sms_db_path = get_sms_db_path(backup_path)
    address_book_db_path = get_address_book_db_path(backup_path)
    if not sms_db_path or not address_book_db_path:
        print("ERROR: Unable to locate backup databases")
        return
    
    print("\nBACKUP IMPORT")
    print("=" * 50)
    
    t = time()
    etl_contacts(address_book_db_path)
    print(f"[Backup] Contacts ETL:      {round(time() - t, 2):>6}s")
    
    t = time()
    etl_chats(sms_db_path)
    print(f"[Backup] Chats ETL:         {round(time() - t, 2):>6}s")
    
    t = time()
    etl_messages(sms_db_path)
    print(f"[Backup] Messages ETL:      {round(time() - t, 2):>6}s")
    
    t = time()
    etl_attachments(sms_db_path)
    print(f"[Backup] Attachments ETL:   {round(time() - t, 2):>6}s")
    
    # Instead of calling the incremental 'etl_conversations', we do a 
    # "fresh split & compare" for each chat so we only replace intervals that truly changed.
    t = time()
    etl_conversations_fresh_split_compare()
    print(f"[Backup] Conversations (fresh split) ETL: {round(time() - t, 2):>6}s")
    
    total_time = round(time() - t0, 2)
    print("-" * 50)
    print(f"Total Backup Import Time:   {total_time:>6}s")
    print("=" * 50)

def import_live_data(since_date: Optional[datetime] = None):
    """
    For the live DB import:
      - If no since_date, we do a "from-scratch" forward logic (extract all).
      - If since_date is provided, we do the incremental approach, only new messages.
    """
    t0 = time()
    sms_db_path = get_live_chat_db_path()
    if not os.path.exists(sms_db_path):
        print(f"ERROR: Messages database not found at {sms_db_path}")
        return
    
    # print("\nLIVE IMPORT") # Commented out
    # print("=" * 50) # Commented out
    # if since_date: # Commented out
        # print(f"Importing data since: {since_date.isoformat()}") # Commented out
    # else: # Commented out
        # print("Importing all live data (no since_date).") # Commented out
    
    # t = time() # Commented out timing for etl_live_contacts
    etl_live_contacts()
    # print(f"[Live] Contacts ETL:      {round(time() - t, 2):>6}s") # Commented out
    
    # t = time() # Commented out timing for etl_chats
    etl_chats(sms_db_path)
    # print(f"[Live] Chats ETL:         {round(time() - t, 2):>6}s") # Commented out
    
    # t = time() # Commented out timing for etl_messages
    etl_messages(sms_db_path, since_date)
    # print(f"[Live] Messages ETL:      {round(time() - t, 2):>6}s") # Commented out
    
    # t = time() # Commented out timing for etl_attachments
    etl_attachments(sms_db_path, since_date)
    # print(f"[Live] Attachments ETL:   {round(time() - t, 2):>6}s") # Commented out
    
    # Use the normal "etl_conversations" incremental approach for live data
    # (or from-scratch if no since_date).
    # t = time() # Commented out timing for etl_conversations
    etl_conversations(since_date=since_date)
    # print(f"[Live] Conversations ETL: {round(time() - t, 2):>6}s") # Commented out
    
    # total_time = round(time() - t0, 2) # Commented out
    # print("-" * 50) # Commented out
    # print(f"Total Live Import Time:   {total_time:>6}s") # Commented out
    # print("=" * 50) # Commented out
