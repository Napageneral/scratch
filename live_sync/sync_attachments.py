import sqlite3
from typing import List, Dict
from local_session_manager import db
from etl_attachments import _convert_apple_timestamp
from .cache import get_message_guid_to_id_map, get_attachment_guids, ATTACHMENT_GUID_CACHE

def sync_attachments(new_rows: List[Dict]) -> int:
    """
    Process new attachments from chat.db into our database.
    
    Args:
        new_rows: List of raw attachment rows from chat.db
        
    Returns:
        Number of imported attachments
    """
    if not new_rows:
        return 0
    
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        
        # Build message guid to id map
        print("DEBUG: Building cache maps for attachments sync")
        message_map = get_message_guid_to_id_map(cursor, force_refresh=False)
        
        # Get existing attachment GUIDs from cache
        attachment_guid_cache = get_attachment_guids(cursor, force_refresh=False)
        
        attachments_to_insert = []
        imported_count = 0
        skipped_count = 0
        
        # Process each attachment
        for row in new_rows:
            # Skip if we already have this attachment
            if row['guid'] in attachment_guid_cache:
                skipped_count += 1
                continue
            
            # Find the message this attachment belongs to
            message_id = message_map.get(row['message_guid'])
            if not message_id:
                print(f"WARNING: Message not found for attachment: {row['guid']}")
                skipped_count += 1
                continue
            
            # Convert the created_date
            created_date = _convert_apple_timestamp(row['created_date'])
            
            # Prepare for insert
            attachments_to_insert.append((
                message_id,
                row['guid'],
                created_date,
                row['filename'],
                row['uti'],
                row['mime_type'],
                row['total_bytes'],
                bool(row['is_sticker'])
            ))
            imported_count += 1
        
        # Bulk insert
        if attachments_to_insert:
            print(f"INFO: Inserting {len(attachments_to_insert)} new attachments")
            cursor.executemany("""
                INSERT INTO attachments (message_id, guid, created_date, file_name, uti, 
                                       mime_type, size, is_sticker)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, attachments_to_insert)
            
            # Update attachment guid cache with newly inserted attachment GUIDs
            for attachment_data in attachments_to_insert:
                ATTACHMENT_GUID_CACHE.add(attachment_data[1])  # [1] is the guid
        
        # Log stats
        if imported_count > 0 or skipped_count > 0:
            print(f"INFO: Attachments sync complete - Imported: {imported_count}, Skipped: {skipped_count}")
        
        return imported_count 