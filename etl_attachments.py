import sqlite3
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from local_session_manager import db
from etl_messages import _convert_apple_timestamp

def etl_attachments(source_db: str, since_date: Optional[datetime] = None) -> Tuple[int, int]:
    print(f"INFO: Starting attachment ETL from: {source_db}")
    raw_attachments = extract_attachments(source_db, since_date)
    imported_count, skipped_count = load_attachments(raw_attachments)
    print(f"INFO: Attachment ETL complete. Imported: {imported_count}, Skipped: {skipped_count}")
    return imported_count, skipped_count

def extract_attachments(source_db: str, since_date: Optional[datetime] = None) -> List[Dict]:
    # First print the schema
    # print("\nSource database attachment table schema:")
    # try:
    #     with sqlite3.connect(source_db) as conn:
    #         cursor = conn.cursor()
    #         cursor.execute("PRAGMA table_info(attachment)")
    #         columns = cursor.fetchall()
    #         for col in columns:
    #             print(f"Column: {col[1]}, Type: {col[2]}")
    # except Exception as e:
    #     print(f"Error getting schema: {e}")
        
    # Continue with original working query
    query = """
    SELECT 
        a.ROWID, a.guid, a.created_date, a.filename, a.uti, a.mime_type,
        a.total_bytes, a.is_sticker, m.guid as message_guid
    FROM attachment a
    JOIN message_attachment_join maj ON a.ROWID = maj.attachment_id
    JOIN message m ON maj.message_id = m.ROWID
    """
    
    if since_date:
        apple_timestamp = int((since_date - datetime(2001, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1e9)
        query += f" WHERE a.created_date > {apple_timestamp}"
    
    query += " ORDER BY a.created_date ASC"
    
    try:
        with sqlite3.connect(source_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            attachments = [dict(row) for row in cursor.fetchall()]
            return attachments
    except Exception as e:
        print(f"\nExtract error: {e}")
        return []

def load_attachments(attachments: List[Dict]) -> Tuple[int, int]:
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        imported_count, skipped_count = 0, 0
        
        # Get message mapping
        cursor.execute("SELECT guid, id FROM messages")
        message_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        attachments_to_insert = []
        for att in attachments:
            message_id = message_map.get(att['message_guid'])
            if not message_id:
                skipped_count += 1
                continue
                
            created_date = _convert_apple_timestamp(att['created_date'])
            try:
                cursor.execute("""
                    INSERT INTO attachments 
                    (message_id, guid, created_date, file_name, uti, mime_type, size, is_sticker)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    message_id,
                    att['guid'],
                    created_date,
                    att['filename'],
                    att['uti'],
                    att['mime_type'],
                    att['total_bytes'],
                    att['is_sticker']
                ))
                imported_count += 1
            except sqlite3.IntegrityError:
                skipped_count += 1
                continue
            
        return imported_count, skipped_count
