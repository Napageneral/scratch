import sqlite3
import os
from typing import List, Dict

# Module-level persistent connection to chat.db
LIVE_DB_CONNECTION = None
def get_chat_db_connection():
    """Returns a persistent connection to chat.db"""
    global LIVE_DB_CONNECTION
    if LIVE_DB_CONNECTION is None:
        # Add a timeout to prevent locking issues
        LIVE_DB_CONNECTION = sqlite3.connect(get_live_chat_db_path(), timeout=1.0)
        LIVE_DB_CONNECTION.row_factory = sqlite3.Row
    return LIVE_DB_CONNECTION

def to_nanos(raw: int) -> int:
    """Normalise Apple epoch timestamp to nanoseconds."""
    if not isinstance(raw, int): # Guard against non-integer inputs
        print(f"WARNING: to_nanos received non-integer input: {raw} of type {type(raw)}")
        # Depending on desired strictness, could raise error or return as-is/None
        return raw # Or handle more gracefully

    if raw > 1e16:  # already ns (approx > 2286 AD)
        return raw
    if raw > 1e13:  # µs -> ns (approx > 2001 AD in µs)
        return raw * 1_000
    if raw > 1e10:  # ms -> ns (approx > 2001 AD in ms)
        return raw * 1_000_000
    return raw * 1_000_000_000  # seconds -> ns

def get_live_chat_db_path() -> str:
    """Get the path to the live Messages database."""
    home = os.path.expanduser('~')
    return os.path.join(home, 'Library', 'Messages', 'chat.db')

LIVE_DB = get_live_chat_db_path()

def fetch_new_messages_by_date(last_apple_epoch: int) -> List[Dict]:
    """
    Legacy method: Fetch new messages from chat.db since the last processed timestamp.
    last_apple_epoch is expected in nanoseconds.
    """
    query = """
    SELECT 
        m.ROWID as message_id, m.guid, m.text, m.attributedBody, m.handle_id, m.service, 
        m.date, m.is_from_me, m.associated_message_guid, m.associated_message_type,
        m.reply_to_guid, sender_handle.id as sender_identifier,
        chat.ROWID as chat_id, GROUP_CONCAT(DISTINCT chat_handle.id) as chat_participants
    FROM message m
    JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    JOIN chat ON cmj.chat_id = chat.ROWID
    LEFT JOIN handle as sender_handle ON m.handle_id = sender_handle.ROWID
    LEFT JOIN chat_handle_join ON chat.ROWID = chat_handle_join.chat_id
    LEFT JOIN handle as chat_handle ON chat_handle_join.handle_id = chat_handle.ROWID
    WHERE m.date > ?
    GROUP BY m.ROWID
    ORDER BY m.date ASC
    """
    try:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        # Assuming chat.db 'date' is mostly microseconds and last_apple_epoch (watermark) is nanoseconds.
        # Adjust query parameter accordingly.
        query_timestamp = last_apple_epoch // 1_000
        cursor.execute(query, (query_timestamp,))
        messages = [dict(row) for row in cursor.fetchall()]
        
        # Normalize the 'date' field of each message to nanoseconds
        for m in messages:
            if 'date' in m and m['date'] is not None:
                try:
                    m['date'] = to_nanos(int(m['date']))
                except (ValueError, TypeError) as e:
                    print(f"ERROR: Could not convert date {m['date']} to int for to_nanos: {e}")
                    # Decide how to handle: skip message, set date to None, etc.
                    # For now, keeps problematic date as is or original type if conversion failed.

        # if messages: # Commented out logging block
        #     # Ensure dates used for logging are valid before attempting min/max
        #     valid_dates = [m['date'] for m in messages if isinstance(m.get('date'), int)]
        #     if valid_dates:
        #         print(f"INFO: [LiveSync] fetch_new_messages → {len(messages)} rows "
        #                     f"(since watermark_ns={last_apple_epoch}, queried as chat.db units ~{query_timestamp}) "
        #                     f"min_date_ns={min(valid_dates)}, "
        #                     f"max_date_ns={max(valid_dates)})")
        #     else:
        #         print(f"INFO: [LiveSync] fetch_new_messages → {len(messages)} rows, but no valid integer dates found after normalization.")
        # else:
        #     print(f"DEBUG: [LiveSync] No new messages since watermark_ns={last_apple_epoch} (queried as chat.db units ~{query_timestamp})")
        return messages
    except sqlite3.Error as e:
        print(f"ERROR: Error fetching new messages: {e}", exc_info=True)
        # If there's an error with the connection, reset it so it will be recreated on next call
        global LIVE_DB_CONNECTION
        try:
            if LIVE_DB_CONNECTION:
                LIVE_DB_CONNECTION.close()
        except:
            pass
        LIVE_DB_CONNECTION = None
        return []

def fetch_new_messages(last_rowid: int, last_apple_epoch: int = None) -> List[Dict]:
    """
    Fetch new messages from chat.db since the last processed ROWID.
    This is much faster than date-based filtering.
    
    Args:
        last_rowid: The last processed message ROWID
        last_apple_epoch: Optional timestamp for logging purposes only
    """
    # Simplified query without GROUP_CONCAT for better performance
    query = """
    SELECT 
        m.ROWID as message_id, m.guid, m.text, m.attributedBody, m.handle_id, m.service, 
        m.date, m.is_from_me, m.associated_message_guid, m.associated_message_type,
        m.reply_to_guid, sender_handle.id as sender_identifier,
        chat.ROWID as chat_id
    FROM message m
    JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    JOIN chat ON cmj.chat_id = chat.ROWID
    LEFT JOIN handle as sender_handle ON m.handle_id = sender_handle.ROWID
    WHERE m.ROWID > ?
    ORDER BY m.ROWID ASC
    """
    try:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (last_rowid,))
        messages = [dict(row) for row in cursor.fetchall()]
        
        # Get max ROWID for watermark update
        max_rowid = max([m['message_id'] for m in messages]) if messages else last_rowid
        
        # Normalize the 'date' field of each message to nanoseconds
        for m in messages:
            if 'date' in m and m['date'] is not None:
                try:
                    m['date'] = to_nanos(int(m['date']))
                except (ValueError, TypeError) as e:
                    print(f"ERROR: Could not convert date {m['date']} to int for to_nanos: {e}")
        
        if messages:
            print(f"INFO: [LiveSync] fetch_new_messages → {len(messages)} rows using ROWID > {last_rowid}, max_rowid={max_rowid}")
        
        return messages, max_rowid
    except sqlite3.Error as e:
        print(f"ERROR: Error fetching new messages by ROWID: {e}", exc_info=True)
        # If there's an error with the connection, reset it so it will be recreated on next call
        global LIVE_DB_CONNECTION
        try:
            if LIVE_DB_CONNECTION:
                LIVE_DB_CONNECTION.close()
        except:
            pass
        LIVE_DB_CONNECTION = None
        return [], last_rowid

def fetch_new_attachments_by_date(last_apple_epoch: int) -> List[Dict]:
    """
    Legacy method: Fetch new attachments from chat.db since the last processed timestamp.
    """
    query = """
    SELECT 
        a.ROWID, a.guid, a.created_date, a.filename, a.uti, a.mime_type,
        a.total_bytes, a.is_sticker, m.guid as message_guid
    FROM attachment a
    JOIN message_attachment_join maj ON a.ROWID = maj.attachment_id
    JOIN message m ON maj.message_id = m.ROWID
    WHERE a.created_date > ?
    ORDER BY a.created_date ASC
    """
    try:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (last_apple_epoch,))
        attachments = [dict(row) for row in cursor.fetchall()]
        # print(f"DEBUG: Extracted {len(attachments)} new attachments from {LIVE_DB}") # Commented out
        return attachments
    except sqlite3.Error as e:
        print(f"ERROR: Error fetching new attachments: {e}", exc_info=True)
        # If there's an error with the connection, reset it so it will be recreated on next call
        global LIVE_DB_CONNECTION
        try:
            if LIVE_DB_CONNECTION:
                LIVE_DB_CONNECTION.close()
        except:
            pass
        LIVE_DB_CONNECTION = None
        return []

def fetch_new_attachments(last_rowid: int) -> List[Dict]:
    """
    Fetch new attachments from chat.db since the last processed ROWID.
    This is much faster than date-based filtering.
    
    Args:
        last_rowid: The last processed attachment ROWID
    """
    query = """
    SELECT 
        a.ROWID, a.guid, a.created_date, a.filename, a.uti, a.mime_type,
        a.total_bytes, a.is_sticker, m.guid as message_guid
    FROM attachment a
    JOIN message_attachment_join maj ON a.ROWID = maj.attachment_id
    JOIN message m ON maj.message_id = m.ROWID
    WHERE a.ROWID > ?
    ORDER BY a.ROWID ASC
    """
    try:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (last_rowid,))
        attachments = [dict(row) for row in cursor.fetchall()]
        
        # Get max ROWID for watermark update
        max_rowid = max([a['ROWID'] for a in attachments]) if attachments else last_rowid
        
        if attachments:
            print(f"INFO: [LiveSync] fetch_new_attachments → {len(attachments)} rows using ROWID > {last_rowid}, max_rowid={max_rowid}")
        
        return attachments, max_rowid
    except sqlite3.Error as e:
        print(f"ERROR: Error fetching new attachments by ROWID: {e}", exc_info=True)
        # If there's an error with the connection, reset it so it will be recreated on next call
        global LIVE_DB_CONNECTION
        try:
            if LIVE_DB_CONNECTION:
                LIVE_DB_CONNECTION.close()
        except:
            pass
        LIVE_DB_CONNECTION = None
        return [], last_rowid

def fetch_new_chats(last_apple_epoch: int = 0) -> List[Dict]:
    """Fetch chats that might have new activity since the last sync."""
    query = """
    SELECT 
        chat.ROWID as chat_id, chat.guid, chat.chat_identifier, chat.display_name,
        chat.service_name, GROUP_CONCAT(DISTINCT handle.id) as participants
    FROM chat
    LEFT JOIN chat_handle_join ON chat.ROWID = chat_handle_join.chat_id
    LEFT JOIN handle ON chat_handle_join.handle_id = handle.ROWID
    JOIN chat_message_join cmj ON chat.ROWID = cmj.chat_id
    JOIN message m ON cmj.message_id = m.ROWID
    WHERE m.date > ?
    GROUP BY chat.ROWID
    """
    try:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (last_apple_epoch,))
        chats = [dict(row) for row in cursor.fetchall()]
        # print(f"DEBUG: Extracted {len(chats)} chats with new activity from {LIVE_DB}") # Commented out
        return chats
    except sqlite3.Error as e:
        print(f"ERROR: Error fetching new chats: {e}", exc_info=True)
        # If there's an error with the connection, reset it so it will be recreated on next call
        global LIVE_DB_CONNECTION
        try:
            if LIVE_DB_CONNECTION:
                LIVE_DB_CONNECTION.close()
        except:
            pass
        LIVE_DB_CONNECTION = None
        return [] 