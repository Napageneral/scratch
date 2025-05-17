from local_session_manager import db
from datetime import datetime, timezone, timedelta
from live_sync.extractors import get_chat_db_connection

def get_watermark() -> int:
    """Get the current watermark timestamp in Apple Epoch nanoseconds."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute("SELECT value FROM live_sync_state WHERE key = 'apple_epoch_ns'")
        result = cursor.fetchone()
        return int(result[0]) if result else None

def set_watermark(value: int):
    """Set the current watermark timestamp."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO live_sync_state (key, value) VALUES ('apple_epoch_ns', ?)",
            (str(value),)
        )
        # print(f"DEBUG: Watermark updated to {value}") # Commented out

def get_message_rowid_watermark() -> int:
    """Get the last processed message ROWID."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute("SELECT value FROM live_sync_state WHERE key = 'last_message_rowid'")
        result = cursor.fetchone()
        return int(result[0]) if result and result[0] else 0

def set_message_rowid_watermark(value: int):
    """Set the last processed message ROWID."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO live_sync_state (key, value) VALUES ('last_message_rowid', ?)",
            (str(value),)
        )

def get_attachment_rowid_watermark() -> int:
    """Get the last processed attachment ROWID."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute("SELECT value FROM live_sync_state WHERE key = 'last_attachment_rowid'")
        result = cursor.fetchone()
        return int(result[0]) if result and result[0] else 0

def set_attachment_rowid_watermark(value: int):
    """Set the last processed attachment ROWID."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO live_sync_state (key, value) VALUES ('last_attachment_rowid', ?)",
            (str(value),)
        )

def initialize_watermark_if_missing():
    """Initialize watermark from max message timestamp if not present, storing in nanoseconds."""
    watermark = get_watermark() # This should return nanoseconds if already set
    if watermark is None:
        # print("INFO: No watermark found, initializing from existing messages (in nanoseconds)") # Commented out
        with db.session_scope() as session:
            cursor = session.connection().connection.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM messages") # App's DB timestamp
            max_ts_row = cursor.fetchone()
            max_ts_datetime = None
            if max_ts_row and max_ts_row[0]:
                if isinstance(max_ts_row[0], str):
                    try:
                        max_ts_datetime = datetime.fromisoformat(max_ts_row[0])
                    except ValueError: # Handle cases where it might not be a full ISO string
                        # Attempt to parse common formats, adjust as necessary for your specific DB format
                        possible_formats = [
                            '%Y-%m-%d %H:%M:%S.%f%z',
                            '%Y-%m-%d %H:%M:%S%z',
                            '%Y-%m-%d %H:%M:%S.%f',
                            '%Y-%m-%d %H:%M:%S'
                        ]
                        for fmt in possible_formats:
                            try:
                                max_ts_datetime = datetime.strptime(max_ts_row[0], fmt)
                                # If strptime succeeds but tzinfo is missing, assume UTC for safety
                                if max_ts_datetime.tzinfo is None and '%z' not in fmt:
                                    max_ts_datetime = max_ts_datetime.replace(tzinfo=timezone.utc)
                                break # Successfully parsed
                            except ValueError:
                                continue
                        if max_ts_datetime is None:
                            print(f"ERROR: Could not parse timestamp string: {max_ts_row[0]}")
                elif isinstance(max_ts_row[0], datetime):
                    max_ts_datetime = max_ts_row[0]
            
            if max_ts_datetime:
                # Ensure datetime is UTC before conversion
                if max_ts_datetime.tzinfo is None:
                    max_ts_datetime = max_ts_datetime.replace(tzinfo=timezone.utc)
                else:
                    max_ts_datetime = max_ts_datetime.astimezone(timezone.utc)
                
                watermark_seconds = (max_ts_datetime - datetime(2001, 1, 1, tzinfo=timezone.utc)).total_seconds()
                watermark = int(watermark_seconds * 1_000_000_000) # Convert to nanoseconds
                # print(f"INFO: Initializing watermark to {watermark} ns from max message timestamp ({max_ts_datetime})") # Commented out
            else:
                # If no messages yet, start from current time minus 1 day
                one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
                watermark_seconds = (one_day_ago - datetime(2001, 1, 1, tzinfo=timezone.utc)).total_seconds()
                watermark = int(watermark_seconds * 1_000_000_000) # Convert to nanoseconds
                # print(f"INFO: No messages found, initializing watermark to {watermark} ns (1 day ago)") # Commented out
            set_watermark(watermark) # set_watermark now stores nanoseconds
    return watermark

def initialize_rowid_watermarks_if_missing():
    """Initialize ROWID watermarks from max ROWIDs in chat.db."""
    message_rowid = get_message_rowid_watermark()
    attachment_rowid = get_attachment_rowid_watermark()
    
    # Only initialize if both are 0 (not set yet)
    if message_rowid == 0 and attachment_rowid == 0:
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        
        # Get max message ROWID
        cursor.execute("SELECT MAX(ROWID) FROM message")
        max_message_rowid = cursor.fetchone()[0]
        if max_message_rowid:
            set_message_rowid_watermark(max_message_rowid)
            print(f"INFO: Initialized message ROWID watermark to {max_message_rowid}")
        else:
            set_message_rowid_watermark(0)
            print("INFO: No messages found in chat.db, initialized message ROWID watermark to 0")
        
        # Get max attachment ROWID
        cursor.execute("SELECT MAX(ROWID) FROM attachment")
        max_attachment_rowid = cursor.fetchone()[0]
        if max_attachment_rowid:
            set_attachment_rowid_watermark(max_attachment_rowid)
            print(f"INFO: Initialized attachment ROWID watermark to {max_attachment_rowid}")
        else:
            set_attachment_rowid_watermark(0)
            print("INFO: No attachments found in chat.db, initialized attachment ROWID watermark to 0")
    
    return get_message_rowid_watermark(), get_attachment_rowid_watermark() 