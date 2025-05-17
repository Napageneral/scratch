import sqlite3
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from local_utils import normalize_phone_number
from local_session_manager import db
from time import time

def get_live_chat_db_path() -> str:
    home = os.path.expanduser('~')
    return os.path.join(home, 'Library', 'Messages', 'chat.db')

def etl_messages(source_db: str, since_date: Optional[datetime] = None) -> Tuple[int, int]:
    def log_time(start_time, step):
        elapsed = round(time() - start_time, 2)
        print(f"  {step}: {elapsed}s")
        return time()
    
    t = time()
    print(f"INFO: Starting message ETL from: {source_db}")
    
    t = time()
    raw_messages = extract_messages(source_db, since_date)
    t = log_time(t, "Extract completed")
    
    transformed_messages = [transform_message(msg) for msg in raw_messages]
    t = log_time(t, "Transform completed")
    
    imported_count, skipped_count = load_messages(transformed_messages)
    t = log_time(t, "Load completed")
    
    if source_db == get_live_chat_db_path():
        update_user_contact_from_account_login(source_db)
    
    print(f"INFO: Message ETL complete. Imported: {imported_count}, Skipped: {skipped_count}")
    return imported_count, skipped_count

def extract_messages(source_db: str, since_date: Optional[datetime] = None) -> List[Dict]:
    query = """
    SELECT message.ROWID as message_id, message.guid, message.text, message.attributedBody,
           message.handle_id, message.service, message.date, message.is_from_me,
           message.associated_message_guid, message.associated_message_type,
           message.reply_to_guid, sender_handle.id as sender_identifier,
           GROUP_CONCAT(DISTINCT chat_handle.id) as chat_participants
    FROM message
    JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
    JOIN chat ON chat_message_join.chat_id = chat.ROWID
    LEFT JOIN handle as sender_handle ON message.handle_id = sender_handle.ROWID
    LEFT JOIN chat_handle_join ON chat.ROWID = chat_handle_join.chat_id
    LEFT JOIN handle as chat_handle ON chat_handle_join.handle_id = chat_handle.ROWID
    """
    params = []
    if since_date:
        apple_timestamp = int((since_date - datetime(2001, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1e9)
        query += " WHERE message.date > ?"
        params.append(apple_timestamp)
    query += " GROUP BY message.ROWID ORDER BY message.date DESC"
    try:
        with sqlite3.connect(source_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            messages = [dict(row) for row in cursor.fetchall()]
            print(f"INFO: Extracted {len(messages)} messages from {source_db}")
            return messages
    except sqlite3.Error as e:
        print(f"ERROR: Error querying message database {source_db}: {e}", exc_info=True)
        return []

def transform_message(message: Dict) -> Dict:
    participants = message['chat_participants'].split(',') if message['chat_participants'] else []
    chat_identifier = generate_chat_identifier(participants)
    sender_identifier = message.get('sender_identifier') or ''
    sender_identifier = normalize_phone_number(sender_identifier) if '@' not in sender_identifier else sender_identifier.lower()
    content = message.get('text')
    if not content and message.get('attributedBody'):
        content = decode_attributed_body(message.get('attributedBody'))
    content = _clean_message_content(content)
    timestamp = _convert_apple_timestamp(message['date'])
    transformed = {
        'chat_identifier': chat_identifier,
        'sender_identifier': sender_identifier,
        'content': content,
        'timestamp': timestamp,
        'is_from_me': bool(message['is_from_me']),
        'message_type': message.get('associated_message_type'),
        'service_name': message['service'],
        'guid': message['guid'],
        'associated_message_guid': _clean_guid(message.get('associated_message_guid')),
        'reply_to_guid': message.get('reply_to_guid')
    }
    return transformed

def load_messages(messages: List[Dict]) -> Tuple[int, int]:
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        print("Building lookup maps...")
        cursor.execute("SELECT COUNT(*) FROM messages")
        print(f"Messages in DB before import: {cursor.fetchone()[0]}")
        cursor.execute("""
            SELECT 'guid', guid FROM messages 
            UNION SELECT 'reaction', guid FROM reactions
            UNION SELECT 'chat', chat_identifier || '|' || id || '|' || COALESCE(chat_name,'') || '|' || is_group FROM chats
            UNION SELECT 'contact', ci.identifier || '|' || c.id 
            FROM contacts c JOIN contact_identifiers ci ON c.id = ci.contact_id
            UNION SELECT 'user', id || '' FROM contacts WHERE is_me = 1
        """)
        existing_message_guids = set()
        existing_reaction_guids = set()
        chat_map = {}
        display_name_map = {}
        contact_map = {}
        user_id = None
        for type_, value in cursor.fetchall():
            if type_ == 'guid':
                existing_message_guids.add(value)
            elif type_ == 'reaction':
                existing_reaction_guids.add(value)
            elif type_ == 'chat':
                identifier, id_, name, is_group = value.rsplit('|', 3)
                chat_map[identifier] = int(id_)
                if int(is_group) and name:
                    display_name_map[name] = int(id_)
            elif type_ == 'contact':
                identifier, id_ = value.rsplit('|', 1)
                contact_map[normalize_phone_number(identifier)] = int(id_)
            elif type_ == 'user':
                user_id = int(value)
        print(f"Found {len(existing_message_guids)} existing messages")
        print(f"Found {len(chat_map)} chats")
        print(f"Found {len(contact_map)} contacts")
        messages_to_insert = []
        messages_to_update = []
        reactions_to_insert = []
        reactions_to_update = []
        chat_counts = defaultdict(int)
        imported_count = skipped_count = 0
        missing_chat_identifiers = set()
        CHUNK_SIZE = 1000000
        for i in range(0, len(messages), CHUNK_SIZE):
            chunk = messages[i:i + CHUNK_SIZE]
            for msg in chunk:
                chat_id = None
                if msg.get('is_group') and msg.get('display_name'):
                    chat_id = display_name_map.get(msg['display_name'])
                if not chat_id:
                    chat_id = chat_map.get(msg['chat_identifier'])
                if not chat_id:
                    missing_chat_identifiers.add(msg['chat_identifier'])
                    skipped_count += 1
                    continue
                if msg['is_from_me']:
                    sender_id = user_id
                else:
                    sender_id = contact_map.get(msg['sender_identifier'])
                    if not sender_id:
                        sender_id = create_contact_for_unknown_sender(cursor, msg['sender_identifier'])
                        contact_map[msg['sender_identifier']] = sender_id
                if msg['message_type'] != 0:
                    reaction_data = (
                        msg['associated_message_guid'],
                        msg.get('message_type'),
                        sender_id,
                        msg['timestamp'],
                        chat_id,
                        msg['guid']
                    )
                    if msg['guid'] in existing_reaction_guids:
                        reactions_to_update.append(reaction_data)
                    else:
                        reactions_to_insert.append(reaction_data)
                else:
                    message_data = (
                        chat_id,
                        sender_id,
                        msg['content'],
                        msg['timestamp'],
                        msg['is_from_me'],
                        msg['message_type'],
                        msg['service_name'],
                        msg['guid']
                    )
                    
                    if msg['guid'] in existing_message_guids:
                        messages_to_update.append(message_data)
                    else:
                        messages_to_insert.append(message_data)
                        chat_counts[chat_id] += 1
                        imported_count += 1
            if messages_to_insert:
                cursor.executemany("""
                    INSERT INTO messages (chat_id, sender_id, content, timestamp, 
                                       is_from_me, message_type, service_name, guid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, messages_to_insert)
                messages_to_insert = []
            if messages_to_update:
                cursor.executemany("""
                    UPDATE messages 
                    SET chat_id = ?, sender_id = ?, content = ?, timestamp = ?,
                        is_from_me = ?, message_type = ?, service_name = ?
                    WHERE guid = ?
                """, messages_to_update)
                messages_to_update = []
            if reactions_to_insert:
                cursor.executemany("""
                    INSERT INTO reactions (original_message_guid, reaction_type, 
                                        sender_id, timestamp, chat_id, guid)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, reactions_to_insert)
                reactions_to_insert = []
            if reactions_to_update:
                cursor.executemany("""
                    UPDATE reactions 
                    SET original_message_guid = ?, reaction_type = ?, 
                        sender_id = ?, timestamp = ?, chat_id = ?
                    WHERE guid = ?
                """, reactions_to_update)
                reactions_to_update = []
        if chat_counts:
            cursor.executemany(
                "UPDATE chats SET total_messages = total_messages + ? WHERE id = ?",
                [(count, chat_id) for chat_id, count in chat_counts.items()]
            )
        
        return imported_count, skipped_count

def create_contact_for_unknown_sender(db_cursor, identifier: str) -> int:
    """
    Creates a contact for an unknown sender using the provided db_cursor.
    Returns the new contact's ID.
    Note: This function now relies on the caller (sync_messages) to update any caches.
    """
    identifier_type = 'Email' if '@' in identifier else 'Phone'
    
    try:
        db_cursor.execute("INSERT INTO contacts (name) VALUES (?)", (identifier,))
        # Use db_cursor.lastrowid for SQLite to get the ID of the inserted row
        contact_id = db_cursor.lastrowid 
        
        db_cursor.execute("""
            INSERT INTO contact_identifiers (contact_id, identifier, type, is_primary)
            VALUES (?, ?, ?, ?)
        """, (contact_id, identifier, identifier_type, True))
        
        return contact_id
    except Exception as e:
        print(f"ERROR: Failed to create contact for {identifier}: {e}")
        # Decide on error handling: re-raise, return None, etc.
        # For now, re-raising to make the issue visible during sync
        raise

def get_user_contact_id(session) -> int:
    cursor = session.connection().connection.cursor()
    result = cursor.execute("SELECT id FROM contacts WHERE is_me = 1").fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO contacts (name, is_me) VALUES (?, ?)", ("Me", True))
    user_id = cursor.execute("SELECT last_insert_rowid()").fetchone()[0]
    return user_id

def generate_chat_identifier(participants: List[str]) -> str:
    normalized = []
    for p in participants:
        p = p.strip()
        if '@' in p:
            normalized.append(p.lower())
        else:
            normalized.append(normalize_phone_number(p))
    normalized = sorted(set(normalized))
    return ','.join(normalized)

def decode_attributed_body(attributed_body) -> str:
    if not attributed_body:
        return ""
    try:
        # First decode with surrogateescape to handle any invalid bytes
        attributed_body = attributed_body.decode('utf-8', errors='surrogateescape')
        
        if "NSNumber" in attributed_body:
            attributed_body = attributed_body.split("NSNumber")[0]
            if "NSString" in attributed_body:
                attributed_body = attributed_body.split("NSString")[1]
                if "NSDictionary" in attributed_body:
                    attributed_body = attributed_body.split("NSDictionary")[0]
                    attributed_body = attributed_body[6:-12]
                    return attributed_body.strip()
        return ""
    except Exception as e:
        print(f"Error decoding attributedBody: {e}")
        return ""

def _convert_apple_timestamp(apple_timestamp):
    """
    Accept Apple-epoch timestamps in:
      • seconds      (<= 1e10)
      • microseconds (<= 1e16)
      • nanoseconds  (>  1e16)
    and return a UTC-aware datetime.
    """
    try:
        if apple_timestamp is None:
            return None

        ts = int(apple_timestamp) # Ensure it's an integer for comparison
        # Note: The plan's comments say 1e10 for seconds, 1e16 for microseconds.
        # Max seconds for 10 digits (1_000_000_0000 - 1) is 9_999_999_999 which is ~year 2286.
        # Max microseconds for 16 digits (10_000_000_000_000_000 - 1) is well beyond any practical date.
        # The logic below uses number of digits which is a robust way for integer inputs.

        if ts <= 1_000_000_0000:          # ≤ 10 digits → seconds (e.g. 728000000 represents a date in 2023)
            ts_sec = float(ts)
        elif ts <= 10_000_000_000_000_000:  # ≤ 16 digits → microseconds (e.g. 728000000000000)
            ts_sec = float(ts) / 1_000_000.0
        else:                             # > 16 digits (typically 17-19 for ns) → nanoseconds
            ts_sec = float(ts) / 1_000_000_000.0
        
        return datetime(2001, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_sec)
    except (ValueError, TypeError, OverflowError) as e: # Added OverflowError for large numbers before float conversion
        # print(f"ERROR: Error converting timestamp: {apple_timestamp}, Error: {e}") # Optional: for debugging
        return None # Or handle error as appropriate

# Pre-compile translation table for problematic characters
CHARS_TO_REMOVE_UNICODE = '\uFFFC\x01\ufffd'
TRANSLATION_TABLE_CLEAN = str.maketrans('', '', CHARS_TO_REMOVE_UNICODE)

def _clean_message_content(content: str) -> str:
    """
    Clean message content by removing problematic characters and ensuring printable characters.
    Uses str.translate for efficient character removal.
    """
    if not content:
        return ""
    try:
        # First remove known problematic characters using translation table
        cleaned = content.translate(TRANSLATION_TABLE_CLEAN)
        
        # Then handle printable characters
        # We still need this step for characters not in our translation table
        filtered_chars = []
        for char in cleaned:
            if char.isprintable() or char in [' ', '\n', '\t']:
                filtered_chars.append(char)
        
        return ''.join(filtered_chars).strip()
    except Exception as e:
        print(f"Error cleaning message content: {e}")
        # If all else fails, try basic ASCII conversion
        try:
            return content.encode('ascii', 'ignore').decode('ascii').strip()
        except:
            return ""

def _clean_guid(guid):
    if guid:
        if '/' in guid:
            return guid.split('/', 1)[-1]
        if ':' in guid:
            return guid.split(':', 1)[-1]
    return guid

def update_user_contact_from_account_login(source_db: str):
    """
    After we ETL messages, we can run this to update the user's 
    phone/email from the chat.account_login that was used most recently.
    """
    # 1) Query phone and email from source chat.db
    with sqlite3.connect(source_db) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Query phone
        cursor.execute("""
            SELECT account_login
            FROM chat
            WHERE account_login LIKE 'P:+%'
              AND account_login != 'P:+'
        """)
        phone_row = cursor.fetchone()
        phone_number = None
        if phone_row and phone_row['account_login']:
            phone_number = phone_row['account_login'].replace("P:+", "").strip()

        # Query email
        cursor.execute("""
            SELECT account_login
            FROM chat
            WHERE account_login LIKE 'E:%'
              AND account_login != 'E:'
        """)
        email_row = cursor.fetchone()
        email_address = None
        if email_row and email_row['account_login']:
            email_address = email_row['account_login'].replace("E:", "").strip().lower()

    # 2) Update our DB with the found identifiers
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        
        # Check if user contact exists
        cursor.execute("SELECT id FROM contacts WHERE is_me = 1")
        result = cursor.fetchone()
        
        if result:
            user_contact_id = result[0]
        else:
            # Create user contact if it doesn't exist
            cursor.execute(
                "INSERT INTO contacts (name, is_me) VALUES (?, ?)",
                ("Me", True)
            )
            user_contact_id = cursor.lastrowid

        # Update phone if found
        if phone_number:
            cursor.execute(
                "DELETE FROM contact_identifiers WHERE contact_id = ? AND type = 'Phone'",
                (user_contact_id,)
            )
            cursor.execute("""
                INSERT INTO contact_identifiers 
                (contact_id, identifier, type, is_primary, last_used)
                VALUES (?, ?, 'Phone', 1, CURRENT_TIMESTAMP)
            """, (user_contact_id, phone_number))

        # Update email if found
        if email_address:
            cursor.execute(
                "DELETE FROM contact_identifiers WHERE contact_id = ? AND type = 'Email'",
                (user_contact_id,)
            )
            cursor.execute("""
                INSERT INTO contact_identifiers 
                (contact_id, identifier, type, is_primary, last_used)
                VALUES (?, ?, 'Email', 1, CURRENT_TIMESTAMP)
            """, (user_contact_id, email_address))

        print("INFO: ✅ User contact updated from chat.account_login")
