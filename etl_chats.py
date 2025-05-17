from datetime import datetime, timedelta, timezone
import sqlite3
from typing import List, Dict, Tuple
from local_utils import _safe_timestamp, normalize_phone_number
from local_session_manager import db
from etl_contacts import find_contact_by_identifier

def etl_chats(source_db: str) -> Dict[str, int]:
    """ETL chats from source database."""
    print(f"INFO: Starting chat ETL from: {source_db}")
    
    raw_chats = extract_chats(source_db)
    transformed_data = [transform_chat(chat) for chat in raw_chats]
    stats = load_chats(transformed_data)
    
    print(f"INFO: Chat ETL complete. New: {stats['new_chats']}, Updated: {stats['updated_chats']}")
    return stats

def extract_chats(source_db: str) -> List[Dict]:
    """Extract chats and their participants from source database."""
    query = """
    SELECT 
        chat.ROWID, chat.guid, chat.chat_identifier, chat.display_name, chat.service_name,
        GROUP_CONCAT(DISTINCT handle.id) as participants,
        MIN(message.date) as created_date, MAX(message.date) as last_message_date,
        CASE WHEN COUNT(DISTINCT chat_handle_join.handle_id) > 1 THEN 1 ELSE 0 END as is_group
    FROM chat
    LEFT JOIN chat_handle_join ON chat.ROWID = chat_handle_join.chat_id
    LEFT JOIN handle ON chat_handle_join.handle_id = handle.ROWID
    LEFT JOIN chat_message_join ON chat.ROWID = chat_message_join.chat_id
    LEFT JOIN message ON chat_message_join.message_id = message.ROWID
    GROUP BY chat.ROWID
    """
    try:
        with sqlite3.connect(source_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            chats = [dict(row) for row in cursor.fetchall()]
            print(f"INFO: Extracted {len(chats)} chats from {source_db}")
            return chats
    except sqlite3.Error as e:
        print(f"ERROR: Error querying chat database {source_db}: {e}", exc_info=True)
        return []

def transform_chat(chat: Dict) -> Tuple[Dict, List[Dict]]:
    participants = transform_participants(chat.get('participants', ''))
    display_name = chat['display_name']
    if not display_name:
        participant_names = [p['name'] if p['name'] != 'Unknown' else p['identifier'] for p in participants]
        display_name = ', '.join(participant_names) if participant_names else f"Chat {chat['ROWID']}"
    display_name = display_name[:100] + '...' if len(display_name) > 100 else display_name
    
    # Use the same timestamp conversion as etl_messages.py
    created_date = _convert_apple_timestamp(chat['created_date'])
    last_message_date = _convert_apple_timestamp(chat['last_message_date'])
    
    sorted_identifiers = []
    for p in participants:
        identifier = p['identifier']
        if '@' in identifier:
            sorted_identifiers.append(identifier.lower())
        else:
            sorted_identifiers.append(normalize_phone_number(identifier))
    sorted_identifiers = sorted(set(sorted_identifiers))
    custom_identifier = ','.join(sorted_identifiers)
    transformed_chat = {
        'chat_identifier': custom_identifier,
        'chat_name': display_name,
        'created_date': created_date,
        'last_message_date': last_message_date,
        'is_group': chat['is_group'],
        'service_name': chat['service_name']
    }
    return transformed_chat, participants

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

def transform_participants(participants_str: str) -> List[Dict]:
    if not participants_str:
        return []
    participants = []
    seen_identifiers = set()
    for identifier in participants_str.split(','):
        identifier = identifier.strip()
        if identifier in seen_identifiers:
            continue
        seen_identifiers.add(identifier)
        normalized_identifier = identifier.lower() if '@' in identifier else normalize_phone_number(identifier)
        contact_info = find_contact_by_identifier(normalized_identifier)
        if contact_info:
            participants.append({
                'name': contact_info['name'],
                'identifier': identifier,
                'contact_id': contact_info['id']
            })
        else:
            participants.append({
                'name': 'Unknown',
                'identifier': identifier
            })
    return participants

def load_chats(chats_and_participants: List[Tuple[Dict, List[Dict]]]) -> Dict[str, int]:
    """Bulk load chats and participants into database."""
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        stats = {"new_chats": 0, "updated_chats": 0}
        
        # Build both display name and identifier maps
        cursor.execute("SELECT chat_identifier, id, chat_name, is_group FROM chats")
        existing_chat_map = {}
        display_name_map = {}
        for row in cursor.fetchall():
            existing_chat_map[row[0]] = row[1]
            if row[3] and row[2]:  # is_group and has name
                display_name_map[row[2]] = row[1]
        
        for chat, participants in chats_and_participants:
            try:
                chat_id = None
                if chat['is_group'] and chat['chat_name']:
                    # Try matching by display name first for groups
                    chat_id = display_name_map.get(chat['chat_name'])
                
                # Fallback to identifier matching
                if not chat_id:
                    chat_id = existing_chat_map.get(chat['chat_identifier'])
                
                if chat_id:
                    cursor.execute("""
                        UPDATE chats SET
                        chat_name = ?,
                        created_date = MIN(created_date, ?),
                        last_message_date = MAX(last_message_date, ?),
                        service_name = ?
                        WHERE id = ?
                    """, (
                        chat['chat_name'],
                        chat['created_date'],
                        chat['last_message_date'],
                        chat['service_name'],
                        chat_id
                    ))
                    stats["updated_chats"] += 1
                else:
                    cursor.execute("""
                        INSERT INTO chats
                        (chat_identifier, chat_name, created_date, last_message_date, is_group, service_name, total_messages)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    """, (
                        chat['chat_identifier'],
                        chat['chat_name'],
                        chat['created_date'],
                        chat['last_message_date'],
                        chat['is_group'],
                        chat['service_name']
                    ))
                    chat_id = cursor.lastrowid
                    existing_chat_map[chat['chat_identifier']] = chat_id
                    if chat['is_group'] and chat['chat_name']:
                        display_name_map[chat['chat_name']] = chat_id
                    stats["new_chats"] += 1
                
                for participant in participants:
                    if 'contact_id' not in participant:
                        continue
                    cursor.execute("""
                        INSERT OR IGNORE INTO chat_participants (chat_id, contact_id)
                        VALUES (?, ?)
                    """, (chat_id, participant['contact_id']))
            except Exception as e:
                print(f"Error processing chat {chat['chat_identifier']}: {e}")
                continue
        return stats