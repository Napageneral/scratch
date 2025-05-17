# /Users/tyler/Desktop/projects/scratch/live_sync/cache.py

from local_utils import normalize_phone_number # Assuming this is in your project structure
from .extractors import get_chat_db_connection

# Module-level cache dictionaries
CONTACT_MAP_CACHE = {}
_contacts_cache_populated = False

MESSAGE_GUID_TO_ID_CACHE = {}
_message_guid_cache_populated = False

CHAT_MAP_CACHE = {}
_chat_map_cache_populated = False

REACTION_GUID_CACHE = set() # Store as a set for efficient lookups
_reaction_guid_cache_populated = False

ATTACHMENT_GUID_CACHE = set() # Store attachment GUIDs as a set
_attachment_guid_cache_populated = False

# Maps chat.db chat ROWID to a comma-separated string of participants
CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE = {}
_chat_participants_cache_populated = False

def get_contact_map(cursor, force_refresh=False):
    """
    Retrieves a cached map of contact identifiers to contact IDs.
    Populates the cache from the database if it's empty or force_refresh is True.
    The cursor provided should be a raw sqlite3 cursor.
    """
    global _contacts_cache_populated
    if not CONTACT_MAP_CACHE or not _contacts_cache_populated or force_refresh: # Check CONTACT_MAP_CACHE for emptiness too
        CONTACT_MAP_CACHE.clear()
        # print("DEBUG: Refreshing contact_map_cache from DB")
        cursor.execute("""
            SELECT ci.identifier, c.id 
            FROM contacts c 
            JOIN contact_identifiers ci ON c.id = ci.contact_id
        """)
        for identifier, contact_id_val in cursor.fetchall():
            if '@' in identifier: # Email
                CONTACT_MAP_CACHE[identifier.lower()] = contact_id_val
            else: # Phone
                CONTACT_MAP_CACHE[normalize_phone_number(identifier)] = contact_id_val
        _contacts_cache_populated = True
    return CONTACT_MAP_CACHE

def get_message_guid_to_id_map(cursor, force_refresh=False):
    """
    Retrieves a cached map of message GU событием to message IDs.
    Populates the cache from the database if it's empty or force_refresh is True.
    The cursor provided should be a raw sqlite3 cursor.
    """
    global _message_guid_cache_populated
    if not MESSAGE_GUID_TO_ID_CACHE or not _message_guid_cache_populated or force_refresh: # Check MESSAGE_GUID_TO_ID_CACHE for emptiness
        MESSAGE_GUID_TO_ID_CACHE.clear()
        # print("DEBUG: Refreshing message_guid_to_id_cache from DB")
        cursor.execute("SELECT guid, id FROM messages")
        for guid, db_id in cursor.fetchall():
            MESSAGE_GUID_TO_ID_CACHE[guid] = db_id
        _message_guid_cache_populated = True
    return MESSAGE_GUID_TO_ID_CACHE

def get_chat_map(cursor, force_refresh=False):
    """
    Retrieves a cached map of chat_identifier to chat_id.
    Populates the cache from the database if it's empty or force_refresh is True.
    The cursor provided should be a raw sqlite3 cursor.
    """
    global _chat_map_cache_populated
    if not CHAT_MAP_CACHE or not _chat_map_cache_populated or force_refresh:
        CHAT_MAP_CACHE.clear()
        # print("DEBUG: Refreshing chat_map_cache from DB")
        cursor.execute("SELECT chat_identifier, id FROM chats")
        for chat_identifier, chat_id_val in cursor.fetchall():
            CHAT_MAP_CACHE[chat_identifier] = chat_id_val
        _chat_map_cache_populated = True
    return CHAT_MAP_CACHE

def get_reaction_guids(cursor, force_refresh=False):
    """
    Retrieves a cached set of reaction GUIDs.
    Populates the cache from the database if it's empty or force_refresh is True.
    The cursor provided should be a raw sqlite3 cursor.
    """
    global _reaction_guid_cache_populated
    if not REACTION_GUID_CACHE or not _reaction_guid_cache_populated or force_refresh:
        REACTION_GUID_CACHE.clear()
        # print("DEBUG: Refreshing reaction_guid_cache from DB")
        cursor.execute("SELECT guid FROM reactions")
        for row in cursor.fetchall():
            REACTION_GUID_CACHE.add(row[0])
        _reaction_guid_cache_populated = True
    return REACTION_GUID_CACHE

def get_attachment_guids(cursor, force_refresh=False):
    """
    Retrieves a cached set of attachment GUIDs.
    Populates the cache from the database if it's empty or force_refresh is True.
    The cursor provided should be a raw sqlite3 cursor.
    """
    global _attachment_guid_cache_populated
    if not ATTACHMENT_GUID_CACHE or not _attachment_guid_cache_populated or force_refresh:
        ATTACHMENT_GUID_CACHE.clear()
        # print("DEBUG: Refreshing attachment_guid_cache from DB")
        cursor.execute("SELECT guid FROM attachments")
        for row in cursor.fetchall():
            ATTACHMENT_GUID_CACHE.add(row[0])
        _attachment_guid_cache_populated = True
    return ATTACHMENT_GUID_CACHE

def get_chat_participants_map(force_refresh=False):
    """
    Retrieves a cached map of chat.db ROWID to comma-separated participants.
    Populates the cache from chat.db if it's empty or force_refresh is True.
    """
    global _chat_participants_cache_populated
    if not CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE or not _chat_participants_cache_populated or force_refresh:
        CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE.clear()
        
        conn = get_chat_db_connection()
        cursor = conn.cursor()
        
        # Query to get all chats with their participants
        query = """
        SELECT 
            chat.ROWID as chat_id,
            GROUP_CONCAT(DISTINCT handle.id) as participants
        FROM chat
        LEFT JOIN chat_handle_join ON chat.ROWID = chat_handle_join.chat_id
        LEFT JOIN handle ON chat_handle_join.handle_id = handle.ROWID
        GROUP BY chat.ROWID
        """
        cursor.execute(query)
        
        for row in cursor.fetchall():
            chat_id = row['chat_id']
            participants = row['participants'] if row['participants'] else ""
            CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE[chat_id] = participants
        
        _chat_participants_cache_populated = True
    
    return CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE

def reset_all_caches():
    """Resets all caches, forcing them to repopulate on next get."""
    global _contacts_cache_populated, _message_guid_cache_populated, _chat_map_cache_populated, _reaction_guid_cache_populated, _attachment_guid_cache_populated, _chat_participants_cache_populated
    # print("DEBUG: Resetting all live_sync caches")
    CONTACT_MAP_CACHE.clear()
    _contacts_cache_populated = False
    MESSAGE_GUID_TO_ID_CACHE.clear()
    _message_guid_cache_populated = False
    CHAT_MAP_CACHE.clear()
    _chat_map_cache_populated = False
    REACTION_GUID_CACHE.clear()
    _reaction_guid_cache_populated = False
    ATTACHMENT_GUID_CACHE.clear()
    _attachment_guid_cache_populated = False
    CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE.clear()
    _chat_participants_cache_populated = False

# Add specific reset functions if needed, e.g.:
# def reset_contact_map_cache():
#     global _contacts_cache_populated
#     CONTACT_MAP_CACHE.clear()
#     _contacts_cache_populated = False

# def reset_message_guid_map_cache():
#     global _message_guid_cache_populated
#     MESSAGE_GUID_TO_ID_CACHE.clear()
#     _message_guid_cache_populated = False 