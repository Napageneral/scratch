# local_session_manager.py
import sqlite3
from contextlib import contextmanager

DB_NAME = "live_sync_test.db"  # Application's database

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS live_sync_state (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        is_me BOOLEAN DEFAULT 0,
        data_source TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS contact_identifiers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_id INTEGER,
        identifier TEXT NOT NULL,
        type TEXT, -- 'Phone', 'Email'
        is_primary BOOLEAN DEFAULT 0,
        last_used TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id),
        UNIQUE (contact_id, identifier, type)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_identifier TEXT UNIQUE, -- e.g., comma-separated normalized phone numbers/emails
        chat_name TEXT,
        created_date TIMESTAMP,
        last_message_date TIMESTAMP,
        is_group BOOLEAN,
        service_name TEXT,
        total_messages INTEGER DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        initiator_id INTEGER, -- contact_id
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        message_count INTEGER,
        gap_threshold INTEGER, -- The gap used to split this conversation
        FOREIGN KEY (chat_id) REFERENCES chats(id),
        FOREIGN KEY (initiator_id) REFERENCES contacts(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        sender_id INTEGER, -- contact_id
        conversation_id INTEGER, -- For grouping messages into conversations
        content TEXT,
        timestamp TIMESTAMP,
        is_from_me BOOLEAN,
        message_type INTEGER, -- 0 for normal, others for reactions/effects
        service_name TEXT, -- 'iMessage', 'SMS'
        guid TEXT UNIQUE, -- From source DB, for deduplication
        associated_message_guid TEXT, -- For reactions, points to original message guid
        reply_to_guid TEXT, -- For threaded replies
        FOREIGN KEY (chat_id) REFERENCES chats(id),
        FOREIGN KEY (sender_id) REFERENCES contacts(id),
        FOREIGN KEY (conversation_id) REFERENCES conversations(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guid TEXT UNIQUE, -- From source DB, for deduplication
        original_message_guid TEXT,
        reaction_type INTEGER,
        sender_id INTEGER, -- contact_id
        timestamp TIMESTAMP,
        chat_id INTEGER,
        FOREIGN KEY (sender_id) REFERENCES contacts(id),
        FOREIGN KEY (chat_id) REFERENCES chats(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER,
        guid TEXT UNIQUE, -- From source DB, for deduplication
        created_date TIMESTAMP,
        file_name TEXT,
        uti TEXT, -- Uniform Type Identifier
        mime_type TEXT,
        size INTEGER, -- in bytes
        is_sticker BOOLEAN,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chat_participants (
        chat_id INTEGER,
        contact_id INTEGER,
        PRIMARY KEY (chat_id, contact_id),
        FOREIGN KEY (chat_id) REFERENCES chats(id),
        FOREIGN KEY (contact_id) REFERENCES contacts(id)
    );
    """
]

class _SQLiteConnectionWrapper:
    def __init__(self, sqlite_conn):
        self.connection = sqlite_conn  # This is the sqlite3.Connection object

class _CursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor
        self._description = cursor.description

    def __iter__(self):
        return self._cursor.__iter__()

    def __next__(self):
        return self._cursor.__next__()

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def scalar(self):
        row = self._cursor.fetchone()
        return row[0] if row else None

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    def mappings(self):
        column_names = [desc[0] for desc in self._description] if self._description else []
        
        class MappingsIterable:
            def __init__(self, cursor, cols):
                self.cursor = cursor # This is the raw sqlite3 cursor
                self.column_names = cols
                # It's better to get a fresh iterator from the raw cursor for each .mappings() call
                # or ensure the raw cursor itself is iterable as expected by this structure.
                # The raw sqlite3 cursor is already iterable.

            def __iter__(self):
                # Each call to iter() on MappingsIterable should ideally allow fresh iteration 
                # over the original cursor's results if possible, or iterate once.
                # Standard sqlite3 cursors are single-pass iterators.
                return self 

            def __next__(self):
                row = self.cursor.fetchone() # Use fetchone from the raw sqlite3 cursor
                if row is None:
                    raise StopIteration
                if not self.column_names:
                    return row 
                return dict(zip(self.column_names, row))
            
            def all(self):
                # This will consume the remainder of the raw cursor from its current position
                return list(self)

        return MappingsIterable(self._cursor, column_names)


class _Session:
    def __init__(self, sqlite_conn):
        self._sqlite_conn = sqlite_conn

    def connection(self): 
        return _SQLiteConnectionWrapper(self._sqlite_conn)

    def execute(self, statement, params=None):
        sql_query = str(statement) 
        cursor = self._sqlite_conn.execute(sql_query, params or {})
        return _CursorWrapper(cursor)

    def commit(self):
        self._sqlite_conn.commit()

@contextmanager
def _session_scope_manager():
    conn = None
    try:
        # Potentially delete DB file for a clean run during testing
        # import os
        # if os.path.exists(DB_NAME):
        #     os.remove(DB_NAME)
        conn = sqlite3.connect(DB_NAME)
        
        # Apply PRAGMAs for performance
        # This is the key change for optimizing commit speed
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA mmap_size=268435456;") # 256 MB
        
        conn.execute("PRAGMA foreign_keys = ON;") # Ensure foreign key constraints are active
        
        cursor = conn.cursor()
        for stmt in SCHEMA:
            cursor.execute(stmt)
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB schema/connection error: {e}")
        if conn:
            conn.rollback()
        raise
    
    try:
        yield _Session(conn)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error during session scope: {e}") 
        raise
    finally:
        if conn:
            conn.close()

class DBGlobal:
    def session_scope(self):
        return _session_scope_manager()

db = DBGlobal() 