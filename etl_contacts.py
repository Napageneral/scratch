import sqlite3
import os
import glob
from typing import List, Dict, Optional
from local_utils import clean_contact_name, normalize_phone_number
from local_session_manager import db

def etl_contacts(source_db: str, is_live: bool = False) -> int:
    """ETL contacts from a single source database."""
    source_type = 'live_addressbook' if is_live else 'backup_addressbook'
    print(f"INFO: Starting ETL from {source_type}: {source_db}")
    
    ensure_user_contact()
    raw_contacts = extract_contacts(source_db, is_live)
    transformed_contacts = [transform_contact(c, source_type) for c in raw_contacts]
    imported_count = load_contacts(transformed_contacts)
            
    print(f"INFO: Imported {imported_count} new contacts from {source_db}")
    return imported_count

def etl_live_contacts():
    """ETL contacts from all live AddressBook databases."""
    total_imported = 0
    for db_path in find_live_address_books():
        total_imported += etl_contacts(db_path, is_live=True)
    return total_imported

def get_address_book_path() -> str:
    home = os.path.expanduser('~')
    return os.path.join(home, 'Library', 'Application Support', 'AddressBook')

def find_live_address_books() -> List[str]:
    """Find all AddressBook databases in the live filesystem, including in Sources subdirectories."""
    db_files = []
    address_book_path = get_address_book_path()
    print(f"INFO: Searching for AddressBook databases in: {address_book_path}")

    # Step 1: Use glob to find all AddressBook-v22.abcddb files recursively
    pattern = os.path.join(address_book_path, '**', 'AddressBook-v22.abcddb')
    try:
        glob_results = glob.glob(pattern, recursive=True)
        for db_path in glob_results:
            db_files.append(db_path)
            print(f"INFO: Found AddressBook database (via glob): {db_path}")
    except Exception as e:
        print(f"ERROR: Error using glob to find AddressBook databases: {e}", exc_info=True)

    # Step 2: Explicitly check the Sources directory as a fallback
    sources_path = os.path.join(address_book_path, 'Sources')
    if os.path.exists(sources_path):
        print(f"INFO: Sources directory found: {sources_path}")
        try:
            for root, dirs, files in os.walk(sources_path):
                for file in files:
                    if file == 'AddressBook-v22.abcddb':
                        db_path = os.path.join(root, file)
                        if db_path not in db_files:  # Avoid duplicates
                            db_files.append(db_path)
                            print(f"INFO: Found additional AddressBook database in Sources: {db_path}")
        except Exception as e:
            print(f"ERROR: Error walking Sources directory: {e}", exc_info=True)
    else:
        print(f"WARNING: Sources directory not found: {sources_path}")

    # Step 3: Check the root AddressBook directory explicitly
    root_db_path = os.path.join(address_book_path, 'AddressBook-v22.abcddb')
    if os.path.exists(root_db_path) and root_db_path not in db_files:
        db_files.append(root_db_path)
        print(f"INFO: Found AddressBook database in root: {root_db_path}")

    # Step 4: Check for direct Sources database
    sources_db_path = os.path.join(sources_path, 'AddressBook-v22.abcddb')
    if os.path.exists(sources_db_path) and sources_db_path not in db_files:
        db_files.append(sources_db_path)
        print(f"INFO: Found AddressBook database directly in Sources: {sources_db_path}")

    if not db_files:
        print("WARNING: No AddressBook databases found. This may indicate permission issues or incorrect paths.")
    else:
        print(f"INFO: Found {len(db_files)} live AddressBook database(s)")
    
    return db_files

def extract_contacts(source_db: str, is_live: bool) -> List[Dict]:
    """Extract contacts from source database."""
    with sqlite3.connect(source_db) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            
            if is_live:
                required_tables = {'ZABCDRECORD', 'ZABCDPHONENUMBER', 'ZABCDMESSAGINGADDRESS'}
                query = """
                SELECT ZABCDRECORD.Z_PK as id, ZABCDRECORD.ZFIRSTNAME as first_name, ZABCDRECORD.ZLASTNAME as last_name, 
                    ZABCDPHONENUMBER.ZFULLNUMBER as identifier
                FROM ZABCDRECORD
                LEFT JOIN ZABCDPHONENUMBER ON ZABCDPHONENUMBER.ZOWNER = ZABCDRECORD.Z_PK
                WHERE ZABCDPHONENUMBER.ZFULLNUMBER IS NOT NULL
                UNION
                SELECT ZABCDRECORD.Z_PK as id, ZABCDRECORD.ZFIRSTNAME as first_name, ZABCDRECORD.ZLASTNAME as last_name, 
                    ZABCDMESSAGINGADDRESS.ZADDRESS as identifier
                FROM ZABCDRECORD
                LEFT JOIN ZABCDMESSAGINGADDRESS ON ZABCDMESSAGINGADDRESS.ZOWNER = ZABCDRECORD.Z_PK
                WHERE ZABCDMESSAGINGADDRESS.ZADDRESS IS NOT NULL
                """
            else:
                required_tables = {'ABPerson', 'ABMultiValue'}
                query = """
                SELECT ABPerson.ROWID as id, ABPerson.First as first_name, ABPerson.Last as last_name, 
                    ABMultiValue.value as identifier
                FROM ABPerson
                LEFT JOIN ABMultiValue ON ABMultiValue.record_id = ABPerson.ROWID
                WHERE ABMultiValue.value IS NOT NULL
                """
            
            missing_tables = required_tables - tables
            if missing_tables:
                print(f"WARNING: Missing required tables in {source_db}: {missing_tables}")
                return []
            
            cursor.execute(query)
            contacts = [dict(row) for row in cursor.fetchall()]
            print(f"INFO: Extracted {len(contacts)} contacts from {source_db}")
            return contacts
            
        except sqlite3.Error as e:
            print(f"ERROR: Error querying AddressBook database {source_db}: {e}", exc_info=True)
            return []

def transform_contact(contact: Dict, source: str) -> Dict:
    """Transform a single contact into the destination format."""
    name = clean_contact_name(f"{contact['first_name']} {contact['last_name']}".strip())
    identifier = contact['identifier']
    
    # Skip system/carrier contacts
    if (name.startswith('#') or 
        identifier.startswith('#') or 
        'VZ' in name or  # Skip Verizon contacts
        'Roadside' in name or 
        'Assistance' in name or
        name.startswith('*') or  # Skip other system contacts
        identifier.startswith('*')):
        return None
        
    identifier_type = 'Phone' if '@' not in identifier else 'Email'
    identifier = normalize_phone_number(identifier) if identifier_type == 'Phone' else identifier.lower()
    
    return {
        'name': name or identifier,
        'identifier': identifier,
        'identifier_type': identifier_type,
        'source': source
    }

def ensure_user_contact():
    """Ensure the user's own contact exists in the database."""
    with db.session_scope() as session:
        user_contact_cursor = session.execute(
            "SELECT id FROM contacts WHERE is_me = 1"
        )
        user_contact = user_contact_cursor.fetchone()
        if user_contact:
            return user_contact[0]
        
        # Create new user contact
        user_name = "Me"  # Could be made configurable
        session.execute(
            "INSERT INTO contacts (name, is_me) VALUES (:name, :is_me)",
            {"name": user_name, "is_me": True}
        )
        user_contact_id_cursor = session.execute("SELECT last_insert_rowid()")
        user_contact_id = user_contact_id_cursor.scalar()
        
        # Add user identifier if available
        user_identifier = None  # Could be made configurable
        if user_identifier:
            identifier_type = 'Phone' if '@' not in user_identifier else 'Email'
            session.execute(
                """                INSERT INTO contact_identifiers (contact_id, identifier, type, is_primary)
                VALUES (:contact_id, :identifier, :type, :is_primary)
                """,
                {
                    "contact_id": user_contact_id,
                    "identifier": user_identifier,
                    "type": identifier_type,
                    "is_primary": True
                }
            )
        
        return user_contact_id

def load_contacts(contacts: List[Dict]) -> int:
    contacts = [c for c in contacts if c is not None]
    print(f"\nAttempting to load {len(contacts)} contacts")
    
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        
        # Get existing contacts
        cursor.execute("""
        SELECT c.name, ci.identifier, ci.type 
        FROM contacts c
        LEFT JOIN contact_identifiers ci ON c.id = ci.contact_id
        """)
        existing_set = {(r[0], r[1], r[2]) for r in cursor.fetchall() if r[1]}
        print(f"Found {len(existing_set)} existing contacts")
        
        # Filter out existing contacts
        new_contacts = []
        for contact in contacts:
            key = (contact['name'], contact['identifier'], contact['identifier_type'])
            if key not in existing_set:
                new_contacts.append(contact)
        
        if not new_contacts:
            print("No new contacts to insert")
            return 0
            
        print(f"Inserting {len(new_contacts)} new contacts")
        
        # Insert contacts one at a time to ensure proper ID mapping
        for contact in new_contacts:
            try:
                cursor.execute(
                    "INSERT INTO contacts (name, data_source) VALUES (?, ?)",
                    (contact['name'], contact['source'])
                )
                contact_id = cursor.execute("SELECT last_insert_rowid()").fetchone()[0]
                
                cursor.execute(
                    """INSERT INTO contact_identifiers 
                    (contact_id, identifier, type, is_primary) 
                    VALUES (?, ?, ?, ?)""",
                    (contact_id, contact['identifier'], contact['identifier_type'], True)
                )
            except Exception as e:
                print(f"Error inserting contact {contact['name']}: {e}")
                continue
        
        return len(new_contacts)


def find_contact_by_identifier(identifier: str) -> Optional[Dict]:
    """Find a contact by their identifier in the database."""
    identifier = normalize_phone_number(identifier)
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        cursor.execute("""
            SELECT c.id, c.name, ci.identifier, ci.type
            FROM contacts c
            JOIN contact_identifiers ci ON c.id = ci.contact_id
            WHERE ci.identifier = ?
        """, (identifier,))
        result = cursor.fetchone()
        
        if result:
            return dict(zip(['id', 'name', 'identifier', 'type'], result))
        return None