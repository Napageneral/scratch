import asyncio
import sys
import os

# Add the project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from data_importer import import_live_data
from live_sync.wal import start_watcher
from live_sync.cache import reset_all_caches, get_contact_map, get_message_guid_to_id_map, get_chat_map, get_reaction_guids, get_attachment_guids, get_chat_participants_map
from live_sync.state import initialize_watermark_if_missing, initialize_rowid_watermarks_if_missing
from local_session_manager import db # To get a cursor for initial cache population

if __name__ == "__main__":
    print("INFO: Starting initial data import...")
    import_live_data() 
    print("INFO: Initial data import finished.")

    print("INFO: Initializing watermarks...")
    # Initialize timestamp watermark (legacy)
    initialize_watermark_if_missing()
    # Initialize ROWID watermarks for messages and attachments
    initialize_rowid_watermarks_if_missing()

    print("INFO: Initializing / Resetting caches before starting watcher...")
    # This ensures caches are built fresh before the watcher starts.
    # Subsequent calls to get_..._map within the watcher loop should use force_refresh=False
    # and rely on incremental updates (for message_guid_map) or the initially populated cache.
    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        reset_all_caches() # Clears the caches
        # Explicitly populate them here:
        print("INFO: Populating contact map cache...")
        get_contact_map(cursor, force_refresh=True)
        print("INFO: Populating message GUID to ID map cache...")
        get_message_guid_to_id_map(cursor, force_refresh=True)
        print("INFO: Populating chat map cache...")
        get_chat_map(cursor, force_refresh=True)
        print("INFO: Populating reaction GUID cache...")
        get_reaction_guids(cursor, force_refresh=True)
        print("INFO: Populating attachment GUID cache...")
        get_attachment_guids(cursor, force_refresh=True)
        print("INFO: All database caches populated.")
    
    print("INFO: Populating chat participants map from chat.db...")
    get_chat_participants_map(force_refresh=True)
    print("INFO: All caches populated.")

    print("INFO: Starting live sync watcher...")
    asyncio.run(start_watcher()) 