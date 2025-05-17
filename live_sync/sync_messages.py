import sqlite3
from typing import List, Dict, Tuple, Set
from datetime import datetime
from local_session_manager import db
from etl_messages import transform_message, _convert_apple_timestamp
from local_utils import normalize_phone_number
from .cache import get_contact_map, get_message_guid_to_id_map, get_chat_map, get_reaction_guids, CONTACT_MAP_CACHE, MESSAGE_GUID_TO_ID_CACHE, REACTION_GUID_CACHE, get_chat_participants_map, CHAT_DB_ROWID_TO_PARTICIPANTS_CACHE, CHAT_MAP_CACHE
import time # Added for timing

def create_chat_if_missing(cursor, chat_identifier: str, is_group: bool = False) -> int:
    """
    Create a new chat entry if it doesn't exist in our database.
    Updates the CHAT_MAP_CACHE with the new chat_id.
    
    Args:
        cursor: SQLite cursor to use for queries
        chat_identifier: The unique identifier for the chat
        is_group: Whether this is a group chat
        
    Returns:
        int: The chat ID (newly created or existing)
    """
    # First check if it's in the cache (may have been created by another process)
    chat_id = CHAT_MAP_CACHE.get(chat_identifier)
    if chat_id:
        return chat_id
    
    # Double-check database directly (perhaps cache was stale)
    cursor.execute("SELECT id FROM chats WHERE chat_identifier = ?", (chat_identifier,))
    result = cursor.fetchone()
    if result:
        chat_id = result[0]
        # Update cache
        CHAT_MAP_CACHE[chat_identifier] = chat_id
        return chat_id
    
    # Create a new chat
    try:
        now = datetime.now()
        cursor.execute(
            """INSERT INTO chats 
               (chat_identifier, created_date, last_message_date, is_group, service_name, total_messages)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (chat_identifier, now, now, is_group, "iMessage", 0)
        )
        chat_id = cursor.lastrowid
        
        # Update cache
        CHAT_MAP_CACHE[chat_identifier] = chat_id
        
        print(f"INFO: Created new chat with identifier: '{chat_identifier}', assigned id: {chat_id}")
        return chat_id
    except Exception as e:
        print(f"ERROR: Failed to create new chat for identifier '{chat_identifier}': {e}")
        return None

def sync_messages(new_rows: List[Dict]) -> Tuple[int, Dict[int, int]]:
    """
    Process new messages from chat.db into our database.
    
    Args:
        new_rows: List of raw message rows from chat.db (from fetch_new_messages)
        
    Returns:
        Tuple containing:
        - Number of imported messages
        - Dict mapping chat_id to number of new messages
    """
    if not new_rows:
        return 0, {}

    # Initialize timings dictionary
    timings = {}
    overall_start_time = time.time()
    current_stage_start_time = time.time()

    with db.session_scope() as session:
        cursor = session.connection().connection.cursor()
        
        chat_map_cache = get_chat_map(cursor, force_refresh=False)
        message_cache_map = get_message_guid_to_id_map(cursor, force_refresh=False)
        reaction_guid_cache = get_reaction_guids(cursor, force_refresh=False)
        # Ensure contact_map_cache is populated (it's a global updated directly later)
        contact_map_cache_global = get_contact_map(cursor, force_refresh=False)
        
        # Get the chat participants map
        chat_participants_map = get_chat_participants_map(force_refresh=False)

        cursor.execute("SELECT id FROM contacts WHERE is_me = 1")
        user_id_row = cursor.fetchone()
        user_id = user_id_row[0] if user_id_row and user_id_row[0] is not None else None
        
        if not user_id:
            cursor.execute("INSERT INTO contacts (name, is_me, data_source) VALUES (?, ?, ?)", ("Me", True, "system_user"))
            user_id = cursor.lastrowid
        
        timings['setup_and_cache_retrieval'] = time.time() - current_stage_start_time
        current_stage_start_time = time.time()
        
        messages_to_insert = []
        reactions_to_insert = []
        chat_counts = {}
        imported_count = 0
        skipped_count = 0
        missing_chat_identifiers = set()
        created_chats_count = 0

        # --- Start: Batch Contact Creation ---
        unknown_sender_details_to_create = {} # {cache_key_for_sender: {'original': original_id, 'type': type, 'name_for_contact': name}}
        
        batch_contact_creation_start_time = time.time()

        # First pass to collect unknown senders (without calling transform_message)
        for row in new_rows:
            # Assuming row directly contains 'is_from_me' and 'sender_identifier' (raw handle from chat.db)
            # These assumptions are based on common patterns and previous discussion.
            # If these fields are named differently or require complex derivation before transform_message,
            # this part might need adjustment.
            is_from_me = row.get('is_from_me', False) 
            original_sender_identifier_from_row = row.get('sender_identifier')

            if not is_from_me and original_sender_identifier_from_row:
                cache_key_for_sender = original_sender_identifier_from_row.lower() if '@' in original_sender_identifier_from_row else normalize_phone_number(original_sender_identifier_from_row)
                
                # Check against the globally populated contact_map_cache (CONTACT_MAP_CACHE)
                if cache_key_for_sender not in CONTACT_MAP_CACHE:
                    if cache_key_for_sender not in unknown_sender_details_to_create: # Ensure uniqueness for this batch
                        identifier_type = 'Email' if '@' in original_sender_identifier_from_row else 'Phone'
                        unknown_sender_details_to_create[cache_key_for_sender] = {
                            'original': original_sender_identifier_from_row, 
                            'type': identifier_type,
                            'name_for_contact': original_sender_identifier_from_row # Using original identifier as name
                        }
        timings['identify_unknown_senders'] = time.time() - batch_contact_creation_start_time
        current_stage_start_time = time.time()

        if unknown_sender_details_to_create:
            # Prepare unique contacts for bulk insertion
            # (name_for_contact, data_source)
            contacts_to_bulk_insert_set = set()
            for details in unknown_sender_details_to_create.values():
                contacts_to_bulk_insert_set.add((details['name_for_contact'], 'live_sync_message_sender'))
            
            contacts_to_bulk_insert_list = list(contacts_to_bulk_insert_set)

            if contacts_to_bulk_insert_list:
                # print(f"DEBUG: Bulk inserting {len(contacts_to_bulk_insert_list)} unique new contacts.")
                cursor.executemany("INSERT INTO contacts (name, data_source) VALUES (?, ?)", contacts_to_bulk_insert_list)
            timings['bulk_insert_new_contacts'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
                
            inserted_contact_names = [item[0] for item in contacts_to_bulk_insert_list]
            retrieved_contact_info = {} # Map name_for_contact to its new ID

            if inserted_contact_names:
                name_placeholders = ','.join('?' for _ in inserted_contact_names)
                query_params = inserted_contact_names + ['live_sync_message_sender']
                cursor.execute(
                    f"SELECT id, name FROM contacts WHERE name IN ({name_placeholders}) AND data_source = ?", 
                    query_params
                )
                for contact_id_val, contact_name in cursor.fetchall():
                    retrieved_contact_info[contact_name] = contact_id_val
            timings['retrieve_new_contact_ids'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
            
            newly_created_contact_identifiers_to_insert = []
            for normalized_key, details in unknown_sender_details_to_create.items():
                contact_name_used_for_insert = details['name_for_contact']
                new_contact_id = retrieved_contact_info.get(contact_name_used_for_insert)

                if new_contact_id:
                    CONTACT_MAP_CACHE[normalized_key] = new_contact_id # Update global cache
                    
                    newly_created_contact_identifiers_to_insert.append((
                        new_contact_id,
                        details['original'], 
                        details['type'],
                        True # is_primary
                    ))
                elif (details['name_for_contact'], 'live_sync_message_sender') in contacts_to_bulk_insert_set:
                    # This implies it was intended for insert but ID wasn't retrieved.
                    # Could be due to a pre-existing contact with same name but different data_source,
                    # or other conflict if INSERT OR IGNORE semantics are not used and name isn't unique for data_source.
                    # Assuming contacts table has (name, data_source) uniqueness or INSERT handles conflicts.
                    print(f"WARNING: Could not retrieve ID for contact name: {contact_name_used_for_insert} (Normalized key: {normalized_key}) after attempting insert. It might have failed insertion due to conflict or already existed differently.")


            if newly_created_contact_identifiers_to_insert:
                # print(f"DEBUG: Bulk inserting {len(newly_created_contact_identifiers_to_insert)} new contact_identifiers.")
                cursor.executemany("""
                    INSERT INTO contact_identifiers (contact_id, identifier, type, is_primary)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(contact_id, identifier, type) DO NOTHING 
                """, newly_created_contact_identifiers_to_insert)
            timings['bulk_insert_new_contact_identifiers'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
        else: # No unknown senders
            timings['identify_unknown_senders'] = 0.0 # Ensure keys exist
            timings['bulk_insert_new_contacts'] = 0.0
            timings['retrieve_new_contact_ids'] = 0.0
            timings['bulk_insert_new_contact_identifiers'] = 0.0
        # --- End: Batch Contact Creation ---


        # Initialize accumulators for loop sub-timings
        loop_timings_transform_total = 0.0
        loop_timings_chat_lookup_total = 0.0
        loop_timings_sender_lookup_total = 0.0
        loop_timings_append_logic_total = 0.0

        # Process each message
        for row in new_rows:
            # If the message doesn't have chat_participants field but has chat_id
            # use the cached participants instead
            if not row.get('chat_participants') and 'chat_id' in row:
                chat_id_from_row = row.get('chat_id')
                if chat_id_from_row in chat_participants_map:
                    row['chat_participants'] = chat_participants_map[chat_id_from_row]
            
            # Fallback to sender_identifier if still no chat_participants
            if not row.get('chat_participants'):
                sender_handle_str = row.get('sender_identifier') # Use raw sender_identifier if chat_participants is empty
                row['chat_participants'] = sender_handle_str if sender_handle_str else ""
            
            iter_transform_start = time.time()
            transformed = transform_message(row) 
            loop_timings_transform_total += time.time() - iter_transform_start
            
            iter_chat_lookup_start = time.time()
            chat_id = chat_map_cache.get(transformed['chat_identifier'])
            
            # If chat not found, create it on-the-fly
            if not chat_id and transformed['chat_identifier']:
                # Determine if this is likely a group chat (more than one participant)
                is_group = False
                if row.get('chat_participants'):
                    participants = row['chat_participants'].split(',')
                    is_group = len(participants) > 1
                
                chat_id = create_chat_if_missing(cursor, transformed['chat_identifier'], is_group)
                if chat_id:
                    created_chats_count += 1
            
            if not chat_id:
                if transformed['chat_identifier'] not in missing_chat_identifiers:
                    if transformed['chat_identifier']:
                         print(f"WARNING: Chat not found for identifier: '{transformed['chat_identifier']}' derived from participants: '{row.get('chat_participants')}' and sender: '{row.get('sender_identifier')}' and could not be created.")
                    missing_chat_identifiers.add(transformed['chat_identifier'])
                skipped_count += 1
                loop_timings_chat_lookup_total += time.time() - iter_chat_lookup_start
                continue
            loop_timings_chat_lookup_total += time.time() - iter_chat_lookup_start
            
            iter_sender_lookup_start = time.time()
            sender_id = None
            if transformed['is_from_me']:
                sender_id = user_id
            else:
                sender_identifier_from_transform = transformed.get('sender_identifier')
                if sender_identifier_from_transform is None:
                    print(f"WARNING: Message GUID {transformed.get('guid')} is not from me but has no sender_identifier. Skipping.")
                    skipped_count += 1
                    loop_timings_sender_lookup_total += time.time() - iter_sender_lookup_start
                    continue

                cache_key = sender_identifier_from_transform.lower() if '@' in sender_identifier_from_transform else normalize_phone_number(sender_identifier_from_transform)
                sender_id = CONTACT_MAP_CACHE.get(cache_key) # Use the global, now updated, cache
                
                if not sender_id:
                    print(f"ERROR: Sender ID not found in cache for '{sender_identifier_from_transform}' (key: '{cache_key}') even after batch creation. Message GUID {transformed.get('guid')}. Skipping.")
                    skipped_count += 1
                    loop_timings_sender_lookup_total += time.time() - iter_sender_lookup_start
                    continue
            loop_timings_sender_lookup_total += time.time() - iter_sender_lookup_start

            iter_append_logic_start = time.time()
            message_guid = transformed.get('guid')
            if not message_guid:
                print(f"WARNING: Message transformed without a GUID. Original row: {row}. Transformed: {transformed}. Skipping.")
                skipped_count +=1
                loop_timings_append_logic_total += time.time() - iter_append_logic_start
                continue

            if transformed['message_type'] != 0 and transformed['message_type'] is not None:
                if message_guid not in reaction_guid_cache:
                    reactions_to_insert.append((
                        transformed['associated_message_guid'],
                        transformed['message_type'],
                        sender_id,
                        transformed['timestamp'],
                        chat_id,
                        message_guid
                    ))
            else:
                if message_guid not in message_cache_map:
                    messages_to_insert.append((
                        chat_id,
                        sender_id,
                        transformed['content'],
                        transformed['timestamp'],
                        transformed['is_from_me'],
                        transformed['message_type'],
                        transformed['service_name'],
                        message_guid
                    ))
                    chat_counts[chat_id] = chat_counts.get(chat_id, 0) + 1
                    imported_count += 1
            loop_timings_append_logic_total += time.time() - iter_append_logic_start

        timings['message_transformation_and_processing_loop'] = time.time() - current_stage_start_time
        timings['loop_transform_total'] = loop_timings_transform_total
        timings['loop_chat_id_lookup_total'] = loop_timings_chat_lookup_total
        timings['loop_sender_id_lookup_total'] = loop_timings_sender_lookup_total
        timings['loop_append_logic_total'] = loop_timings_append_logic_total
        current_stage_start_time = time.time() # Reset for DB inserts
        
        newly_inserted_message_guids_and_ids = []
        if messages_to_insert:
            guids_of_messages_inserted = [msg_data[-1] for msg_data in messages_to_insert]
            cursor.executemany("""
                INSERT INTO messages (chat_id, sender_id, content, timestamp, 
                                      is_from_me, message_type, service_name, guid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", messages_to_insert)
            timings['insert_new_messages'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
            
            if guids_of_messages_inserted:
                placeholders = ','.join('?' for _ in guids_of_messages_inserted)
                cursor.execute(f"SELECT guid, id FROM messages WHERE guid IN ({placeholders})", guids_of_messages_inserted)
                newly_inserted_message_guids_and_ids = cursor.fetchall()
                
                for guid, db_id in newly_inserted_message_guids_and_ids:
                    MESSAGE_GUID_TO_ID_CACHE[guid] = db_id
            timings['update_message_cache_after_insert'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
        else:
            timings['insert_new_messages'] = 0.0
            timings['update_message_cache_after_insert'] = 0.0
            current_stage_start_time = time.time()


        if reactions_to_insert:
            guids_of_reactions_inserted = [reaction_data[-1] for reaction_data in reactions_to_insert]
            cursor.executemany("""
                INSERT INTO reactions (original_message_guid, reaction_type, 
                                       sender_id, timestamp, chat_id, guid)
                VALUES (?, ?, ?, ?, ?, ?)""", reactions_to_insert)
            for guid_val in guids_of_reactions_inserted:
                REACTION_GUID_CACHE.add(guid_val)
            timings['insert_new_reactions'] = time.time() - current_stage_start_time
            current_stage_start_time = time.time()
        else:
            timings['insert_new_reactions'] = 0.0
            current_stage_start_time = time.time() # Reset start time even if no reactions
            
        if chat_counts:
            cursor.executemany(
                "UPDATE chats SET total_messages = total_messages + ? WHERE id = ?",
                [(count, chat_id_val) for chat_id_val, count in chat_counts.items()]
            )
        timings['update_chat_counts'] = time.time() - current_stage_start_time
        
        timings['total_sync_messages_internal'] = time.time() - overall_start_time
        
        # Log the detailed breakdown
        timing_details_str = ", ".join(f"{k}={v:.4f}s" for k, v in timings.items())
        print(f"INFO: [sync_messages breakdown] Total: {timings['total_sync_messages_internal']:.4f}s. Details: ({timing_details_str})")
        if skipped_count > 0:
             print(f"INFO: [sync_messages] Skipped {skipped_count} messages during processing.")
        if created_chats_count > 0:
             print(f"INFO: [sync_messages] Created {created_chats_count} new chats during processing.")

        return imported_count, chat_counts 