import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Set, List, Dict

from .state import get_watermark, set_watermark, initialize_watermark_if_missing
from .state import get_message_rowid_watermark, set_message_rowid_watermark
from .state import get_attachment_rowid_watermark, set_attachment_rowid_watermark
from .extractors import fetch_new_messages, fetch_new_attachments, get_live_chat_db_path
from .sync_messages import sync_messages
from .sync_attachments import sync_attachments
from etl_conversations import etl_conversations
from local_session_manager import db
from .timing import timed

# Debounce delay in milliseconds
DEBOUNCE_MS = 50
LIVE_DB = get_live_chat_db_path()
APPLE_UNIX_EPOCH_DIFF_NS = 978307200 * 1_000_000_000 # Seconds between 1970 and 2001, in ns

async def poll_for_changes(queue: asyncio.Queue, db_path: str, wal_path: str, polling_interval_s: float):
    """
    Periodically polls the database and WAL file for modifications.
    """
    last_db_mtime = 0.0
    last_wal_mtime = 0.0

    try:
        last_db_mtime = os.path.getmtime(db_path)
    except FileNotFoundError:
        pass # DB not initially present
    except Exception as e:
        print(f"ERROR: [Polling] Could not get initial mtime for {db_path}: {e}", exc_info=True)

    try:
        if os.path.exists(wal_path):
            last_wal_mtime = os.path.getmtime(wal_path)
    except FileNotFoundError:
        pass # WAL not initially present
    except Exception as e:
        print(f"ERROR: [Polling] Could not get initial mtime for {wal_path}: {e}", exc_info=True)

    while True:
        await asyncio.sleep(polling_interval_s)
        
        current_db_mtime = 0.0
        current_wal_mtime = 0.0
        changed = False

        try:
            current_db_mtime = os.path.getmtime(db_path)
            if current_db_mtime != last_db_mtime:
                changed = True
            last_db_mtime = current_db_mtime
        except FileNotFoundError:
            if last_db_mtime != 0.0:
                changed = True
            last_db_mtime = 0.0
        except Exception as e:
            print(f"WARNING: [Polling] Error stating main DB {db_path}: {e}", exc_info=True)

        try:
            wal_exists_now = os.path.exists(wal_path)
            if wal_exists_now:
                current_wal_mtime = os.path.getmtime(wal_path)
                if current_wal_mtime != last_wal_mtime:
                    changed = True
                elif last_wal_mtime == 0.0 and current_wal_mtime != 0.0:
                    changed = True
                last_wal_mtime = current_wal_mtime
            elif last_wal_mtime != 0.0:
                changed = True
                last_wal_mtime = 0.0
        except FileNotFoundError:
            if last_wal_mtime != 0.0:
                changed = True
            last_wal_mtime = 0.0
        except Exception as e:
            print(f"WARNING: [Polling] Error stating WAL file {wal_path}: {e}", exc_info=True)

        if changed:
            queue.put_nowait(time.time())

async def watch():
    """
    Main watcher function that uses polling to monitor chat.db and chat.db-wal
    for changes and triggers synchronization when changes are detected.
    """
    change_queue: asyncio.Queue = asyncio.Queue()
    
    wal_file_path = LIVE_DB + "-wal"
    polling_interval_s = 0.05

    poller_task = asyncio.create_task(poll_for_changes(change_queue, LIVE_DB, wal_file_path, polling_interval_s))
    
    # Initialize watermarks
    current_watermark_ns = get_watermark() or 0  # Legacy timestamp watermark
    current_message_rowid = get_message_rowid_watermark()  # ROWID watermark
    current_attachment_rowid = get_attachment_rowid_watermark()  # ROWID watermark
    
    print(f"INFO: Starting watcher with message ROWID watermark: {current_message_rowid}, attachment ROWID watermark: {current_attachment_rowid}")
    
    last_sync_time = 0.0
    
    try:
        while True:
            _ = await change_queue.get() # event_time not used with new logging
            
            now = time.time()
            if now - last_sync_time < DEBOUNCE_MS / 1000:
                continue
            
            await asyncio.sleep(DEBOUNCE_MS / 1000 + 0.025)
            last_sync_time = time.time()
            
            timings = {}
            
            try:
                # Fetch new messages and attachments by ROWID (faster)
                with timed("fetch_messages_by_rowid", timings):
                    new_messages, new_message_rowid = fetch_new_messages(current_message_rowid, current_watermark_ns)
                with timed("fetch_attachments_by_rowid", timings):
                    new_attachments, new_attachment_rowid = fetch_new_attachments(current_attachment_rowid)
                
                if not new_messages and not new_attachments:
                    continue
                
                if new_messages:
                    prev_watermark_ns_for_this_batch = current_watermark_ns
                    with timed("sync_messages", timings):
                        imported_count, chat_counts = sync_messages(new_messages)
                    
                    # Update ROWID watermark even if no messages were imported
                    current_message_rowid = new_message_rowid
                    set_message_rowid_watermark(current_message_rowid)
                    
                    affected_chats: Set[int] = set(chat_counts.keys())
                    
                    if affected_chats:
                        with timed("etl_conversations", timings):
                            for chat_id_val in affected_chats:
                                watermark_dt = datetime(2001, 1, 1, tzinfo=timezone.utc) + \
                                            timedelta(seconds=prev_watermark_ns_for_this_batch / 1_000_000_000.0)
                                etl_conversations(chat_id=chat_id_val, since_date=watermark_dt)
                    
                    if imported_count > 0:
                        valid_message_dates_ns = [m['date'] for m in new_messages if isinstance(m.get('date'), int)]
                        if not valid_message_dates_ns:
                            print("ERROR: [LiveSync] No valid integer dates in new messages to advance watermark.")
                        else:
                            new_watermark_ns_candidate = max(valid_message_dates_ns)
                            guids_in_current_batch = []
                            message_data_by_guid = {}
                            for msg_raw in new_messages:
                                raw_date_ns = msg_raw.get('date')
                                raw_guid = msg_raw.get('guid')
                                if isinstance(raw_date_ns, int) and raw_guid and \
                                   prev_watermark_ns_for_this_batch < raw_date_ns <= new_watermark_ns_candidate:
                                    guids_in_current_batch.append(raw_guid)
                                    message_data_by_guid[raw_guid] = msg_raw

                            if guids_in_current_batch:
                                with timed("latency_logging_db_query", timings):
                                    try:
                                        prev_watermark_dt_for_latency_log = datetime(2001, 1, 1, tzinfo=timezone.utc) + \
                                            timedelta(seconds=prev_watermark_ns_for_this_batch / 1_000_000_000.0)
                                        
                                        with db.session_scope() as session:
                                            cursor = session.connection().connection.cursor()
                                            placeholders = ', '.join(['?'] * len(guids_in_current_batch))
                                            sql_query = f"""
                                                SELECT guid, content, timestamp 
                                                FROM messages 
                                                WHERE guid IN ({placeholders}) AND timestamp > ?
                                                ORDER BY timestamp ASC
                                            """
                                            query_params = guids_in_current_batch + [prev_watermark_dt_for_latency_log]
                                            cursor.execute(sql_query, query_params)
                                            imported_messages_from_db = cursor.fetchall()

                                        log_time_ns = time.time_ns()
                                        for imported_msg_row in imported_messages_from_db:
                                            m_guid, m_content, _ = imported_msg_row[0], imported_msg_row[1], imported_msg_row[2]
                                            original_message_data = message_data_by_guid.get(m_guid)
                                            if original_message_data and 'date' in original_message_data:
                                                wal_time_apple_epoch_ns = original_message_data['date']
                                                if isinstance(wal_time_apple_epoch_ns, (int, float)) and isinstance(log_time_ns, (int, float)):
                                                    wal_time_unix_epoch_ns = wal_time_apple_epoch_ns + APPLE_UNIX_EPOCH_DIFF_NS
                                                    latency_ns = log_time_ns - wal_time_unix_epoch_ns
                                                    latency_ms = latency_ns / 1_000_000
                                                    escaped_content = str(m_content).replace("'", "\\'")
                                                    print(f"Message: {{content: '{escaped_content}'}}, WAL to Log Latency: {latency_ms:.2f} ms")
                                                else:
                                                    escaped_content_na = str(m_content).replace("'", "\\'")
                                                    print(f"Message: {{content: '{escaped_content_na}'}}, Latency: N/A (Invalid time data for GUID {m_guid})")
                                            else:
                                                escaped_content_na_wal = str(m_content).replace("'", "\\'")
                                                print(f"Message: {{content: '{escaped_content_na_wal}'}}, Latency: N/A (Original WAL time not found for GUID {m_guid})")
                                    except Exception as e_db_query:
                                        print(f"ERROR: [LiveSync] Failed to query/log imported messages from target DB: {e_db_query}", exc_info=True)
                            
                            # Update timestamp watermark (legacy approach)
                            with timed("set_timestamp_watermark", timings):
                                set_watermark(new_watermark_ns_candidate)
                                current_watermark_ns = new_watermark_ns_candidate
                
                if new_attachments:
                    with timed("sync_attachments", timings):
                        imported_attachments = sync_attachments(new_attachments)
                    
                    # Update attachment ROWID watermark
                    current_attachment_rowid = new_attachment_rowid
                    set_attachment_rowid_watermark(current_attachment_rowid)
                
                total_duration = sum(timings.values())
                timing_details = ", ".join(f'{k}={v:.3f}s' for k, v in timings.items())
                print(f"[LiveSync] Batch processed. Total: {total_duration:.3f}s. Breakdown: ({timing_details})")

            except Exception as e:
                print(f"ERROR: Error in live sync processing loop: {e}", exc_info=True)
    
    except Exception as e:
        print(f"ERROR: Main processing loop error: {e}", exc_info=True)
    finally:
        if poller_task:
            poller_task.cancel()
            try:
                await poller_task
            except asyncio.CancelledError:
                pass # Task cancelled as expected
            except Exception as e:
                print(f"ERROR: [Polling] Error during poller task shutdown: {e}", exc_info=True)

async def start_watcher():
    """Helper function to start the watcher."""
    try:
        await watch()
    except Exception as e:
        print(f"ERROR: Failed to start watcher: {e}", exc_info=True) 