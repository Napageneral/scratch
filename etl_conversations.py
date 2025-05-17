from datetime import datetime, timedelta
from time import time
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

from local_session_manager import db
from local_models import Message, Conversation, Contact

DEFAULT_GAP_THRESHOLD = 10800  # 3 hours

#
# ========== 1) Standard "etl_conversations" for forward/incremental logic ==========
#

def etl_conversations(
    chat_id: Optional[int] = None,
    since_date: Optional[datetime] = None,
    gap_threshold: int = DEFAULT_GAP_THRESHOLD
) -> Tuple[int, int]:
    """
    ETL conversations for specified chat(s) and optional date range.
    - If 'since_date' is None, it processes all messages from scratch, but 
      can still do "append or create" logic.
    - If 'since_date' is given, it only processes messages from that date forward.
    - We rely on load_conversations() to handle simple forward merges.
    """
    def log_time(start_time, step):
        elapsed = round(time() - start_time, 2)
        print(f"  {step}: {elapsed}s")
        return time()
    
    t = time()
    print(f"Starting conversation ETL (chat_id={chat_id}, since_date={since_date}) gap_threshold={gap_threshold}")
    
    raw_conversations = extract_conversations(chat_id, since_date, gap_threshold)
    t = log_time(t, "Extract completed")
    
    transformed_conversations = transform_conversations(raw_conversations, gap_threshold)
    t = log_time(t, "Transform completed")
    
    imported_count, updated_count = load_conversations(transformed_conversations, chat_id)
    t = log_time(t, "Load completed")
    
    return imported_count, updated_count

def extract_conversations(
    chat_id: Optional[int] = None,
    since_date: Optional[datetime] = None,
    gap_threshold: int = DEFAULT_GAP_THRESHOLD
) -> List[Dict]:
    """Extract raw conversation data from 'messages' using basic SQL, sorted by timestamp."""
    with db.session_scope() as session:
        conditions = []
        params = {}
        if chat_id:
            conditions.append("m.chat_id = :chat_id")
            params['chat_id'] = chat_id
        if since_date:
            conditions.append("m.timestamp >= :since_date")
            params['since_date'] = since_date
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        query = f"""            SELECT 
                m.id,
                m.chat_id,
                m.sender_id,
                strftime('%Y-%m-%d %H:%M:%f', m.timestamp) as timestamp,
                m.content,
                m.is_from_me,
                m.message_type,
                m.service_name,
                m.guid
            FROM messages m
            {where_clause}
            ORDER BY m.chat_id, m.timestamp
        """        
        results = session.execute(query, params)
        
        chat_messages = defaultdict(list)
        for row in results:
            msg_time = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S.%f')
            msg_dict = {
                'id': row[0],
                'chat_id': row[1],
                'sender_id': row[2],
                'timestamp': msg_time,
                'content': row[4],
                'is_from_me': row[5],
                'message_type': row[6],
                'service_name': row[7],
                'guid': row[8]
            }
            chat_messages[msg_dict['chat_id']].append(msg_dict)
        
        raw_conversations = []
        for cid, msgs in chat_messages.items():
            raw_conversations.extend(_split_into_conversations(msgs, gap_threshold))
            
        return raw_conversations

def _split_into_conversations(
    messages: List[Dict], 
    gap_threshold: int = 1800
) -> List[Dict]:
    """Split a sorted list of messages into conversation blocks based on time gaps."""
    if not messages:
        return []
    # Assume 'messages' is sorted by timestamp
    conversations = []
    current_msgs = []
    start_time = None
    last_time = None
    
    for msg in messages:
        msg_time = msg['timestamp']
        if start_time is None:
            # Start a new conversation
            start_time = msg_time
            current_msgs = [msg]
            last_time = msg_time
            continue
        
        # Check gap
        time_gap = (msg_time - last_time).total_seconds()
        if time_gap > gap_threshold:
            # Close off the current conversation
            if current_msgs:
                conversations.append({
                    'messages': current_msgs,
                    'start_time': start_time,
                    'end_time': last_time,
                    'chat_id': msg['chat_id']
                })
            # Begin a new conversation
            start_time = msg_time
            current_msgs = []
        
        current_msgs.append(msg)
        last_time = msg_time
    
    # Don't forget the last block
    if current_msgs:
        conversations.append({
            'messages': current_msgs,
            'start_time': start_time,
            'end_time': last_time,
            'chat_id': messages[0]['chat_id']
        })
    
    return conversations

def transform_conversations(raw_conversations: List[Dict], gap_threshold: int) -> List[Dict]:
    """
    Convert conversation blocks into a format suitable for insertion into DB.
    """
    transformed = []
    for conv in raw_conversations:
        msgs = conv['messages']
        if not msgs:
            continue
        
        # We'll pick the first msg that has a sender_id as 'initiator'
        initiator_msg = next((m for m in msgs if m['sender_id'] is not None), None)
        if not initiator_msg:
            # If we can't find a message with a real sender, skip
            continue
        
        participant_ids = {m['sender_id'] for m in msgs if m['sender_id'] is not None}
        if not participant_ids:
            continue
        
        transformed.append({
            'chat_id': conv['chat_id'],
            'initiator_id': initiator_msg['sender_id'],
            'start_time': conv['start_time'],
            'end_time': conv['end_time'],
            'message_count': len(msgs),
            'participant_ids': list(participant_ids),
            'message_ids': [m['id'] for m in msgs],
            'gap_threshold': gap_threshold
        })
    return transformed

def load_conversations(conversations: List[Dict], chat_id: Optional[int] = None) -> Tuple[int, int]:
    """
    Incremental load with minimal merging. We only append to the most recent 
    conversation if the new block starts after or exactly at that conversation's end 
    AND the gap is <= gap_threshold. Otherwise, we create a new conversation row.
    """
    if not conversations:
        return (0, 0)
    
    imported_count = 0
    updated_count = 0
    
    with db.session_scope() as session:
        # We'll track "the most recent conversation" per chat (by end_time) 
        # so we can see if we can append.
        chat_ids = [chat_id] if chat_id else list({c['chat_id'] for c in conversations})
        existing_convs_by_chat = {}

        for cid in chat_ids:
            last_conv_cursor = session.execute("""                SELECT id, strftime('%Y-%m-%d %H:%M:%f', end_time), message_count
                FROM conversations
                WHERE chat_id = :chat_id
                ORDER BY end_time DESC
                LIMIT 1
            """, {'chat_id': cid})
            last_conv = last_conv_cursor.fetchone()
            if last_conv:
                existing_convs_by_chat[cid] = {
                    'id': last_conv[0],
                    'end_time': datetime.strptime(last_conv[1], '%Y-%m-%d %H:%M:%S.%f'),
                    'message_count': last_conv[2]
                }

        for conv in conversations:
            cid = conv['chat_id']
            last_conv = existing_convs_by_chat.get(cid)
            
            # Decide if we can append
            can_append = False
            if last_conv:
                time_diff = (conv['start_time'] - last_conv['end_time']).total_seconds()
                if time_diff >= 0 and time_diff <= conv['gap_threshold']:
                    can_append = True
            
            if can_append:
                # Append to existing conversation
                updated_count += 1
                
                session.execute("""                    UPDATE conversations
                    SET end_time = :new_end,
                        message_count = message_count + :msg_count
                    WHERE id = :conv_id
                """, {
                    'new_end': conv['end_time'],
                    'msg_count': conv['message_count'],
                    'conv_id': last_conv['id']
                })
                
                msg_ids_str = ",".join(str(m) for m in conv['message_ids'])
                session.execute(
                    f"UPDATE messages SET conversation_id = :conv_id WHERE id IN ({msg_ids_str})",
                    {'conv_id': last_conv['id']}
                )
                
                # Update in-memory end_time
                existing_convs_by_chat[cid]['end_time'] = conv['end_time']
                existing_convs_by_chat[cid]['message_count'] += conv['message_count']
            
            else:
                # Create a new conversation
                imported_count += 1
                new_id_cursor = session.execute("""                    INSERT INTO conversations
                    (chat_id, initiator_id, start_time, end_time, message_count, gap_threshold)
                    VALUES 
                    (:chat_id, :initiator_id, :start_time, :end_time, :message_count, :gap_threshold)
                    RETURNING id
                """, conv)
                new_id = new_id_cursor.scalar()
                
                msg_ids_str = ",".join(str(m) for m in conv['message_ids'])
                session.execute(
                    f"UPDATE messages SET conversation_id = :c WHERE id IN ({msg_ids_str})",
                    {'c': new_id}
                )
                
                # Update in-memory reference
                existing_convs_by_chat[cid] = {
                    'id': new_id,
                    'end_time': conv['end_time'],
                    'message_count': conv['message_count']
                }
    
    return (imported_count, updated_count)

#
# ========== 2) "Fresh Split & Compare" Approach for Backup Imports ==========
#

def etl_conversations_fresh_split_compare(gap_threshold: int = DEFAULT_GAP_THRESHOLD) -> None:
    """
    1) Gather *all* messages from the DB.
    2) Generate a "fresh" set of conversation intervals by re-running _split_into_conversations.
    3) Compare each new interval to existing intervals:
       - If identical (same message set), reuse the old conversation_id.
       - Otherwise, remove overlapping old intervals and insert a brand-new conversation row.
    """
    print(f"Starting 'fresh split & compare' for all chats, gap_threshold={gap_threshold}")
    with db.session_scope() as session:
        all_msgs_cursor = session.execute("""            SELECT m.id, m.chat_id,
                   strftime('%Y-%m-%d %H:%M:%f', m.timestamp) as ts,
                   m.sender_id
            FROM messages m
            ORDER BY m.chat_id, m.timestamp
        """)
        all_msgs = all_msgs_cursor.mappings().all()
        
        # Group messages by chat
        chat_messages = defaultdict(list)
        for row in all_msgs:
            ts = datetime.strptime(row['ts'], '%Y-%m-%d %H:%M:%S.%f')
            msg_dict = {
                'id': row['id'],
                'chat_id': row['chat_id'],
                'timestamp': ts,
                'sender_id': row['sender_id']
            }
            chat_messages[msg_dict['chat_id']].append(msg_dict)

    # For each chat, create new blocks
    for cid, msgs in chat_messages.items():
        splitted = _split_into_conversations(msgs, gap_threshold)
        # fully handle creation, overlap removal, or reuse
        _replace_or_reuse_intervals(cid, splitted, gap_threshold)

    print("Finished 'fresh split & compare'")

def _replace_or_reuse_intervals(chat_id: int, new_blocks: List[Dict], gap_threshold: int):
    """
    For each 'new block' from the fresh split:
      - Check if there's an existing conversation with exactly the same set of messages.
        -> If yes, do nothing (reuse).
        -> If no, remove any old conversation that overlaps in time, 
           then create a brand-new conversation row and attach the messages.
    """
    with db.session_scope() as session:
        # 1) Load existing conversations for this chat
        existing_convs_cursor = session.execute("""            SELECT id, 
                   strftime('%Y-%m-%d %H:%M:%f', start_time) as start_time,
                   strftime('%Y-%m-%d %H:%M:%f', end_time) as end_time
            FROM conversations
            WHERE chat_id = :c
            ORDER BY start_time
        """, {'c': chat_id})
        existing_convs = existing_convs_cursor.mappings().all()

        # 2) Build a map: conv_id -> sorted list of message IDs
        conv_msg_map = {}
        for conv_row in existing_convs:
            c_id = conv_row['id']
            msg_rows_cursor = session.execute("""                SELECT id 
                FROM messages 
                WHERE conversation_id = :cid
            """, {'cid': c_id})
            msg_rows = msg_rows_cursor.mappings().all()
            msg_id_list = sorted(mr['id'] for mr in msg_rows)
            conv_msg_map[c_id] = msg_id_list

        # 3) For each new splitted block, see if it matches an existing conversation
        for block in new_blocks:
            block_msg_ids = sorted(m['id'] for m in block['messages'])
            start_ = block['start_time']
            end_   = block['end_time']

            # Check for exact match in conv_msg_map
            reused_conv_id = None
            for existing_cid, existing_ids in conv_msg_map.items():
                if existing_ids == block_msg_ids:
                    reused_conv_id = existing_cid
                    break

            if reused_conv_id:
                # Reuse existing conversation ID => do nothing
                # (Optionally update start/end_time if you want it to match the new block times.)
                continue
            else:
                # 4) Remove old overlapping conv(s)
                # Overlap means: old_conv.start_time <= block.end_ 
                #                AND old_conv.end_time   >= block.start_
                session.execute("""                    UPDATE messages
                    SET conversation_id = NULL
                    WHERE conversation_id IN (
                        SELECT id FROM conversations
                        WHERE chat_id = :c
                          AND start_time <= :end_
                          AND end_time >= :start_
                    )
                """, {'c': chat_id, 'start_': start_, 'end_': end_})

                session.execute("""                    DELETE FROM conversations
                    WHERE chat_id = :c
                      AND start_time <= :end_
                      AND end_time >= :start_
                """, {'c': chat_id, 'start_': start_, 'end_': end_})

                # 5) Insert a brand-new conversation
                new_cid_cursor = session.execute("""                    INSERT INTO conversations (
                      chat_id, initiator_id, start_time, end_time, 
                      message_count, gap_threshold
                    )
                    VALUES (
                      :chat_id,
                      (SELECT sender_id FROM messages WHERE id=:first_msg_id LIMIT 1),
                      :start_, 
                      :end_, 
                      :msg_count, 
                      :gap_threshold
                    )
                    RETURNING id
                """, {
                    'chat_id': chat_id,
                    'first_msg_id': block_msg_ids[0] if block_msg_ids else None,
                    'start_': start_,
                    'end_': end_,
                    'msg_count': len(block_msg_ids),
                    'gap_threshold': gap_threshold
                })
                new_cid = new_cid_cursor.scalar()

                # 6) Attach the block's messages to the new conversation
                if block_msg_ids:
                    id_str = ",".join(str(x) for x in block_msg_ids)
                    session.execute(
                        f"UPDATE messages SET conversation_id = :c WHERE id IN ({id_str})",
                        {'c': new_cid}
                    )

        session.commit()
