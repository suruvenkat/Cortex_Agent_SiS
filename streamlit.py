import streamlit as st
import json
from datetime import datetime
import pandas as pd
import _snowflake
from snowflake.snowpark.context import get_active_session

# =========================
# App / API Configuration
# =========================
st.set_page_config(page_title="Cortex Agent Chat (Threaded)", layout="wide")
st.title("💬 Cortex Agent Chat — Threaded & Persistent")

session = get_active_session()

API_ENDPOINT_RUN = "/api/v2/cortex/agent:run"
API_ENDPOINT_THREADS = "/api/v2/cortex/threads"  # POST {} returns {"thread_id": "..."}
API_TIMEOUT_MS = 60000

# Tools (optional): bind your semantic model & search service if you want tool-use
CORTEX_SEARCH_SERVICES = "VEHICLE360_DB.PUBLIC.SERVICE_MANUAL"
SEMANTIC_MODELS = "@VEHICLE360_DB.PUBLIC.v360/vehicle_data.yaml"

USER_AGENT = "Vehicle360AgentChat/1.0"

THREADS_TABLE = "VEHICLE360_DB.PUBLIC.AGENT_THREADS"
MESSAGES_TABLE = "VEHICLE360_DB.PUBLIC.AGENT_MESSAGES"
AUDIT_TABLE = "VEHICLE360_DB.PUBLIC.API_CALL_AUDIT"

# =========================
# Helpers: DB persistence
# =========================
def get_current_user() -> str:
    return session.sql("SELECT CURRENT_USER()").to_pandas().iloc[0, 0]

def list_threads_for_user(user: str) -> pd.DataFrame:
    return session.sql(f"""
        SELECT THREAD_ID, USER_NAME, CREATED_AT, COALESCE(TITLE,'') AS TITLE
        FROM {THREADS_TABLE}
        WHERE USER_NAME = %s
        ORDER BY CREATED_AT DESC
    """, params=[user]).to_pandas()

def insert_thread(thread_id: str, user: str, title: str = None):
    session.sql(f"""
        INSERT INTO {THREADS_TABLE} (THREAD_ID, USER_NAME, CREATED_AT, TITLE)
        VALUES (%s, %s, CURRENT_TIMESTAMP(), %s)
    """, params=[thread_id, user, title]).collect()

def insert_message(thread_id: str, message_id: int, role: str, content: str):
    # Sanitize single quotes for SQL safety if using params-less insert later
    session.sql(f"""
        INSERT INTO {MESSAGES_TABLE} (THREAD_ID, MESSAGE_ID, ROLE, CONTENT, CREATED_AT)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
    """, params=[thread_id, message_id, role, content]).collect()

def load_messages(thread_id: str) -> pd.DataFrame:
    return session.sql(f"""
        SELECT THREAD_ID, MESSAGE_ID, ROLE, CONTENT, CREATED_AT
        FROM {MESSAGES_TABLE}
        WHERE THREAD_ID = %s
        ORDER BY MESSAGE_ID ASC, CREATED_AT ASC
    """, params=[thread_id]).to_pandas()

def log_api_call(start_ts, end_ts, thread_id, parent_message_id, prompt, status_code):
    session.sql(f"""
        INSERT INTO {AUDIT_TABLE}
        (START_TS, END_TS, THREAD_ID, PARENT_MESSAGE_ID, PROMPT, HTTP_STATUS)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, params=[start_ts, end_ts, thread_id, parent_message_id, prompt, status_code]).collect()

# =========================
# Helpers: Thread & Agent
# =========================
def create_thread() -> str:
    """
    Calls the Cortex Threads API to create a new thread and returns thread_id.
    """
    headers = {"User-Agent": USER_AGENT}
    start_ts = datetime.utcnow()
    resp = _snowflake.send_snow_api_request(
        method="POST",
        path=API_ENDPOINT_THREADS,
        headers=headers,
        params={},
        body={},           # create with defaults
        request_guid=None,
        timeout=API_TIMEOUT_MS
    )
    end_ts = datetime.utcnow()
    status = resp.get("status", -1)
    # We don't have a thread yet to log; pass None safely for AUDIT if you want (or skip)
    try:
        content = json.loads(resp.get("content", "{}"))
        thread_id = content.get("thread_id")
    except Exception:
        thread_id = None
    # optional: audit create-thread call
    log_api_call(start_ts, end_ts, thread_id, 0, "CREATE_THREAD", status)
    if status != 200 or not thread_id:
        st.error(f"Failed to create thread. Status={status}")
        return None
    return thread_id

def agent_run(prompt: str, thread_id: str, parent_message_id: int):
    """
    Calls agent:run with thread_id + parent_message_id and streams result.
    Returns (aggregated_text, last_sql, assistant_message_id).
    """
    payload = {
        "model": "claude-3-5-sonnet",
        "thread_id": thread_id,
        "parent_message_id": parent_message_id,  # 0 for first turn in a thread
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}]
            }
        ],
        # Attach tools if needed:
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "search1"}}
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {"name": CORTEX_SEARCH_SERVICES, "max_results": 10}
        }
    }

    headers = {"User-Agent": USER_AGENT}

    start_ts = datetime.utcnow()
    resp = _snowflake.send_snow_api_request(
        method="POST",
        path=API_ENDPOINT_RUN,
        headers=headers,
        params={},
        body=payload,
        request_guid=None,
        timeout=API_TIMEOUT_MS
    )
    end_ts = datetime.utcnow()
    status = resp.get("status", -1)

    # log API call
    log_api_call(start_ts, end_ts, thread_id, parent_message_id, prompt, status)

    if status != 200:
        st.error(f"Agent call failed with status {status}")
        return "", "", parent_message_id

    # The run endpoint streams events; content is a JSON lines array or list of events
    try:
        events = json.loads(resp["content"])
    except Exception as e:
        st.error(f"Failed to parse agent response: {e}")
        return "", "", parent_message_id

    aggregated_text = ""
    last_sql = ""
    assistant_message_id = parent_message_id  # default to parent if not provided

    # Parse event stream; collect text & sql and capture message_id when seen
    for ev in events:
        event_type = ev.get("event")
        data = ev.get("data", {})
        # Most incremental content arrives via "message.delta"
        if event_type == "message.delta":
            delta = data.get("delta", {})
            for item in delta.get("content", []):
                t = item.get("type")
                if t == "text":
                    aggregated_text += item.get("text", "")
                elif t == "tool_results":
                    # Some tools return a json blob with 'text' and 'sql'
                    for res in item.get("tool_results", {}).get("content", []):
                        if res.get("type") == "json":
                            j = res.get("json", {})
                            aggregated_text += j.get("text", "")
                            last_sql = j.get("sql", last_sql)
        # Some implementations return a final "message.completed" with message_id
        if event_type in ("message.completed", "message.created", "response.completed"):
            mid = data.get("message_id")
            if mid is not None:
                assistant_message_id = mid

    return aggregated_text, last_sql, assistant_message_id

# =========================
# UI State: threads/messages
# =========================
if "current_thread_id" not in st.session_state:
    st.session_state.current_thread_id = None
if "parent_message_id" not in st.session_state:
    st.session_state.parent_message_id = 0  # first turn in thread
if "messages" not in st.session_state:
    st.session_state.messages = []  # local echo; persisted copy in table

user = get_current_user()

with st.sidebar:
    st.subheader("🧵 Threads")
    threads_df = list_threads_for_user(user)
    options = ["➕ New Thread"] + [f"{r.THREAD_ID}  —  {r.TITLE or '(untitled)'}" for _, r in threads_df.iterrows()]
    choice = st.selectbox("Select a thread", options, index=0)

    if st.button("Start / Switch"):
        if choice.startswith("➕"):
            # Create new thread
            new_thread_id = create_thread()
            if new_thread_id:
                insert_thread(new_thread_id, user, title=None)
                st.session_state.current_thread_id = new_thread_id
                st.session_state.parent_message_id = 0
                st.session_state.messages = []
                st.success(f"Created thread: {new_thread_id}")
        else:
            # Switch to existing
            selected_id = choice.split("—")[0].strip()
            st.session_state.current_thread_id = selected_id
            # Load persisted messages & set parent_message_id to last assistant/user id seen
            hist = load_messages(selected_id)
            st.session_state.messages = []
            last_msg_id = 0
            for _, row in hist.iterrows():
                # Rebuild chat history UI-friendly
                st.session_state.messages.append({"role": row["ROLE"], "content": row["CONTENT"]})
                if isinstance(row["MESSAGE_ID"], (int, float)) and row["MESSAGE_ID"] is not None:
                    last_msg_id = int(row["MESSAGE_ID"])
            st.session_state.parent_message_id = last_msg_id
            st.info(f"Switched to thread {selected_id}")

# Main chat area
st.markdown("### Conversation")

# Render prior messages from session state
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# Guard: ensure we have a thread selected
if not st.session_state.current_thread_id:
    st.warning("Select a thread on the left (or create a new one) to begin.")
else:
    # Chat input
    if prompt := st.chat_input("Type your message…"):
        # Show user bubble
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        # Persist user message with a synthetic message_id = parent_message_id + 1 (until agent returns id)
        provisional_user_id = (st.session_state.parent_message_id or 0) + 1
        insert_message(st.session_state.current_thread_id, provisional_user_id, "user", prompt)

        # Call agent
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                text, sql, assistant_message_id = agent_run(
                    prompt,
                    st.session_state.current_thread_id,
                    st.session_state.parent_message_id or 0
                )

                # Display response text (if any)
                if text:
                    st.markdown(text)
                    st.session_state.messages.append({"role": "assistant", "content": text})
                else:
                    st.info("No text returned.")

                # Display SQL (optional)
                if sql:
                    with st.expander("SQL generated", expanded=False):
                        st.code(sql, language="sql")
                        # Optional: run + show results
                        try:
                            df = session.sql(sql).to_pandas()
                            st.dataframe(df)
                        except Exception as e:
                            st.error(f"SQL run failed: {e}")

        # Persist assistant message with returned message_id (or fallback)
        final_assistant_id = int(assistant_message_id) if isinstance(assistant_message_id, (int, float)) else provisional_user_id + 1
        insert_message(st.session_state.current_thread_id, final_assistant_id, "assistant", text or "")

        # Advance the parent_message_id for the next turn
        st.session_state.parent_message_id = final_assistant_id

st.caption("Context is preserved per thread via `thread_id` and `parent_message_id`, and all messages are stored in Snowflake tables.")
