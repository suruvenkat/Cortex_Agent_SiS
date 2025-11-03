# streamlit_app.py
# Pure chat app for Cortex Agent with threaded context + persistence (no %s placeholders)

import streamlit as st
import json
from datetime import datetime
import pandas as pd
import _snowflake
from snowflake.snowpark.context import get_active_session

# =========================
# App / API Configuration
# =========================
st.set_page_config(page_title="Cortex Agent Chat (Threaded, Persistent)", layout="wide")
st.title("💬 Cortex Agent Chat — Threaded & Persistent")

session = get_active_session()

API_ENDPOINT_RUN = "/api/v2/cortex/agent:run"
API_ENDPOINT_THREADS = "/api/v2/cortex/threads"   # POST {} -> {"thread_id": "..."}
API_TIMEOUT_MS = 60000

# Optional tools (bind your own resources or remove 'tools' + 'tool_resources' below)
CORTEX_SEARCH_SERVICES = "VEHICLE360_DB.PUBLIC.SERVICE_MANUAL"
SEMANTIC_MODELS        = "@VEHICLE360_DB.PUBLIC.v360/vehicle_data.yaml"

USER_AGENT = "Vehicle360AgentChat/1.0"

THREADS_TABLE = "VEHICLE360_DB.PUBLIC.AGENT_THREADS"
MESSAGES_TABLE = "VEHICLE360_DB.PUBLIC.AGENT_MESSAGES"
AUDIT_TABLE   = "VEHICLE360_DB.PUBLIC.API_CALL_AUDIT"

# =========================
# Helpers: DB persistence
# =========================
def get_current_user() -> str:
    return session.sql("SELECT CURRENT_USER()").to_pandas().iloc[0, 0]

def list_threads_for_user(user: str) -> pd.DataFrame:
    # Safe inline literal for read
    user_esc = (user or "").replace("'", "''")
    q = f"""
        SELECT THREAD_ID, USER_NAME, CREATED_AT, COALESCE(TITLE,'') AS TITLE
        FROM {THREADS_TABLE}
        WHERE USER_NAME = '{user_esc}'
        ORDER BY CREATED_AT DESC
    """
    return session.sql(q).to_pandas()

def insert_thread(thread_id: str, user: str, title: str | None = None):
    row = [[thread_id, user, datetime.utcnow(), title]]
    session.create_dataframe(
        row, schema=["THREAD_ID", "USER_NAME", "CREATED_AT", "TITLE"]
    ).write.mode("append").save_as_table(THREADS_TABLE)

def insert_message(thread_id: str, message_id: int, role: str, content: str):
    row = [[thread_id, int(message_id or 0), role, content, datetime.utcnow()]]
    session.create_dataframe(
        row, schema=["THREAD_ID", "MESSAGE_ID", "ROLE", "CONTENT", "CREATED_AT"]
    ).write.mode("append").save_as_table(MESSAGES_TABLE)

def load_messages(thread_id: str) -> pd.DataFrame:
    tid_esc = (thread_id or "").replace("'", "''")
    q = f"""
        SELECT THREAD_ID, MESSAGE_ID, ROLE, CONTENT, CREATED_AT
        FROM {MESSAGES_TABLE}
        WHERE THREAD_ID = '{tid_esc}'
        ORDER BY MESSAGE_ID ASC, CREATED_AT ASC
    """
    return session.sql(q).to_pandas()

def log_api_call(start_ts, end_ts, thread_id, parent_message_id, prompt, status_code):
    row = [[start_ts, end_ts, thread_id, int(parent_message_id or 0), prompt, int(status_code or -1)]]
    session.create_dataframe(
        row, schema=["START_TS", "END_TS", "THREAD_ID", "PARENT_MESSAGE_ID", "PROMPT", "HTTP_STATUS"]
    ).write.mode("append").save_as_table(AUDIT_TABLE)

# =========================
# Helpers: Thread & Agent
# =========================
def create_thread() -> str | None:
    """
    Calls the Cortex Threads API to create a new thread and returns thread_id.
    Uses positional args for _snowflake.send_snow_api_request.
    """
    headers = {"User-Agent": USER_AGENT}
    start_ts = datetime.utcnow()

    # NOTE: positional args only
    resp = _snowflake.send_snow_api_request(
        "POST",                  # method
        API_ENDPOINT_THREADS,    # path: "/api/v2/cortex/threads"
        headers,                 # headers dict
        {},                      # params dict
        {},                      # body dict (empty to create default thread)
        None,                    # request_guid
        API_TIMEOUT_MS           # timeout in ms
    )

    end_ts = datetime.utcnow()
    status = resp.get("status", -1)

    thread_id = None
    try:
        content = json.loads(resp.get("content", "{}") or "{}")
        thread_id = content.get("thread_id")
    except Exception:
        thread_id = None

    # audit create-thread call (thread_id may be None on failure)
    log_api_call(start_ts, end_ts, thread_id, 0, "CREATE_THREAD", status)

    if status != 200 or not thread_id:
        st.error(f"Failed to create thread. Status={status}")
        return None
    return thread_id

def agent_run(prompt: str, thread_id: str, parent_message_id: int):
    """
    Calls agent:run with thread_id + parent_message_id and parses streamed events.
    Uses positional args for _snowflake.send_snow_api_request.
    Returns (aggregated_text, last_sql, assistant_message_id).
    """
    payload = {
        "model": "claude-3-5-sonnet",
        "thread_id": thread_id,
        "parent_message_id": int(parent_message_id or 0),
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        # Keep or remove tools/resources as needed
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "search1"}},
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1":  {"name": CORTEX_SEARCH_SERVICES, "max_results": 10},
        },
    }

    headers = {"User-Agent": USER_AGENT}

    start_ts = datetime.utcnow()
    # NOTE: positional args only
    resp = _snowflake.send_snow_api_request(
        "POST",               # method
        API_ENDPOINT_RUN,     # path: "/api/v2/cortex/agent:run"
        headers,              # headers dict
        {},                   # params dict
        payload,              # body dict
        None,                 # request_guid
        API_TIMEOUT_MS        # timeout in ms
    )
    end_ts = datetime.utcnow()
    status = resp.get("status", -1)

    # audit the agent call
    log_api_call(start_ts, end_ts, thread_id, parent_message_id, prompt, status)

    if status != 200:
        st.error(f"Agent call failed with status {status}")
        return "", "", parent_message_id

    # Parse event stream (JSON list of event objects)
    try:
        events = json.loads(resp.get("content", "[]") or "[]")
    except Exception as e:
        st.error(f"Failed to parse agent response: {e}")
        return "", "", parent_message_id

    aggregated_text = ""
    last_sql = ""
    assistant_message_id = parent_message_id  # default if none returned

    for ev in events:
        et = ev.get("event")
        data = ev.get("data", {})

        if et == "message.delta":
            delta = data.get("delta", {})
            for item in delta.get("content", []):
                t = item.get("type")
                if t == "text":
                    aggregated_text += item.get("text", "")
                elif t == "tool_results":
                    for res in item.get("tool_results", {}).get("content", []):
                        if res.get("type") == "json":
                            j = res.get("json", {}) or {}
                            aggregated_text += j.get("text", "")
                            last_sql = j.get("sql", last_sql)

        if et in ("message.completed", "message.created", "response.completed"):
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
    st.session_state.parent_message_id = 0  # first turn
if "messages" not in st.session_state:
    st.session_state.messages = []          # UI echo

user = get_current_user()

# Sidebar: thread picker
with st.sidebar:
    st.subheader("🧵 Threads")
    threads_df = list_threads_for_user(user)
    options = ["➕ New Thread"] + [
        f"{row.THREAD_ID}  —  {row.TITLE or '(untitled)'}"
        for _, row in threads_df.iterrows()
    ]
    choice = st.selectbox("Select a thread", options, index=0, key="thread_select")

    if st.button("Start / Switch", key="start_switch"):
        if choice.startswith("➕"):
            new_thread_id = create_thread()
            if new_thread_id:
                insert_thread(new_thread_id, user, title=None)
                st.session_state.current_thread_id = new_thread_id
                st.session_state.parent_message_id = 0
                st.session_state.messages = []
                st.success(f"Created thread: {new_thread_id}")
        else:
            selected_id = choice.split("—")[0].strip()
            st.session_state.current_thread_id = selected_id
            # load persisted messages
            hist = load_messages(selected_id)
            st.session_state.messages = []
            last_msg_id = 0
            for _, row in hist.iterrows():
                st.session_state.messages.append({"role": row["ROLE"], "content": row["CONTENT"]})
                # track the highest message id seen
                try:
                    if row["MESSAGE_ID"] is not None:
                        last_msg_id = max(last_msg_id, int(row["MESSAGE_ID"]))
                except Exception:
                    pass
            st.session_state.parent_message_id = last_msg_id
            st.info(f"Switched to thread {selected_id}")

# Main chat area
st.markdown("### Conversation")

# Render history
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# Guard: need a thread selected
if not st.session_state.current_thread_id:
    st.warning("Select a thread on the left (or create a new one) to begin.")
else:
    # Chat input
    if prompt := st.chat_input("Type your message…"):
        # Display and persist user msg
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Persist user message with a provisional id (parent_id + 1)
        provisional_user_id = int(st.session_state.parent_message_id or 0) + 1
        insert_message(st.session_state.current_thread_id, provisional_user_id, "user", prompt)

        # Call agent
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                text, sql, assistant_message_id = agent_run(
                    prompt,
                    st.session_state.current_thread_id,
                    int(st.session_state.parent_message_id or 0)
                )

                if text:
                    st.markdown(text)
                    st.session_state.messages.append({"role": "assistant", "content": text})
                else:
                    st.info("No text returned.")

                if sql:
                    with st.expander("SQL generated", expanded=False):
                        st.code(sql, language="sql")
                        # Optionally run SQL
                        try:
                            df = session.sql(sql).to_pandas()
                            st.dataframe(df)
                        except Exception as e:
                            st.error(f"SQL run failed: {e}")

        # Persist assistant message & advance parent_message_id
        final_assistant_id = (
            int(assistant_message_id)
            if isinstance(assistant_message_id, (int, float, str)) and str(assistant_message_id).isdigit()
            else provisional_user_id + 1
        )
        insert_message(st.session_state.current_thread_id, final_assistant_id, "assistant", text or "")
        st.session_state.parent_message_id = final_assistant_id

st.caption("Context is preserved per thread via `thread_id` + `parent_message_id`, with all messages and API calls stored in Snowflake tables.")
