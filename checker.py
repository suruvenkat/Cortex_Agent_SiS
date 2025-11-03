import json, uuid, streamlit as st, _snowflake

def api_sanity_check():
    tests = [
        ("POST", "/api/v2/cortex/threads", {}, {}, {}, None, 5000),              # create thread (should be 200 if enabled)
        ("GET",  "/api/v2/cortex/threads", {}, {}, {}, None, 5000),              # likely 405/404 (method not allowed or not listed)
        ("POST", "/api/v2/cortex/agent:run", {}, {}, {"messages":[]}, None, 5000) # will 400 (missing fields) but proves call shape
    ]
    results = []
    for t in tests:
        try:
            resp = _snowflake.send_snow_api_request(*t)
            results.append({
                "call": {"method": t[0], "path": t[1], "timeout_ms": t[6]},
                "status": resp.get("status"),
                "content_sample": (resp.get("content") or "")[:200],
            })
        except Exception as e:
            results.append({
                "call": {"method": t[0], "path": t[1], "timeout_ms": t[6]},
                "error": str(e),
            })
    st.subheader("API sanity check")
    st.json(results)

# Call once to see results:
# api_sanity_check()

import json
import _snowflake

API_PATH = "/api/v2/cortex/threads"

def create_thread(origin_application: str = "python-test-app"):
    """
    Minimal test to create a Cortex thread (equivalent to the curl you shared).
    Returns (status_code, parsed_json_or_none, raw_response_dict).
    """
    # You can pass {} for headers; auth is handled by SiS.
    # If you want to mirror curl headers explicitly, you can use:
    # headers = {"Content-Type": "application/json", "Accept": "application/json"}
    headers = {}

    body = {"origin_application": origin_application}

    resp = _snowflake.send_snow_api_request(
        "POST",              # method
        API_PATH,            # path
        headers,             # headers dict (auth handled by SiS)
        {},                  # params dict
        body,                # JSON body
        None,                # request_guid
        30000                # timeout (ms)
    )

    status = resp.get("status", -1)
    parsed = None
    try:
        parsed = json.loads(resp.get("content", "") or "{}")
    except Exception:
        parsed = None

    return status, parsed, resp

# --- Quick test run ---
if __name__ == "__main__":
    status, parsed, raw = create_thread("oneApp_thread_test")
    print("Status:", status)
    print("Parsed:", json.dumps(parsed, indent=2) if parsed else None)
    # If you want to inspect raw:
    # print("Raw:", raw)

