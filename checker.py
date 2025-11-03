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
