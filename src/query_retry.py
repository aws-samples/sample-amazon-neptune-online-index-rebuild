"""
query_retry.py - Retry pattern for APPLICATION queries during an online index rebuild.

During a rebuild, some Gremlin, openCypher, and SPARQL queries can fail with
CancelledByMaintenanceException. The docs say to treat it like
ConcurrentModificationException and retry with exponential backoff. This file
shows the pattern; copy it into the code path where your application runs queries.

Do NOT use this to retry bulk loads or fast resets. Those are rejected for as long
as a rebuild runs (which can be hours). Wait for the rebuild to reach a terminal
state, or cancel it, before resubmitting them.
"""

import json
import random
import time

RETRIABLE_CODES = ("CancelledByMaintenanceException", "ConcurrentModificationException")


def is_retriable(err):
    """Best-effort classification across clients.

    boto3 neptunedata raises ClientError with an error code. Other clients
    (gremlinpython GremlinServerError, the neo4j Bolt driver, SPARQL over HTTP)
    surface the Neptune error code in the message text, so match on that.
    """
    response = getattr(err, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code", "")
        if code in RETRIABLE_CODES:
            return True
    text = str(err)
    return any(code in text for code in RETRIABLE_CODES)


def run_with_retry(fn, *, max_attempts=8, base_delay=0.2, max_delay=10.0):
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as err:  # noqa: BLE001 - classified below
            if not is_retriable(err) or attempt >= max_attempts:
                raise
            time.sleep(random.uniform(0, min(max_delay, base_delay * 2 ** (attempt - 1))))


# --- openCypher through boto3 neptunedata --------------------------------------
def opencypher_example(client, query, params):
    return run_with_retry(lambda: client.execute_open_cypher_query(
        openCypherQuery=query, parameters=json.dumps(params)))


# --- Gremlin through gremlinpython (pip install gremlinpython) -----------------
#   result = run_with_retry(lambda: g.V().has("Item", "id", vid).values("name").to_list())
#
# --- openCypher through Bolt (pip install neo4j) -------------------------------
#   def tx(tx): return tx.run("MATCH (n {id:$id}) RETURN n", id=vid).data()
#   rows = run_with_retry(lambda: session.execute_read(tx))
#   (If you use the driver's managed transactions, also make sure its own retry
#    policy treats these Neptune errors as transient.)
#
# --- SPARQL over HTTP (SigV4-signed requests) ----------------------------------
#   resp = run_with_retry(lambda: _raise_for_neptune(session.post(url, data=...)))
#   where _raise_for_neptune raises an exception containing the response body
#   (which includes the Neptune error code) when status_code >= 400.
