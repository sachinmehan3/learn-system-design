"""
LESSON 02.03 -- HTTP AND API DESIGN (REST vs gRPC vs GraphQL)
=============================================================

HTTP IN ONE PARAGRAPH
---------------------
HTTP is a request/response protocol. A request has a METHOD, a PATH, HEADERS,
and an optional BODY. A response has a STATUS CODE, HEADERS, and a BODY.
HTTP is STATELESS: each request stands alone (state lives in cookies, tokens,
or the server's database) -- which is exactly what lets us scale servers
horizontally.

METHODS, SAFETY, AND IDEMPOTENCY (interviewers love this table)
---------------------------------------------------------------
    Method   Purpose                  Safe?  Idempotent?
    GET      read a resource          yes    yes
    HEAD     GET without body         yes    yes
    POST     create / trigger action  no     NO   <- retrying may create duplicates!
    PUT      replace a resource       no     yes  (setting x=5 twice = setting it once)
    PATCH    partial update           no     not necessarily ("increment by 1" isn't)
    DELETE   remove a resource        no     yes

* SAFE = doesn't change server state.
* IDEMPOTENT = doing it N times has the same effect as doing it once.
Why it matters: networks fail, and clients/proxies RETRY. Retrying an
idempotent request is harmless. To make POST safe to retry, clients send an
IDEMPOTENCY KEY header; the server remembers keys it has seen and returns the
original result instead of acting twice (Stripe does exactly this). This
server implements it below.

STATUS CODES
------------
    2xx success   : 200 OK, 201 Created, 202 Accepted (async - processing later), 204 No Content
    3xx redirect  : 301 Moved Permanently (browsers cache it!), 302/307 temporary, 304 Not Modified
    4xx client err: 400 Bad Request, 401 Unauthenticated, 403 Forbidden, 404 Not Found,
                    409 Conflict, 429 Too Many Requests (rate limited)
    5xx server err: 500 Internal Error, 502 Bad Gateway, 503 Unavailable, 504 Gateway Timeout
Rule for retries: 5xx and 429 are usually retryable (with backoff); 4xx are not.

HTTP VERSIONS
-------------
* HTTP/1.1: one request at a time per TCP connection (browsers open ~6
  connections per host). Keep-alive reuses connections.
* HTTP/2: MULTIPLEXING -- many concurrent streams over ONE connection; binary
  framing; header compression (HPACK). Still suffers TCP head-of-line blocking.
* HTTP/3: HTTP over QUIC (UDP). Independent streams, faster handshakes,
  connection migration (switching Wi-Fi -> 4G keeps the connection).

CACHING HEADERS
---------------
* Cache-Control: max-age=3600, public/private, no-store
* ETag + If-None-Match: server sends a version fingerprint; client re-asks with
  it; server replies 304 Not Modified with NO body if unchanged. Saves bandwidth.
  Demonstrated below.

API STYLES
----------
REST (Representational State Transfer)
  * Resources are nouns in URLs (/users/42/orders); HTTP verbs are the actions.
  * JSON over HTTP; human-readable; cacheable by HTTP infrastructure (CDNs).
  * Default choice for PUBLIC APIs.
  - Over-fetching (get the whole user when you need the name) and
    under-fetching (N+1 requests to assemble a page).

gRPC
  * Remote procedure calls defined in Protocol Buffers (a typed schema);
    binary encoding (smaller, faster to parse); runs on HTTP/2; supports
    streaming in both directions; generates client/server code.
  * Great for INTERNAL service-to-service calls in a microservice system.
  - Not browser-native (needs gRPC-Web proxy); not human-readable.

GraphQL
  * One endpoint; the CLIENT sends a query describing exactly the fields it
    needs, possibly across related objects. Solves over/under-fetching.
  * Great for rich front-ends (mobile apps) aggregating many backends.
  - Harder to cache at the HTTP layer; clients can write expensive queries
    (need query cost limits); N+1 problems move to the server (use DataLoader
    batching).

API DESIGN BEST PRACTICES
-------------------------
* VERSIONING: /v1/... in the path (or a header). Never break existing clients.
* PAGINATION: never return unbounded lists.
    - OFFSET (?offset=1000&limit=20): simple, allows "jump to page 50", but the
      DB still scans+discards 1000 rows, and inserts cause skipped/duplicate items.
    - CURSOR / KEYSET (?after=<last_id>&limit=20): "WHERE id > last_id LIMIT 20"
      uses the index directly, stable under inserts. Standard for feeds.
      Implemented below.
* Consistent error format, rate limiting (429 + Retry-After), authentication
  (module 10), and idempotency keys for POST.

The code below runs a REAL HTTP server on localhost in a background thread
and calls it with urllib, so you can see these ideas on the wire.
"""

import hashlib
import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------------------
# Server side: a tiny REST API for "books".
# ---------------------------------------------------------------------------
BOOKS = {}              # id -> dict  (our "database")
NEXT_ID = [1]
IDEMPOTENCY_KEYS = {}   # key -> (status, body) of the original response
LOCK = threading.Lock()  # ThreadingHTTPServer handles requests concurrently


class BookAPI(BaseHTTPRequestHandler):
    def log_message(self, *args):   # silence default request logging
        pass

    def _send(self, status, body=None, headers=None):
        payload = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    # GET /v1/books?after=<id>&limit=<n>   (cursor pagination)
    # GET /v1/books/<id>                   (with ETag support)
    def do_GET(self):
        path, _, query = self.path.partition("?")
        parts = path.strip("/").split("/")
        if parts[:2] != ["v1", "books"]:
            return self._send(404, {"error": "not found"})

        if len(parts) == 2:
            params = dict(p.split("=") for p in query.split("&") if p)
            after = int(params.get("after", 0))
            limit = min(int(params.get("limit", 2)), 100)   # cap page size!
            with LOCK:
                ids = sorted(i for i in BOOKS if i > after)[:limit]
                page = [BOOKS[i] for i in ids]
            next_cursor = ids[-1] if len(ids) == limit else None
            return self._send(200, {"items": page, "next_cursor": next_cursor})

        book = BOOKS.get(int(parts[2]))
        if not book:
            return self._send(404, {"error": "no such book"})
        etag = '"' + hashlib.md5(json.dumps(book, sort_keys=True).encode()).hexdigest()[:12] + '"'
        if self.headers.get("If-None-Match") == etag:
            return self._send(304, None, {"ETag": etag})       # unchanged: no body sent
        return self._send(200, book, {"ETag": etag, "Cache-Control": "max-age=60"})

    # POST /v1/books  (create; honours Idempotency-Key)
    def do_POST(self):
        key = self.headers.get("Idempotency-Key")
        with LOCK:
            if key and key in IDEMPOTENCY_KEYS:
                status, body = IDEMPOTENCY_KEYS[key]
                return self._send(status, body, {"Idempotent-Replayed": "true"})
            data = self._read_json()
            if "title" not in data:
                return self._send(400, {"error": "title is required"})
            book = {"id": NEXT_ID[0], "title": data["title"]}
            BOOKS[book["id"]] = book
            NEXT_ID[0] += 1
            if key:
                IDEMPOTENCY_KEYS[key] = (201, book)
        return self._send(201, book, {"Location": f"/v1/books/{book['id']}"})

    # PUT /v1/books/<id>  (full replace; naturally idempotent)
    def do_PUT(self):
        book_id = int(self.path.strip("/").split("/")[2])
        data = self._read_json()
        with LOCK:
            BOOKS[book_id] = {"id": book_id, "title": data["title"]}
        return self._send(200, BOOKS[book_id])

    def do_DELETE(self):
        book_id = int(self.path.strip("/").split("/")[2])
        with LOCK:
            BOOKS.pop(book_id, None)   # deleting twice is fine -> idempotent
        return self._send(204)


# ---------------------------------------------------------------------------
# Client side helper.
# ---------------------------------------------------------------------------
def call(base, method, path, body=None, headers=None):
    req = urllib.request.Request(base + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return resp.status, dict(resp.headers), json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:        # 3xx/4xx/5xx land here
        raw = e.read()
        return e.code, dict(e.headers), json.loads(raw) if raw else None


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 0), BookAPI)   # port 0 = any free port
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    print(f"REST API running at {base}\n")

    print("--- Create books (POST -> 201 Created + Location header) ---")
    for title in ("Designing Data-Intensive Applications", "Site Reliability Engineering",
                  "Database Internals", "Understanding Distributed Systems", "The Tail at Scale"):
        status, headers, body = call(base, "POST", "/v1/books", {"title": title})
        print(f"  {status} {body}  Location={headers.get('Location')}")

    print("\n--- Validation error (400) ---")
    print("  ", call(base, "POST", "/v1/books", {"name": "oops"})[::2])

    print("\n--- Retrying a POST WITHOUT an idempotency key creates duplicates ---")
    a = call(base, "POST", "/v1/books", {"title": "Dup Book"})[2]
    b = call(base, "POST", "/v1/books", {"title": "Dup Book"})[2]
    print(f"  first try -> id {a['id']}, 'retry' -> id {b['id']}   <- two books, customer charged twice!")

    print("\n--- Retrying a POST WITH an idempotency key is safe ---")
    h = {"Idempotency-Key": "order-7f3a"}
    s1, _, b1 = call(base, "POST", "/v1/books", {"title": "Once Only"}, h)
    s2, h2, b2 = call(base, "POST", "/v1/books", {"title": "Once Only"}, h)
    print(f"  first: {s1} {b1}")
    print(f"  retry: {s2} {b2}  replayed={h2.get('Idempotent-Replayed')}")

    print("\n--- Cursor pagination (limit=3) ---")
    cursor, page_no = 0, 1
    while cursor is not None:
        _, _, body = call(base, "GET", f"/v1/books?after={cursor}&limit=3")
        print(f"  page {page_no}: {[b['id'] for b in body['items']]}  next_cursor={body['next_cursor']}")
        cursor, page_no = body["next_cursor"], page_no + 1

    print("\n--- Conditional GET with ETag (saves bandwidth) ---")
    s, h, body = call(base, "GET", "/v1/books/1")
    print(f"  first GET : {s} ETag={h['ETag']} body={body}")
    s, h, body = call(base, "GET", "/v1/books/1", headers={"If-None-Match": h["ETag"]})
    print(f"  re-GET    : {s} Not Modified, body={body}  <- nothing re-sent")
    call(base, "PUT", "/v1/books/1", {"title": "DDIA (2nd edition)"})
    s, h, body = call(base, "GET", "/v1/books/1", headers={"If-None-Match": '"stale"'})
    print(f"  after PUT : {s} new ETag={h['ETag']} body={body}")

    print("\n--- DELETE is idempotent ---")
    print("  first delete :", call(base, "DELETE", "/v1/books/2")[0])
    print("  second delete:", call(base, "DELETE", "/v1/books/2")[0], "(same outcome, no error)")
    print("  GET deleted  :", call(base, "GET", "/v1/books/2")[::2])

    server.shutdown()
    print("""
INTERVIEW TALKING POINTS
------------------------
* Define the API early in the interview: resources, methods, request/response.
* REST for public APIs, gRPC between internal services, GraphQL for flexible
  client-driven aggregation.
* Use cursor pagination for feeds/lists; cap page sizes.
* POST isn't idempotent - use idempotency keys for anything involving money
  or side effects, because clients WILL retry.
""")
