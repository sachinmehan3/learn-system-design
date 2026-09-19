"""
LESSON 10.02 -- SECURITY BASICS FOR SYSTEM DESIGN: AUTHN, AUTHZ, SESSIONS, JWT, OAUTH2
======================================================================================

AUTHENTICATION vs AUTHORIZATION
-------------------------------
* AUTHENTICATION (AuthN): WHO are you? (password, OTP, passkey, SSO)
* AUTHORIZATION (AuthZ): WHAT may you do? (roles, permissions, ownership)
  Models: RBAC (role-based: admin/editor/viewer), ABAC (attribute-based
  policies), ReBAC (relationship-based: "can edit if owner of the doc's
  folder" -- Google Zanzibar, used by Drive/YouTube; OpenFGA, SpiceDB).
HTTP: 401 = not authenticated; 403 = authenticated but not allowed.

STORING PASSWORDS
-----------------
NEVER store plaintext or a fast hash (MD5/SHA-256): attackers can try
billions of guesses per second on a GPU. Use a SLOW, SALTED, purpose-built
function: bcrypt, scrypt, Argon2id (or PBKDF2 with a high iteration count).
  * SALT: random per-user value stored with the hash -> identical passwords
    get different hashes; defeats precomputed rainbow tables.
  * Work factor: tune so one hash takes ~100 ms -- fine for a login, brutal
    for an attacker making billions of guesses.
  * Compare hashes in constant time (hmac.compare_digest) to avoid timing leaks.
Plus: rate-limit logins, lock/slow down after failures, offer MFA.

SESSIONS vs TOKENS
------------------
1) SERVER-SIDE SESSIONS: on login, create a random session ID, store
   session data in Redis/DB, send the ID in a cookie (HttpOnly, Secure,
   SameSite). Each request: look up the session.
   + Easy to revoke instantly (delete the session). - A store lookup per request.
2) JWT (JSON Web Token): header.payload.signature, base64url-encoded. The
   server SIGNS claims ({"sub": 42, "role": "admin", "exp": ...}) with a
   secret (HS256) or private key (RS256/ES256). Any service can VERIFY it
   locally without a DB lookup -> great for stateless microservices.
   - Hard to revoke before expiry -> use SHORT-LIVED access tokens (5-15 min)
     + long-lived REFRESH tokens (stored server-side, revocable, rotated).
   - Payload is only encoded, NOT encrypted: never put secrets in it.
   - Always verify the signature AND check exp; pin the algorithm (reject
     "alg": "none" and algorithm-confusion tricks).

OAUTH 2.0 AND OPENID CONNECT
----------------------------
* OAuth 2.0 = DELEGATED AUTHORIZATION: "let app X read my Google Calendar"
  without giving X my password. Roles: resource owner (user), client (app),
  authorization server (Google), resource server (Calendar API).
* AUTHORIZATION CODE FLOW + PKCE (the standard for web and mobile apps):
    1. App redirects the user to the auth server with client_id, scopes,
       redirect_uri, and a PKCE code_challenge.
    2. User logs in at the auth server and consents.
    3. Auth server redirects back with a short-lived CODE.
    4. App exchanges code (+ code_verifier) for tokens, server-to-server.
* OPENID CONNECT (OIDC) = identity layer on OAuth 2.0: adds an ID TOKEN (a
  JWT describing WHO the user is). That's "Sign in with Google/Apple".
* Client-credentials flow for service-to-service auth.

OTHER ESSENTIALS TO NAME IN AN INTERVIEW
----------------------------------------
* TLS everywhere (HTTPS); mTLS between internal services (service mesh).
* Encryption at rest (KMS-managed keys), secrets in a vault (not in code).
* Input validation / parameterized queries (SQL injection), output encoding
  (XSS), CSRF protection (SameSite cookies, tokens), CORS configuration.
* Least privilege IAM; audit logs; WAF + rate limiting + DDoS protection at the edge.
* Signed, expiring URLs for private objects in S3/CDN (pre-signed URLs).

The demo implements salted password hashing and a minimal HS256 JWT from
scratch with only the standard library (for learning -- use a vetted library
such as PyJWT in real code).
"""

import base64
import hashlib
import hmac
import json
import os
import time


# ---------------------------------------------------------------------------
# Password hashing (PBKDF2 from the stdlib; prefer Argon2id/bcrypt in production)
# ---------------------------------------------------------------------------
def hash_password(password, iterations=200_000):
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password, stored):
    _, iterations, salt_hex, digest_hex = stored.split("$")
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), int(iterations))
    return hmac.compare_digest(candidate.hex(), digest_hex)      # constant-time comparison


# ---------------------------------------------------------------------------
# Minimal JWT (HS256)
# ---------------------------------------------------------------------------
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def jwt_encode(claims, secret):
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = f"{b64url(json.dumps(header).encode())}.{b64url(json.dumps(claims).encode())}"
    sig = hmac.new(secret, signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{b64url(sig)}"


def jwt_decode(token, secret, now=None):
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        raise ValueError("malformed token")
    header = json.loads(b64url_decode(header_b64))
    if header.get("alg") != "HS256":                       # pin the algorithm!
        raise ValueError(f"unexpected alg {header.get('alg')!r}")
    expected = hmac.new(secret, f"{header_b64}.{payload_b64}".encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, b64url_decode(sig_b64)):
        raise ValueError("bad signature (token was tampered with or signed by someone else)")
    claims = json.loads(b64url_decode(payload_b64))
    if claims.get("exp", 0) < (now or time.time()):
        raise ValueError("token expired")
    return claims


def authorize(claims, action, resource_owner):
    """Tiny RBAC + ownership check (AuthZ happens AFTER AuthN)."""
    if claims["role"] == "admin":
        return True
    if action == "read":
        return True
    return action == "edit" and claims["sub"] == resource_owner


if __name__ == "__main__":
    print("=" * 78)
    print("PASSWORD HASHING (salted, slow)")
    print("=" * 78)
    t = time.perf_counter()
    stored = hash_password("correct horse battery staple")
    print(f"  stored: {stored[:70]}...  ({(time.perf_counter() - t) * 1000:.0f} ms to compute)")
    print(f"  same password hashed again differs (new salt): {hash_password('x')[:40] != hash_password('x')[:40]}")
    print(f"  verify correct password: {verify_password('correct horse battery staple', stored)}")
    print(f"  verify wrong password  : {verify_password('password123', stored)}")
    fast = time.perf_counter()
    for _ in range(100_000):
        hashlib.sha256(b"guess").digest()
    sha_rate = 100_000 / (time.perf_counter() - fast)
    slow = time.perf_counter()
    verify_password("guess", stored)
    pbkdf2_rate = 1 / (time.perf_counter() - slow)
    print(f"  -> guesses/second on this CPU: plain SHA-256 ~{sha_rate:,.0f}, this PBKDF2 ~{pbkdf2_rate:,.0f}.")
    print(f"     Brute-forcing is ~{sha_rate / pbkdf2_rate:,.0f}x slower. That's the point.")

    print()
    print("=" * 78)
    print("JWT: stateless, signed (not encrypted!) tokens")
    print("=" * 78)
    secret = os.urandom(32)
    now = time.time()
    token = jwt_encode({"sub": "user42", "role": "editor", "exp": now + 900}, secret)
    header, payload, sig = token.split(".")
    print(f"  token: {header[:20]}...{payload[:20]}...{sig[:12]}...")
    print(f"  payload is merely base64 -- anyone can read it: {b64url_decode(payload).decode()}")
    print(f"  verified claims: {jwt_decode(token, secret)}")

    forged_claims = b64url(json.dumps({"sub": "user42", "role": "admin", "exp": now + 900}).encode())
    for label, bad, when in (
        ("tampered role=admin", f"{header}.{forged_claims}.{sig}", None),
        ("signed with another key", jwt_encode({"sub": "x", "role": "admin", "exp": now + 900}, b"wrong"), None),
        ("alg=none attack", f"{b64url(json.dumps({'alg': 'none'}).encode())}.{forged_claims}.", None),
        ("expired (checked 20 min later)", token, now + 1200),
    ):
        try:
            jwt_decode(bad, secret, now=when)
            print(f"  {label:32s}: ACCEPTED (bug!)")
        except ValueError as e:
            print(f"  {label:32s}: rejected -> {e}")

    print()
    print("=" * 78)
    print("AUTHORIZATION (after authentication)")
    print("=" * 78)
    claims = jwt_decode(token, secret)
    for action, owner in (("read", "user7"), ("edit", "user42"), ("edit", "user7")):
        verdict = "allowed" if authorize(claims, action, owner) else "403 Forbidden"
        print(f"  {claims['sub']} ({claims['role']}) {action} doc owned by {owner}: {verdict}")

    print("""
INTERVIEW TALKING POINTS
------------------------
* "The gateway validates short-lived JWT access tokens (signature + expiry)
   so services stay stateless; refresh tokens are stored server-side and
   revocable. Login goes through OAuth2/OIDC (authorization code + PKCE)."
* Passwords: Argon2id/bcrypt with per-user salts, rate-limited logins, MFA.
* AuthZ is separate from AuthN: RBAC for simple apps, relationship-based
  (Zanzibar-style) for sharing models like Google Docs.
* TLS in transit, KMS encryption at rest, secrets in a vault, least privilege.
""")
