"""
LESSON 02.02 -- DNS: THE INTERNET'S PHONE BOOK
==============================================

WHAT DNS DOES
-------------
Translates a human name (www.example.com) into an IP address (93.184.216.34).
It's the FIRST step of almost every request in every system you'll design.

HOW A LOOKUP WORKS (recursive resolution)
-----------------------------------------
    Browser cache -> OS cache -> RECURSIVE RESOLVER (your ISP, 8.8.8.8, 1.1.1.1)
    The resolver, if it has no cached answer, walks the hierarchy:
        1. ROOT server:        "who handles .com?"         -> the .com TLD servers
        2. TLD server (.com):  "who handles example.com?"  -> example.com's nameservers
        3. AUTHORITATIVE server for example.com: "www.example.com?" -> 93.184.216.34
    Every answer is cached for its TTL (time to live) at every level.

RECORD TYPES YOU SHOULD KNOW
    A      name -> IPv4 address
    AAAA   name -> IPv6 address
    CNAME  name -> another name (alias), e.g. www -> myapp.cdnprovider.net
    MX     mail servers for a domain
    NS     which nameservers are authoritative for a zone
    TXT    arbitrary text (domain verification, SPF/DKIM for email)

TTL TRADE-OFF
-------------
* Long TTL (hours/days): fewer lookups, faster, less load on your DNS...
  but changes (e.g. failing over to a new IP) take a long time to propagate.
* Short TTL (30-60 s): fast failover and traffic shifting... more queries.
Before a planned migration, lower the TTL days in advance.

DNS AS A SYSTEM-DESIGN TOOL
---------------------------
* DNS LOAD BALANCING: return multiple A records (round-robin). Crude: clients
  cache answers, and DNS doesn't know if a server is healthy (unless your DNS
  provider does health checks).
* GEODNS / LATENCY-BASED ROUTING: return the IP of the datacenter closest to
  the user (Route 53, Cloudflare). This is how multi-region systems send users
  to the nearest region, and how you fail over a whole region.
* CDNs use a CNAME to their own DNS, which picks the nearest edge server.

DNS is an AP system: hierarchical, heavily cached, eventually consistent.

The simulation below implements a recursive resolver with a TTL cache and
GeoDNS answers.
"""


class AuthoritativeServer:
    """Knows records for some zone. For GeoDNS it can answer based on client region."""

    def __init__(self, name, records):
        self.name = name
        self.records = records  # (domain) -> answer, or dict region -> answer
        self.queries = 0

    def query(self, domain, client_region):
        self.queries += 1
        answer = self.records.get(domain)
        if isinstance(answer, dict):  # GeoDNS record
            answer = answer.get(client_region, answer["default"])
        return answer


class RecursiveResolver:
    """
    Walks root -> TLD -> authoritative, caching every answer with a TTL.
    `now` is passed in so we can simulate time passing.
    """

    def __init__(self, root, ttl_seconds):
        self.root = root
        self.ttl = ttl_seconds
        self.cache = {}           # (domain, region) -> (answer, expires_at)
        self.hits = self.misses = 0

    def resolve(self, domain, client_region, now):
        key = (domain, client_region)
        cached = self.cache.get(key)
        if cached and cached[1] > now:
            self.hits += 1
            return cached[0], "cache"

        self.misses += 1
        tld = domain.rsplit(".", 1)[-1]
        tld_server = self.root.query(tld, client_region)              # step 1: root
        zone = ".".join(domain.split(".")[-2:])
        auth_server = tld_server.query(zone, client_region)           # step 2: TLD
        answer = auth_server.query(domain, client_region)             # step 3: authoritative
        self.cache[key] = (answer, now + self.ttl)
        return answer, "root->tld->authoritative (3 extra round trips)"


if __name__ == "__main__":
    # Build a tiny internet.
    example_auth = AuthoritativeServer("ns1.example.com", {
        "www.example.com": {"us": "10.0.1.1 (us-east)", "eu": "10.0.2.1 (eu-west)",
                            "asia": "10.0.3.1 (ap-south)", "default": "10.0.1.1 (us-east)"},
        "api.example.com": "10.0.9.9",
    })
    com_tld = AuthoritativeServer("tld .com", {"example.com": example_auth})
    root = AuthoritativeServer("root", {"com": com_tld})

    resolver = RecursiveResolver(root, ttl_seconds=60)

    print("=" * 70)
    print("PART 1: Recursive resolution + caching (TTL = 60 s)")
    print("=" * 70)
    for t, region in ((0, "us"), (5, "us"), (30, "us"), (61, "us")):
        answer, path = resolver.resolve("www.example.com", region, now=t)
        print(f"  t={t:3d}s  www.example.com -> {answer:22s} via {path}")
    print(f"  cache hits={resolver.hits}, misses={resolver.misses}, "
          f"authoritative server load={example_auth.queries} queries")

    print()
    print("=" * 70)
    print("PART 2: GeoDNS - same name, different answer per region")
    print("=" * 70)
    for region in ("us", "eu", "asia", "africa"):
        answer, _ = resolver.resolve("www.example.com", region, now=100)
        print(f"  user in {region:7s} -> {answer}")

    print()
    print("=" * 70)
    print("PART 3: The TTL trade-off during a failover")
    print("=" * 70)
    print("  us-east dies at t=200. We update DNS to point US users at eu-west.")
    example_auth.records["www.example.com"]["us"] = "10.0.2.1 (eu-west)"
    for ttl in (30, 3600):
        r = RecursiveResolver(root, ttl_seconds=ttl)
        r.resolve("www.example.com", "us", now=199)  # cached just before the failure
        stale_until = 199 + ttl
        print(f"  TTL={ttl:5d}s: clients keep hitting the dead server until t={stale_until} "
              f"({ttl}s of errors for cached clients)")
    example_auth.records["www.example.com"]["us"] = "10.0.1.1 (us-east)"

    print("""
INTERVIEW TALKING POINTS
------------------------
* DNS is step 1 of the request path; mention GeoDNS for multi-region routing.
* TTL trades lookup cost against how fast you can change where traffic goes.
* DNS round-robin is NOT a real load balancer (no health checks, sticky client
  caches) - put a proper LB behind the IP DNS hands out.
""")
