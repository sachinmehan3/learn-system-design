"""
CASE STUDY 08 -- PROXIMITY SERVICE / RIDE SHARING (Uber, Lyft, Yelp "nearby")
=============================================================================

THE CORE PROBLEM
----------------
"Find the drivers (or restaurants) within 2 km of this point." A naive query
    SELECT * FROM drivers WHERE lat BETWEEN .. AND lng BETWEEN ..
can't use a normal 1-D index well for a 2-D range, and scanning millions of
rows per request is too slow. We need a SPATIAL INDEX.

STEP 1 -- REQUIREMENTS (ride sharing)
-------------------------------------
Functional: drivers send their location every ~4 s; riders request a ride and
see nearby drivers; match rider to a driver; track the trip.
Non-functional: location updates are a MASSIVE write stream; nearby queries
must be fast (< 100 ms); matching must be consistent (a driver can't be
assigned to two riders); high availability.

STEP 2 -- ESTIMATION
--------------------
  1M active drivers x 1 update / 4 s = 250k writes/s of tiny records.
  Only the LATEST location matters -> keep it IN MEMORY (Redis GEO /
  in-memory grid service), not in a disk-based DB. Stream history to Kafka
  for analytics/trip records.

SPATIAL INDEXING OPTIONS
------------------------
1) GEOHASH: interleave the bits of latitude and longitude and base32-encode.
   Nearby points usually share a common PREFIX, so a 2-D proximity search
   becomes a 1-D prefix lookup (works with any sorted index / KV store).
     precision 5 chars ~ 4.9 km x 4.9 km cell, 6 chars ~ 1.2 km x 0.6 km.
   EDGE PROBLEM: two points can be 10 m apart but in different cells (even
   with no common prefix at the equator/meridian). Fix: always search the
   target cell AND its 8 NEIGHBOURS, then filter by exact distance.
   Redis GEOADD/GEOSEARCH uses geohash-encoded sorted sets.
2) QUADTREE: recursively split a region into 4 quadrants until each cell has
   <= K points. Adapts to density (Manhattan gets tiny cells, the desert huge
   ones). Usually built in memory; rebuild/update as points move.
3) GOOGLE S2 / UBER H3: hierarchical cells on a sphere (H3 uses hexagons --
   all 6 neighbours are equidistant, nice for surge-pricing zones). Uber uses H3.
4) Database spatial indexes: PostGIS (R-tree / GiST) -- great for static-ish
   data like restaurants (Yelp), less so for 250k updates/s.

STEP 3 -- HIGH-LEVEL DESIGN (ride sharing)
------------------------------------------
  driver app --(location every 4s, WebSocket)--> Location service
        --> in-memory geo index (Redis GEO / sharded by geohash/H3 cell)
        --> Kafka (trip history, ETA models, analytics)
  rider app --> Ride service --> "nearby available drivers" query on the geo
        index --> Matching service ranks by ETA (road network, not straight
        line), offers the ride to one driver at a time, with a timeout
        --> ASSIGNMENT must be atomic: compare-and-set driver status
        AVAILABLE -> ASSIGNED (a distributed lock / conditional write), so
        two riders can never get the same driver.
  Sharding: by geography (city / cell) -- but hot cells (stadium after a
  concert) need finer splits.

The demo builds a geohash encoder, a geohash-bucketed driver index with
neighbour search, a small quadtree, and an atomic driver assignment.
"""

import math
import random
import threading
from collections import defaultdict

BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(lat, lng, precision=6):
    lat_rng, lng_rng = [-90.0, 90.0], [-180.0, 180.0]
    bits, bit_count, out, even = 0, 0, [], True
    while len(out) < precision:
        rng, val = (lng_rng, lng) if even else (lat_rng, lat)   # alternate lng/lat bits
        mid = (rng[0] + rng[1]) / 2
        if val >= mid:
            bits = (bits << 1) | 1
            rng[0] = mid
        else:
            bits <<= 1
            rng[1] = mid
        even = not even
        bit_count += 1
        if bit_count == 5:
            out.append(BASE32[bits])
            bits, bit_count = 0, 0
    return "".join(out)


def cell_size_deg(precision):
    total_bits = precision * 5
    lng_bits = (total_bits + 1) // 2
    lat_bits = total_bits // 2
    return 180 / 2 ** lat_bits, 360 / 2 ** lng_bits


def neighbours(lat, lng, precision):
    """The cell containing the point plus its 8 surrounding cells."""
    dlat, dlng = cell_size_deg(precision)
    return {geohash_encode(lat + i * dlat, lng + j * dlng, precision)
            for i in (-1, 0, 1) for j in (-1, 0, 1)}


def haversine_km(a, b):
    lat1, lng1, lat2, lng2 = map(math.radians, (*a, *b))
    x = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lng2 - lng1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(x))


class GeohashIndex:
    """In-memory: geohash cell -> set of driver ids (like Redis GEO / a sharded location service)."""

    def __init__(self, precision=5):
        self.precision = precision
        self.cells = defaultdict(set)
        self.location = {}

    def update(self, driver, lat, lng):
        old = self.location.get(driver)
        if old:
            self.cells[geohash_encode(*old, self.precision)].discard(driver)
        self.location[driver] = (lat, lng)
        self.cells[geohash_encode(lat, lng, self.precision)].add(driver)

    def nearby(self, lat, lng, radius_km):
        candidates = set()
        for cell in neighbours(lat, lng, self.precision):
            candidates |= self.cells.get(cell, set())
        hits = [(haversine_km((lat, lng), self.location[d]), d) for d in candidates]
        return sorted((round(dist, 2), d) for dist, d in hits if dist <= radius_km), len(candidates)


class QuadTree:
    def __init__(self, bounds, capacity=8, depth=0):
        self.bounds, self.capacity, self.depth = bounds, capacity, depth   # (min_lat, min_lng, max_lat, max_lng)
        self.points, self.children = [], None

    def insert(self, p):
        lat, lng, _ = p
        a, b, c, d = self.bounds
        if not (a <= lat < c and b <= lng < d):
            return False
        if self.children is None:
            if len(self.points) < self.capacity or self.depth > 12:
                self.points.append(p)
                return True
            self._split()
        return any(ch.insert(p) for ch in self.children)

    def _split(self):
        a, b, c, d = self.bounds
        mlat, mlng = (a + c) / 2, (b + d) / 2
        self.children = [QuadTree(q, self.capacity, self.depth + 1) for q in
                         ((a, b, mlat, mlng), (a, mlng, mlat, d), (mlat, b, c, mlng), (mlat, mlng, c, d))]
        pts, self.points = self.points, []
        for p in pts:
            any(ch.insert(p) for ch in self.children)

    def count_leaves(self):
        return 1 if self.children is None else sum(ch.count_leaves() for ch in self.children)

    def max_depth(self):
        return self.depth if self.children is None else max(ch.max_depth() for ch in self.children)


class DriverStatus:
    """Atomic assignment: compare-and-set AVAILABLE -> ASSIGNED (Redis SET NX / conditional write)."""

    def __init__(self, drivers):
        self.status = {d: "AVAILABLE" for d in drivers}
        self.lock = threading.Lock()

    def try_assign(self, driver, rider):
        with self.lock:
            if self.status[driver] == "AVAILABLE":
                self.status[driver] = f"ASSIGNED:{rider}"
                return True
            return False


if __name__ == "__main__":
    rng = random.Random(9)
    sf = (37.7749, -122.4194)

    print("=" * 78)
    print("Geohash: nearby points share prefixes")
    print("=" * 78)
    for name, (lat, lng) in (("SF City Hall", sf), ("~300 m away", (37.7775, -122.4183)),
                             ("Oakland", (37.8044, -122.2712)), ("New York", (40.7128, -74.0060))):
        print(f"  {name:14s} {geohash_encode(lat, lng, 7)}")
    for p in (4, 5, 6, 7):
        dlat, dlng = cell_size_deg(p)
        print(f"  precision {p}: cell ~ {dlat * 111:.2f} km x {dlng * 111 * math.cos(math.radians(37.77)):.2f} km")

    print()
    print("=" * 78)
    print("20,000 drivers around the Bay Area; rider asks for drivers within 1.5 km")
    print("=" * 78)
    index = GeohashIndex(precision=5)
    drivers = [f"d{i}" for i in range(20_000)]
    for d in drivers:
        index.update(d, sf[0] + rng.gauss(0, 0.15), sf[1] + rng.gauss(0, 0.15))
    for d in rng.sample(drivers, 5000):             # a round of location updates (drivers moving)
        lat, lng = index.location[d]
        index.update(d, lat + rng.uniform(-0.002, 0.002), lng + rng.uniform(-0.002, 0.002))
    result, examined = index.nearby(*sf, radius_km=1.5)
    print(f"  examined {examined} candidates in 9 cells (not all 20,000); {len(result)} within 1.5 km")
    print(f"  closest: {result[:5]}")

    print()
    print("=" * 78)
    print("Quadtree adapts to density")
    print("=" * 78)
    qt = QuadTree((37.0, -123.0, 38.5, -121.5), capacity=50)
    for d in drivers:
        qt.insert((*index.location[d], d))
    print(f"  {qt.count_leaves()} leaf cells, max depth {qt.max_depth()} "
          "(deep, small cells downtown; shallow, large cells at the edges)")

    print()
    print("=" * 78)
    print("Matching: two riders race for the same nearest driver")
    print("=" * 78)
    status = DriverStatus(drivers)
    nearest = result[0][1]
    outcomes = {}

    def rider_books(rider):
        for _, d in result:                         # try nearest first, fall back to next
            if status.try_assign(d, rider):
                outcomes[rider] = d
                return

    threads = [threading.Thread(target=rider_books, args=(r,)) for r in ("rider-A", "rider-B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  nearest driver {nearest}; assignments: {outcomes}  <- never the same driver twice")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* Why keep locations in memory? (250k updates/s, only latest matters, TTL
  expiry for drivers who go offline)
* Geohash edge cases? (search neighbour cells; filter by exact distance)
* Hot spots (stadium)? (finer cells / quadtree; shard by cell with splitting)
* Nearest by straight line isn't nearest by road -> rank candidates by ETA
  from a routing service.
* How do you avoid double-assigning a driver? (atomic CAS on driver state,
  offer to one driver at a time with an acceptance timeout)
""")
