"""
CASE STUDY 03 -- DESIGN A CHAT SYSTEM (WhatsApp, Messenger, Slack)
==================================================================

STEP 1 -- REQUIREMENTS
----------------------
Functional: 1:1 and group chats (up to ~500 members); real-time delivery;
message history on any device; delivery/read receipts; online presence;
push notifications when offline; media attachments.
Non-functional: low latency (<200 ms delivery when both online), messages
NEVER lost, ordered within a conversation, highly available, massive scale.

STEP 2 -- ESTIMATION
--------------------
  500M DAU x 40 messages/day = 20B messages/day -> ~230k messages/s avg,
  ~700k/s peak. ~100 B text each -> ~2 TB/day of text (+ replication).
  Tens of millions of CONCURRENT open connections -> connection tier matters.

API / PROTOCOL
  * WebSocket for real-time send/receive (client <-> gateway), persistent.
  * REST for everything else: GET /chats/{id}/messages?before=<msg_id>
    (history), POST /media (upload -> S3 pre-signed URL), login, etc.

DATA MODEL
  messages: PRIMARY KEY ((chat_id), message_id) -- wide-column (Cassandra /
      HBase / ScyllaDB). Partition by chat_id so a conversation is one
      partition; cluster by message_id (time-sortable Snowflake-style) so
      "latest 50 messages" is a single sequential read. Huge write volume ->
      LSM-tree store is a great fit.
  chats / members: chat_id -> member list (SQL or KV).
  user inbox / sync state: per user, per chat: last_read_message_id.

STEP 3 -- HIGH-LEVEL DESIGN
---------------------------
   clients ==WebSocket==> [Gateway 1] [Gateway 2] ... [Gateway N]   (stateful: hold connections)
                                |             ^
                                v             |
                         Chat service --> message store (Cassandra, by chat_id)
                                |
                                +--> Session registry: user_id -> gateway id (Redis)
                                +--> Pub/sub or direct RPC to the recipient's gateway
                                +--> Push notification service (APNs/FCM) if offline
                                +--> Kafka (analytics, search indexing, moderation)

SEND FLOW (Alice -> Bob):
  1. Alice's client sends {client_msg_id, chat_id, text} over her WebSocket.
  2. Gateway -> chat service: assign a server message_id (ordering), PERSIST
     to the message store (durability BEFORE acking), ack to Alice ("sent" /
     one tick). client_msg_id makes retries idempotent.
  3. Look up Bob's gateway in the session registry; forward the message.
     Bob's gateway pushes it down his WebSocket; Bob's client acks
     ("delivered" / two ticks).
  4. If Bob is offline: push notification; when he reconnects, his client
     SYNCS by asking for messages after its last known message_id per chat.

STEP 4 -- DEEP DIVES
--------------------
* CONNECTION TIER: each gateway holds ~100k-1M connections; LB with
  consistent routing; on deploy, drain connections gracefully; clients
  reconnect with exponential backoff + jitter (avoid reconnect storms).
* ORDERING: per-chat ordering via message_id assigned by the chat's owner
  (e.g. a per-chat sequence from the partition leader) -- not client clocks.
* DELIVERY GUARANTEES: at-least-once to devices + idempotent client dedup by
  message_id; the client keeps a "last synced" cursor to catch up.
* GROUP CHATS: fan-out on write to each member's gateway (fine for <=500
  members). Very large channels (Slack/Discord with 100k members) -> fan-out
  on read: members fetch from the channel's log.
* PRESENCE: heartbeats over the WebSocket; online/offline stored in Redis
  with TTL; presence changes are fanned out only to interested contacts,
  debounced (flapping networks) and often lazily fetched.
* MULTI-DEVICE: registry maps user -> set of (device, gateway); each device
  has its own sync cursor.
* END-TO-END ENCRYPTION (Signal protocol): server only relays ciphertext;
  can't index/search content server-side.
* MEDIA: upload to object storage, send the URL/key in the message; CDN.

The simulation below implements gateways, a session registry, persistence
before ack, delivery receipts, offline sync, and a group message.
"""

import itertools
from collections import defaultdict


class MessageStore:
    """Wide-column style: partition = chat_id, rows ordered by message_id."""

    def __init__(self):
        self.partitions = defaultdict(list)
        self.seen_client_ids = set()

    def append(self, chat_id, message):
        self.partitions[chat_id].append(message)

    def after(self, chat_id, last_id, limit=50):
        return [m for m in self.partitions[chat_id] if m["id"] > last_id][:limit]


class Client:
    def __init__(self, user):
        self.user = user
        self.inbox = []
        self.cursor = defaultdict(int)          # chat_id -> last message_id seen (sync state)
        self.gateway = None

    def receive(self, msg):
        if msg["id"] <= self.cursor[msg["chat"]]:
            return False                       # duplicate -> ignore (idempotent client)
        self.inbox.append(msg)
        self.cursor[msg["chat"]] = msg["id"]
        return True


class Gateway:
    """Holds WebSocket connections for some users."""

    def __init__(self, name, service):
        self.name, self.service = name, service
        self.connections = {}

    def connect(self, client):
        self.connections[client.user] = client
        client.gateway = self
        self.service.registry[client.user] = self.name       # user -> gateway (Redis in reality)
        self.service.sync(client)                            # catch up on missed messages

    def disconnect(self, client):
        self.connections.pop(client.user, None)
        client.gateway = None
        self.service.registry.pop(client.user, None)

    def push(self, user, msg):
        client = self.connections.get(user)
        return bool(client) and client.receive(msg)


class ChatService:
    def __init__(self):
        self.store = MessageStore()
        self.registry = {}                     # user -> gateway name
        self.gateways = {}
        self.chats = {}                        # chat_id -> members
        self.ids = itertools.count(1)          # stand-in for per-chat sequence / Snowflake IDs
        self.push_notifications = []
        self.log = []

    def add_gateway(self, name):
        self.gateways[name] = Gateway(name, self)
        return self.gateways[name]

    def send(self, sender, chat_id, text, client_msg_id):
        # Idempotency: a client retry with the same client_msg_id is not stored twice.
        if (sender, client_msg_id) in self.store.seen_client_ids:
            return "duplicate send ignored"
        self.store.seen_client_ids.add((sender, client_msg_id))
        msg = {"id": next(self.ids), "chat": chat_id, "from": sender, "text": text}
        self.store.append(chat_id, msg)        # 1) persist FIRST (durability)
        status = ["sent"]                      # 2) ack to sender
        for member in self.chats[chat_id]:
            if member == sender:
                continue
            gw_name = self.registry.get(member)
            if gw_name and self.gateways[gw_name].push(member, msg):
                status.append(f"delivered->{member}@{gw_name}")
            else:
                self.push_notifications.append((member, text))
                status.append(f"push-notif->{member}")
        return ", ".join(status)

    def sync(self, client):
        for chat_id, members in self.chats.items():
            if client.user in members:
                for m in self.store.after(chat_id, client.cursor[chat_id]):
                    client.receive(m)


if __name__ == "__main__":
    svc = ChatService()
    gw1, gw2 = svc.add_gateway("gw-1"), svc.add_gateway("gw-2")
    alice, bob, carol = Client("alice"), Client("bob"), Client("carol")
    svc.chats = {"dm:alice:bob": ["alice", "bob"], "group:trip": ["alice", "bob", "carol"]}
    gw1.connect(alice)
    gw2.connect(bob)
    gw2.connect(carol)

    print("=" * 78)
    print("1:1 and group messages; users on different gateways")
    print("=" * 78)
    print("  alice -> bob       :", svc.send("alice", "dm:alice:bob", "hi bob!", "c1"))
    print("  bob -> group       :", svc.send("bob", "group:trip", "flights booked", "c2"))
    print("  alice retries c1   :", svc.send("alice", "dm:alice:bob", "hi bob!", "c1"))
    print(f"  registry (user -> gateway): {svc.registry}")

    print()
    print("=" * 78)
    print("Carol goes offline; messages are persisted + push-notified; she syncs on reconnect")
    print("=" * 78)
    gw2.disconnect(carol)
    print("  alice -> group     :", svc.send("alice", "group:trip", "bring sunscreen", "c3"))
    print("  bob -> group       :", svc.send("bob", "group:trip", "and snacks", "c4"))
    print(f"  push notifications queued: {svc.push_notifications}")
    gw1.connect(carol)                                   # reconnects via a DIFFERENT gateway
    print(f"  carol reconnects to gw-1 and syncs from cursor: "
          f"{[m['text'] for m in carol.inbox]}")

    print()
    print("=" * 78)
    print("Inboxes")
    print("=" * 78)
    for c in (alice, bob, carol):
        print(f"  {c.user:6s}: {[(m['id'], m['from'], m['text']) for m in c.inbox]}")
    print(f"  message store partition 'group:trip' (ordered by id): "
          f"{[m['id'] for m in svc.store.partitions['group:trip']]}")

    print("""
FOLLOW-UP QUESTIONS TO EXPECT
-----------------------------
* How do you route a message to the right gateway? (session registry
  user->gateway in Redis, or pub/sub channel per user/gateway)
* What if the recipient's gateway crashes mid-delivery? (message already
  persisted; client reconnects and syncs from its cursor)
* How do you order messages? (server-assigned per-chat sequence IDs)
* Group of 100k members? (fan-out on read; members pull the channel log)
* Read receipts at scale? (batch/aggregate, send only to online senders)
""")
