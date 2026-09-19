"""
LESSON 02.01 -- NETWORK LAYERS, TCP vs UDP
==========================================

THE LAYER MODEL (simplified OSI / TCP-IP)
-----------------------------------------
    L7 Application : HTTP, gRPC, WebSocket, DNS, SMTP     <- what your code speaks
    L6/L5          : TLS (encryption), sessions            (usually folded into L7)
    L4 Transport   : TCP, UDP (+ QUIC, which runs on UDP)  <- ports, reliability
    L3 Network     : IP                                    <- addresses, routing between networks
    L2 Data link   : Ethernet, Wi-Fi                       <- MAC addresses, one hop
    L1 Physical    : cables, radio

Why it matters in system design: load balancers and proxies are described by
the layer they work at. An L4 load balancer sees only IPs and ports (fast,
dumb). An L7 load balancer understands HTTP (can route /api vs /images, read
cookies, terminate TLS) but does more work per request. See module 03.

TCP -- TRANSMISSION CONTROL PROTOCOL
-----------------------------------
* CONNECTION-ORIENTED: a 3-way handshake (SYN, SYN-ACK, ACK) before any data
  = 1 round trip of pure overhead. TLS adds 1 more RTT (TLS 1.3) or 2 (TLS 1.2).
  => Reuse connections! (HTTP keep-alive, connection pools to databases.)
* RELIABLE: every byte is numbered (sequence numbers), acknowledged (ACKs), and
  retransmitted if the ACK doesn't arrive before a timeout.
* ORDERED: the receiver reassembles bytes in order, even if packets arrive
  shuffled. Downside: HEAD-OF-LINE BLOCKING -- one lost packet stalls all the
  data behind it until it's retransmitted.
* FLOW CONTROL (don't overwhelm the receiver) and CONGESTION CONTROL (don't
  overwhelm the network: slow start, back off on loss).
* Use for: almost everything -- web, APIs, databases, file transfer, email.

UDP -- USER DATAGRAM PROTOCOL
----------------------------
* CONNECTIONLESS: just fire packets ("datagrams"). No handshake.
* UNRELIABLE: no ACKs, no retransmits, no ordering. Packets can be lost,
  duplicated, or reordered.
* Very low overhead and latency; no head-of-line blocking.
* Use for: live video/voice calls, online games (a late packet is useless --
  better to skip it), DNS lookups (tiny request/response; just retry),
  metrics (StatsD), and as the base for QUIC / HTTP/3.

QUIC / HTTP/3: reliability and encryption rebuilt on top of UDP, with
per-stream ordering (no cross-stream head-of-line blocking) and 0-1 RTT
connection setup. Great on lossy mobile networks.

The simulation below sends 20 packets over a lossy, reordering network using
(a) raw UDP semantics and (b) a mini TCP-like protocol with sequence numbers,
ACKs, retransmission and in-order reassembly.
"""

import random


class LossyNetwork:
    """Delivers packets with some probability of loss and random reordering."""

    def __init__(self, loss_rate, rng):
        self.loss_rate = loss_rate
        self.rng = rng
        self.packets_sent = 0

    def transmit(self, packets):
        self.packets_sent += len(packets)
        delivered = [p for p in packets if self.rng.random() > self.loss_rate]
        self.rng.shuffle(delivered)  # packets may take different routes -> reordering
        return delivered


def send_udp(data, net):
    """UDP: send once, hope for the best. Receiver gets whatever arrives, in arrival order."""
    return [payload for _, payload in net.transmit(list(enumerate(data)))]


def send_tcp_like(data, net, max_rounds=50):
    """
    A simplified reliable protocol:
      1. Number every segment (sequence number).
      2. Receiver ACKs what it got (here: we just learn which seq numbers arrived).
      3. Sender retransmits everything not ACKed (after a "timeout" = next round).
         ACKs can be lost too in real life; we model loss on data packets only.
      4. Receiver buffers out-of-order segments and delivers them IN ORDER.
    """
    received = {}                      # seq -> payload (the receive buffer)
    unacked = dict(enumerate(data))
    rounds = 0
    while unacked and rounds < max_rounds:
        rounds += 1
        for seq, payload in net.transmit(list(unacked.items())):
            received[seq] = payload    # duplicates are harmless: same seq overwrites
            unacked.pop(seq, None)     # the ACK for this seq "arrives" at the sender
    ordered = [received[i] for i in sorted(received)]
    return ordered, rounds


if __name__ == "__main__":
    rng = random.Random(11)
    message = [f"p{i:02d}" for i in range(20)]

    print("=" * 70)
    print("Sending 20 packets over a network with 20% loss + reordering")
    print("=" * 70)

    net = LossyNetwork(0.2, rng)
    got = send_udp(message, net)
    print(f"\nUDP  : received {len(got)}/20 packets, sent {net.packets_sent}")
    print(f"       {' '.join(got)}")
    print("       -> lost packets are gone, order is scrambled. Fine for a video frame, not a bank transfer.")

    net = LossyNetwork(0.2, rng)
    got, rounds = send_tcp_like(message, net)
    print(f"\nTCP* : received {len(got)}/20 packets, sent {net.packets_sent} "
          f"(retransmissions: {net.packets_sent - 20}), {rounds} round trips")
    print(f"       {' '.join(got)}")
    print("       -> complete and in order, at the cost of extra packets and extra round trips.")

    print("\nCost of connection setup (round trips before the first byte of the response):")
    for name, rtts in (("TCP + TLS 1.2", 1 + 2 + 1), ("TCP + TLS 1.3", 1 + 1 + 1),
                       ("QUIC (HTTP/3) new connection", 1 + 1), ("QUIC 0-RTT resumption", 0 + 1)):
        print(f"  {name:30s}: {rtts} RTT  -> {rtts * 150:4d} ms on a 150 ms transatlantic link")

    print("""
INTERVIEW TALKING POINTS
------------------------
* TCP for correctness (APIs, DBs, payments); UDP for real-time media/gaming
  where late data is worthless.
* Connection setup is expensive over long distances -> keep-alive, connection
  pooling, TLS termination at an edge close to the user (CDN / edge PoP).
* L4 vs L7 matters when choosing load balancers (module 03).
""")
