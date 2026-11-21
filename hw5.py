"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


HEADER = struct.Struct("!BI")
HEADER_SIZE = HEADER.size
PAYLOAD_SIZE = homework5.MAX_PACKET - HEADER_SIZE

DATA, ACK, FIN, FINACK, BYE = 0, 1, 2, 3, 4

RTO_FLOAT = struct.Struct("!d")

ALPHA = 0.125       
BETA = 0.25          
INITIAL_RTO = 1.0     
MIN_MARGIN = 0.01   
MAX_RTO = 4.0
FIN_RETRIES = 8      
FINACK_RETRIES = 4    


def make_packet(ptype: int, seq: int, payload: bytes = b"") -> bytes:
    return HEADER.pack(ptype, seq) + payload


def parse_packet(raw: bytes) -> typing.Optional[typing.Tuple[int, int, bytes]]:
    if len(raw) < HEADER_SIZE:
        return None
    ptype, seq = HEADER.unpack_from(raw)
    return ptype, seq, raw[HEADER_SIZE:]


class RttEstimator:
    """EWMA RTT estimator (RFC 6298 / Kurose & Ross)."""

    def __init__(self):
        self.estimated = None
        self.deviation = 0.0
        self.rto = INITIAL_RTO

    def sample(self, rtt: float):
        if self.estimated is None:
            self.estimated = rtt
            self.deviation = rtt / 2
        else:
            self.deviation = (1 - BETA) * self.deviation + BETA * abs(rtt - self.estimated)
            self.estimated = (1 - ALPHA) * self.estimated + ALPHA * rtt
        margin = max(4 * self.deviation, MIN_MARGIN)
        self.rto = min(self.estimated + margin, MAX_RTO)


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """
    
    logger = homework5.logging.get_logger("hw5-sender")
    rtt = RttEstimator()

    def send_and_wait(packet: bytes, want_type: int, want_seq: int,
                      max_tries: typing.Optional[int] = None) -> bool:
        """Send `packet` until a reply of (want_type, want_seq) arrives.
        Returns False only if max_tries is reached."""
        tries = 0
        retransmitted = False
        timeout = rtt.rto        
        while max_tries is None or tries < max_tries:
            tries += 1
            sent_at = time.time()
            deadline = sent_at + timeout
            sock.send(packet)
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                sock.settimeout(remaining)
                try:
                    raw = sock.recv(homework5.MAX_PACKET)
                except socket.timeout:
                    break
                except OSError:
                    continue
                parsed = parse_packet(raw)
                if parsed is None:
                    continue
                ptype, seq, _ = parsed
                if ptype == want_type and seq == want_seq:
                    if not retransmitted:          
                        rtt.sample(time.time() - sent_at)
                    return True

            retransmitted = True
            timeout = min(timeout * 2, MAX_RTO)
            logger.info("Timeout on seq %d, retrying with %.3f", want_seq, timeout)
        return False

    # Data phase
    seq = 0
    for offset in range(0, len(data), PAYLOAD_SIZE):
        chunk = data[offset:offset + PAYLOAD_SIZE]
        send_and_wait(make_packet(DATA, seq, chunk), ACK, seq)
        seq += 1

    fin = make_packet(FIN, seq, RTO_FLOAT.pack(rtt.rto))
    if send_and_wait(fin, FINACK, seq, max_tries=FIN_RETRIES):
        sock.send(make_packet(BYE, seq))
    logger.info("Sender done")


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    logger = homework5.logging.get_logger("hw5-receiver")
    num_bytes = 0
    expected = 0
    sock.settimeout(None)

    # Data phase
    while True:
        try:
            raw = sock.recv(homework5.MAX_PACKET)
        except OSError:
            continue
        parsed = parse_packet(raw)
        if parsed is None:
            continue
        ptype, seq, payload = parsed

        if ptype == DATA:
            if seq == expected:
                dest.write(payload)
                dest.flush()
                num_bytes += len(payload)
                expected += 1
            if seq < expected:
                sock.send(make_packet(ACK, seq))
        elif ptype == FIN and seq == expected:
            sender_rto = INITIAL_RTO
            if len(payload) >= RTO_FLOAT.size:
                sender_rto = RTO_FLOAT.unpack_from(payload)[0]
            break

 
    wait = min(max(sender_rto * 1.5, 0.05), MAX_RTO)
    sock.send(make_packet(FINACK, expected))
    tries = 1
    deadline = time.time() + wait
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            if tries >= FINACK_RETRIES:
                break
            sock.send(make_packet(FINACK, expected))
            tries += 1
            deadline = time.time() + wait
            continue
        sock.settimeout(remaining)
        try:
            raw = sock.recv(homework5.MAX_PACKET)
        except socket.timeout:
            continue
        except OSError:
            continue
        parsed = parse_packet(raw)
        if parsed is None:
            continue
        ptype, seq, _ = parsed
        if ptype == BYE:
            break
        if ptype == FIN:
            sock.send(make_packet(FINACK, expected))
        elif ptype == DATA and seq < expected:
            sock.send(make_packet(ACK, seq))

    logger.info("Receiver done, %d bytes", num_bytes)
    return num_bytes
