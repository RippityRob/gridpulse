#!/usr/bin/env python3
"""Dev tool: listen for GridPulse sensor packets, verify CRCs, print readings.

Usage: python3 decode.py [port]   (default 9000)

This doubles as an executable spec of the wire format until the Go
collector exists, and stays useful afterwards for protocol debugging.
"""
import socket
import struct
import sys
from datetime import datetime, timezone

PACKET_SIZE = 32
MAGIC = 0x4750
HEADER = struct.Struct(">HBBHIQIIIH")  # magic, ver, flags, id, seq, ts, mV, mA, mHz, crc


def crc16_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def handle(conn: socket.socket, addr) -> None:
    print(f"sensor stream connected from {addr[0]}:{addr[1]}")
    buf = b""
    ok = bad = 0
    try:
        while chunk := conn.recv(4096):
            buf += chunk
            while len(buf) >= PACKET_SIZE:
                pkt, buf = buf[:PACKET_SIZE], buf[PACKET_SIZE:]
                magic, ver, flags, sid, seq, ts, mv, ma, mhz, crc = HEADER.unpack(pkt)
                if magic != MAGIC or crc != crc16_ccitt(pkt[:30]):
                    bad += 1
                    print(f"!! bad packet (magic={magic:#06x}) good={ok} bad={bad}")
                    continue
                ok += 1
                t = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                fault = " FAULT" if flags & 0x01 else ""
                print(
                    f"[{t:%H:%M:%S.%f}] sensor={sid} seq={seq} "
                    f"V={mv/1000:8.1f}V  I={ma/1000:6.1f}A  f={mhz/1000:6.3f}Hz"
                    f" (v{ver}){fault}"
                )
    finally:
        conn.close()
        print(f"stream closed: {ok} valid, {bad} invalid packets")


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", port))
        srv.listen()
        print(f"decoder listening on :{port}")
        while True:
            handle(*srv.accept())


if __name__ == "__main__":
    main()
