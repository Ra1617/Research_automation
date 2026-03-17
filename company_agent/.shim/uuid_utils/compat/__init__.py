import os
import time
import uuid


def uuid7(timestamp=None, nanos=None):
    if timestamp is None and nanos is None and hasattr(uuid, "uuid7"):
        return uuid.uuid7()

    if timestamp is None:
        millis = time.time_ns() // 1_000_000
    else:
        if nanos is None:
            nanos = 0
        millis = (int(timestamp) * 1_000_000_000 + int(nanos)) // 1_000_000

    rand = bytearray(os.urandom(16))
    rand[0:6] = (millis & 0xFFFFFFFFFFFF).to_bytes(6, "big")
    rand[6] = 0x70 | (rand[6] & 0x0F)
    rand[8] = 0x80 | (rand[8] & 0x3F)
    return uuid.UUID(bytes=bytes(rand))
