"""Test TCP verso 192.168.0.5 con diversi IP sorgente."""
import socket

TARGET = ("192.168.0.5", 1433)
SOURCES = [None, "192.168.0.23", "192.168.3.18"]

print("=== TCP bind test to 192.168.0.5:1433 ===")
for src in SOURCES:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        if src:
            s.bind((src, 0))
        s.connect(TARGET)
        print(f"from {src or 'default'}: OK local={s.getsockname()}")
    except Exception as e:
        print(f"from {src or 'default'}: FAIL {type(e).__name__}: {e}")
    finally:
        s.close()
