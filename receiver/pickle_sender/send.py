import time
import pickle
import struct
import socket

data = [("pickle.test", (time.time(), 42))]

payload = pickle.dumps(data, protocol=2)
header = struct.pack("!L", len(payload))
message = header + payload

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 2004))
s.sendall(message)
s.close()
print 'sent', repr(data)
