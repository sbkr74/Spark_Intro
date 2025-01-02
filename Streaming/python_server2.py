import socket
import time

# Create a socket server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)
print("Listening on port 9999...")

# Accept a connection from the client (PySpark)
conn, addr = server.accept()
print(f"Connection established with {addr}")

# List of messages to send
messages = [
    "hello Spark",
    "Structured Streaming example",
    "this is a test Spark code",
    "data flows every 10 seconds"
]

# Send a message every 10 seconds
for message in messages:
    print(f"Sending message: {message}")
    conn.sendall((message + "\n").encode('utf-8'))  # Add newline to separate messages
    time.sleep(10)  # Wait for 10 seconds

# Close the connection
conn.close()
server.close()
print("Server stopped.")
