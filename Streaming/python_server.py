import socket

# Create a socket server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)
print("Listening on port 9999...")

conn, addr = server.accept()
print(f"Connection established with {addr}")

while True:
    message = input("Enter message to send: ")
    conn.sendall(message.encode('utf-8'))
