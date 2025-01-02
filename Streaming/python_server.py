import socket
import time

# Create a socket server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)
print("Listening on port 9999...")

conn, addr = server.accept()
print(f"Connection established with {addr}")
count = 0
while True:
    message = input("Enter message to send: ")
    conn.sendall((message+"\n").encode('utf-8'))
    time.sleep(2)
    count+=1
    if count == 5:
        break

# Close the connection
conn.close()
server.close()
print("Server stopped.")