import socket

HOST = '127.0.0.1'
PORT = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
        q = input("Enter an sql query(Type exit if you want to close the client): ")
        if q=="exit":
            break
        else:
            s.sendall(q.encode())
            data = s.recv(1024)
            print('Output:', data.decode())
