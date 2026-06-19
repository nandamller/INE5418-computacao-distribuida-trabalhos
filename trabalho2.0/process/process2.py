import os

import socket
from Process import BaseProcess


HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT","6062"))


class Process2(BaseProcess):
    def execute(self):
        print(f"[Process 2] Started. Listening on {self.host}:{self.port}...")
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Allows re-binding to the port immediately after closing
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            
            connection, address = s.accept()
            with connection:
                print(f"[Process 2] Connection established with {address}")
                
                while True:
                    data = connection.recv(1024)
                    if not data:
                        break
                    
                    received_msg = data.decode('utf-8')
                    print(f"[Process 2] Received: '{received_msg}'")
                    
                    # Echo back response
                    response = f"Process 2 confirmed: {received_msg}"
                    connection.sendall(response.encode('utf-8'))
                    
        print("[Process 2] Finished.")

if __name__ == '__main__':
    p1 = Process2(host=HOST, port=PORT)
    p1.execute()