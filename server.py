import socket
from database import Database
from table import Table
from io import StringIO
import sys

HOST = '127.0.0.1'
PORT = 65432  

def getTable(inp):
    t = inp.split("from")[1]
    t = t.split(" ")[1]
    return t.strip()
def getCols(inp):
    cols = inp.split("from")[0]
    cols = cols.split("select")[1]
    if "," in cols:
        cols = cols.split(",")
    if type(cols) is list:
        for i in range(len(cols)):
            cols[i] = cols[i].strip()
        return cols
    else:
        cols = cols.strip()
        return [cols]
def getCond(inp):
    if "where" in inp:
        return inp.split("where")[1].strip()
    else:
        return ""

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        print('Connected by', addr)
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode("utf-8")
                data = data.lower()
                if data.startswith("select"):
                    try:
                        table = getTable(data)
                        cols = getCols(data)
                        cond = getCond(data).replace(" ","")
                        db = Database("smdb")
                        result = StringIO()
                        sys.stdout = result
                        if db.is_locked(table):
                            db.unlock_table(table)
                        if cond == "" and cols[0]=="*":
                            db.select(table,cols[0])
                        elif cond != "" and cols[0]=="*":
                            db.select(table,cols[0],cond)
                        elif cond == "" and len(cols)>1:
                            db.select(table,cols)
                        elif cond != "" and len(cols)>1:
                            db.select(table,cols,cond)
                        result_string = result.getvalue()
                        conn.sendall(result_string.encode())
                    except:
                        conn.sendall("Input Error".encode())