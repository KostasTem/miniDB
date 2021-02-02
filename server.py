import socket
from database import Database
from table import Table
from io import StringIO
import sys

HOST = '127.0.0.1'
PORT = 65432  

def getTable(inp):
    t = inp[inp.index("from")+1]
    return t
def getCols(inp):
    st = inp.index("select")
    end = inp.index("from")
    cols = list()
    if end-(st+1)==1:
        if "," in inp[st+1]:
            tmp = inp[st+1].split(",")
            for el in tmp:
                cols.append(el.strip())
        else:
            cols.append(inp[st+1])
    elif end-st>1:
        for i in range(1,end-st):
            if inp[st+i].strip()!=",":
                cols.append(inp[st+i].strip().replace(",",""))
    return cols
def getCond(inp):
    if "where" in inp:
        w = inp.index("where")
        if len(inp) - w == 1:
            return inp[w+1]
        else:
            tmp = ""
            for i in range(1,len(inp)-w):
                tmp += inp[w+i].strip()
            return tmp
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
                data = data.split(" ")
                for el in data:
                    el = el.strip()
                    if el == " ":
                        data.remove(el)
                if "select" not in data and "from" not in data:
                    conn.sendall("Incorrect Input".encode())
                elif data[0] == "select":
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
                        elif cond == "" and cols[0]!="*" and len(cols)==1:
                            db.select(table,cols)
                        elif cond != "" and cols[0]!="*" and len(cols)==1:
                            db.select(table,cols)
                        elif cond == "" and len(cols)>1:
                            db.select(table,cols)
                        elif cond != "" and len(cols)>1:
                            db.select(table,cols,cond)
                        result_string = result.getvalue()
                        conn.sendall(result_string.encode())
                    except Exception as e:
                        conn.sendall(str(e).encode())