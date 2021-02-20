import socket
from database import Database
from table import Table
from io import StringIO
import sys

HOST = '127.0.0.1'
PORT = 65432  

def getTable(inp):#This function finds the table in the sql query
    t = inp[inp.index("from")+1]
    return t
def getCols(inp):#This function finds the columns in the sql query
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
def getCond(inp):#This function finds the condition in the sql query if one exists
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
                        db = Database("smdb")#The database the server functions on can be changed here
                        if db.is_locked(table):#Unlock table if it is locked
                            db.unlock_table(table)
                        result = StringIO()
                        sys.stdout = result
                        if cond == "" and cols[0]=="*":#Retrive every column from the given table with no condition
                            db.select(table,cols[0])
                        elif cond != "" and cols[0]=="*":#Retrive every column from the given table with the given condition
                            db.select(table,cols[0],cond)
                        elif cond == "" and cols[0]!="*":#Retrive specific cols from the given table with no condition
                            db.select(table,cols)
                        elif cond != "" and cols[0]!="*":#Retrive specific cols from the given table with the given condition
                            db.select(table,cols,cond)
                        result_string = result.getvalue()
                        conn.sendall(result_string.encode())
                    except KeyError:#Catch table error
                        conn.sendall("Table does not exist.".encode())
                    except ValueError as e:#Catch column or condition error
                        if "is not in list" in str(e):
                            conn.sendall("Column does not exist.".encode())
                        elif "Condition is not valid" in str(e):
                            conn.sendall("Condition arguement is not correct".encode())
                    except Exception as e:#Catch any other type of error
                        conn.sendall(str(e).encode())
