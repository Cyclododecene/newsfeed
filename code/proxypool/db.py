from multiprocessing.sharedctypes import Value
import sqlite3

con = sqlite3.connect("data/proxy.db")
cur = con.cursor()

# create database
def create_db(cursor=cur):
    """
    create table for  proxy with:
    1. RowID PRIMARY KEY - AUTO INCREMENT
    2. IP
    3. Proxy Type
    4. Status
    4. Response Time
    5. Anonymous Level
    6. Country
    7. Country Short Code
    8. Google Passed or not 
    """
    try:
        cur.execute("select count(*) from Proxy")
    except Exception as e:
        cur.execute("create table Proxy(ID INTEGER PRIMARY KEY AUTOINCREMENT,\
                                        IP TEXT NOT NULL, \
                                        ProxyType TEXT,\
                                        Status TEXT,\
                                        ResponseTime FLOAT,\
                                        Anonymity TEXT, \
                                        Country TEXT,\
                                        ShortCode TEXT, \
                                        Google INT);")

def pass_data(cursor = cur, data:dict = None):
    """
    pass data to database
    """
    if data == None:
        return ValueError("No data to pass")
    #cur.execute('INSERT INTO Student VALUES(?,?,?,?,?)', (170141000,'亮','男',21,'滋麻开花'))
    else:
        cur.execute("insert into Proxy values(NULL, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(data["origin"], data["type"], data["status"], 
                                                                                                            data["response_time"], data["anonymity"], data["country"], 
                                                                                                            data["short_code"], data["google_passed"]))
        con.commit()

def update_data(cursor = cur, data:dict = None):
    """
    update data in database
    """
    if data == None:
        return ValueError("No data to update")
    else: