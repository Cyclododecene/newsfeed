import sqlite3

con = sqlite3.connect("data/proxy.db")
cur = con.cursor()

# create database
def create_db(cursor=cur, db_name="Proxy"):
    """
    create table for  proxy with:
    1. RowID PRIMARY KEY - AUTO INCREMENT
    2. IP
    3. Proxy Type
    4. Status
    5. Response Time
    6. Anonymous Level
    7. Country
    8. Country Short Code
    9. Google Passed or not 
    10. ISP
    """
    try:
        cur.execute("select count(*) from {}".format(db_name))
    except Exception as e:
        cur.execute("create table {}(ID INTEGER PRIMARY KEY AUTOINCREMENT,\
                                        IP TEXT NOT NULL, \
                                        ProxyType TEXT,\
                                        Status TEXT,\
                                        ResponseTime FLOAT,\
                                        Anonymity TEXT, \
                                        Country TEXT,\
                                        ShortCode TEXT, \
                                        Google INT, \
                                        ISP TEXT);".format(db_name))

def pass_data(cursor = cur, data:dict = None):
    """
    pass data to database
    """
    if data == None:
        return ValueError("No data to pass")
    #cur.execute('INSERT INTO Student VALUES(?,?,?,?,?)', (170141000,'亮','男',21,'滋麻开花'))
    else:
        cur.execute("insert into Proxy values(NULL, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(data["origin"], data["type"], data["status"], 
                                                                                                                data["response_time"], data["anonymity"], data["country"], 
                                                                                                                data["short_code"], data["google_passed"], data["isp"]))
        con.commit()

"""
def update_data(cursor = cur, data:dict = None):
    if data == None:
        return ValueError("No data to update")
    else:
"""

def update_adapted_db(cursor = cur, source_db:str = "Proxy", target_db:str = "AdaptedProxy") -> int:
    cur.execute("DROP TABLE IF EXISTS {};".format(target_db))
    create_db(cursor = cur, db_name = target_db)
    cur.execute("INSERT INTO {} SELECT * FROM {} WHERE Status = 1 GROUP BY IP".format(target_db, source_db))
    con.commit()
    return 1

def update_db_data(cursor = cur, db_name:str = "Proxy", data:dict = None):
    if data == None:
        return ValueError("No data to update")
    else:
        cur.execute("UPDATE {} SET Status = {} WHERE ID = '{}'".format(db_name, data["status"], data["ID"]))
        return 1

def select_db_data(cursor = cur, db_name:str = "Proxy", condition:str = "ID = 1") -> list:
    cur.execute("SELECT * FROM {} WHERE {}".format(db_name, condition))
    return cur.fetchall()


if __name__ == "__main__":
    create_db(db_name = "Proxy")