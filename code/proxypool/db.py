import sqlite3

# create database
<<<<<<< HEAD
def create_db(cursor = None, db_name = "Proxy"):
=======
def create_db(cursor=cur, db_name="Proxy"):
>>>>>>> c8e34eeff2153189cb65c9250f5488c81f6174d9
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
<<<<<<< HEAD
        cursor.execute("select count(*) from {}".format(db_name))
    except Exception as e:
        cursor.execute("create table {}(ID INTEGER PRIMARY KEY AUTOINCREMENT,\
=======
        cur.execute("select count(*) from {}".format(db_name))
    except Exception as e:
        cur.execute("create table {}(ID INTEGER PRIMARY KEY AUTOINCREMENT,\
>>>>>>> c8e34eeff2153189cb65c9250f5488c81f6174d9
                                        IP TEXT NOT NULL, \
                                        ProxyType TEXT,\
                                        Status TEXT,\
                                        ResponseTime FLOAT,\
                                        Anonymity TEXT, \
                                        Country TEXT,\
                                        ShortCode TEXT, \
                                        Google INT, \
                                        ISP TEXT);".format(db_name))

def pass_data(cursor = None, connect = None, data:dict = None):
    """
    pass data to database
    """
    if data == None:
        return ValueError("No data to pass")
    #cur.execute('INSERT INTO Student VALUES(?,?,?,?,?)', (170141000,'亮','男',21,'滋麻开花'))
    else:
        cursor.execute("insert into Proxy values(NULL, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(data["origin"], data["type"], data["status"], 
                                                                                                                data["response_time"], data["anonymity"], data["country"], 
                                                                                                                data["short_code"], data["google_passed"], data["isp"]))
        connect.commit()

"""
def update_data(cursor = cur, data:dict = None):
    if data == None:
        return ValueError("No data to update")
    else:
"""

<<<<<<< HEAD
def update_adapted_db(cursor = None, connect = None,  source_db:str = "Proxy", target_db:str = "AdaptedProxy") -> int:
    cursor.execute("DROP TABLE IF EXISTS {};".format(target_db))
    create_db(cursor = cursor, db_name = target_db)
    cursor.execute("INSERT INTO {} SELECT * FROM {} WHERE Status = 1 GROUP BY IP".format(target_db, source_db))
    connect.commit()
    return 1

def update_db_data(cursor = None, db_name:str = "Proxy", data:dict = None):
    if data == None:
        return ValueError("No data to update")
    else:
        cursor.execute("UPDATE {} SET Status = {} WHERE ID = '{}'".format(db_name, data["status"], data["ID"]))
        return 1

def select_db_data(cursor = None, db_name:str = "Proxy", condition:str = "ID = 1") -> list:
    cursor.execute("SELECT * FROM {} WHERE {}".format(db_name, condition))
    return cursor.fetchall()

def get_db_data(cursor=None, db_name:str = "Proxy", condition:bool=False, data:str = None):
    if condition == True and data == None:
        return ValueError("No data to get")
    elif condition == False or data == None:
        cursor.execute("SELECT * FROM {}".format(db_name))
        return cursor.fetchall()
    else:
        cursor.execute("SELECT * FROM {} WHERE {}".format(db_name, data))

def random_get_db_data(cursor=None, db_name:str = "AdaptedProxy", proxy_type: str = "http-https", num:int = 1) -> list:
    cursor.execute('''SELECT * FROM %s WHERE ProxyType = '%s' ORDER BY RANDOM() LIMIT %d''' % (db_name, proxy_type, int(num)))
    return cursor.fetchall()

=======
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
>>>>>>> c8e34eeff2153189cb65c9250f5488c81f6174d9


if __name__ == "__main__":
    create_db(db_name = "Proxy")