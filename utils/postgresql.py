import psycopg2


def getConnection():
    conn = psycopg2.connect(database="windpowerdb", user="gpadmin", password="gpadmin", host="10.0.2.23",
                            port="1809")

    print("Opened database successfully")

    return conn


def getRows(sql):
    conn = getConnection()

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    conn.close

    return rows
