import psycopg2


def getConnection(host, port, user, password, databse):
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=databse)
    print("Operation[Connection Create] successfully")
    return conn


def getRows(connection, sql):
    cur = connection.cursor()

    cur.execute(sql)
    rows = cur.fetchall()
    print("Operation[SELECT] done successfully")

    connection.close()
    print("Operation[Connection Close] done successfully")

    return rows
