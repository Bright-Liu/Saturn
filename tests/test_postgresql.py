from utils import postgresql

rows = postgresql.getRows('SELECT name,age FROM test_schema.t_user')
for row in rows:
    print(row[0], row[1], "\n")
