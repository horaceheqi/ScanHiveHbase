from impala.dbapi import connect

if __name__ == '__main__':
    cursor = connect(host='10.10.67.48', port=10000, user='hadoop', password='hadoop').cursor()
    print(cursor)
    cursor.execute('SHOW TABLES')
    print(cursor.description)
    status = cursor.poll().operationState
    for table in cursor.fetchall():
        print(table)
    results = cursor.fetchall()
