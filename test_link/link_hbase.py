import happybase

if __name__ == '__main__':
    connection = happybase.Connection('10.10.67.48')
    table = connection.table('cid_md5')
    for key, data in table.scan(limit=10000, batch_size=10):
        print(key, data)