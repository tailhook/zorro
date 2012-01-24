from zorro import Hub, zmq, mysql


hub = Hub()
sql = mysql.Mysql(host='localhost', user='test', database='test_zorro')


def replier(uri):
    uri = uri.decode('utf-8')
    sql.execute_prepared("INSERT INTO visits (uri, visits) VALUES (?, 1)"
                " ON DUPLICATE KEY UPDATE"
                " visits = visits + VALUES(visits)", uri)
    for row in sql.query_prepared(
        "SELECT visits FROM visits WHERE uri = ?", uri):
        nvisits = row[0]
    return uri + ' ' + str(nvisits)


@hub.run
def main():
    sock = zmq.rep_socket(replier)
    sock.connect('tcp://localhost:7004')
