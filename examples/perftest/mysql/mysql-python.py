import zmq
import MySQLdb


ctx = zmq.Context(1)
sock = ctx.socket(zmq.REP)
sock.connect('tcp://localhost:7004')
mysql = MySQLdb.connect(host='localhost', user='test', db='test_zorro')


while True:
    uri, = sock.recv_multipart()
    uri = uri.decode('utf-8')
    cur = mysql.cursor()
    cur.execute("INSERT INTO visits (uri, visits) VALUES (%s, 1)"
                " ON DUPLICATE KEY UPDATE"
                " visits = visits + VALUES(visits)", uri)
    cur.execute("SELECT visits FROM visits WHERE uri = %s", uri)
    for row in cur:
        nvisits = row[0]
    sock.send_multipart([(uri + ' ' + str(nvisits)).encode('utf-8')])
