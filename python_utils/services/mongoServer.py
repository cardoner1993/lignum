from pymongo import MongoClient


class MongoServer(object):

    def __init__(self, host, port, authDb, user, password):
        self._host = host
        self._port = port
        self._authDb = authDb
        self._user = user
        self._password = password
        self._connectionString = "mongodb://%s:%s@%s:%s/?authSource=%s" % \
                                 (self._user, self._password, self._host, self._port, self._authDb)
        self._connection = MongoClient(self._connectionString)

    def connection(self):
        return self._connection





