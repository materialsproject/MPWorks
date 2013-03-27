from pymongo import MongoClient
from fireworks.utilities.fw_serializers import FWSerializable

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 27, 2013'


class SubmissionHandler(FWSerializable):

    def __init__(self, host, port, db, username=None, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
        if username:
            self.database.authenticate(username, password)

        self.jobs = self.database.jobs

    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password}
        return d

    @classmethod
    def from_dict(cls, d):
        return SubmissionHandler(d['host'], d['port'], d['db'], d['username'], d['password'])