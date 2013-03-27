from pymongo import MongoClient
import time
from fireworks.core.launchpad import LaunchPad
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.workflows.snl_to_wf import snl_to_wf
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 27, 2013'


class SubmissionHandler(FWSerializable):

    def __init__(self, host, port, db, username, password, launchpad):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
        self.database.authenticate(username, password)

        self.launchpad = launchpad

        self.jobs = self.database.jobs

    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password, 'launchpad': self.launchpad.to_dict()}
        return d

    @classmethod
    def from_dict(cls, d):
        lp = LaunchPad.from_dict(d['launchpad'])
        return SubmissionHandler(d['host'], d['port'], d['db'], d['username'], d['password'], d['launchpad'])


    def process_submission(self):
        # TODO: sort by date (FIFO), priority
        job = self.jobs.find_and_modify({'status': 'waiting'}, {'status': 'checked out'})
        if job:
            submission_id = str(job['_id'])

            snl = StructureNL.from_dict(job)
            snl.data['_materialsproject'] = snl.data.get(['materialsproject'], {})
            snl.data['_materialsproject']['submission_id'] = submission_id

            # TODO: create a real SNL step
            snl.data['_materialsproject']['snl_id'] = submission_id
            snl.data['_materialsproject']['snlgroup_id'] = submission_id
            snl.data['_materialsproject']['snlgroupSG_id'] = submission_id

            # create a workflow
            wf = snl_to_wf(snl)
            self.launchpad.add_wf(wf)
            print 'ADDED A JOB TO THE WORKFLOW!'
            return int(job['_id'])

    def process_submissions(self):
        last_id = -1
        while last_id:
            last_id = self.process_submission()

    def sleep_and_process(self):
        while True:
            self.process_submissions()
            print 'looked for submissions, sleeping 60s'
            time.sleep(60)
