import os
from bson.objectid import ObjectId
from pymongo import MongoClient
import time
from fireworks.core.fw_config import FWConfig
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

    def __init__(self, host, port, db, username, password):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
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

    def process_submission(self, launchpad):
        # TODO: sort by date (FIFO), priority
        job = self.jobs.find_and_modify({'status': 'queued'}, {'$set': {'status': 'waiting to run'}})
        if job:
            submission_id = str(job['_id'])

            snl = StructureNL.from_dict(job)
            snl.data['_materialsproject'] = snl.data.get('_materialsproject', {})
            snl.data['_materialsproject']['submission_id'] = submission_id

            # TODO: create a real SNL step
            snl.data['_materialsproject']['snl_id'] = submission_id
            snl.data['_materialsproject']['snlgroup_id'] = submission_id
            snl.data['_materialsproject']['snlgroupSG_id'] = submission_id

            # create a workflow
            wf = snl_to_wf(snl)
            launchpad.add_wf(wf)
            print 'ADDED A JOB TO THE WORKFLOW!'
            return submission_id

    def process_submissions(self, launchpad):
        last_id = -1
        while last_id:
            last_id = self.process_submission(launchpad)

    def sleep_and_process(self, launchpad):
        while True:
            self.process_submissions(launchpad)
            print 'looked for submissions, sleeping 60s'
            time.sleep(60)

    def update_status(self, oid, task_type, tid):
        status = 'finished ' + task_type
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {'status': status}})

        task_key = 'task_dict.' + task_type
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {task_key: tid}})

if __name__ == '__main__':

    s_dir = os.environ['DB_LOC']
    s_file = os.path.join(s_dir, 'submission.yaml')
    sh = SubmissionHandler.from_file(s_file)

    l_dir = FWConfig().CONFIG_FILE_DIR
    l_file = os.path.join(l_dir, 'my_launchpad.yaml')
    lp = LaunchPad.from_file(l_file)

    sh.sleep_and_process(lp)