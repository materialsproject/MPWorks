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


class SubmissionHandler():
    class Submissions(FWSerializable):

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
            return SubmissionHandler.Submissions(d['host'], d['port'], d['db'], d['username'], d['password'])

    def __init__(self, submissions, launchpad):
        self.jobs = submissions.jobs
        self.launchpad = launchpad

    def _process_submission(self):
        # TODO: sort by date (FIFO), priority
        job = self.jobs.find_and_modify({'status': 'queued'}, {'$set': {'status': 'waiting'}})
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
            self.launchpad.add_wf(wf)
            print 'ADDED A JOB TO THE WORKFLOW!'
            return submission_id

    def _process_state(self, wf):
        states = [fw.state for fw in wf.fws]
        if all([s == 'COMPLETED' for s in states]):
            return 'completed'
        elif any([s == 'FIZZLED' for s in states]):
            return 'fizzled'
        elif any([s == 'COMPLETED' for s in states]) or any([s == 'RUNNING' for s in states]):
            return 'running'

        return 'waiting'

    def _update_states(self):
        # find all submissions that are not completed and update the state
        for s_id in self.jobs.find({'status': {'$in': ['waiting', 'running']}}, {'_id': 1}):
            s_id = str(s_id['_id'])
            print 'PROCESSING', s_id
            # get a fw_id with this submission id
            # TODO: make this cleaner
            # TODO: note this assumes each submission has 1 workflow only
            fw_id = self.launchpad.get_fw_ids({'spec.submission_id': s_id})[0]
            print 'GOT FW', fw_id
            # get a workflow
            wf = self.launchpad.get_wf_by_fw_id(fw_id)
            # update workflow
            new_state = self._process_state(wf)
            self.update_status(s_id, new_state)
            print 'UPDATED TO', new_state

    def process_submissions(self):
        last_id = -1
        while last_id:
            last_id = self._process_submission()
        self._update_states()

    def sleep_and_process(self):
        while True:
            self.process_submissions()
            print 'looked for submissions, sleeping 60s'
            time.sleep(60)

    def update_status(self, oid, status):
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {'status': status}})

    def update_taskstatus(self, oid, task_type, tid):
        status = 'finished ' + task_type
        self.update_status(oid, status)

        task_key = 'task_dict.' + task_type
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {task_key: tid}})

    @classmethod
    def auto_load(cls):
        s_dir = os.environ['DB_LOC']
        s_file = os.path.join(s_dir, 'submission.yaml')
        s = SubmissionHandler.Submissions.from_file(s_file)

        l_dir = FWConfig().CONFIG_FILE_DIR
        l_file = os.path.join(l_dir, 'my_launchpad.yaml')
        lp = LaunchPad.from_file(l_file)

        return SubmissionHandler(s, lp)

if __name__ == '__main__':
    sh = SubmissionHandler.auto_load()
    sh.sleep_and_process()