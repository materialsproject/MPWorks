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
        job = self.jobs.find_and_modify({'status': 'submitted'}, {'$set': {'status': 'waiting'}})
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

    def _process_state(self, wf, s_id):

        # get status
        m_state = 'waiting'
        states = [fw.state for fw in wf.fws]
        if all([s == 'COMPLETED' for s in states]):
            m_state = 'completed'
        elif any([s == 'FIZZLED' for s in states]):
            m_state = 'fizzled'
        elif any([s == 'COMPLETED' for s in states]) or any([s == 'RUNNING' for s in states]):
            m_state = 'running'

        self.update_status(s_id, m_state)

        details = m_state
        for fw in wf.fws:
            if fw.state == 'READY':
                details = 'waiting to run: {}'.format(fw.spec['task_type'])
            elif fw.state in ['RESERVED', 'RUNNING', 'FIZZLED']:
                machine_name = 'unknown'
                for l in fw.launches:
                    if l.state == fw.state:
                        machine_name = '{}-{}'.format(l.host, l.ip)
                        break
                if fw.state == 'RESERVED':
                    details = 'queued to run: {} on {}'.format(fw.spec['task_type'], machine_name)
                if fw.state == 'RUNNING':
                    details = 'running: {} on {}'.format(fw.spec['task_type'], machine_name)
                if fw.state == 'FIZZLED':
                    details = 'fizzled while running: {} on {}'.format(fw.spec['task_type'], machine_name)

        self.update_detailed_status(s_id, details)

        m_taskdict = {}
        if any([s == 'COMPLETED' for s in states]):
            for fw in wf.fws:
                if fw.state == 'COMPLETED' and fw.spec['task_type'] == 'VASP db insertion':
                    for l in fw.launches:
                        if l.state == 'COMPLETED':
                            t_id = l.action.stored_data['task_id']
                            m_taskdict[fw.spec['prev_task_type']] = t_id
                            break

        self.update_taskdict(s_id, m_taskdict)

        return m_state, details, m_taskdict

    def _update_states(self):
        # find all submissions that are not completed and update the state
        for s_id in self.jobs.find({'status': {'$in': ['waiting', 'running']}}, {'_id': 1}):
            s_id = str(s_id['_id'])
            print 'PROCESSING', s_id
            # get a fw_id with this submission id
            # TODO: make this cleaner
            # TODO: note this assumes each submission has 1 workflow only
            fw_id = self.launchpad.get_fw_ids({'spec.submission_id': s_id})[0]
            # get a workflow
            wf = self.launchpad.get_wf_by_fw_id(fw_id)
            # update workflow
            new_state, details, new_dict = self._process_state(wf, s_id)
            print 'UPDATED TO', new_state, details, new_dict

    def process_submissions(self):
        last_id = -1
        while last_id:
            last_id = self._process_submission()
        self._update_states()

    def sleep_and_process(self):
        while True:
            self.process_submissions()
            print 'looked for submissions, sleeping 30s'
            time.sleep(30)

    def update_status(self, oid, status):
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {'status': status}})

    def update_detailed_status(self, oid, status):
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {'detailed_status': status}})

    def update_taskdict(self, oid, task_dict):
        self.jobs.find_and_modify({'_id': ObjectId(oid)}, {'$set': {'task_dict': task_dict}})

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