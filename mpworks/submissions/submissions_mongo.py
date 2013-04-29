import os
import traceback
import datetime
from pymongo import MongoClient, ASCENDING
import time
from fireworks.core.fw_config import FWConfig
from fireworks.core.launchpad import LaunchPad
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.submissions.submission_handler import SubmissionHandler
from mpworks.workflows.snl_to_wf import snl_to_wf
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 26, 2013'

# TODO: support priority as a parameter
# TODO: vary the workflow depending on params


class SubmissionMongoAdapter(FWSerializable):
    # This is the user interface to submissions

    def __init__(self, host='localhost', port=27017, db='snl', username=None, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
        if self.username:
            self.database.authenticate(username, password)

        self.jobs = self.database.jobs
        self.id_assigner = self.database.id_assigner

        self._update_indices()

    def _reset(self):
        self._restart_id_assigner_at(1, 1)
        self.jobs.remove()

    def _update_indices(self):
        self.snl.ensure_index('submission_id', unique=True)
        self.snl.ensure_index('state')
        self.snl.ensure_index('submitter_email')

    def _get_next_submission_id(self):
        return self.id_assigner.find_and_modify(query={}, update={'$inc': {'next_submission_id': 1}})['next_submission_id']

    def _restart_id_assigner_at(self, next_submission_id):
        self.id_assigner.remove()
        self.id_assigner.insert({"next_submission_id": next_submission_id})

    def submit_snl(self, snl, submitter_email, parameters=None):
        parameters = parameters if parameters else {}

        d = snl.to_dict
        d['submitter_email'] = submitter_email
        d['parameters'] = parameters
        d['state'] = 'submitted'
        d['state_details'] = {}
        d['task_dict'] = {}
        d['submission_id'] = self._get_next_submission_id()
        d['submitted_at'] = datetime.datetime.utcnow().isoformat()
        self.jobs.insert(d)

        return d['submission_id']

    def cancel_submission(self, submission_id):
        # TODO: implement me
        # set state to 'cancelled'
        # in the SubmissionProcessor, detect this state and defuse the FW
        raise NotImplementedError()

    def get_state(self, submission_id):
        info = self.jobs.find_one({'submission_id': submission_id}, {'state': 1, 'state_details': 1, 'task_dict': 1})
        return info['state'], info['state_details'], info['task_dict']

    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password}
        return d

    def update_state(self, submission_id, state, state_details, task_dict):
        self.jobs.find_and_modify({'submission_id': submission_id}, {'$set': {'state': state}})

    @classmethod
    def from_dict(cls, d):
        return SubmissionMongoAdapter(d['host'], d['port'], d['db'], d['username'], d['password'])

    @classmethod
    def auto_load(cls):
        s_dir = os.environ['DB_LOC']
        s_file = os.path.join(s_dir, 'submission_db.yaml')
        return SubmissionMongoAdapter.from_file(s_file)


class SubmissionProcessor():
    # This is run on the server end
    def __init__(self, sma, launchpad):
        self.sma = sma
        self.jobs = sma.jobs
        self.launchpad = launchpad

    def run(self):
        while True:
            self.submit_all_new_workflows()
            self.update_existing_workflows()
            print 'sleeping 30s'
            time.sleep(30)

    def submit_all_new_workflows(self):
        last_id = -1
        while last_id:
            last_id = self.submit_new_workflow()

    def submit_new_workflow(self):
        # finds a submitted job, creates a workflow, and submits it to FireWorks
        job = self.jobs.find_and_modify({'status': 'submitted'}, {'$set': {'status': 'waiting'}})
        if job:
            submission_id = job['submission_id']
            try:
                snl = StructureNL.from_dict(job)
                snl.data['_materialsproject'] = snl.data.get('_materialsproject', {})
                snl.data['_materialsproject']['submission_id'] = submission_id

                # create a workflow
                wf = snl_to_wf(snl)
                self.launchpad.add_wf(wf)
                print 'ADDED A JOB TO THE WORKFLOW!'
            except:
                traceback.format_exc()
                self.jobs.find_and_modify({'snl_id': submission_id}, {'$set': {'status': 'error'}})
            return submission_id

    def update_existing_workflows(self):
        # updates the state of existing workflows by querying the FireWorks database
        for submission_id in self.jobs.find({'status': {'$in': ['waiting', 'running']}}, {'submission_id': 1}):
            submission_id = str(submission_id['submission_id'])
            try:
                # get a fw_id with this submission id
                fw_id = self.launchpad.get_fw_ids({'spec.submission_id': submission_id})[0]
                # get a workflow
                wf = self.launchpad.get_wf_by_fw_id(fw_id)
                # update workflow
                self.update_wf_state(wf, submission_id)
            except:
                print 'ERROR while processing s_id', submission_id
                traceback.print_exc()

    def update_wf_state(self, wf, submission_id):
        # state of the workflow

        details = '(none available)'
        for fw in wf.fws:
            if fw.state == 'READY':
                details = 'waiting to run: {}'.format(fw.spec['task_type'])
            elif fw.state in ['RESERVED', 'RUNNING', 'FIZZLED']:
                machine_name = 'unknown'
                for l in fw.launches:
                    if l.state == fw.state:
                        machine_name = 'unknown'
                        if 'hopper' in l.host or 'nid' in l.host:
                            machine_name = 'hopper'
                        elif 'c' in l.host:
                            machine_name = 'mendel/carver'
                        break
                if fw.state == 'RESERVED':
                    details = 'queued to run: {} on {}'.format(fw.spec['task_type'], machine_name)
                if fw.state == 'RUNNING':
                    details = 'running: {} on {}'.format(fw.spec['task_type'], machine_name)
                if fw.state == 'FIZZLED':
                    details = 'fizzled while running: {} on {}'.format(fw.spec['task_type'], machine_name)


        m_taskdict = {}
        states = [fw.state for fw in self.fws]
        if any([s == 'COMPLETED' for s in states]):
            for fw in wf.fws:
                if fw.state == 'COMPLETED' and fw.spec['task_type'] == 'VASP db insertion':
                    for l in fw.launches:
                        if l.state == 'COMPLETED':
                            t_id = l.action.stored_data['task_id']
                            m_taskdict[fw.spec['prev_task_type']] = t_id
                            break

        self.sma.update_state(wf.state, details, m_taskdict)
        return wf.state, details, m_taskdict

    @classmethod
    def auto_load(cls):
        sma = SubmissionMongoAdapter.auto_load()

        l_dir = FWConfig().CONFIG_FILE_DIR
        l_file = os.path.join(l_dir, 'my_launchpad.yaml')
        lp = LaunchPad.from_file(l_file)

        return SubmissionHandler(sma, lp)