import os
import time
import traceback
from fireworks.core.fw_config import FWConfig
from fireworks.core.launchpad import LaunchPad
from fireworks.core.firework import FireWork, Workflow

from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.structure_prediction.prediction_mongo import SPSubmissionsMongoAdapter
from mpworks.firetasks.structure_prediction_task import StructurePredictionTask
from pymatgen import Specie

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 08, 2013'

# Turn submissions into workflows, and updates the state of the submissions DB

class SPSubmissionProcessor():

    # This is run on the server end
    def __init__(self, spsma, launchpad):
        self.spsma = spsma
        self.jobs = spsma.jobs
        self.launchpad = launchpad

    def run(self, sleep_time=None, infinite=False):
        sleep_time = sleep_time if sleep_time else 30
        while True:
            self.submit_all_new_workflows()
            self.update_existing_workflows()
            if not infinite:
                break
            print 'sleeping', sleep_time
            time.sleep(sleep_time)

    def submit_all_new_workflows(self):
        last_id = -1
        while last_id:
            last_id = self.submit_new_workflow()

    def submit_new_workflow(self):
        # finds a submitted job, creates a workflow, and submits it to FireWorks
        job = self.jobs.find_and_modify({'state': 'SUBMITTED'}, {'$set': {'state': 'WAITING'}})
        if job:
            submission_id = job['submission_id']
            try:
                firework = FireWork([StructurePredictionTask()], 
                                    spec = {'species' : job['species'],
                                            'threshold' : job['threshold'],
                                            'submission_id' : submission_id})
                wf = Workflow([firework], metadata={'submission_id' : submission_id})
                self.launchpad.add_wf(wf)
                print 'ADDED WORKFLOW FOR {}'.format(job['species'])
            except:
                self.jobs.find_and_modify({'submission_id': submission_id},
                                          {'$set': {'state': 'ERROR'}})
                traceback.print_exc()
            return submission_id

    def update_existing_workflows(self):
        # updates the state of existing workflows by querying the FireWorks database
        for submission in self.jobs.find({'state': {'$nin': ['COMPLETED', 'ERROR', 'REJECTED', 'SUBMITTED']}},
                                         {'submission_id': 1}):
            submission_id = submission['submission_id']
            try:
                # get a wf with this submission id
                fw_id = self.launchpad.get_wf_ids({'metadata.submission_id': submission_id}, limit=1)[0]
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
                details = 'waiting to run'
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
                    details = 'queued to run on {}'.format(machine_name)
                if fw.state == 'RUNNING':
                    details = 'running on {}'.format(machine_name)
                if fw.state == 'FIZZLED':
                    details = 'fizzled while running on {}'.format(machine_name)

        self.spsma.update_state(submission_id, wf.state, details)
        return wf.state, details

    @classmethod
    def auto_load(cls):
        spsma = SPSubmissionsMongoAdapter.auto_load()
        lp = LaunchPad.auto_load()

        return SPSubmissionProcessor(spsma, lp)