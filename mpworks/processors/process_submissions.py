import time
import traceback
from fireworks.core.launchpad import LaunchPad
from mpworks.snl_utils.mpsnl import MPStructureNL
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from mpworks.workflows.snl_to_wf import snl_to_wf
from mpworks.workflows.wf_utils import NO_POTCARS
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 08, 2013'

# Turn submissions into workflows, and updates the state of the submissions DB

class SubmissionProcessor():
    MAX_SITES = 200

    # This is run on the server end
    def __init__(self, sma, launchpad):
        self.sma = sma
        self.jobs = sma.jobs
        self.launchpad = launchpad

    def run(self, sleep_time=None, infinite=False):
        sleep_time = sleep_time if sleep_time else 30
        while True:
            self.submit_all_new_workflows()
            print "Updating existing workflows..."
            self.update_existing_workflows()  # for updating the display
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
                if 'snl_id' in job:
                    snl = MPStructureNL.from_dict(job)
                else:
                    snl = StructureNL.from_dict(job)
                if len(snl.structure.sites) > SubmissionProcessor.MAX_SITES:
                    self.sma.update_state(submission_id, 'REJECTED', 'too many sites', {})
                    print 'REJECTED WORKFLOW FOR {} - too many sites ({})'.format(
                        snl.structure.formula, len(snl.structure.sites))
                elif not job['is_valid']:
                    self.sma.update_state(submission_id, 'REJECTED',
                                          'invalid structure (atoms too close)', {})
                    print 'REJECTED WORKFLOW FOR {} - invalid structure'.format(
                        snl.structure.formula)
                elif len(set(NO_POTCARS) & set(job['elements'])) > 0:
                    self.sma.update_state(submission_id, 'REJECTED',
                                          'invalid structure (no POTCAR)', {})
                    print 'REJECTED WORKFLOW FOR {} - invalid element (No POTCAR)'.format(
                        snl.structure.formula)
                elif not job['is_ordered']:
                    self.sma.update_state(submission_id, 'REJECTED',
                                          'invalid structure (disordered)', {})
                    print 'REJECTED WORKFLOW FOR {} - invalid structure'.format(
                        snl.structure.formula)
                else:
                    snl.data['_materialsproject'] = snl.data.get('_materialsproject', {})
                    snl.data['_materialsproject']['submission_id'] = submission_id

                    # create a workflow
                    wf = snl_to_wf(snl, job['parameters'])
                    self.launchpad.add_wf(wf)
                    print 'ADDED WORKFLOW FOR {}'.format(snl.structure.formula)
            except:
                self.jobs.find_and_modify({'submission_id': submission_id},
                                          {'$set': {'state': 'ERROR'}})
                traceback.print_exc()

            return submission_id

    def update_existing_workflows(self):
        # updates the state of existing workflows by querying the FireWorks database
        # this is an optional step that updates the submissions db with jobs info
        # it is useful for the frontend display but not needed for workflow execution
        for submission in self.jobs.find({'state': {'$nin': ['COMPLETED', 'ERROR', 'REJECTED', 'CANCELLED']}},
                                         {'submission_id': 1}):
            submission_id = submission['submission_id']
            try:
                self.update_wf_state(submission_id)
            except:
                print 'ERROR while processing s_id', submission_id
                traceback.print_exc()
        

    def update_wf_state(self, submission_id):
        # state of the workflow
        tasks = {}
        
        wf = self.launchpad.workflows.find_one({'metadata.submission_id': submission_id},
                                               sort=[('updated_on', -1)])
        details = '(none)'
        for e in self.launchpad.fireworks.find({'fw_id': {'$in' : wf['nodes']}},
                            {'spec.task_type': 1 ,'state': 1, 'launches': 1}):
            if e['spec']['task_type'] == 'VASP db insertion' and \
                    e['state'] == 'COMPLETED':
                for launch in self.launchpad.launches.find({'launch_id': {'$in' : e['launches']}},
                                                           {'action.stored_data.task_id': 1,
                                                            'action.update_spec.prev_task_type' : 1}):
                    try:
                        tasks[launch['action']['update_spec']['prev_task_type']] \
                            = launch['action']['stored_data']['task_id']
                        break
                    except:
                        pass
        
        self.sma.update_state(submission_id, wf['state'], details, tasks)    
        return wf['state'], details, tasks

    @classmethod
    def auto_load(cls):
        sma = SubmissionMongoAdapter.auto_load()
        lp = LaunchPad.auto_load()

        return SubmissionProcessor(sma, lp)