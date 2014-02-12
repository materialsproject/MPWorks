import time
import traceback

from fireworks.core.firework import FireWork, Workflow
from structure_prediction_tasks.prediction_mongo import SPSubmissionsMongoAdapter
from mpworks.firetasks.structure_prediction_task import StructurePredictionTask


__author__ = 'William Richards'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 08, 2013'

# Turn submissions into workflows
class SPSubmissionProcessor():
    # This is run on the server end
    def __init__(self, apps_db, launchpad):
        self.apps_db = apps_db
        self.launchpad = launchpad
        self.spsma = SPSubmissionsMongoAdapter(apps_db)

    def run(self, sleep_time=30, infinite=False):
        while True:
            self.submit_workflows()
            if not infinite:
                break
            print 'sleeping', sleep_time
            time.sleep(sleep_time)
            
    def submit_workflows(self):
        for e in self.spsma.pred_coll.find({'state': 'SUBMITTED'}):
            self.spsma.pred_coll.update({'_id' : e['_id']}, {'$set': {'state': 'WAITING'}})
            submission_id = e['structure_predictor_id']
            try:
                firework = FireWork([StructurePredictionTask()], 
                                    spec = {'elements' : e['elements'],
                                            'element_oxidation_states': e['element_oxidation_states'],
                                            'threshold' : e['threshold'],
                                            'structure_predictor_id' : submission_id})
                wf = Workflow([firework], metadata={'sp_id' : submission_id})
                self.launchpad.add_wf(wf)
                print 'ADDED WORKFLOW FOR {}'.format(submission_id)
            except:
                self.spsma.pred_coll.find_and_modify({'structure_predictor_id': submission_id},
                                          {'$set': {'state': 'ERROR',
                                    'state_details': traceback.format_exc()}})
                traceback.print_exc()

