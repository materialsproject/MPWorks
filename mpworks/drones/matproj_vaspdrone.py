import json
import os
from matgendb.creator import VaspToDbTaskDrone

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 26, 2013'


class MatprojVaspDrone(VaspToDbTaskDrone):

    @classmethod
    def post_process(cls, dir_name, d):
        # run the post-process of the superclass
        VaspToDbTaskDrone.post_process(dir_name, d)

        # custom Materials Project post-processing
        with open(os.path.join(dir_name, 'FW.json')) as f:
            fw_dict = json.load(f)
            d['fw_id'] = fw_dict['fw_id']
            d['snl'] = fw_dict['spec']['snl']
            d['snl_id'] = fw_dict['spec']['snl_id']
            d['snlgroup_id'] = fw_dict['spec']['snlgroup_id']
            d['snlgroupSG_id'] = fw_dict['spec']['snlgroupSG_id']
            d['submission_id'] = fw_dict['spec'].get('submission_id', None)
            d['task_type'] = fw_dict['spec']['prev_task_type']