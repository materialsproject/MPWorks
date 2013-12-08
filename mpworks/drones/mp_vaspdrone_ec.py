__author__ = 'weichen'
from mpworks.drones.mp_vaspdrone import MPVaspDrone
from matgendb.creator import get_basic_analysis_and_error_checks
import os
import logging
import json

logger = logging.getLogger(__name__)

class MPVaspDrone_ec(MPVaspDrone):
    def __init__(self, host="127.0.0.1", port=27017, database="vasp",
                 user=None, password=None,  collection="tasks",
                 parse_dos=False, simulate_mode=False,
                 additional_fields=None, update_duplicates=True,
                 mapi_key=None, parse_type="force_convergence", clean_task_doc=True):
        super(MPVaspDrone_ec, self).__init__(self, host, port, database,
                 user, password,  collection, parse_dos, simulate_mode,
                 additional_fields, update_duplicates, mapi_key)
        self._parse_type=parse_type
        self._clean_task_doc=clean_task_doc

    def generate_doc(self, dir_name, vasprun_files, parse_dos,
                     additional_fields, max_force_threshold=10.0):
        d=super(MPVaspDrone_ec, self).generate_doc(dir_name, vasprun_files, parse_dos, additional_fields)
        if self._parse_type=="force_convergence":
            max_force_threshold=0.5
        try:
            force_state=get_basic_analysis_and_error_checks(d,max_force_threshold)
            d.update(force_state)
        except Exception as ex:
            logger.error("Error in " + os.path.abspath(dir_name) +
                         ".\nError msg: " + str(ex))
            return None
        if self._clean_task_doc:
            for doc in d["calculations"]:
                doc["input"]["kpoints"].pop("actual_kpoints", None)
                doc["output"].pop("eigenvalues", None)
        else:
            return d


    def process_fw(self, dir_name, d):
        super(MPVaspDrone_ec, self).process_fw(dir_name, d)
        with open(os.path.join(dir_name, 'FW.json')) as f:
            fw_dict = json.load(f)
            d['deformation_matrix'] = fw_dict['spec']['deformation_matrix']
            d['original_task_id']=fw_dict['spec']["original_task_id"]
