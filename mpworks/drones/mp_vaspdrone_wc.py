__author__ = 'weichen'
__author__ = 'weichen'
from mpworks.drones.mp_vaspdrone import MPVaspDrone
from matgendb.creator import get_basic_analysis_and_error_checks
import os
import logging
import json

logger = logging.getLogger(__name__)

class MPVaspDrone_ec(MPVaspDrone):
    _parse_type="force_convergence"
    _clean_task_doc=True

    @classmethod
    def generate_doc(cls, dir_name, vasprun_files, parse_dos,
                     additional_fields, max_force_threshold=10.0):
        d=super(MPVaspDrone_ec, cls).generate_doc(dir_name, vasprun_files, parse_dos, additional_fields)
        if cls._parse_type=="force_convergence":
            max_force_threshold=0.5
        try:
            force_state=get_basic_analysis_and_error_checks(d,max_force_threshold)
            d.update(force_state)
        except Exception as ex:
            logger.error("Error in " + os.path.abspath(dir_name) +
                         ".\nError msg: " + str(ex))
            return None
        if cls._clean_task_doc:
            for doc in d["calculations"]:
                doc["input"]["kpoints"].pop("actual_points", None)
                doc["output"].pop("eigenvalues", None)
        return d


    def process_fw(self, dir_name, d):
        super(MPVaspDrone_ec, self).process_fw(dir_name, d)
        if self._parse_type=="deformed_structure":
            with open(os.path.join(dir_name, 'FW.json')) as f:
                fw_dict = json.load(f)
                d['deformation_matrix'] = fw_dict['spec']['deformation_matrix']
                d['original_task_id']=fw_dict['spec']["original_task_id"]

class MPVaspDrone_surface(MPVaspDrone):
    _parse_type="Vasp_surfaces"
    _clean_task_doc=False

    @classmethod
    def generate_doc(cls, dir_name, vasprun_files, parse_dos,
                     additional_fields, max_force_threshold=100.0):
        d=super(MPVaspDrone_surface, cls).generate_doc(dir_name, vasprun_files, parse_dos, additional_fields)
        if cls._parse_type=="force_convergence":
            max_force_threshold=0.5
        try:
            force_state=get_basic_analysis_and_error_checks(d,max_force_threshold)
            d.update(force_state)
        except Exception as ex:
            logger.error("Error in " + os.path.abspath(dir_name) +
                         ".\nError msg: " + str(ex))
            return None
        if cls._clean_task_doc:
            for doc in d["calculations"]:
                doc["input"]["kpoints"].pop("actual_points", None)
                doc["output"].pop("eigenvalues", None)
        return d


    def process_fw(self, dir_name, d):
        super(MPVaspDrone_ec, self).process_fw(dir_name, d)