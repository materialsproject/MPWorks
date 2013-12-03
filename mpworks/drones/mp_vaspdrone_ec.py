__author__ = 'weichen'
from mpworks.drones.mp_vaspdrone import MPVaspDrone
from matgendb.creator import get_basic_analysis_and_error_checks
import os
import logging
import json

logger = logging.getLogger(__name__)

class MPVaspDrone_ec(MPVaspDrone):
    @classmethod
    def generate_doc(cls, dir_name, vasprun_files, parse_dos,
                     additional_fields, max_force_threshold=10.0):
        d=super(MPVaspDrone_ec, cls).generate_doc(dir_name, vasprun_files, parse_dos, additional_fields)
        try:
            force_state=get_basic_analysis_and_error_checks(d,max_force_threshold)
            d.update(force_state)
            return d
        except Exception as ex:
            logger.error("Error in " + os.path.abspath(dir_name) +
                         ".\nError msg: " + str(ex))
            return None


    def process_fw(self, dir_name, d):
        super(MPVaspDrone_ec, super).process_fw()
        with open(os.path.join(dir_name, 'FW.json')) as f:
            fw_dict = json.load(f)
            d['strain'] = fw_dict['strain']
