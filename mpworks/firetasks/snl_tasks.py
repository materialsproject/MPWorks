import os
from fireworks.core.firework import FireTaskBase, FWAction
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 25, 2013'


class AddSNLTask(FireTaskBase, FWSerializable):
    """
    Add a new SNL into the SNL database, and build duplicate groups
    """

    _fw_name = "Add SNL Task"

    def run_task(self, fw_spec):
        # get the SNL mongo adapter
        db_dir = os.environ['DB_LOC']
        db_path = os.path.join(db_dir, 'snl_db.yaml')
        sma = SNLMongoAdapter.from_file(db_path)

        # get the SNL
        snl = StructureNL.from_dict(fw_spec['snl'])

        # add snl
        mpsnl, snlgroup_id = sma.add_snl(snl)

        return FWAction(update_spec={'mpsnl': mpsnl.to_dict, 'snlgroup_id': snlgroup_id})