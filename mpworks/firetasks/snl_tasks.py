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
        # pass-through option for when we start with an mpsnl and don't actually want to add
        if 'force_mpsnl' in fw_spec and 'force_snlgroup_id' in fw_spec:
            return FWAction(update_spec={'mpsnl': fw_spec['force_mpsnl'], 'snlgroup_id': fw_spec['force_snlgroup_id']})

        sma = SNLMongoAdapter.auto_load()
        snl = StructureNL.from_dict(fw_spec['snl'])
        mpsnl, snlgroup_id = sma.add_snl(snl)

        return FWAction(update_spec={'mpsnl': mpsnl.to_dict, 'snlgroup_id': snlgroup_id})