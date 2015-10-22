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
        sma = SNLMongoAdapter.auto_load()
        snl = fw_spec['snl']
        mpsnl, snlgroup_id, spec_group = sma.add_snl(snl)
        mod_spec = [{"_push": {"run_tags": "species_group={}".format(spec_group)}}] if spec_group else None

        return FWAction(update_spec={'mpsnl': mpsnl.as_dict(), 'snlgroup_id': snlgroup_id}, mod_spec=mod_spec)