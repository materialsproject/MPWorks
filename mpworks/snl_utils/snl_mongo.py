import datetime
from pymongo import MongoClient, DESCENDING
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen import Structure
from pymatgen.matproj.snl import StructureNL
from pymatgen.symmetry.finder import SymmetryFinder

import numpy as np

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 24, 2013'

# Parameters for spacegroup and mps_unique_id determination
SPACEGROUP_TOLERANCE = 0.1  # as suggested by Shyue, 6/19/2012

# TODO: add logging

class SNLMongoAdapter(FWSerializable):

    def __init__(self, host='localhost', port=27017, db='snl', username=None, password=None, reset=False):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
        if self.username:
            self.database.authenticate(username, password)

        self.snl = self.database.snl
        self.snlgroups = self.database.snlgroups
        self.id_assigner = self.database.id_assigner

        self._update_indices()

        if reset:
            self.restart_id_assigner_at(1, 1)
            self.snl.remove()
            self.snlgroups.remove()

    def _update_indices(self):
        self.snl.ensure_index('snl_id', unique=True)
        self.snl.ensure_index('autometa.nsites')
        self.snl.ensure_index('autometa.nelements')
        self.snl.ensure_index('autometa.nlements')
        self.snl.ensure_index('autometa.formula')
        self.snl.ensure_index('autometa.formula_abc_red')
        self.snl.ensure_index('autometa.formula_red')
        self.snl.ensure_index('autometa.is_ordered')

        self.snlgroups.ensure_index('snlgroup_id', unique=True)
        self.snlgroups.ensure_index('all_snl_ids')
        self.snlgroups.ensure_index('canonical_snl.snl_id')
        self.snlgroups.ensure_index('autometa.nsites')
        self.snlgroups.ensure_index('autometa.nelements')
        self.snlgroups.ensure_index('autometa.nlements')
        self.snlgroups.ensure_index('autometa.formula')
        self.snlgroups.ensure_index('autometa.formula_abc_red')
        self.snlgroups.ensure_index('autometa.formula_red')
        self.snlgroups.ensure_index('autometa.is_ordered')

    def _get_next_snl_id(self):
        snl_id = self.id_assigner.find_and_modify(query={}, update={'$inc': {'next_snl_id': 1}})['next_snl_id']
        return snl_id

    def _get_next_snlgroup_id(self):
        snlgroup_id = self.id_assigner.find_and_modify(query={}, update={'$inc': {'next_snlgroup_id': 1}})['next_snlgroup_id']
        return snlgroup_id

    def restart_id_assigner_at(self, next_snl_id, next_snlgroup_id):
        self.id_assigner.remove()
        self.id_assigner.insert({"next_snl_id": next_snl_id, "next_snlgroup_id":next_snlgroup_id})

    def add_snl(self, snl):
        snl_id = self._get_next_snl_id()
        sf = SymmetryFinder(snl.structure, SPACEGROUP_TOLERANCE)
        sf.get_spacegroup()
        mpsnl = MPStructureNL.from_snl(snl, snl_id, sf.get_spacegroup_number(), sf.get_spacegroup_symbol(), sf.get_hall(), sf.get_crystal_system(), sf.get_lattice_type())
        snlgroup, add_new = self.add_mpsnl(mpsnl)
        return mpsnl, snlgroup.snlgroup_id

    def add_mpsnl(self, mpsnl):
        snl_d = mpsnl.to_dict
        snl_d['snl_timestamp'] = datetime.datetime.utcnow().isoformat()
        self.snl.insert(snl_d)
        return self.build_groups(mpsnl)


    def build_groups(self, mpsnl):
        add_new = True

        for entry in self.snlgroups.find({'snlgroup_key': mpsnl.snlgroup_key}, sort=[("num_snl", DESCENDING)]):
            snlgroup = SNLGroup.from_dict(entry)
            if snlgroup.add_if_belongs(mpsnl):
                add_new = False
                print 'MATCH FOUND, grouping (snl_id, snlgroup): {}'.format((mpsnl.snl_id, snlgroup.snlgroup_id))
                self.snlgroups.update({'snlgroup_id': snlgroup.snlgroup_id}, snlgroup.to_dict)
                break

        if add_new:
            # add a new SNLGroup
            snlgroup_id = self._get_next_snlgroup_id()
            snlgroup = SNLGroup(snlgroup_id, mpsnl)
            self.snlgroups.insert(snlgroup.to_dict)

        return snlgroup, add_new

    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password}
        return d

if __name__ == '__main__':
    sma = SNLMongoAdapter(reset=True)
    s1 = Structure(np.eye(3, 3) * 3, ["Fe"], [[0, 0, 0]])
    s2 = Structure(np.eye(3, 3) * 3, ["Al"], [[0, 0, 0]])

    snl1 = StructureNL(s1, 'Anubhav Jain <ajain@lbl.gov>')
    sma.add_snl(snl1)

    snl2 = StructureNL(s1, 'Captain America <ajain@lbl.gov>')
    sma.add_snl(snl2)

    snl3 = StructureNL(s2, 'Captain America <ajain@lbl.gov>')
    sma.add_snl(snl3)

    snl4 = StructureNL(s2, 'Anubhav Jain <ajain@lbl.gov>')
    sma.add_snl(snl4)