import datetime
from pymongo import MongoClient
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.snl_utils.mpsnl import MPStructureNL
from pymatgen.symmetry.finder import SymmetryFinder

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

    def __init__(self, host, port, db, username, password):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self.connection = MongoClient(host, port, j=False)
        self.database = self.connection[db]
        self.database.authenticate(username, password)

        self.snl = self.database.snl
        self.snlgroups = self.database.snlgroups
        self.id_assigner = self.database.id_assigner

        self._update_indices()

    def _update_indices(self):
        self.snl.ensure_index('snl_id', unique=True)
        self.snl_coll.ensure_index('autometa.nsites')
        self.snl_coll.ensure_index('autometa.nelements')
        self.snl_coll.ensure_index('autometa.nlements')
        self.snl_coll.ensure_index('autometa.formula')
        self.snl_coll.ensure_index('autometa.formula_abc_red')
        self.snl_coll.ensure_index('autometa.formula_red')
        self.snl_coll.ensure_index('autometa.is_ordered')

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

    def add_snl(self, snl, build_groups=True):
        snl_id = self._get_next_snl_id()
        sf = SymmetryFinder(self.structure, SPACEGROUP_TOLERANCE)
        sf.get_spacegroup()
        mpsnl = MPStructureNL.from_snl(snl, snl_id, sf.get_spacegroup_number(), sf.get_spacegroup_symbol())
        self.add_mpsnl(mpsnl, build_groups)

    def add_mpsnl(self, mpsnl, build_groups=True):
        snl_d = mpsnl.to_dict
        snl_d['mp_timestamp'] = datetime.datetime.utcnow().isoformat()
        self.snl.insert(mpsnl.to_dict)
        if build_groups:
            self.build_groups(mpsnl.snl_id)

    def build_groups(self, snl_id):
        # TODO: implement me
        pass


    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password}
        return d