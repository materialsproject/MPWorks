import datetime
from pymongo import MongoClient
from fireworks.utilities.fw_serializers import FWSerializable
from mpworks.snl_utils.mpsnl import MPStructureNL

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
        # TODO: get real sg_num
        sg_num = -1
        mpsnl = MPStructureNL.from_snl(snl, snl_id, sg_num)
        self.add_mpsnl(mpsnl, build_groups)

    def add_mpsnl(self, mpsnl, build_groups=True):
        snl_d = mpsnl.to_dict
        snl_d['mp_timestamp'] = datetime.datetime.utcnow().isoformat()
        self.snl.insert(mpsnl.to_dict)
        if build_groups:
            self.build_groups(mpsnl.snl_id)

    def build_groups(self, snl_id):
        pass


    def to_dict(self):
        """
        Note: usernames/passwords are exported as unencrypted Strings!
        """
        d = {'host': self.host, 'port': self.port, 'db': self.db, 'username': self.username,
             'password': self.password}
        return d
"""
    def post_process_mps_groups(self, update_all=True):
        self.logger.info("beginning post-process of MPSGroups collection")
        query = {} if update_all else {"autometa.icsd_ids": {"$exists":False}}
        for mpsgroup in self._mpsgroups_coll.find(query, {"mpsgroup_id":1, "all_mps_ids":1}, timeout=False):
            self.logger.info("processing mpsgroup_id: {}".format(mpsgroup['mpsgroup_id']))
            icsd_ids = []
            for mps_id in mpsgroup['all_mps_ids']:
                result = self._mps_coll.find_one({"mps_id":mps_id, "mps_autometa.icsd_id":{"$exists":True}}, {"mps_autometa.icsd_id"})
                if result:
                    icsd_ids.append(result['mps_autometa']['icsd_id'])

            self.logger.info("mpsgroup_id: {} has {} ICSD entries.".format(mpsgroup['mpsgroup_id'], len(icsd_ids)))
            self._mpsgroups_coll.update({"mpsgroup_id": mpsgroup['mpsgroup_id']}, {"$set": {"autometa.icsd_ids": icsd_ids}})
            self._mpsgroups_coll.update({"mpsgroup_id": mpsgroup['mpsgroup_id']}, {"$set": {"autometa.num_icsd": len(icsd_ids)}})

    def clear(self):
        self._mps_coll.remove()

    def _add_spacegroups(self, update_all):
        query = {} if update_all else {"mps_autometa.spacegroup.number":{"$exists":False}, "mps_id": {'$gte':286260}}  # BLAH this is a hack!
        for mp_dict in self._mps_coll.find(query, timeout=False):
            mps_id = mp_dict['mps_id']

            if mp_dict['about']['metadata']['info'].get('mps_assert_spacegroup', None):
                sgroup_num = mp_dict['about']['metadata']['info']['mps_assert_spacegroup']
                self.logger.info("getting spacegroup from metadata assertion: {} for mps_id: {}".format(sgroup_num, mps_id))
                self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup': {}}})
                self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup.number': sgroup_num}})
            else:
                try:
                    structure = Structure.from_dict(mp_dict)
                    self.logger.info("processing spacegroup for mps_id: {}".format(mps_id))
                    sg = SymmetryFinder(structure, SPACEGROUP_TOLERANCE).get_spacegroup()
                    self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup': {}}})
                    self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup.number': sg.int_number}})
                    self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup.symbol': sg.int_symbol}})
                    self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup.parameters': {"tolerance": SPACEGROUP_TOLERANCE}}})
                    self._mps_coll.find_and_modify(query={"mps_id":mps_id}, update={'$set': {'mps_autometa.spacegroup.assigner': "spglib/pymatgen"}})
                except:
                    self.logger.error("Could not determine the spacegroup for mps_id: {}".format(mps_id))
                    self.logger.error(traceback.format_exc())



    def change_canonical_mps(self, mpsgroup_id, mps_id):
        self.logger.info("Changing the representative for mpsgroup_id: {} to mps_id: {}".format(mpsgroup_id, mps_id))

        if self._mpsgroups_coll.find({"mpsgroup_id": mpsgroup_id, "all_mps_ids": mps_id}).count() == 0:
            self.logger.error("Cannot update the representative. mps_id: {} does not exist as a part of mpsgroup_id: {} !".format(mps_id, mpsgroup_id))
            raise ValueError("Invalid mps id")

        mps = MaterialsProjectSource.from_dict(self._mps_coll.find_one({"mps_id":mps_id}))
        # Update the canonical MPS
        self._mpsgroups_coll.update({"mpsgroup_id": mpsgroup_id}, {"$set": {"canonical_mps": mps.to_dict()}})


    def clear_mpsgroup_key(self, mpsgroup_key):
        self.logger.info("Clearing mpsgroup: {}".format(mpsgroup_key))

        # clear the mpsgroup collection
        self._mpsgroups_coll.remove({'mpsgroup_key': mpsgroup_key})

        # clear the mps collection
        self._mps_coll.update({"mps_autometa.mpsgroup.mpsgroup_key": mpsgroup_key}, {"$unset": {"mps_autometa.mpsgroup":1}}, multi=True)
"""