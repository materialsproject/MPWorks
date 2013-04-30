import json
import os
from pymongo import MongoClient
from matgendb.creator import VaspToDbTaskDrone
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from pymatgen.core.structure import Structure
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 26, 2013'


class MatprojVaspDrone(VaspToDbTaskDrone):

    def assimilate(self, path):
        """
        Parses vasp runs. Then insert the result into the db. and return the
        task_id or doc of the insertion.

        Returns:
            If in simulate_mode, the entire doc is returned for debugging
            purposes. Else, only the task_id of the inserted doc is returned.
        """

        # set the task_id in advance, because it's needed by post-process in order to auto-link SNL history
        conn = MongoClient(self.host, self.port)
        db = conn[self.database]
        if self.user:
            db.authenticate(self.user, self.password)
        coll = db[self.collection]
        task_id = db.counter.find_and_modify(query={"_id": "taskid"}, update={"$inc": {"c": 1}})["c"]
        self.additional_fields = self.additional_fields if self.additional_fields else {}
        self.additional_fields.update({'task_id': task_id})

        d = self.get_task_doc(path, self.parse_dos,
                              self.additional_fields)
        tid = self._insert_doc(d)
        return tid, d

    @classmethod
    def post_process(cls, dir_name, d):
        # run the post-process of the superclass
        VaspToDbTaskDrone.post_process(dir_name, d)

        # custom Materials Project post-processing
        with open(os.path.join(dir_name, 'FW.json')) as f:
            fw_dict = json.load(f)
            d['fw_id'] = fw_dict['fw_id']
            d['snl'] = fw_dict['spec']['mpsnl']
            d['snlgroup_id'] = fw_dict['spec']['snlgroup_id']
            d['submission_id'] = fw_dict['spec'].get('submission_id')
            d['run_tags'] = fw_dict['spec'].get('run_tags', [])
            d['vaspinputset_name'] = fw_dict['spec'].get('vaspinputset_name')
            d['task_type'] = fw_dict['spec']['task_type']

            if 'optimize structure' in d['task_type']:
                # create a new SNL based on optimized structure
                new_s = Structure.from_dict(d['output']['crystal'])
                old_snl = StructureNL.from_dict(d['snl'])
                history = old_snl.history
                history.append(
                    {'name':'Materials Project structure optimization',
                     'url':'http://www.materialsproject.org',
                     'description':{'task_type': d['task_type'], 'fw_id': d['fw_id'], 'task_id': d['task_id']}})
                new_snl = StructureNL(new_s, old_snl.authors, old_snl.projects,
                                      old_snl.references, old_snl.remarks,
                                      old_snl.data, history)

                # enter new SNL into SNL db
                # get the SNL mongo adapter
                sma = SNLMongoAdapter.auto_load()

                # add snl
                mpsnl, snlgroup_id = sma.add_snl(new_snl)
                d['snl_final'] = mpsnl.to_dict
                d['snlgroup_id_final'] = snlgroup_id
                d['snlgroup_changed'] = d['snlgroup_id'] != d['snlgroup_id_final']