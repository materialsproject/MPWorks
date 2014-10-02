from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from matgendb.builders.core import Builder

class SNLGroupCrossChecker(Builder):
    """cross-check all SNL Groups via StructureMatcher.fit of their canonical SNLs"""

    def get_items(self, snlgroups=None):
        """iterator over same-composition groups of SNLGroups rev-sorted by size

        :param snlgroups: 'snlgroups' collection in 'snl_mp_prod' DB
        :type snlgroups: QueryEngine
        """
        self._snlgroups = snlgroups
        pipeline = [ { '$limit': 1000 } ]
        group_expression = {
            '_id': '$reduced_cell_formula_abc',
            'num_snlgroups': { '$sum': 1 },
            'snlgroup_ids': { '$addToSet': "$snlgroup_id" }
        }
        pipeline.append({ '$group': group_expression })
        pipeline.append({ '$sort': { 'num_snlgroups': -1 } })
        pipeline.append({ '$project': { 'snlgroup_ids': 1 } })
        return self._snlgroups.collection.aggregate(pipeline)['result']

    def process_item(self, item):
        """combine all SNL Groups for current composition (item)"""
        if len(item['snlgroup_ids']) < 2: return 0
        snlgroups = {}

        def _get_snl_group(gid):
            if gid not in snlgroups:
                snlgrp_dict = self._snlgroups.collection.find_one({ "snlgroup_id": gid })
                snlgroups[gid] = SNLGroup.from_dict(snlgrp_dict)
            return snlgroups[gid]

        for snlgroup_id in item['snlgroup_ids']:
            print snlgroup_id, _get_snl_group(snlgroup_id)
        print snlgroups.keys()

