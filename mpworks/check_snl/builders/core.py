from matgendb.builders.util import get_builder_log
from base import SNLGroupBaseChecker

_log = get_builder_log("snl_group_checks")

class SNLGroupCrossChecker(SNLGroupCrossChecker):
    """cross-check all SNL Groups via StructureMatcher.fit of their canonical SNLs"""

    def process_item(self, item, index):
        nrow, ncol, snlgroups = super(SNLGroupCrossChecker, self).process_item(item, index)

        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            cat_key = ''
            local_mismatch_dict = dict((k,[]) for k in categories[self.checker_name])
            primary_group = snlgroups[primary_id]
            if not isinstance(primary_group, str):
                composition, primary_sg_num = primary_group.canonical_snl.snlgroup_key.split('--')
            else:
                local_mismatch_dict[primary_group].append('%d' % primary_id)
                _log.info(local_mismatch_dict)
                self._increase_counter(nrow, ncol, local_mismatch_dict)
                continue
            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = snlgroups[secondary_id]
                if not isinstance(secondary_group, str):
                    secondary_sg_num = secondary_group.canonical_snl.snlgroup_key.split('--')[1]
                else:
                    local_mismatch_dict[secondary_group].append('%d' % secondary_id)
                    continue
                is_match = self._matcher.fit(
                    primary_group.canonical_structure,
                    secondary_group.canonical_structure
                )
                if not is_match: continue
                cat_key = 'same SGs' if primary_sg_num == secondary_sg_num else 'diff. SGs'
                local_mismatch_dict[cat_key].append('(%d,%d)' % (primary_id, secondary_id))
            if cat_key:
              _log.info('(%d) %r', self._snlgroup_counter_total.value, local_mismatch_dict)
            self._increase_counter(nrow, ncol, local_mismatch_dict)


class SNLGroupIcsdChecker(SNLGroupBaseChecker):
    """check one-to-one mapping of SNLGroup to ICSD ID
    
    check if two different SNLGroups have any entries that share an ICSD id.
    Should not happen at all due to 1-to-1 mapping of MP to ICSD material
    """

    def process_item(self, item, index):
        nrow, ncol, snlgroups = super(SNLGroupIcsdChecker, self).process_item(item, index)

        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            primary_group = snlgroups[primary_id]
            if not isinstance(primary_group, str):
                primary_mpsnl_dicts = self._snls.collection.find({
                    'snl_id': {'$in': primary_group.all_snl_ids},
                    '$or': [
                        {'about._icsd.icsd_id': { '$type': 16 }},
                        {'about._icsd.icsd_id': { '$type': 18 }},
                    ]
                }, { '_id': 0, 'snl_id': 1, 'about._icsd.icsd_id': 1 })
            else:
                _log.info('%d: %s' % (primary_id, primary_group))
                continue

            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = snlgroups[secondary_id]
                if not isinstance(secondary_group, str):
                    secondary_mpsnl_dicts = self._snls.collection.find({
                        'snl_id': {'$in': secondary_group.all_snl_ids},
                        '$or': [
                            {'about._icsd.icsd_id': { '$type': 16 }},
                            {'about._icsd.icsd_id': { '$type': 18 }},
                        ]
                    }, { '_id': 0, 'snl_id': 1, 'about._icsd.icsd_id': 1 }) # remove if sym needed
                else:
                    _log.info('%d: %s' % (secondary_id, secondary_group))
                    continue

                for primary_mpsnl_dict in primary_mpsnl_dicts:
                    primary_icsd_id = primary_mpsnl_dict['about']['_icsd']['icsd_id']
                    # primary_mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                    # primary_mpsnl.structure.remove_oxidation_states()
                    # primary_sf = SpacegroupAnalyzer(primary_snl.structure, symprec=0.1)
                    # primary_sg_num = primary_sf.get_spacegroup_number()
                    for secondary_mpsnl_dict in secondary_mpsnl_dicts:
                        secondary_icsd_id = secondary_mpsnl_dict['about']['_icsd']['icsd_id']
                        if primary_icsd_id == secondary_icsd_id:
                            _log.info('SNLGroups (%d, %d): SNL IDs (%d, %d) share ICSD ID %d' % (
                                primary_id, secondary_id,
                                primary_mpsnl_dict['snl_id'],
                                secondary_mpsnl_dict['snl_id'],
                                primary_icsd_id
                            ))
