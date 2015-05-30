from base import SNLGroupBaseChecker

class SNLGroupIcsdChecker(SNLGroupBaseChecker):
    """check one-to-one mapping of SNLGroup to ICSD ID
    
    check if two different SNLGroups have any entries that share an ICSD id.
    Should not happen at all due to 1-to-1 mapping of MP to ICSD material
    """

    def process_item(self, item, index):
        snlgroups = super(SNLGroupIcsdChecker, self).process_item(item, index)

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
