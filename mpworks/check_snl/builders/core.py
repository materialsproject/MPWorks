from matgendb.builders.util import get_builder_log
from base import SNLGroupBaseChecker, categories
from mpworks.snl_utils.mpsnl import MPStructureNL

_log = get_builder_log("snl_group_checks")

class SNLGroupCrossChecker(SNLGroupBaseChecker):
    """cross-check all SNL Groups via StructureMatcher.fit of their canonical SNLs"""
    def process_item(self, item, index):
        nrow, ncol, snlgroups = super(SNLGroupCrossChecker, self).process_item(item, index)
        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            cat_key = ''
            local_mismatch_dict = dict((k,[]) for k in categories[self.checker_name])
            primary_group = snlgroups[primary_id]
            composition, primary_sg_num = primary_group.canonical_snl.snlgroup_key.split('--')
            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = snlgroups[secondary_id]
                secondary_sg_num = secondary_group.canonical_snl.snlgroup_key.split('--')[1]
                if not self._matcher.fit(
                    primary_group.canonical_structure,
                    secondary_group.canonical_structure
                ): continue
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
    def get_snl_query(self, snl_ids):
        or_conds = [{'about._icsd.icsd_id': {'$type': i}} for i in [16, 18]]
        return [
            {'snl_id': {'$in': snl_ids}, '$or': or_conds},
            {'_id': 0, 'snl_id': 1, 'about._icsd.icsd_id': 1} # remove if sym needed
        ]

    def process_item(self, item, index):
        nrow, ncol, snlgroups = super(SNLGroupIcsdChecker, self).process_item(item, index)
        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            cat_key = ''
            local_mismatch_dict = dict((k,[]) for k in categories[self.checker_name])
            primary_group = snlgroups[primary_id]
            primary_mpsnl_dicts = self._snls.collection.find(
                *self.get_snl_query(primary_group.all_snl_ids))
            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = snlgroups[secondary_id]
                secondary_mpsnl_dicts = self._snls.collection.find(
                    *self.get_snl_query(secondary_group.all_snl_ids))
                for primary_mpsnl_dict in primary_mpsnl_dicts:
                    primary_icsd_id = primary_mpsnl_dict['about']['_icsd']['icsd_id']
                    for secondary_mpsnl_dict in secondary_mpsnl_dicts:
                        secondary_icsd_id = secondary_mpsnl_dict['about']['_icsd']['icsd_id']
                        if primary_icsd_id != secondary_icsd_id: continue
                        cat_key = 'same ICSDs'
                        local_mismatch_dict[cat_key].append('(%d, %d): (%d, %d) -> %d' % (
                            primary_id, secondary_id,
                            primary_mpsnl_dict['snl_id'],
                            secondary_mpsnl_dict['snl_id'],
                            primary_icsd_id
                        ))
            if cat_key:
              _log.info('(%d) %r', self._snlgroup_counter_total.value, local_mismatch_dict)
            self._increase_counter(nrow, ncol, local_mismatch_dict)

class SNLGroupMemberChecker(SNLGroupBaseChecker):
    """check whether SNLs in each SNLGroup still match resp. canonical SNL"""
    def process_item(self, item, index):
        nrow, ncol, snlgroups = super(SNLGroupMemberChecker, self).process_item(item, index)
        for snlgroup_id in item['snlgroup_ids']:
            local_mismatch_dict = dict((k,[]) for k in categories[self.checker_name])
            snlgrp = snlgroups[snlgroup_id]
            mismatch_snls = []
            entry = '%d,%d:' % (snlgrp.snlgroup_id, snlgrp.canonical_snl.snl_id)
            for idx,snl_id in enumerate(snlgrp.all_snl_ids):
                if snl_id == snlgrp.canonical_snl.snl_id: continue
                try:
                    mpsnl_dict = self._snls.collection.find_one({'snl_id': snl_id})
                    mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    _log.info('%r %r', exc_type, exc_value)
                    local_mismatch_dict[categories[self.checker_name][-1]].append('%s%d' % (entry, snl_id))
                    continue
                if self._matcher.fit(mpsnl.structure, snlgrp.canonical_structure): continue
                mismatch_snls.append(str(snl_id))
                _log.info('%s %d', entry, snl_id)
            if len(mismatch_snls) > 0:
                full_entry = '%s%s' % (entry, ','.join(mismatch_snls))
                local_mismatch_dict[categories[self.checker_name][0]].append(full_entry)
                _log.info('(%d) %r', self._snlgroup_counter_total.value, local_mismatch_dict)
            self._increase_counter(nrow, ncol, local_mismatch_dict)
