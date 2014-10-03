import sys, multiprocessing, os, time
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
from mpworks.check_snl.utils import div_plus_mod, sleep

creds = tls.get_credentials_file()
stream_ids = creds['stream_ids'][:2] # NOTE index
_log = get_builder_log("cross_checker")

class SNLGroupCrossChecker(Builder):
    """cross-check all SNL Groups via StructureMatcher.fit of their canonical SNLs"""

    def get_items(self, snlgroups=None):
        """iterator over same-composition groups of SNLGroups rev-sorted by size

        :param snlgroups: 'snlgroups' collection in 'snl_mp_prod' DB
        :type snlgroups: QueryEngine
        """
        self._matcher = StructureMatcher(
            ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
            attempt_supercell=False, comparator=ElementComparator()
        )
        self._lock = self._mgr.Lock()
        self._ncols = 2 if not self._seq else 1 # TODO increase from 2 for more proc
        self._nrows = div_plus_mod(self._ncores, self._ncols) if not self._seq else 1
        self._snlgroup_counter = self.shared_list()
        self._snlgroup_counter.extend([[0]*self._ncols for i in range(self._nrows)])
        self._snlgroup_counter_total = multiprocessing.Value('d', 0)
        self._mismatch_counter = self.shared_list()
        self._mismatch_counter.extend([0, 0])
        self._streams = [ py.Stream(stream_id) for stream_id in stream_ids ]
        for s in self._streams: s.open()
        self._snlgroups = snlgroups
        # start pipeline to prepare aggregation of items
        pipeline = [ { '$limit': 5000 } ]
        group_expression = {
            '_id': '$reduced_cell_formula_abc',
            'num_snlgroups': { '$sum': 1 },
            'snlgroup_ids': { '$addToSet': "$snlgroup_id" }
        }
        pipeline.append({ '$group': group_expression })
        pipeline.append({ '$match': { 'num_snlgroups': { '$gt': 1 } } })
        pipeline.append({ '$sort': { 'num_snlgroups': -1 } })
        pipeline.append({ '$project': { 'snlgroup_ids': 1 } })
        result = self._snlgroups.collection.aggregate(pipeline)['result']
        self._num_snlgroups = sum(
            len(v)-1 for d in result for k,v in d.iteritems() if k == 'snlgroup_ids'
        )
        _log.info('#snlgroups = %d', self._num_snlgroups)
        return result

    def process_item(self, item):
        """combine all SNL Groups for current composition (item)"""
        proc_id = multiprocessing.current_process()._identity[0]-2 if not self._seq else 0 # parent gets first id(=1)
        snlgroups = {} # keep {snlgroup_id: SNLGroup} to avoid dupe queries

        def _get_snl_group(gid):
            if gid not in snlgroups:
                try:
                    snlgrp_dict = self._snlgroups.collection.find_one({ "snlgroup_id": gid })
                    snlgroups[gid] = SNLGroup.from_dict(snlgrp_dict)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    _log.info(exc_type, exc_value)
                    return None # TODO: return error category
            return snlgroups[gid]

        def _increase_counter(a, b):
            # https://docs.python.org/2/library/multiprocessing.html#multiprocessing.managers.SyncManager.list
            self._lock.acquire()
            mc = self._mismatch_counter
            mc[0] += a
            mc[1] += b
            self._mismatch_counter = mc
            nrow, ncol = proc_id/self._ncols, proc_id%self._ncols
            currow = self._snlgroup_counter[nrow]
            currow[ncol] += 1
            self._snlgroup_counter[nrow] = currow
            self._snlgroup_counter_total.value += 1
            if not self._snlgroup_counter_total.value % (5*self._ncols*self._nrows) \
               or self._snlgroup_counter_total.value == self._num_snlgroups:
                self._streams[0].write(Heatmap(z=self._snlgroup_counter._getvalue()))
                self._streams[1].write(Bar(x=self._mismatch_counter._getvalue()))
            self._lock.release()

        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            local_mismatch_counter = [0, 0]
            primary_group = _get_snl_group(primary_id)
            if primary_group is None: continue
            composition, primary_sg_num = primary_group.canonical_snl.snlgroup_key.split('--')
            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = _get_snl_group(secondary_id)
                if secondary_group is None: continue
                is_match = self._matcher.fit(
                    primary_group.canonical_structure,
                    secondary_group.canonical_structure
                )
                if not is_match: continue
                secondary_sg_num = secondary_group.canonical_snl.snlgroup_key.split('--')[1]
                local_mismatch_counter[primary_sg_num==secondary_sg_num] += 1
                _log.info('%s: %d(%s), %d(%s)' % (
                    composition, primary_id, primary_sg_num, secondary_id, secondary_sg_num
                ))
            _increase_counter(*local_mismatch_counter)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('ncols', help='number of columns', type=int)
    parser.add_argument('nrows', help='number of rows', type=int)
    args = parser.parse_args()
    maxpoints = args.ncols*args.nrows
    data = Data()
    data.append(Bar(
        y=['diff. SGs', 'same SGs'], x=[0, 0], orientation='h',
        stream=Stream(token=stream_ids[1], maxpoints=2),
        xaxis='x1', yaxis='y1'
    ))
    data.append(Heatmap(
        z=[[0]*args.ncols for i in range(args.nrows)],
        stream=Stream(token=stream_ids[0], maxpoints=maxpoints),
        xaxis='x2', yaxis='y2'
    ))
    fig = tls.get_subplots(rows=1, columns=2)
    fig['data'] = data
    fig['layout'].update({'showlegend':False})
    py.plot(fig, filename='test', auto_open=False)
