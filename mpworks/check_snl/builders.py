import sys, multiprocessing, os, time
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
from mpworks.check_snl.utils import div_plus_mod, sleep
from fnmatch import fnmatch

creds = tls.get_credentials_file()
stream_ids = creds['stream_ids'][:3] # NOTE index
_log = get_builder_log("cross_checker")
categories = ['diff. SGs', 'same SGs', 'pybtex', 'others']

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
        self._lock = self._mgr.Lock() if not self._seq else None
        self._ncols = 4 if not self._seq else 1 # TODO increase from 2 for more proc
        self._nrows = div_plus_mod(self._ncores, self._ncols) if not self._seq else 1
        self._snlgroup_counter = self.shared_list()
        self._snlgroup_counter.extend([[0]*self._ncols for i in range(self._nrows)])
        self._snlgroup_counter_total = multiprocessing.Value('d', 0)
        self._mismatch_dict = self.shared_dict()
        self._mismatch_dict.update(dict((k,[]) for k in categories))
        self._mismatch_counter = self.shared_list()
        self._mismatch_counter.extend([0]*len(self._mismatch_dict.keys()))
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
                    _log.info('%r %r', exc_type, exc_value)
                    return 'pybtex' if fnmatch(str(exc_type), '*pybtex*') else 'others'
            return snlgroups[gid]

        def _increase_counter(mismatch_dict):
            # https://docs.python.org/2/library/multiprocessing.html#multiprocessing.managers.SyncManager.list
	    if self._lock is not None: self._lock.acquire()
            mc = self._mismatch_counter
            for k in categories:
                mc[categories.index(k)] += len(mismatch_dict[k])
            self._mismatch_counter = mc
            for k,v in mismatch_dict.iteritems():
                self._mismatch_dict[k] += v
            nrow, ncol = proc_id/self._ncols, proc_id%self._ncols
            currow = self._snlgroup_counter[nrow]
            currow[ncol] += 1
            self._snlgroup_counter[nrow] = currow
            self._snlgroup_counter_total.value += 1
            if not self._snlgroup_counter_total.value % (5*self._ncols*self._nrows) \
               or self._snlgroup_counter_total.value == self._num_snlgroups:
		heatmap_z = self._snlgroup_counter._getvalue() if not self._seq else self._snlgroup_counter
		bar_x = self._mismatch_counter._getvalue() if not self._seq else self._mismatch_counter
                self._streams[0].write(Heatmap(z=heatmap_z))
                self._streams[1].write(Bar(x=bar_x))
		md = self._mismatch_dict._getvalue() if not self._seq else self._mismatch_dict
                for k,v in md.iteritems():
                    if len(v) < 1: continue
                    self._streams[2].write(Scatter(
                        x=self._mismatch_counter[categories.index(k)], y=k,
                        text='<br>'.join(v)
                    ))
                    time.sleep(0.052)
                self._mismatch_dict.update(dict((k,[]) for k in categories)) # clean
	    if self._lock is not None: self._lock.release()

        for idx,primary_id in enumerate(item['snlgroup_ids'][:-1]):
            cat_key = ''
            local_mismatch_dict = dict((k,[]) for k in categories)
            primary_group = _get_snl_group(primary_id)
            if not isinstance(primary_group, str):
                composition, primary_sg_num = primary_group.canonical_snl.snlgroup_key.split('--')
            else:
                local_mismatch_dict[primary_group].append('%d' % primary_id)
                _log.info(local_mismatch_dict)
                _increase_counter(local_mismatch_dict)
                continue
            for secondary_id in item['snlgroup_ids'][idx+1:]:
                secondary_group = _get_snl_group(secondary_id)
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
            if cat_key: _log.info(local_mismatch_dict)
            _increase_counter(local_mismatch_dict)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('ncols', help='number of columns', type=int)
    parser.add_argument('nrows', help='number of rows', type=int)
    args = parser.parse_args()
    maxpoints = args.ncols*args.nrows
    data = Data()
    data.append(Bar(
        y=categories, x=[0]*len(categories), orientation='h',
        stream=Stream(token=stream_ids[1], maxpoints=2),
        xaxis='x1', yaxis='y1'
    ))
    data.append(Heatmap(
        z=[[0]*args.ncols for i in range(args.nrows)],
        stream=Stream(token=stream_ids[0], maxpoints=maxpoints),
        xaxis='x2', yaxis='y2'
    ))
    data.append(Scatter(
        y=[], x=[], xaxis='x1', yaxis='y1', mode='markers',
        stream=Stream(token=stream_ids[2], maxpoints=10000)
    ))
    fig = tls.get_subplots(rows=1, columns=2)
    fig['data'] = data
    fig['layout'].update({'showlegend':False})
    py.plot(fig, filename='test', auto_open=False)
