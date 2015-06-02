import sys, multiprocessing, time
from mpworks.snl_utils.mpsnl import SNLGroup
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
from mpworks.check_snl.utils import div_plus_mod
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from init_plotly import py, stream_ids, categories
if py is not None:
    from plotly.graph_objs import *

_log = get_builder_log("snl_group_checks")

class SNLGroupBaseChecker(Builder):
    def __init__(self, *args, **kwargs):
        self.checker_name = type(self).__name__
        _log.info(self.checker_name)
        Builder.__init__(self, *args, **kwargs)

    def get_items(self, snls=None, snlgroups=None, ncols=None):
        """iterator over same-composition groups of SNLGroups rev-sorted by size

        :param snls: 'snl' collection in 'snl_mp_prod' DB
        :type snls: QueryEngine
        :param snlgroups: 'snlgroups' collection in 'snl_mp_prod' DB
        :type snlgroups: QueryEngine
        :param ncols: number of columns for 2D plotly
        :type ncols: int
        """
        self._matcher = StructureMatcher(
            ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
            attempt_supercell=False, comparator=ElementComparator()
        )
        self._lock = self._mgr.Lock() if not self._seq else None
        self._ncols = ncols if not self._seq else 1
        self._nrows = div_plus_mod(self._ncores, self._ncols) if not self._seq else 1
        self._counter = self.shared_list()
        self._counter.extend([[0]*self._ncols for i in range(self._nrows)])
        self._counter_total = multiprocessing.Value('d', 0)
        self._mismatch_dict = self.shared_dict()
        self._mismatch_dict.update(dict((k,[]) for k in categories[self.checker_name]))
        self._mismatch_counter = self.shared_list()
        self._mismatch_counter.extend([0]*len(self._mismatch_dict.keys()))
        if py is not None:
            self._streams = [ py.Stream(stream_id) for stream_id in stream_ids ]
            for s in self._streams: s.open()
        self._snls = snls
        self._snlgroups = snlgroups
        if 'SNLGroup' in self.checker_name:
            _log.info('analyzing %d SNLGroups', self._snlgroups.collection.count())
            # start pipeline to prepare aggregation of items
            pipeline = [{ '$project': {
                'reduced_cell_formula_abc': 1, 'snlgroup_id': 1, '_id': 0
            }}]
            group_expression = {
                '_id': '$reduced_cell_formula_abc',
                'num_snlgroups': { '$sum': 1 },
                'snlgroup_ids': { '$addToSet': "$snlgroup_id" }
            }
            pipeline.append({ '$group': group_expression })
            pipeline.append({ '$match': { 'num_snlgroups': { '$gt': 1 } } })
            pipeline.append({ '$sort': { 'num_snlgroups': -1 } })
            pipeline.append({ '$project': { 'snlgroup_ids': 1 } })
            return self._snlgroups.collection.aggregate(pipeline, cursor={})
        else:
            _log.info('analyzing %d SNLs', snls.collection.count())
            return self._snls.query(distinct_key='snl_id')

    def process_item(self, item, index):
        nrow, ncol = index/self._ncols, index%self._ncols
        snlgroups = {} # keep {snlgroup_id: SNLGroup} to avoid dupe queries
        if isinstance(item, dict) and 'snlgroup_ids' in item:
            for gid in item['snlgroup_ids']:
                try:
                    snlgrp_dict = self._snlgroups.collection.find_one({ "snlgroup_id": gid })
                    snlgroups[gid] = SNLGroup.from_dict(snlgrp_dict)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    _log.info('%r %r', exc_type, exc_value)
                    self._increase_counter(nrow, ncol, {categories[self.checker_name]: [str(gid)]})
        return nrow, ncol, snlgroups

    def _push_to_plotly(self):
        heatmap_z = self._counter._getvalue() if not self._seq else self._counter
        bar_x = self._mismatch_counter._getvalue() if not self._seq else self._mismatch_counter
        md = self._mismatch_dict._getvalue() if not self._seq else self._mismatch_dict
	try:
	  self._streams[0].write(Heatmap(z=heatmap_z))
	except:
          exc_type, exc_value, exc_traceback = sys.exc_info()
          _log.info('%r %r', exc_type, exc_value)
	  _log.info('_push_to_plotly ERROR: heatmap=%r', heatmap_z)
	try:
	  self._streams[1].write(Bar(x=bar_x))
	except:
          exc_type, exc_value, exc_traceback = sys.exc_info()
          _log.info('%r %r', exc_type, exc_value)
	  _log.info('_push_to_plotly ERROR: bar=%r', bar_x)
        for k,v in md.iteritems():
            if len(v) < 1: continue
            try:
                self._streams[2].write(Scatter(
                    x=self._mismatch_counter[categories[self.checker_name].index(k)],
                    y=k, text='<br>'.join(v)
                ))
                _log.info('_push_to_plotly: mismatch_dict[%r]=%r', k, v)
                self._mismatch_dict.update({k:[]}) # clean
                time.sleep(0.052)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                _log.info('%r %r', exc_type, exc_value)
                _log.info('_push_to_plotly ERROR: mismatch_dict=%r', md)
                _log.info(
                    'self._mismatch_dict=%r',
                    self._mismatch_dict._getvalue() if not self._seq
                    else self._mismatch_dict
                )

    def _increase_counter(self, nrow, ncol, mismatch_dict):
        # https://docs.python.org/2/library/multiprocessing.html#multiprocessing.managers.SyncManager.list
        if self._lock is not None: self._lock.acquire()
        mc = self._mismatch_counter
        for k in categories[self.checker_name]:
            mc[categories[self.checker_name].index(k)] += len(mismatch_dict[k])
        self._mismatch_counter = mc
        for k,v in mismatch_dict.iteritems():
            self._mismatch_dict[k] += v
        currow = self._counter[nrow]
        currow[ncol] += 1
        self._counter[nrow] = currow
        self._counter_total.value += 1
        if py is not None and not \
           self._counter_total.value % (10*self._ncols*self._nrows):
            self._push_to_plotly()
        if (not self._counter_total.value%2500):
            _log.info('processed %d items', self._counter_total.value)
        if self._lock is not None: self._lock.release()

    def finalize(self, errors):
	if py is not None: self._push_to_plotly()
        _log.info("%d items processed.", self._counter_total.value)
        return True

Builder.register(SNLGroupBaseChecker)
