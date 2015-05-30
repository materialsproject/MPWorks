import sys, multiprocessing
from mpworks.snl_utils.mpsnl import SNLGroup
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
from mpworks.check_snl.utils import div_plus_mod
from pybtex.exceptions import PybtexError

try:
  import plotly.plotly as py
  import plotly.tools as tls
  from plotly.graph_objs import *
except ImportError:
  py, tls = None, None

_log = get_builder_log("snl_group_checks")

class SNLGroupBaseChecker(Builder):

    def get_items(self, snls=None, snlgroups=None, ncols=None):
        """iterator over same-composition groups of SNLGroups rev-sorted by size

        :param snls: 'snl' collection in 'snl_mp_prod' DB
        :type snls: QueryEngine
        :param snlgroups: 'snlgroups' collection in 'snl_mp_prod' DB
        :type snlgroups: QueryEngine
        :param ncols: number of columns for 2D plotly
        :type ncols: int
        """
        self._lock = self._mgr.Lock() if not self._seq else None
        self._ncols = ncols if not self._seq else 1
        self._nrows = div_plus_mod(self._ncores, self._ncols) if not self._seq else 1
        self._snlgroup_counter = self.shared_list()
        self._snlgroup_counter.extend([[0]*self._ncols for i in range(self._nrows)])
        self._snlgroup_counter_total = multiprocessing.Value('d', 0)
        self._mismatch_dict = self.shared_dict()
        self._mismatch_dict.update(dict(
            (k,[]) for k in ['diff. SGs', 'same SGs', 'pybtex', 'others'] # TODO categories
        ))
        self._mismatch_counter = self.shared_list()
        self._mismatch_counter.extend([0]*len(self._mismatch_dict.keys()))
	if py is not None:
	  self._streams = [ py.Stream(stream_id) for stream_id in stream_ids ]
	  for s in self._streams: s.open()
        self._snls = snls
        self._snlgroups = snlgroups
        _log.info('#SNLGroups = %d', self._snlgroups.collection.count())
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

    def process_item(self, item, index):
        """iterate all SNLGroups for current composition (item)"""
        nrow, ncol = index/self._ncols, index%self._ncols
        snlgroups = {} # keep {snlgroup_id: SNLGroup} to avoid dupe queries
        for gid in item['snlgroup_ids']:
            try:
                snlgrp_dict = self._snlgroups.collection.find_one({ "snlgroup_id": gid })
                snlgroups[gid] = SNLGroup.from_dict(snlgrp_dict)
            except PybtexError:
                snlgroups[gid] = 'pybtex'
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                _log.info('%r %r', exc_type, exc_value)
                snlgroups[gid] = 'others'
        return snlgroups

Builder.register(SNLGroupBaseChecker)
