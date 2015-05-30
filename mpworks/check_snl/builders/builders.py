import sys, multiprocessing, os, time
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from matgendb.builders.core import Builder
from matgendb.builders.util import get_builder_log
from mpworks.check_snl.utils import div_plus_mod, sleep
from fnmatch import fnmatch
from pybtex.exceptions import PybtexError

try:
  import plotly.plotly as py
  import plotly.tools as tls
  from plotly.graph_objs import *
except ImportError:
  py, tls = None, None

if py is not None:
  creds = tls.get_credentials_file()
  stream_ids = creds['stream_ids'][:3] # NOTE index

_log = get_builder_log("snl_group_checks")
categories = [
    ['SG change', 'SG default', 'pybtex', 'others'], # SNLSpaceGroupChecker
    ['mismatch', 'others'], # SNLGroupMemberChecker
    ['diff. SGs', 'same SGs', 'pybtex', 'others'] # SNLGroupCrossChecker
]
titles = [
    'Spacegroup Consistency Check', # SNLSpaceGroupChecker
    'SNLGroup Members Consistency Check', # SNLGroupMemberChecker
    'Cross-Check of Canonical SNLs / SNLGroups', # SNLGroupCrossChecker
]
xtitles = [
    '# affected SNLs', # SNLSpaceGroupChecker
    '# affected SNLGroups', # SNLGroupMemberChecker
    '# affected SNLGroups', # SNLGroupCrossChecker
]
colorbar_titles = [
    '#SNLs', # SNLSpaceGroupChecker
    '#SNLGroups', # SNLGroupMemberChecker
    '#SNLGroups', # SNLGroupCrossChecker
]

class SNLSpaceGroupChecker(Builder):
    """check spacegroups of all available SNLs"""

    def get_items(self, snls=None, ncols=None):
        """SNLs iterator

        :param snls: 'snl' collection in 'snl_mp_prod' DB
        :type snls: QueryEngine
        :param ncols: number of columns for 2D plotly
        :type ncols: int
        """
        self._snls = snls
        self._lock = self._mgr.Lock() if not self._seq else None
        self._ncols = ncols if not self._seq else 1
        self._nrows = div_plus_mod(self._ncores, self._ncols) if not self._seq else 1
        self._num_snls = snls.collection.count()
        _log.info('#SNLs = %d', self._num_snls)
        self._snl_counter = self.shared_list()
        self._snl_counter.extend([[0]*self._ncols for i in range(self._nrows)])
        self._snl_counter_total = multiprocessing.Value('d', 0)
        self._mismatch_dict = self.shared_dict()
        self._mismatch_dict.update(dict((k,[]) for k in categories[0]))
        self._mismatch_counter = self.shared_list()
        self._mismatch_counter.extend([0]*len(self._mismatch_dict.keys()))
        if py is not None:
          self._streams = [ py.Stream(stream_id) for stream_id in stream_ids ]
          for s in self._streams: s.open()
        return self._snls.query(distinct_key='snl_id')

    def _push_to_plotly(self):
        heatmap_z = self._snl_counter._getvalue() if not self._seq else self._snl_counter
        bar_x = self._mismatch_counter._getvalue() if not self._seq else self._mismatch_counter
        md = self._mismatch_dict._getvalue() if not self._seq else self._mismatch_dict
	try:
	  self._streams[0].write(Heatmap(z=heatmap_z))
	except:
	  _log.info('_push_to_plotly ERROR: heatmap=%r', heatmap_z)
	try:
	  self._streams[1].write(Bar(x=bar_x))
	except:
	  _log.info('_push_to_plotly ERROR: bar=%r', bar_x)
	for k,v in md.iteritems():
	  if len(v) < 1: continue
	  try:
	    self._streams[2].write(Scatter(
	      x=self._mismatch_counter[categories[0].index(k)], y=k,
	      text='<br>'.join(v)
	    ))
	    _log.info('_push_to_plotly: mismatch_dict[%r]=%r', k, v)
	    self._mismatch_dict.update({k:[]}) # clean
	    time.sleep(0.052)
	  except:
	    _log.info('_push_to_plotly ERROR: mismatch_dict=%r', md)
	    _log.info(
	      'self._mismatch_dict=%r',
	      self._mismatch_dict._getvalue() if not self._seq
	      else self._mismatch_dict
	    )

    def _increase_counter(self, nrow, ncol, mismatch_dict):
        if self._lock is not None: self._lock.acquire()
        mc = self._mismatch_counter
        for k in categories[0]:
            mc[categories[0].index(k)] += len(mismatch_dict[k])
        self._mismatch_counter = mc
        for k,v in mismatch_dict.iteritems():
            self._mismatch_dict[k] += v
        currow = self._snl_counter[nrow]
        currow[ncol] += 1
        self._snl_counter[nrow] = currow
        self._snl_counter_total.value += 1
        if py is not None and (
            not self._snl_counter_total.value % (100*self._ncols*self._nrows)
            or self._snl_counter_total.value == self._num_snls):
                  self._push_to_plotly()
        if (not self._snl_counter_total.value%2500):
            _log.info('processed %d SNLs', self._snl_counter_total.value)
        if self._lock is not None: self._lock.release()

    def process_item(self, item, index):
        """compare SG in db with SG from SpacegroupAnalyzer"""
        nrow, ncol = index/self._ncols, index%self._ncols
        local_mismatch_dict = dict((k,[]) for k in categories[0])
        category = ''
        try:
            mpsnl_dict = self._snls.collection.find_one({ 'snl_id': item })
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            mpsnl.structure.remove_oxidation_states()
            sf = SpacegroupAnalyzer(mpsnl.structure, symprec=0.1)
            if sf.get_spacegroup_number() != mpsnl.sg_num:
                category = categories[0][int(sf.get_spacegroup_number() == 0)]
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            category = categories[0][2 if fnmatch(str(exc_type), '*pybtex*') else 3]
        if category:
            local_mismatch_dict[category].append(str(item))
            _log.info('(%d) %r', self._snl_counter_total.value, local_mismatch_dict)
        self._increase_counter(nrow, ncol, local_mismatch_dict)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('ntest', help='test number (0-2)', type=int)
    parser.add_argument('ncols', help='number of columns', type=int)
    parser.add_argument('nrows', help='number of rows', type=int)
    args = parser.parse_args()
    if py is not None:
      maxpoints = args.ncols*args.nrows
      data = Data()
      data.append(Bar(
        y=categories[args.ntest], x=[0]*len(categories[args.ntest]),
        orientation='h', xaxis='x1', yaxis='y1',
        stream=Stream(token=stream_ids[1], maxpoints=2)
      ))
      data.append(Heatmap(
        z=[[0]*args.ncols for i in range(args.nrows)],
        stream=Stream(token=stream_ids[0], maxpoints=maxpoints),
        xaxis='x2', yaxis='y2', colorscale='Bluered',
        colorbar=ColorBar(title=colorbar_titles[args.ntest])
      ))
      data.append(Scatter(
        y=[], x=[], xaxis='x1', yaxis='y1', mode='markers',
        stream=Stream(token=stream_ids[2], maxpoints=10000)
      ))
      fig = tls.get_subplots(rows=1, columns=2)
      layout = Layout(
          showlegend=False, hovermode='closest',
          title = titles[args.ntest],
          xaxis1=XAxis(
              domain=[0,0.49], showgrid=False, anchor='y1',
              title=xtitles[args.ntest]
          ),
          yaxis1=YAxis(
              showgrid=False, title='error category', anchor='x1'
          ),
          xaxis2=XAxis(
              domain=[0.51,1.], showgrid=False, anchor='y2',
              title='CPU index = x+%dy' % args.ncols,
              autotick=False, tick0=0, dtick=1
          ),
          yaxis2=YAxis(
              showgrid=False, anchor='x2',
              autotick=False, tick0=0, dtick=1
          ),
      )
      fig['data'] = data
      fig['layout'] = layout
      py.plot(fig, filename='test', auto_open=False)
      #py.image.save_as(fig, 'test.png')
    else:
      print 'plotly ImportError'
