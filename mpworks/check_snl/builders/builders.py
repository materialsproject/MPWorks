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
