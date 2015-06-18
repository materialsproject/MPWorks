import os
try:
  import plotly.plotly as py
  import plotly.tools as tls
  from plotly.graph_objs import *
except ImportError:
  py, tls = None, None

stream_ids = ['zotjax1o9n', '4r7oj2r35i', 'nobvv4bxvw']
if py is not None:
    py.sign_in(
        os.environ.get('MP_PLOTLY_USER'),
        os.environ.get('MP_PLOTLY_APIKEY'),
        stream_ids=stream_ids
    )

categories = {
    'SNLSpaceGroupChecker': ['SG change', 'SG default', 'others'],
    'SNLGroupMemberChecker': ['mismatch', 'others'],
    'SNLGroupCrossChecker': ['diff. SGs', 'same SGs', 'others'],
    'SNLGroupIcsdChecker': ['same ICSDs', 'others'],
}
titles = {
    'SNLSpaceGroupChecker': 'Spacegroup Consistency Check',
    'SNLGroupMemberChecker': 'SNLGroup Members Consistency Check',
    'SNLGroupCrossChecker': 'Cross-Check of Canonical SNLs / SNLGroups',
    'SNLGroupIcsdChecker': 'Cross-Check of 1-to-1 SNLGroup-ICSD mapping',
}
xtitles = {
    'SNLSpaceGroupChecker': '# affected SNLs',
    'SNLGroupMemberChecker': '# affected SNLGroups',
    'SNLGroupCrossChecker': '# affected SNLGroups',
    'SNLGroupIcsdChecker': '# affected SNLGroups',
}
colorbar_titles = {
    'SNLSpaceGroupChecker': '#SNLs',
    'SNLGroupMemberChecker': '#SNLGroups',
    'SNLGroupCrossChecker': '#SNLGroups',
    'SNLGroupIcsdChecker': '#SNLGroups',
}

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('name', help='checker name', type=str)
    parser.add_argument('ncols', help='number of columns', type=int)
    parser.add_argument('nrows', help='number of rows', type=int)
    args = parser.parse_args()
    if py is not None:
      maxpoints = args.ncols*args.nrows
      data = Data()
      data.append(Bar(
        y=categories[args.name], x=[0]*len(categories[args.name]),
        orientation='h', xaxis='x1', yaxis='y1',
        stream=Stream(token=stream_ids[1], maxpoints=2)
      ))
      data.append(Heatmap(
        z=[[0]*args.ncols for i in range(args.nrows)],
        stream=Stream(token=stream_ids[0], maxpoints=maxpoints),
        xaxis='x2', yaxis='y2', colorscale='Bluered', zauto=True,
        colorbar=ColorBar(title=colorbar_titles[args.name])
      ))
      data.append(Scatter(
        y=[], x=[], xaxis='x1', yaxis='y1', mode='markers',
        stream=Stream(token=stream_ids[2], maxpoints=10000)
      ))
      fig = tls.make_subplots(rows=1, cols=2)
      layout = Layout(
          showlegend=False, hovermode='closest',
          title = titles[args.name],
          xaxis1=XAxis(
              domain=[0,0.49], showgrid=False, anchor='y1',
              title=xtitles[args.name], autorange=True
          ),
          yaxis1=YAxis(
              showgrid=False, title='error category', anchor='x1',
              autorange=True
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
      py.plot(fig, filename='builder_stream', auto_open=False)
    else:
      print 'plotly ImportError'
