import datetime
from pandas.io.parsers import read_csv
from pyana.ccsgp.ccsgp import make_plot
from pyana.ccsgp.utils import getOpts
from pandas import Series
from collections import OrderedDict

import plotly.plotly as py
from plotly.graph_objs import *

def sg1_vs_sg2():
    """plot SG #1 vs #2 via ccsgp"""
    df = read_csv('mpworks/check_snl/results/bad_snlgroups_2.csv')
    df_view = df[['composition', 'sg_num 1', 'sg_num 2']]
    grouped = df_view.groupby('composition')
    data = OrderedDict()
    for i, (composition, group) in enumerate(grouped):
        del group['composition']
        for col in ['dx', 'dy1', 'dy2']:
            group[col] = Series([0]*group.shape[0], index=group.index)
        data[composition] = group.as_matrix()
        #if i > 10: break
    nSets = len(data)
    make_plot(
        data = data.values(),
        properties = [ getOpts(i) for i in xrange(nSets) ],
        titles = data.keys(),
        xlabel = 'SG #2', ylabel = 'SG #1',
        title="Spacegroups of 1927 matching SNLGroups",
        xr = [-1,300], yr = [-1,300]
    )

def sg1_vs_sg2_plotly():
    """plot SG #1 vs #2 via plotly"""
    out_fig = Figure()
    bisectrix = Scatter(x=[0,230], y=[0,230], mode='lines', name='bisectrix', showlegend=False)
    inmatdb_df = read_csv('mpworks/check_snl/results/bad_snlgroups_2_in_matdb.csv')
    inmatdb_text = map(','.join, zip(
        inmatdb_df['task_id 1'], inmatdb_df['task_id 2']
    ))
    inmatdb_trace = Scatter(
        x=inmatdb_df['sg_num 2'].as_matrix(), y=inmatdb_df['sg_num 1'].as_matrix(),
        text=inmatdb_text, mode='markers', name='in MatDB'
    )
    notinmatdb_df = read_csv('mpworks/check_snl/results/bad_snlgroups_2_notin_matdb.csv')
    notinmatdb_text = map(','.join, zip(
        map(str, notinmatdb_df['snlgroup_id 1']), map(str, notinmatdb_df['snlgroup_id 2'])
    ))
    notinmatdb_trace = Scatter(
        x=notinmatdb_df['sg_num 2'].as_matrix()+0.1,
        y=notinmatdb_df['sg_num 1'].as_matrix()+0.1,
        text=notinmatdb_text, mode='markers', name='not in MatDB'
    )
    out_fig['data'] = Data([bisectrix, notinmatdb_trace, inmatdb_trace])
    out_fig['layout'] = Layout(
        hovermode='closest',
        title='Spacegroup Assignment Comparison of matching Canonical SNLs',
        xaxis=XAxis(showgrid=False, title='SG #2', range=[0,230]),
        yaxis=YAxis(showgrid=False, title='SG #1', range=[0,230]),
    )
    filename = 'spacegroup_canonicals_'
    filename += datetime.datetime.now().strftime('%Y-%m-%d') 
    py.plot(out_fig, filename=filename, auto_open=False)
    py.image.save_as(out_fig, 'canonicals_spacegroups.png')

if __name__ == '__main__':
    sg1_vs_sg2_plotly()
