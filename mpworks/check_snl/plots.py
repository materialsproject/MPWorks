from pandas.io.parsers import read_csv
from pyana.ccsgp.ccsgp import make_plot
from pyana.ccsgp.utils import getOpts
from pandas import Series
from collections import OrderedDict

def sg1_vs_sg2():
    """plot SG #1 vs #2"""
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

if __name__ == '__main__':
    sg1_vs_sg2()
