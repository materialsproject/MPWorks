import os, csv
from builders.init_plotly import py
if py is not None:
    fig = py.get_figure("https://plot.ly/~plotly.materialsproject/112")
    with open('mpworks/check_snl/results/shared_icsds.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow([
            'snlgroup_id 1', 'snlgroup_id 2', 'snl_id 1', 'snl_id 2', 'shared icsd_id', 'matches'
        ])
        for category, text in zip(fig['data'][2]['y'], fig['data'][2]['text']):
            for line in text.split('<br>'):
                before_colon, after_colon = line.split(':')
                snlgroup1, snlgroup2 = map(int, before_colon[1:-1].split(','))
                snls, icsd_matches = after_colon.split('->')
                snl1, snl2 = map(int, snls[2:-2].split(','))
                icsd, matches = icsd_matches.strip().split(' ')
                writer.writerow([snlgroup1, snlgroup2, snl1, snl2, int(icsd), matches[1:-1]])
