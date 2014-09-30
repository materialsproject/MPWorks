"""
A runnable script to check all SNL groups
"""
__author__ = 'Patrick Huck'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Patrick Huck'
__email__ = 'phuck@lbl.gov'
__date__ = 'September 22, 2014'

import sys, time, datetime, csv
from itertools import izip_longest
from collections import OrderedDict
from argparse import ArgumentParser
from fnmatch import fnmatch
from collections import Counter
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator, SpeciesComparator
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
creds = tls.get_credentials_file()
stream_ids = creds['stream_ids']
min_sleep = 0.052

sma = SNLMongoAdapter.auto_load()
matcher = StructureMatcher(
    ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
    attempt_supercell=False, comparator=ElementComparator()
)

num_ids_per_stream = 20000
num_ids_per_stream_k = num_ids_per_stream/1000
num_snls = sma.snl.count()
num_snl_streams = num_snls / num_ids_per_stream
if num_snls % num_ids_per_stream: num_snl_streams += 1
num_snlgroups = sma.snlgroups.count()
num_snlgroup_streams = num_snlgroups / num_ids_per_stream
if num_snlgroups % num_ids_per_stream: num_snlgroup_streams += 1
num_pairs_per_job = 200000

checks = ['spacegroups', 'groupmembers', 'canonicals']
categories = [ 'SG Change', 'SG Default', 'PybTeX', 'Others' ]
num_categories = len(categories)
category_colors = ['red', 'blue', 'green', 'orange']

def _get_filename(day=True):
    filename = 'snl_group_check_'
    filename += datetime.datetime.now().strftime('%Y-%m-%d') if day else 'stream'
    return filename

def _get_shades_of_gray(num_colors):
    colors=[]
    for i in range(0, 8*num_colors, 8):
        colors.append('rgb'+str((i, i, i)))
    return colors

def _get_id_range_from_index(index):
    start_id_k = index*num_ids_per_stream_k
    return '%dk - %dk' % (start_id_k, start_id_k+num_ids_per_stream_k)

def _sleep(start_time):
    sleep_time = min_sleep - time.clock() + start_time
    if sleep_time > 0: time.sleep(sleep_time)

class Pair:
    """simple pair of integers with some properties and methods"""
    def __init__(self, i, j):
        self.primary = i
        self.secondary = j
    def copy(self):
        return Pair(self.primary, self.secondary)
    def next_pair(self):
        self.secondary += 1
        if self.secondary > num_snlgroups:
            self.primary += 1
            self.secondary = self.primary + 1
    def __repr__(self):
        return 'Pair(%d,%d)' % (self.primary, self.secondary)

class PairIterator:
    """iterator of specific length for pairs (i,j) w/ j>i"""
    def __init__(self, job_id):
        # TODO: get proper initial pair from job_id
        self.current_pair = Pair(1,178134)
        self.num_pairs = 1
    def __iter__(self):
        return self
    def next(self):
        if self.num_pairs > num_pairs_per_job:
            raise StopIteration
        else:
            self.num_pairs += 1
            current_pair_copy = self.current_pair.copy()
            self.current_pair.next_pair()
            return current_pair_copy


def init_plotly(args):
    """init all plots on plot.ly"""
    # 'spacegroups' & 'groupmembers'
    stream_ids_iter = iter(stream_ids)
    data = Data()
    for check_id,num_streams in enumerate([num_snl_streams, num_snlgroup_streams]):
        for index in range(num_streams):
            data.append(Scatter(
                x=[], y=[], text=[], stream=Stream(
                    token=next(stream_ids_iter), maxpoints=num_ids_per_stream),
                mode='markers', name=_get_id_range_from_index(index),
                xaxis='x%d' % (2*check_id+2), yaxis='y%d' % (2*check_id+2)
            ))
            data.append(Bar(
                x=[0], y=index, stream=Stream(token=next(stream_ids_iter), maxpoints=1),
                name=_get_id_range_from_index(index), orientation='h',
                marker=Marker(color=_get_shades_of_gray(num_streams)[index]),
                xaxis='x%d' % (2*check_id+1), yaxis='y%d' % (2*check_id+1)
            ))
    # total error counts in 'spacegroups' check
    data.append(Bar(
        x=[0.1]*num_categories, y=categories, name='#bad SNLs', xaxis='x5',
        yaxis='y5', orientation='h', marker=Marker(color=category_colors)
    ))
    # layout
    layout = Layout(
        title="SNL Group Checks Stream", showlegend=False, hovermode='closest',
        autosize=False, width=850, height=1300,
        # x-axes
        xaxis1=XAxis(
            domain=[0,.49], range=[0,5000], anchor='y1',
            showgrid=False, title='# good SNLs'
        ),
        xaxis2=XAxis(
            domain=[0,1], range=[0,5000], anchor='y2', showgrid=False,
            title='"relative" ID of bad SNLs (= SNL ID %% %dk)' % num_ids_per_stream_k
        ),
        xaxis3=XAxis(
            domain=[0,.49], range=[0,5000], anchor='y3',
            showgrid=False, title='# good SNL Groups'
        ),
        xaxis4=XAxis(
            domain=[.51,1], anchor='y4', showgrid=False, range=[0,5000],
            title='"relative" ID of bad SNL Groups (= SNL Group ID %% %dk)' % num_ids_per_stream_k
        ),
        xaxis5=XAxis(
            domain=[.51,1], anchor='y5', showgrid=False, title='# bad SNLs'
        ),
        # y-axes
        yaxis1=YAxis(
            domain=[.7,1], range=[-.5,num_snl_streams-.5], anchor='x1', showgrid=False,
            title='range index (= SNL ID / %dk)' % num_ids_per_stream_k
        ),
        yaxis2=YAxis(
            domain=[.35,.65], range=[-.5,num_snl_streams-.5], anchor='x2', showgrid=False,
            title='range index (= SNL ID / %dk)' % num_ids_per_stream_k
        ),
        yaxis3=YAxis(
            domain=[0,.3], range=[-.5,num_snlgroup_streams-.5], anchor='x3', showgrid=False,
            title='range index (= SNL Group ID / %dk)' % num_ids_per_stream_k
        ),
        yaxis4=YAxis(
            domain=[0,.3], range=[-.5,num_snlgroup_streams-.5], anchor='x4',
            showgrid=False, zeroline=False
        ),
        yaxis5=YAxis(
            domain=[.7,1], anchor='x5', side='right', showgrid=False, title='category'
        ),
    )
    fig = Figure(data=data, layout=layout)
    filename = _get_filename(day=False)
    py.plot(fig, filename=filename, auto_open=False)

def check_snl_spacegroups(args):
    """check spacegroups of all available SNLs"""
    range_index = args.start / num_ids_per_stream
    idxs = [range_index*2]
    idxs += [idxs[0]+1]
    s = [py.Stream(stream_ids[i]) for i in idxs]
    for i in range(len(idxs)): s[i].open()
    end = num_snls if args.end > num_snls else args.end
    id_range = {"$gt": args.start, "$lte": end}
    mpsnl_cursor = sma.snl.find({ "snl_id": id_range})
    num_good_ids = 0
    colors=[]
    for mpsnl_dict in mpsnl_cursor:
        start_time = time.clock()
        exc_raised = False
        try:
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc_raised = True
        is_good = (not exc_raised and sf.get_spacegroup_number() == mpsnl.sg_num)
        if is_good: # Bar (good)
            num_good_ids += 1
            data = dict(x=[num_good_ids], y=[range_index])
        else: # Scatter (bad)
            if exc_raised:
                category = 2 if fnmatch(str(exc_type), '*pybtex*') else 3
                text = ' '.join([str(exc_type), str(exc_value)])
            else:
                category = int(sf.get_spacegroup_number() == 0)
                text = '%s: %d' % (mpsnl.snlgroup_key, sf.get_spacegroup_number())
            colors.append(category_colors[category])
            data = dict(
                x=mpsnl_dict['snl_id']%num_ids_per_stream,
                y=range_index, text=text, marker=Marker(color=colors)
            )
        s[is_good].write(data)
    for i in range(len(idxs)): s[i].close()

def check_snls_in_snlgroups(args):
    """check whether SNLs in each SNLGroup still match resp. canonical SNL"""
    range_index = args.start / num_ids_per_stream
    idxs = [2*(num_snl_streams+range_index)]
    idxs += [idxs[0]+1]
    s = [py.Stream(stream_ids[i]) for i in idxs]
    for i in range(len(idxs)): s[i].open()
    end = num_snlgroups if args.end > num_snlgroups else args.end
    id_range = {"$gt": args.start, "$lte": end}
    snlgrp_cursor = sma.snlgroups.find({ "snlgroup_id": id_range})
    colors = []
    num_good_ids = 0
    for snlgrp_dict in snlgrp_cursor:
        start_time = time.clock()
        try:
            snlgrp = SNLGroup.from_dict(snlgrp_dict)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            text = ' '.join([str(exc_type), str(exc_value)])
            colors.append(category_colors[-1]) # Others
            data = dict(
                x=snlgrp_dict['snlgroup_id']%num_ids_per_stream,
                y=range_index, text=text, marker=Marker(color=colors)
            )
            s[0].write(data)
            _sleep(start_time)
            continue
        if len(snlgrp.all_snl_ids) <= 1:
            num_good_ids += 1
            data = dict(x=[num_good_ids], y=[range_index])
            s[1].write(data)
            _sleep(start_time)
            continue
        exc_raised = False
        all_snls_good = True
        for snl_id in snlgrp.all_snl_ids:
            if snl_id == snlgrp.canonical_snl.snl_id: continue
            mpsnl_dict = sma.snl.find_one({ "snl_id": snl_id })
            try:
                mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                is_match = matcher.fit(mpsnl.structure, snlgrp.canonical_structure)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                exc_raised = True
            if exc_raised or not is_match: # Scatter (bad)
                if exc_raised:
                    category = 2 if fnmatch(str(exc_type), '*pybtex*') else 3
                    text = ' '.join([str(exc_type), str(exc_value)])
                else:
                    category = 0
                    text = '%d != can:%d' % (mpsnl_dict['snl_id'], snlgrp.canonical_snl.snl_id)
                colors.append(category_colors[category])
                data = dict(
                    x=snlgrp_dict['snlgroup_id']%num_ids_per_stream,
                    y=range_index, text=text, marker=Marker(color=colors)
                )
                s[0].write(data)
                all_snls_good = False
                _sleep(start_time)
                break
        if all_snls_good: # Bar (good)
            num_good_ids += 1
            data = dict(x=[num_good_ids], y=[range_index])
            s[1].write(data)
            _sleep(start_time)
    for i in range(len(idxs)): s[i].close()

def crosscheck_canonical_snls(args):
    """check whether canonical SNLs of two different SNL groups match"""
    plotly_stream = py.Stream(stream_ids[-1]) #TODO
    plotly_stream.open()
    snlgrp_dict1 = sma.snlgroups.find_one({ "snlgroup_id": args.primary })
    snlgrp1 = SNLGroup.from_dict(snlgrp_dict1)
    secondary_range = range(args.secondary_start, args.secondary_end)
    num_id2 = len(secondary_range)
    for id2 in secondary_range:
        snlgrp_dict2 = sma.snlgroups.find_one({ "snlgroup_id": id2 })
        snlgrp2 = SNLGroup.from_dict(snlgrp_dict2)
        # check composition AND spacegroup via snlgroup_key
        if snlgrp1.canonical_snl.snlgroup_key != snlgrp2.canonical_snl.snlgroup_key:
            continue
        if matcher.fit(snlgrp1.canonical_structure, snlgrp2.canonical_structure):
            # how many other SNLGroups match the current (primary) group?
            data = dict(x=args.primary, y=3)
            plotly_stream.write(data)
            time.sleep(min_sleep)
    plotly_stream.close()

def analyze(args):
    """analyze data at any point for a copy of the streaming figure"""
    # NOTE: make copy online first with suffix _%Y-%m-%d and note figure id
    fig = py.get_figure(creds['username'], args.fig_id)
    errors = Counter()
    bad_snls = OrderedDict((cat,[]) for cat in categories)
    bad_snlgroups = []
    for i,d in enumerate(fig['data']):
        if not isinstance(d, Scatter): continue
        if not 'x' in d or not 'y' in d or not 'text' in d: continue
        start_id = int(d['name'].split(' - ')[0][:-1])*1000
        marker_colors = d['marker']['color']
        if i < 2*num_snl_streams: # spacegroups
            errors += Counter(marker_colors)
            for idx,color in enumerate(marker_colors):
                snl_id = start_id + d['x'][idx]
                color_index = category_colors.index(color)
                bad_snls[categories[color_index]].append(snl_id)
        else: # groupmembers
            for idx,color in enumerate(marker_colors):
                if color != category_colors[0]: continue
                snlgroup_id = start_id + d['x'][idx]
                mismatch_snl_id, canonical_snl_id = d['text'][idx].split(' != ')
                canonical_snl_id = canonical_snl_id[4:]
                bad_snlgroups.append([snlgroup_id, canonical_snl_id, mismatch_snl_id])
    print errors
    fig_data = fig['data'][-1]
    fig_data['x'] = [ errors[color] for color in fig_data['marker']['color'] ]
    filename = _get_filename()
    print filename
    #py.plot(fig, filename=filename)
    with open('mpworks/check_snl/bad_snls.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(categories)
        for row in izip_longest(*bad_snls.values()):
            writer.writerow(row)
    with open('mpworks/check_snl/bad_snlgroups.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(['snlgroup_id', 'canonical_snl_id', 'first_mismatching_snl_id'])
        for row in bad_snlgroups:
            writer.writerow(row)
    #py.image.save_as(fig, _get_filename()+'.png')
    # NOTE: service unavailable!? static images can also be saved by appending
    # the appropriate extension (pdf,jpg,png,eps) to the public URL

if __name__ == '__main__':
    # create top-level parser
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    # sub-command: init
    parser_init = subparsers.add_parser('init')
    parser_init.set_defaults(func=init_plotly)

    # sub-command: analyze
    parser_ana = subparsers.add_parser('analyze')
    parser_ana.add_argument('--fig-id', help='plotly figure id', default=6, type=int)
    parser_ana.set_defaults(func=analyze)

    # sub-command: spacegroups
    # This task can be split in multiple parallel jobs by SNL id ranges
    parser_task0 = subparsers.add_parser('spacegroups')
    parser_task0.add_argument('--start', help='start SNL Id', default=0, type=int)
    parser_task0.add_argument('--end', help='end SNL Id', default=10, type=int)
    parser_task0.set_defaults(func=check_snl_spacegroups)

    # sub-command: groupmembers
    # This task can be split in multiple parallel jobs by SNLGroup id ranges
    parser_task1 = subparsers.add_parser('groupmembers')
    parser_task1.add_argument('--start', help='start SNLGroup Id', default=0, type=int)
    parser_task1.add_argument('--end', help='end SNLGroup Id', default=10, type=int)
    parser_task1.set_defaults(func=check_snls_in_snlgroups)

    # sub-command: canonicals
    # This task can be split in multiple parallel jobs by SNLGroup combinations
    # of (primary, secondary) ID's. The range for the secondary id always starts
    # at primary+1 (to avoid dupes)
    # To keep the load balanced for each job, a constant number of
    # primary-secondary-id combination/pairs is submitted with each. Hence, the
    # respective job/pair-range id is given as a mandatory arg on the command line.
    parser_task2 = subparsers.add_parser('canonicals')
    parser_task2.add_argument('job-id', help='index/id for job/pair-range', type=int)
    parser_task2.set_defaults(func=crosscheck_canonical_snls)

    # parse args and call function
    args = parser.parse_args()
    args.func(args)
