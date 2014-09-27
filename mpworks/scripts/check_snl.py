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

sma = SNLMongoAdapter.auto_load()
matcher = StructureMatcher(
    ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
    attempt_supercell=False, comparator=ElementComparator()
)
num_ids_per_stream = 20000
num_snls = sma.snl.count()
num_snlgroups = sma.snlgroups.count()
checks = ['spacegroups', 'groupmembers', 'canonicals']
categories = [ 'SG Change', 'SG Default', 'PybTeX', 'Others' ]
num_categories = len(categories)
category_colors = ['red', 'blue', 'green', 'orange']

def _get_filename(check, day=True):
    filename = 'snl_group_check_%s' % check
    if day: filename += datetime.datetime.now().strftime('_%Y-%m-%d')
    else: filename += '_stream'
    return filename

def _get_shades_of_gray(num_colors):
    colors=[]
    for i in range(0, 8*num_colors, 8):
        colors.append('rgb'+str((i, i, i)))
    return colors

def init_plotly(args):
    """init all plots on plot.ly"""
    streams_counter = 0
    for check in checks:
        num_ids = num_snls if check == 'spacegroups' else num_snlgroups
        num_streams = num_ids / num_ids_per_stream
        if num_ids % num_ids_per_stream: num_streams += 1
        # data
        data = Data()
        for index in range(num_streams):
            # TODO: it seems only maxpoints <= 10000 allowed
            # => problematic if more than half of IDs in Stream are bad
            stream = Stream(token=stream_ids[streams_counter], maxpoints=num_ids_per_stream)
            name = '%dk - %dk' % (index*num_ids_per_stream/1000, (index+1)*num_ids_per_stream/1000)
            data.append(Scatter(
                x=[], y=[], text=[], stream=stream, mode='markers',
                name=name, xaxis='x1', yaxis='y1'
            ))
            streams_counter += 1
        for index in range(num_streams):
            stream = Stream(token=stream_ids[streams_counter], maxpoints=1)
            name = '%dk - %dk' % (index*num_ids_per_stream/1000, (index+1)*num_ids_per_stream/1000)
            color = _get_shades_of_gray(num_streams)[index]
            data.append(Bar(
                x=[15000], y=index, stream=stream, name=name,
                xaxis='x2', yaxis='y2', orientation='h', marker=Marker(color=color)
            ))
            streams_counter += 1
        stream = Stream(token=stream_ids[streams_counter], maxpoints=num_categories)
        data.append(Bar(
            x=[0.1]*num_categories, y=categories, stream=stream, name='#bad SNLs',
            xaxis='x3', yaxis='y3', orientation='h', marker=Marker(color=category_colors)
        ))
        # layout
        # TODO Give general description somewhere in figure
        layout = Layout(
            title="SNL Group Checks Stream",
            showlegend=False, hovermode='closest',
            xaxis1=XAxis(
                domain=[0,1], range=[0,num_ids_per_stream], anchor='y1',
                showgrid=False,
                title='"relative" ID of bad SNL%ss (= SNL ID %% %dk)' % (
                    'Group' if check != 'spacegroups' else '', num_ids_per_stream/1000)
            ),
            yaxis1=YAxis(
                domain=[0,.49], range=[-.5,num_streams-.5], anchor='x1', showgrid=False,
                title='range index (= SNL ID / %dk)' % (num_ids_per_stream/1000)
            ),
            xaxis2=XAxis(
                domain=[0,.49], range=[0,num_ids_per_stream], anchor='y2',
                side='top', showgrid=False, title='# good SNL%ss (max. %dk)' % (
                    'Group' if check != 'spacegroups' else '', num_ids_per_stream/1000)
            ),
            yaxis2=YAxis(
                domain=[.51,1], range=[-.5,num_streams-.5], anchor='x2', showgrid=False,
                title='range index (= SNL ID / %dk)' % (num_ids_per_stream/1000)
            ),
            xaxis3=XAxis(
                domain=[.51,1], anchor='y3', side='top', showgrid=False,
                title='# bad SNL%ss' % ('Group' if check != 'spacegroups' else '')
            ),
            yaxis3=YAxis(
                domain=[.51,1], anchor='x3', side='right',
                showgrid=False, title='error category'
            )
        )
        fig = Figure(data=data, layout=layout)
        filename = _get_filename(check, day=False)
        py.plot(fig, filename=filename, auto_open=False)
        break # remove to also init groupmembers and canonicals

def check_snl_spacegroups(args):
    """check spacegroups of all available SNLs"""
    num_streams = num_snls / num_ids_per_stream
    if num_snls % num_ids_per_stream: num_streams += 1
    idxs = [args.start / num_ids_per_stream]
    idxs += [idxs[0] + num_streams]
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
            data = dict(x=[num_good_ids], y=[idxs[0]])
        else: # Scatter (bad)
            if exc_raised:
                category = 2 if fnmatch(str(exc_type), '*pybtex*') else 3
                text = ' '.join([str(exc_type), str(exc_value)])
            else:
                category = int(sf.get_spacegroup_number() == 0)
                text = '%s: %d' % (mpsnl.snlgroup_key, sf.get_spacegroup_number())
            colors.append(category_colors[category])
            data = dict(
                x=mpsnl_dict['snl_id']%num_ids_per_stream, y=idxs[0],
                text=text, marker=Marker(color=colors)
            )
        s[is_good].write(data)
        sleep_time = 0.052 - time.clock() + start_time
        if sleep_time > 0: time.sleep(sleep_time)
    for i in range(len(idxs)): s[i].close()

def check_snls_in_snlgroups(args):
    """check whether SNLs in each SNLGroup still match resp. canonical SNL"""
    plotly_stream = py.Stream(stream_ids[-1]) #TODO
    plotly_stream.open()
    id_range = {"$gt": args.start, "$lte": args.end}
    snlgrp_cursor = sma.snlgroups.find({ "snlgroup_id": id_range})
    for snlgrp_dict in snlgrp_cursor:
        snlgrp = SNLGroup.from_dict(snlgrp_dict)
        num_snl = len(snlgrp.all_snl_ids)
        all_snls_match = True
        start_time = time.clock()
        for snl_id in snlgrp.all_snl_ids:
            # TODO: add num_snl attribute in SNLGroup
            if snl_id == snlgrp.canonical_snl.snl_id or num_snl <= 1: continue
            mpsnl_dict = sma.snl.find_one({ "snl_id": snl_id })
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            if not matcher.fit(mpsnl.structure, snlgrp.canonical_structure):
                all_snls_match = False
                break
        time_diff = time.clock() - start_time
        if all_snls_match:
            data = dict(x=snlgrp.canonical_snl.snl_id, y=2)
            sleep_time = 0.08 - time_diff
            if sleep_time > 0: time.sleep(sleep_time)
            plotly_stream.write(data)
    plotly_stream.close()

def crosscheck_canonical_snls(args):
    """check whether canonical SNLs of two different SNL groups match"""
    plotly_stream = py.Stream(stream_ids[-1]) #TODO
    plotly_stream.open()
    snlgrp_dict1 = sma.snlgroups.find_one({ "snlgroup_id": args.primary })
    snlgrp1 = SNLGroup.from_dict(snlgrp_dict1)
    secondary_range = range(args.secondary_start, args.secondary_end)
    num_id2 = len(secondary_range)
    start_time = time.clock()
    for id2 in secondary_range:
        snlgrp_dict2 = sma.snlgroups.find_one({ "snlgroup_id": id2 })
        snlgrp2 = SNLGroup.from_dict(snlgrp_dict2)
        # check composition AND spacegroup via snlgroup_key
        # matcher.fit only does composition check and returns None when different compositions
        # TODO: add snlgroup_key attribute to SNLGroup for convenience
        if time.clock() - start_time > 5.: # heartbeat
            print id2
            start_time = time.clock()
        if snlgrp1.canonical_snl.snlgroup_key != snlgrp2.canonical_snl.snlgroup_key:
            continue
        if matcher.fit(snlgrp1.canonical_structure, snlgrp2.canonical_structure):
            # how many other SNLGroups match the current (primary) group?
            data = dict(x=args.primary, y=3)
            plotly_stream.write(data)
            time.sleep(0.08)
    plotly_stream.close()

def analyze(args):
    """analyze data at any point for a copy of the streaming figure"""
    if args.check not in checks:
        print "no analysis available for %s. Choose one of %r" % (args.check, checks)
        return
    # NOTE: make copy online first with suffix _%Y-%m-%d and note figure id
    fig = py.get_figure(creds['username'], args.fig_id)
    errors = Counter()
    sg_change_snls = []
    for d in fig['data']:
        if isinstance(d, Scatter) and 'x' in d and 'y' in d and 'text' in d:
            start_id = int(d['name'].split(' - ')[0][:-1])*1000
            marker_colors = d['marker']['color']
            errors += Counter(marker_colors)
            sg_change_snls += [
                start_id + d['x'][idx] for idx,color in enumerate(marker_colors)
                if color == category_colors[0]
            ]
    # NOTE: following worked shortly, then spinning wheel only
    #fig['layout'].update(annotations=Annotations([Annotation(
    #    x=0, y=1.2875, text=str(errors), showarrow=False, align='left'
    #)]))
    #py.plot(fig, filename=_get_filename(args.check))
    with open('mpworks/scripts/bad_snls.csv', 'wb') as f:
        writer = csv.writer(f)
        for row in sg_change_snls:
            writer.writerow([row])
    #py.image.save_as(fig, _get_filename(args.check)+'.png')
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
    parser_ana.add_argument('check', help='which check to analyze')
    parser_ana.add_argument('--fig-id', help='plotly figure id', default=2, type=int)
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
    parser_task2 = subparsers.add_parser('canonicals')
    parser_task2.add_argument('--primary', help='primary SNLGroup Id', default=1, type=int)
    parser_task2.add_argument('--secondary-start', help='secondary start SNLGroup Id', default=2, type=int)
    parser_task2.add_argument('--secondary-end', help='secondary end SNLGroup Id', default=1000, type=int)
    parser_task2.set_defaults(func=crosscheck_canonical_snls)

    # parse args and call function
    args = parser.parse_args()
    args.func(args)
