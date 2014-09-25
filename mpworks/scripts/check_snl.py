"""
A runnable script to check all SNL groups
"""
__author__ = 'Patrick Huck'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Patrick Huck'
__email__ = 'phuck@lbl.gov'
__date__ = 'September 22, 2014'

import sys, time
from argparse import ArgumentParser
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.symmetry.finder import SymmetryFinder
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator, SpeciesComparator
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
stream_ids = tls.get_credentials_file()['stream_ids']

sma = SNLMongoAdapter.auto_load()
matcher = StructureMatcher(
    ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
    attempt_supercell=False, comparator=ElementComparator()
)
num_ids_per_stream = 20000
num_snls = sma.snl.count()
num_snlgroups = sma.snlgroups.count()

def init_plotly(args):
    checks = ['spacegroups', 'groupmembers', 'canonicals']
    streams_counter = 0
    for check in checks:
        num_ids = num_snls if check == 'spacegroups' else num_snlgroups
        num_streams = num_ids / num_ids_per_stream
        if num_ids % num_ids_per_stream: num_streams += 1
        data = []
        for index in range(num_streams):
            stream = Stream(token=stream_ids[streams_counter], maxpoints=num_ids_per_stream)
            name = '%d - %d' % (index*num_ids_per_stream, (index+1)*num_ids_per_stream)
            data.append(Scatter(
                x=[], y=[], text=[], stream=stream, mode='markers', name=name
            ))
            streams_counter += 1
        xaxis = XAxis(title='SNL ID' if check == 'spacegroups' else 'SNL Group ID')
        yaxis = YAxis(title='Status (>=1: ok, <1: !ok)')
        layout = Layout(title=check, xaxis=xaxis, yaxis=yaxis)
        fig = Figure(data=Data(data), layout=layout)
        unique_url = py.plot(fig, filename='snl_group_check_%s' % check)
        break # remove to also init groupmembers and canonicals

def check_snl_spacegroups(args):
    """check spacegroups of all available SNLs"""
    stream_index = args.start/num_ids_per_stream
    s = py.Stream(stream_ids[stream_index])
    s.open()
    end = num_snls if args.end > num_snls else args.end
    id_range = {"$gt": args.start, "$lte": end}
    mpsnl_cursor = sma.snl.find({ "snl_id": id_range})
    for mpsnl_dict in mpsnl_cursor:
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        is_match = sf.get_spacegroup_number() == mpsnl.sg_num
        data = dict(
            x=mpsnl_dict['snl_id']%num_ids_per_stream,
            y=int(is_match) + stream_index*0.05,
            text='sg_num: %d' % (mpsnl.sg_num) if is_match \
            else 'sg_num: %d -> %d' % (mpsnl.sg_num, sf.get_spacegroup_number())
        )
        s.write(data)
        time.sleep(0.08)
    s.close()

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

if __name__ == '__main__':
    # create top-level parser
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    # sub-command: init
    parser_init = subparsers.add_parser('init')
    parser_init.set_defaults(func=init_plotly)

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
