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

def init_plotly(args):
    stream = Stream(token=stream_ids[0], maxpoints=500) # TODO maxpoints
    data = Data([Histogram2d(
        x=[], y=[], autobinx=False, autobiny=False, stream=stream,
        xbins=XBins(start=0.5,end=100.5,size=1),
        ybins=YBins(start=0.5,end=3.5,size=1), # 'sg', 'gm', 'can'
    )])
    xaxis = XAxis(title='SNL or SNL Group ID')
    layout = Layout(title='SNL group checks', xaxis=xaxis)
    fig = Figure(data=data, layout=layout)
    unique_url = py.plot(fig, filename='snl_group_check_stream')

def check_snl_spacegroups(args):
    """check spacegroups of all available SNLs"""
    id_range = {"$gt": args.start, "$lte": args.end}
    mpsnl_cursor = sma.snl.find({ "snl_id": id_range})
    s = py.Stream(stream_ids[0]) # keep open in HPC env
    s.open()
    data = dict(x=[], y=[])
    start_time = time.clock()
    for mpsnl_dict in mpsnl_cursor:
        mpsnl = MPStructureNL.from_dict(mpsnl_dict)
        sf = SymmetryFinder(mpsnl.structure, symprec=0.1)
        if sf.get_spacegroup_number() == mpsnl.sg_num:
            data['x'].append(mpsnl_dict['snl_id'])
            data['y'].append(1)
            time_diff = time.clock() - start_time
            if time_diff > 0.08:
                print data
                s.write(data)
                data['x'][:] = []
                data['y'][:] = []
                start_time = time.clock()
    sleep_time =  0.08 - time.clock() + start_time
    time.sleep(sleep_time)
    print data
    s.write(data)
    s.close()

def check_snls_in_snlgroups(args):
    """check whether SNLs in each SNLGroup still match resp. canonical SNL"""
    id_range = {"$gt": args.start, "$lte": args.end}
    snlgrp_cursor = sma.snlgroups.find({ "snlgroup_id": id_range})
    s = py.Stream(stream_ids[1]) # keep open in HPC env
    s.open()
    for snlgrp_dict in snlgrp_cursor:
        snlgrp = SNLGroup.from_dict(snlgrp_dict)
        print snlgrp.all_snl_ids
        num_snl = len(snlgrp.all_snl_ids)
        for i,snl_id in enumerate(snlgrp.all_snl_ids):
            # TODO: add num_snl attribute in SNLGroup
            if snl_id == snlgrp.canonical_snl.snl_id or num_snl <= 1: continue
            mpsnl_dict = sma.snl.find_one({ "snl_id": snl_id })
            mpsnl = MPStructureNL.from_dict(mpsnl_dict)
            structures_match = matcher.fit(mpsnl.structure, snlgrp.canonical_structure)
            offset = 0.3/num_snl * i
            data_point = dict(
                x = snlgrp.canonical_snl.snl_id + offset,
                y = int(structures_match) + offset,
                text = 'snl_id: %d' % snl_id
            )
            print data_point
            s.write(data_point)
            time.sleep(0.08)
    s.close()

def crosscheck_canonical_snls(args):
    """check whether canonical SNLs of two different SNL groups match"""
    snlgrp_dict1 = sma.snlgroups.find_one({ "snlgroup_id": args.primary })
    snlgrp1 = SNLGroup.from_dict(snlgrp_dict1)
    s = py.Stream(stream_ids[2]) # keep open in HPC env
    s.open()
    secondary_range = range(args.secondary_start, args.secondary_end)
    num_id2 = len(secondary_range)
    for i,id2 in enumerate(secondary_range):
        snlgrp_dict2 = sma.snlgroups.find_one({ "snlgroup_id": id2 })
        snlgrp2 = SNLGroup.from_dict(snlgrp_dict2)
        # check composition AND spacegroup via snlgroup_key
        # TODO: add snlgroup_key attribute to SNLGroup for convenience
        if snlgrp1.canonical_snl.snlgroup_key != snlgrp2.canonical_snl.snlgroup_key:
            if not i%1000:
                s.write({"x":"\n"}) # needs to be once a minute to keep stream open
                print '-->', id2
            continue
        # matcher.fit only does composition check and returns None when different compositions
        match = matcher.fit(snlgrp1.canonical_structure, snlgrp2.canonical_structure)
        offset = 0.3/num_id2 * i
        data_point = dict(
            x = args.primary + offset + 0.1,
            y = int(match) + offset,
            text = 'snlgroup_id: %d' % id2
        )
        print data_point
        s.write(data_point)
        time.sleep(0.08)
    s.close()

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
    parser_task2.add_argument('--secondary-end', help='secondary end SNLGroup Id', default=10000, type=int)
    parser_task2.set_defaults(func=crosscheck_canonical_snls)

    # parse args and call function
    args = parser.parse_args()
    args.func(args)
