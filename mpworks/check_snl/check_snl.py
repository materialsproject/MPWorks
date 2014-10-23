"""
A runnable script to check all SNL groups
"""
__author__ = 'Patrick Huck'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Patrick Huck'
__email__ = 'phuck@lbl.gov'
__date__ = 'September 22, 2014'

import sys, time, datetime, csv, os
from math import sqrt
from collections import OrderedDict
from argparse import ArgumentParser
from fnmatch import fnmatch
from collections import Counter
from mpworks.snl_utils.snl_mongo import SNLMongoAdapter
from mpworks.snl_utils.mpsnl import MPStructureNL, SNLGroup
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator, SpeciesComparator
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
from mpworks.check_snl.utils import div_plus_mod, sleep
from ast import literal_eval as make_tuple
from itertools import chain

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
num_snlgroups = sma.snlgroups.count()
num_pairs_per_job = 1000 * num_ids_per_stream
num_pairs_max = num_snlgroups*(num_snlgroups-1)/2

num_snl_streams = div_plus_mod(num_snls, num_ids_per_stream)
num_snlgroup_streams = div_plus_mod(num_snlgroups, num_ids_per_stream)
num_jobs = div_plus_mod(num_pairs_max, num_pairs_per_job)
print num_snl_streams, num_snlgroup_streams, num_jobs

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

def _get_snl_extra_info(mpsnl):
    return [
        str(mpsnl.structure.num_sites),
        ' / '.join(mpsnl.remarks),
        ' / '.join(mpsnl.projects),
        ' / '.join([author.email for author in mpsnl.authors])
    ]

class Pair:
    """simple pair of integers with some properties and methods"""
    def __init__(self, i, j):
        self.primary = i if i < num_snlgroups else num_snlgroups
        self.secondary = j if j <= num_snlgroups else num_snlgroups
    def copy(self):
        return Pair(self.primary, self.secondary)
    def next_pair(self):
        self.secondary += 1
        if self.secondary > num_snlgroups:
            self.primary += 1
            if self.primary > num_snlgroups:
                raise StopIteration
            self.secondary = self.primary + 1
    def __repr__(self):
        return 'Pair(%d,%d)' % (self.primary, self.secondary)

class PairIterator:
    """iterator of specific length for pairs (i,j) w/ j>i

    The combinatorial task of comparing pairs of SNLGroups can be split in
    multiple parallel jobs by SNLGroup combinations of (primary, secondary)
    ID's. The range for the secondary id always starts at primary+1 (to avoid
    dupes) To keep the load balanced for each job, a constant number of
    primary-secondary-id combination/pairs is submitted with each. Hence, the
    respective job/pair-range id is given as a mandatory arg on the command
    line.
    """
    def __init__(self, job_id):
        if job_id * num_pairs_per_job > num_pairs_max:
            raise ValueError('job_id cannot be larger than %d', num_jobs-1)
        self.current_pair = self._get_initial_pair(job_id)
        self.num_pairs = 1
    def __iter__(self):
        return self
    def _get_initial_pair(self, job_id):
        N, J, M = num_snlgroups, job_id, num_pairs_per_job
        i = int(N+.5-sqrt(N*(N-1)+.25-2*J*M))
        j = J*M-(i-1)*(2*N-i)/2+i+1
        return Pair(i,j)
    def next(self):
        if self.num_pairs > num_pairs_per_job:
            raise StopIteration
        else:
            self.num_pairs += 1
            current_pair_copy = self.current_pair.copy()
            self.current_pair.next_pair()
            return current_pair_copy
    def __repr__(self):
        return 'PairIterator(%r, %d)' % (self.current_pair, self.num_pairs)


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
            sf = SpacegroupAnalyzer(mpsnl.structure, symprec=0.1)
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
            sleep(start_time)
            continue
        if len(snlgrp.all_snl_ids) <= 1:
            num_good_ids += 1
            data = dict(x=[num_good_ids], y=[range_index])
            s[1].write(data)
            sleep(start_time)
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
                sleep(start_time)
                break
        if all_snls_good: # Bar (good)
            num_good_ids += 1
            data = dict(x=[num_good_ids], y=[range_index])
            s[1].write(data)
            sleep(start_time)
    for i in range(len(idxs)): s[i].close()

def analyze(args):
    """analyze data at any point for a copy of the streaming figure"""
    # NOTE: make copy online first with suffix _%Y-%m-%d and note figure id
    fig = py.get_figure(creds['username'], args.fig_id)
    if args.t:
        if args.fig_id == 12:
            label_entries = filter(None, '<br>'.join(fig['data'][2]['text']).split('<br>'))
            pairs = map(make_tuple, label_entries)
            grps = set(chain.from_iterable(pairs))
            snlgrp_cursor = sma.snlgroups.aggregate([
                { '$match': { 'snlgroup_id': { '$in': list(grps) } } },
                { '$project': { 'snlgroup_id': 1, 'canonical_snl.snlgroup_key': 1, '_id': 0 } }
            ], cursor={})
            snlgroup_keys = {}
            for d in snlgrp_cursor:
                snlgroup_keys[d['snlgroup_id']] = d['canonical_snl']['snlgroup_key']
            print snlgroup_keys[40890]
            sma2 = SNLMongoAdapter.from_file(
                os.path.join(os.environ['DB_LOC'], 'materials_db.yaml')
            )
            materials_cursor = sma2.database.materials.aggregate([
                { '$match': { 'snlgroup_id_final': { '$in': list(grps) } } },
                { '$project': {
                    'snlgroup_id_final': 1, '_id': 0, 'task_id': 1,
                    'final_energy_per_atom': 1,
                    'band_gap.search_gap.band_gap': 1,
                    'volume': 1, 'nsites': 1
                }}
            ], cursor={})
            snlgroup_data = {}
            for material in materials_cursor:
                snlgroup_id = material['snlgroup_id_final']
                final_energy_per_atom = material['final_energy_per_atom']
                band_gap = material['band_gap']['search_gap']['band_gap']
                volume_per_atom = material['volume'] / material['nsites']
                snlgroup_data[snlgroup_id] = {
                    'final_energy_per_atom': final_energy_per_atom,
                    'band_gap': band_gap, 'task_id': material['task_id'],
                    'volume_per_atom': volume_per_atom
                }
            print snlgroup_data[40890]
            filestem = 'mpworks/check_snl/results/bad_snlgroups_2_'
            with open(filestem+'in_matdb.csv', 'wb') as f, \
                    open(filestem+'notin_matdb.csv', 'wb') as g:
                writer1, writer2 = csv.writer(f), csv.writer(g)
                header = [
                    'category', 'composition',
                    'snlgroup_id 1', 'sg_num 1', 'task_id 1',
                    'snlgroup_id 2', 'sg_num 2', 'task_id 2',
                    'delta_energy', 'delta_bandgap', 'delta_volume_per_atom',
                    'rms_dist', 'scenario'
                ]
                writer1.writerow(header)
                writer2.writerow(header)
                for primary_id, secondary_id in pairs:
                    composition, primary_sg_num = snlgroup_keys[primary_id].split('--')
                    secondary_sg_num = snlgroup_keys[secondary_id].split('--')[1]
                    category = 'same SGs' if primary_sg_num == secondary_sg_num else 'diff. SGs'
                    if primary_id not in snlgroup_data or secondary_id not in snlgroup_data:
                        delta_energy, delta_bandgap, delta_volume_per_atom = '', '', ''
                    else:
                        delta_energy = "{0:.3g}".format(abs(
                            snlgroup_data[primary_id]['final_energy_per_atom'] - \
                            snlgroup_data[secondary_id]['final_energy_per_atom']
                        ))
                        delta_bandgap = "{0:.3g}".format(abs(
                            snlgroup_data[primary_id]['band_gap'] - \
                            snlgroup_data[secondary_id]['band_gap']
                        ))
                        delta_volume_per_atom = "{0:.3g}".format(abs(
                            snlgroup_data[primary_id]['volume_per_atom'] - \
                            snlgroup_data[secondary_id]['volume_per_atom']
                        ))
                    scenario, rms_dist_str = '', ''
                    if category == 'diff. SGs' and delta_energy and delta_bandgap:
                        scenario = 'different' if (
                            float(delta_energy) > 0.01 or float(delta_bandgap) > 0.1
                        ) else 'similar'
                        snlgrp1_dict = sma.snlgroups.find_one({ "snlgroup_id": primary_id })
                        snlgrp2_dict = sma.snlgroups.find_one({ "snlgroup_id": secondary_id })
                        snlgrp1 = SNLGroup.from_dict(snlgrp1_dict)
                        snlgrp2 = SNLGroup.from_dict(snlgrp2_dict)
                        primary_structure = snlgrp1.canonical_structure
                        secondary_structure = snlgrp2.canonical_structure
                        rms_dist = matcher.get_rms_dist(primary_structure, secondary_structure)
                        if rms_dist is not None:
                            rms_dist_str = "({0:.3g},{1:.3g})".format(*rms_dist)
                            print rms_dist_str
                    row = [
                        category, composition,
                        primary_id, primary_sg_num,
                        snlgroup_data[primary_id]['task_id'] \
                        if primary_id in snlgroup_data else '',
                        secondary_id, secondary_sg_num,
                        snlgroup_data[secondary_id]['task_id'] \
                        if secondary_id in snlgroup_data else '',
                        delta_energy, delta_bandgap, delta_volume_per_atom,
                        rms_dist_str, scenario
                    ]
                    if delta_energy and delta_bandgap: writer1.writerow(row)
                    else: writer2.writerow(row)
        elif args.fig_id == 16:
            out_fig = Figure()
            badsnls_trace = Scatter(x=[], y=[], text=[], mode='markers', name='SG Changes')
            bisectrix = Scatter(x=[0,230], y=[0,230], mode='lines', name='bisectrix')
            print 'pulling bad snls from plotly ...'
            bad_snls = OrderedDict()
            for category, text in zip(fig['data'][2]['y'], fig['data'][2]['text']):
                for snl_id in map(int, text.split('<br>')):
                    bad_snls[snl_id] = category
            with open('mpworks/check_snl/results/bad_snls.csv', 'wb') as f:
                print 'pulling bad snls from database ...'
                mpsnl_cursor = sma.snl.find({
                    'snl_id': { '$in': bad_snls.keys() },
                    'about.projects': {'$ne': 'CederDahn Challenge'}
                })
                writer = csv.writer(f)
                writer.writerow([
                    'snl_id', 'category', 'snlgroup_key', 'nsites', 'remarks', 'projects', 'authors'
                ])
                print 'writing bad snls to file ...'
                for mpsnl_dict in mpsnl_cursor:
                    mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                    row = [ mpsnl.snl_id, bad_snls[mpsnl.snl_id], mpsnl.snlgroup_key ]
                    row += _get_snl_extra_info(mpsnl)
                    writer.writerow(row)
                    sg_num = mpsnl.snlgroup_key.split('--')[1]
                    if (bad_snls[mpsnl.snl_id] == 'SG default' and sg_num != '-1') or \
                       bad_snls[mpsnl.snl_id] == 'SG change':
                        mpsnl.structure.remove_oxidation_states()
                        sf = SpacegroupAnalyzer(mpsnl.structure, symprec=0.1)
                        badsnls_trace['x'].append(mpsnl.sg_num)
                        badsnls_trace['y'].append(sf.get_spacegroup_number())
                        badsnls_trace['text'].append(mpsnl.snl_id)
                        if bad_snls[mpsnl.snl_id] == 'SG default':
                            print sg_num, sf.get_spacegroup_number()
                print 'plotting out-fig ...'
                out_fig['data'] = Data([bisectrix, badsnls_trace])
                out_fig['layout'] = Layout(
                    showlegend=False, hovermode='closest',
                    title='Spacegroup Assignment Changes',
                    xaxis=XAxis(showgrid=False, title='old SG number', range=[0,230]),
                    yaxis=YAxis(showgrid=False, title='new SG number', range=[0,230]),
                )
                filename = 'spacegroup_changes_'
                filename += datetime.datetime.now().strftime('%Y-%m-%d') 
                py.plot(out_fig, filename=filename, auto_open=False)
        elif args.fig_id == 18:
            print 'pulling data from plotly ...'
            trace = Scatter(x=[], y=[], text=[], mode='markers', name='mismatches')
            bad_snls = OrderedDict() # snlgroup_id : [ mistmatching snl_ids ]
            for category, text in zip(fig['data'][2]['y'], fig['data'][2]['text']):
                if category != 'mismatch': continue
                for entry in text.split('<br>'):
                    fields = entry.split(':')
                    snlgroup_id = fields[0].split(',')[0]
                    bad_snls[int(snlgroup_id)] = fields[1].split(',')
                    for i, snl_id in enumerate(bad_snls[int(snlgroup_id)]):
                        trace['x'].append(snlgroup_id)
                        trace['y'].append(i+1)
                        trace['text'].append(snl_id)
            with open('mpworks/check_snl/results/bad_snlgroups.csv', 'wb') as f:
                print 'pulling bad snlgroups from database ...'
                snlgroup_cursor = sma.snlgroups.find({
                    'snlgroup_id': { '$in': bad_snls.keys() },
                })
                writer = csv.writer(f)
                writer.writerow(['snlgroup_id', 'snlgroup_key', 'mismatching snl_ids'])
                print 'writing bad snlgroups to file ...'
                for snlgroup_dict in snlgroup_cursor:
                    snlgroup = SNLGroup.from_dict(snlgroup_dict)
                    row = [
                        snlgroup.snlgroup_id, snlgroup.canonical_snl.snlgroup_key,
                        ' '.join(bad_snls[snlgroup.snlgroup_id])
                    ]
                    writer.writerow(row)
            print 'plotting out-fig ...'
            out_fig = Figure()
            out_fig['data'] = Data([trace])
            out_fig['layout'] = Layout(
                showlegend=False,
                title='Member Mismatches of SNLGroup Canonicals',
                xaxis=XAxis(showgrid=False, title='snlgroup_id'),
                yaxis=YAxis(showgrid=False, title='# mismatching SNLs'),
            )
            filename = 'groupmember_mismatches_'
            filename += datetime.datetime.now().strftime('%Y-%m-%d') 
            py.plot(out_fig, filename=filename, auto_open=False)
    else:
        errors = Counter()
        bad_snls = OrderedDict()
        bad_snlgroups = OrderedDict()
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
                    category = categories[color_index]
                    bad_snls[snl_id] = category
            else: # groupmembers
                for idx,color in enumerate(marker_colors):
                    if color != category_colors[0]: continue
                    snlgroup_id = start_id + d['x'][idx]
                    mismatch_snl_id, canonical_snl_id = d['text'][idx].split(' != ')
                    bad_snlgroups[snlgroup_id] = int(mismatch_snl_id)
        print errors
        fig_data = fig['data'][-1]
        fig_data['x'] = [ errors[color] for color in fig_data['marker']['color'] ]
        filename = _get_filename()
        print filename
        #py.plot(fig, filename=filename)
        with open('mpworks/check_snl/results/bad_snls.csv', 'wb') as f:
            mpsnl_cursor = sma.snl.find({ 'snl_id': { '$in': bad_snls.keys() } })
            writer = csv.writer(f)
            writer.writerow([
                'snl_id', 'category', 'snlgroup_key', 'nsites', 'remarks', 'projects', 'authors'
            ])
            for mpsnl_dict in mpsnl_cursor:
                mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                row = [ mpsnl.snl_id, bad_snls[mpsnl.snl_id], mpsnl.snlgroup_key ]
                row += _get_snl_extra_info(mpsnl)
                writer.writerow(row)
        with open('mpworks/check_snl/results/bad_snlgroups.csv', 'wb') as f:
            snlgrp_cursor = sma.snlgroups.find({ 'snlgroup_id': { '$in': bad_snlgroups.keys() } })
            first_mismatch_snls_cursor = sma.snl.find({ 'snl_id': { '$in': bad_snlgroups.values() } })
            first_mismatch_snl_info = OrderedDict()
            for mpsnl_dict in first_mismatch_snls_cursor:
                mpsnl = MPStructureNL.from_dict(mpsnl_dict)
                first_mismatch_snl_info[mpsnl.snl_id] = _get_snl_extra_info(mpsnl)
            writer = csv.writer(f)
            writer.writerow([
                'snlgroup_id', 'snlgroup_key',
                'canonical_snl_id', 'first_mismatching_snl_id',
                 'nsites', 'remarks', 'projects', 'authors'
            ])
            for snlgrp_dict in snlgrp_cursor:
                snlgrp = SNLGroup.from_dict(snlgrp_dict)
                first_mismatch_snl_id = bad_snlgroups[snlgrp.snlgroup_id]
                row = [
                    snlgrp.snlgroup_id, snlgrp.canonical_snl.snlgroup_key,
                    snlgrp.canonical_snl.snl_id, first_mismatch_snl_id
                ]
                row += [
                    ' & '.join(pair) if pair[0] != pair[1] else pair[0]
                    for pair in zip(
                        _get_snl_extra_info(snlgrp.canonical_snl),
                        first_mismatch_snl_info[int(first_mismatch_snl_id)]
                    )
                ]
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
    parser_ana.add_argument('-t', help='whether fig-id is a test plot', action='store_true')
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

    # parse args and call function
    args = parser.parse_args()
    args.func(args)
