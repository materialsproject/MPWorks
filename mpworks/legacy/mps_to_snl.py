import datetime
import traceback
from pymongo import MongoClient
from pymatgen.core.structure import Structure
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 08, 2013'


def mps_dict_to_snl(mps_dict):
    m = mps_dict

    if 'deprecated' in m['about']['metadata']['info'] and m['about']['metadata']['info']['deprecated']:
        return None

    if 'Carbon Capture Storage Initiative (CCSI)' in m['about']['metadata']['project_names']:
        print 'rejected old CCSI'
        return None

    mps_ids = [m['mps_id']]

    remarks = []
    for remark in m['about']['metadata']['remarks']:
        if 'This entry replaces deprecated mps_id' in remark:
            mps_ids.append(int(remark.split()[-1]))  # add the deprecated MPS id to this SNL
        else:
            remarks.append(remark)
    for remark in m['about']['metadata']['keywords']:
        remarks.append(remark)

    projects = []
    for project in m['about']['metadata']['project_names']:
        projects.append(project)

    data = {'_materialsproject': {'deprecated': {'mps_ids': mps_ids}}, '_icsd': {}}
    for k, v in m['about']['metadata']['info'].iteritems():
        if k == 'icsd_comments':
            data['_icsd']['comments'] = v
        elif k == 'icsd_id':
            data['_icsd']['icsd_id'] = v
        elif k == 'remark':
            data['_materialsproject']['ordering_remark'] = v
        elif 'deprecated' in k or 'original_structure' in k:
            data['_materialsproject']['deprecated'][k] = v
        elif 'assert' in k or 'universe' in k or 'mp_duplicates' in k:
            pass
        else:
            data['_materialsproject'][k] = v

    authors = []
    for a in m['about']['authors']:
        authors.append({'name': a['name'], 'email': a['email']})
    for a in m['about']['acknowledgements']:
        authors.append({'name': a['name'], 'email': a['email']})

    cites = [m['about']['please_cite']['bibtex']]
    if m['about']['supporting_info']:
        cites.append(m['about']['supporting_info']['bibtex'])
    references = '\n'.join(cites)

    history = []
    for h in m['about']['links']:
        if 'direct_copy' in h['description']:
            del h['description']['direct_copy']
        history.append({'name': h['name'], 'url': h['url'], 'description': h['description']})

    struct = Structure.from_dict(m)

    created_at = datetime.datetime.strptime(m['about']['created_at'], "%Y-%m-%d %H:%M:%S")

    return StructureNL(struct, authors, projects, references, remarks, data, history, created_at)

















