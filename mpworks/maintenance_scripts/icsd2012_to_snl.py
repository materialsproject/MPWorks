import re
import random
import unicodedata
import datetime
from pymatgen import Structure
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 12, 2013'

# Convert ICSD database (already converted to MongoDB by MIT from an SQL source) into SNL

def icsd_dict_to_snl(icsd_dict):
    if 'structure' not in icsd_dict:
        return None

    struct = Structure.from_dict(icsd_dict['structure'])
    references = _get_icsd_reference(icsd_dict)

    data = {'_icsd': {}}
    excluded_data = ['_id', 'a_len', 'b_len', 'c_len', 'alpha', 'beta', 'gamma', 'compostion', 'composition', 'created_at', 'crystal_id', 'idnum', 'journal', 'tstruct', 'updated_at', 'username']
    for k, v in icsd_dict.iteritems():
        if k not in excluded_data:
            if isinstance(v, datetime.datetime):
                v = v.strftime(format='%Y-%m-%d %H:%M:%S')
            data['_icsd'][k] = v

    projects = None
    remarks = None

    history = [{'name': 'Inorganic Crystal Structure Database (ICSD)', 'url': 'http://icsd.fiz-karlsruhe.de/', 'description': {'icsd_id': data['_icsd']['icsd_id']}}, {'name': 'pymatgen', 'url': 'https://pypi.python.org/pypi/pymatgen', 'description': {'comment': 'converted to explicit structure'}}]

    authors = 'William Davidson Richards <wrichard@mit.edu>, Shyue Ping Ong <shyue@mit.edu>, Stephen Dacek <sdacek@mit.edu>, Anubhav Jain <ajain@lbl.gov>'

    return StructureNL(struct, authors, projects, references, remarks, data, history)


def _get_icsd_reference(icsd_dict):

    if icsd_dict and 'journal' in icsd_dict and icsd_dict['journal']['authors']:
        pages = ""
        if icsd_dict['journal']['PAGE_FIRST']:
            pages = str(icsd_dict['journal']['PAGE_FIRST'])

        if icsd_dict['journal']['PAGE_LAST']:
            pages = pages + "--" + str(icsd_dict['journal']['PAGE_LAST'])

        bibtex_str = "@article{"
        #author last name as key
        m_key = icsd_dict['journal']['authors'][0]
        m_key = re.sub(r'\s', '_', m_key)
        m_key = m_key[0:m_key.find(',')]
        bibtex_str += m_key
        #year + random
        bibtex_str += str(icsd_dict['journal']['YEAR']) + "_" + str(random.randrange(1, 1000)) + ",\n"

        bibtex_str += "title = {{" + icsd_dict['au_title']+ "}},\n"

        auth_str = "author = {" + " and ".join(icsd_dict['journal']['authors']) + "},\n"
        # sanitize authors so there are no parentheses (weird ICSD conversion thing)
        regex = re.compile('\(.+?\)')
        auth_str = regex.sub('', auth_str)
        bibtex_str += auth_str

        if icsd_dict['journal']['YEAR']:
            bibtex_str += "year = {" + str(icsd_dict['journal']['YEAR']) + "},\n"

        if icsd_dict['journal']['J_TITLE']:
            bibtex_str += "journal = {" + icsd_dict['journal']['J_TITLE'] + "},\n"

        if icsd_dict['journal']['VOLUME']:
            bibtex_str += "volume = {" + str(icsd_dict['journal']['VOLUME']) + "},\n"

        if icsd_dict['journal']['ISSUE']:
            bibtex_str += "issue = {" + str(icsd_dict['journal']['ISSUE']) + "},\n"
        bibtex_str += "pages = {" + pages + "},\n"

        if icsd_dict['journal']['ISSN']:
            bibtex_str += "issn = " + icsd_dict['journal']['ISSN'] + "\n"

        bibtex_str += "}"
        bibtex_str = unicodedata.normalize('NFKD', bibtex_str).encode('ascii','ignore')

        return bibtex_str


    return None