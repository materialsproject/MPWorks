from mpworks.processors.submit_canonical import clear_and_submit

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'

"""
A runnable script for submitting test jobs
"""

from argparse import ArgumentParser

__author__ = "Anubhav Jain"
__copyright__ = "Copyright 2013, The Materials Project"
__version__ = "0.1"
__maintainer__ = "Anubhav Jain"
__email__ = "ajain@lbl.gov"
__date__ = "Jan 14, 2013"


def go_testing():
    m_description = 'This program is used to clear and submit jobs from the database'

    parser = ArgumentParser(description=m_description)
    parser.add_argument('-c', '--clear', help='clear old databases', action='store_true')
    parser.add_argument('-n', '--names', help='csv of compound names', default=None)
    parser.add_argument('--noboltztrap', help='do NOT run boltztrap', action='store_true')
    parser.add_argument('--exact', help='exact structure', action='store_true')
    args = parser.parse_args()

    names = [x.strip() for x in args.names.split(',')] if args.names else None

    params = {}
    if args.noboltztrap:
        params['boltztrap'] = False
    if args.exact:
        params['exact_structure'] = True
    clear_and_submit(args.clear, names, params)

if __name__ == '__main__':
    go_testing()
