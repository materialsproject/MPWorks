from mpworks.submissions.submit_tests import clear_and_submit

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
    parser.add_argument('--clear', help='clear old databases', action='store_true')
    args = parser.parse_args()
    
    clear_and_submit(args.clear)

if __name__ == '__main__':
    go_testing()