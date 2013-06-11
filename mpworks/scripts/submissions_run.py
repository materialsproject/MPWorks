from mpworks.processors.process_submissions import SubmissionProcessor

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'May 06, 2013'

"""
A runnable script for submissions
"""

from argparse import ArgumentParser

__author__ = "Anubhav Jain"
__copyright__ = "Copyright 2013, The Materials Project"
__version__ = "0.1"
__maintainer__ = "Anubhav Jain"
__email__ = "ajain@lbl.gov"
__date__ = "Jan 14, 2013"


def go_submissions():
    m_description = 'This program is used to pull jobs from the Submissions database, create FireWorks workflows from those submissions, and then monitor all previous submissions for updates to state (so that the submission database can be updated)'

    parser = ArgumentParser(description=m_description)
    parser.add_argument('--sleep', help='sleep time between loops', default=None, type=int)
    parser.add_argument('--infinite', help='loop infinite times', action='store_true')
    args = parser.parse_args()

    sp = SubmissionProcessor.auto_load()
    sp.run(args.sleep, args.infinite)

if __name__ == '__main__':
    go_submissions()