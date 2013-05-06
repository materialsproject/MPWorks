from mpworks.submissions.submissions_mongo import SubmissionProcessor

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

    # no options yet ...
    # TODO: add sleep time as arg
    parser = ArgumentParser(description=m_description)
    args = parser.parse_args()

    sp = SubmissionProcessor.auto_load()
    sp.run()

if __name__ == '__main__':
    go_submissions()