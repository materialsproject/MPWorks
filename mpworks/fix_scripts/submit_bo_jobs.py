import json
import os
from fireworks.core.launchpad import LaunchPad
from mpworks.submission.submission_mongo import SubmissionMongoAdapter
from pymatgen.matproj.snl import StructureNL

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2014, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Jan 24, 2014'

if __name__ == "__main__":
    sma = SubmissionMongoAdapter.from_file('submission.yaml')

    module_dir = os.path.dirname(os.path.abspath(__file__))
    lp_f = os.path.join(module_dir, 'my_launchpad.yaml')
    lpdb = LaunchPad.from_file(lp_f)

    for s in os.listdir(os.path.join(module_dir, "snls")):
        if '.json' in s:
            print 'submitting', s
            with open(os.path.join(module_dir, "snls",s)) as f:
                snl = StructureNL.from_dict(json.load(f))
                sma.submit_snl(snl, 'anubhavster@gmail.com', {"priority": 10})
            print 'DONE submitting', s


print 'DONE!'