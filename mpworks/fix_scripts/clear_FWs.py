import multiprocessing
import os
from fireworks.core.launchpad import LaunchPad

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Aug 01, 2013'


class ClearFWs():

    @classmethod
    def setup(cls):
        module_dir = os.path.dirname(__file__)
        cls.lp = LaunchPad.from_file(os.path.join(module_dir, 'my_launchpad.yaml'))

    def archive_fw(self, fw_id):
        self.lp.archive_wf(fw_id)
        return True


def _archive_fw(data):
    b = ClearFWs()
    return b.archive_fw(data)

if __name__ == '__main__':

    cfw = ClearFWs()
    cfw.setup()
    lp = ClearFWs.lp
    fw_ids = []
    for d in lp.workflows.find({"state": "READY"}, {'nodes': 1}):
        fw_ids.append(d['nodes'][0])
    print 'GOT all fw_ids...'
    pool = multiprocessing.Pool(8)
    states = pool.map(_archive_fw, fw_ids)
    print 'DONE', all(states)