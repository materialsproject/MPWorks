import os
import time
from fireworks.core.launchpad import LaunchPad
from fireworks.core.fw_config import FWConfig

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Mar 29, 2013'

if __name__ == '__main__':

    while True:
        l_dir = FWConfig().CONFIG_FILE_DIR
        l_file = os.path.join(l_dir, 'my_launchpad.yaml')
        lp = LaunchPad.from_file(l_file)
        print 'FIZZLED: ', lp.detect_fizzled(FWConfig().RUN_EXPIRATION_SECS, True)
        time.sleep(3600)