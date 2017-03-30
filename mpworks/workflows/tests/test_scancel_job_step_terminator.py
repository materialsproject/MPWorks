import os
from unittest import TestCase

from mpworks.workflows.wf_utils import ScancelJobStepTerminator

test_dir = os.path.join(os.path.dirname(__file__), "..",
                        'test_wfs', "scancel")

class TestScancelJobStepTerminator(TestCase):
    def test_parse_srun_step_number(self):
        std_err_file = os.path.join(test_dir, "srun_std_err_example.txt")
        terminator = ScancelJobStepTerminator(std_err_file)
        step_id = terminator.parse_srun_step_number()
        self.assertEqual(step_id, "2667797.4")

