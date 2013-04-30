import glob
import os
import re

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 29, 2013'

from mpworks.drones.signals_base import SignalDetector, SignalDetectorSimple, string_list_in_file

# TODO: This is all really ugly...


def last_file(filename):

    dirname = os.path.dirname(filename)
    relaxations = glob.glob('%s.relax*' % filename)
    if relaxations:
        return os.path.join(dirname, relaxations[-1])
    else:
        return os.path.join(dirname, filename)


class VASPOutSignal(SignalDetectorSimple):

    def __init__(self):
        err_code = {}
        err_code["TETRAHEDRON_FAIL"] = "Tetrahedron method fails for"
        err_code["KPOINT_DETECTION_FAIL"] = "Fatal error detecting k-mesh"
        err_code["ROTMAT_NONINT"] = "Found some non-integer element in rotation matrix"
        err_code["TETIRR_FAIL"] = "Routine TETIRR needs special values"
        err_code["CLASSROTMAT_FAIL"] = 'Reciprocal lattice and k-lattice belong'
        err_code["KPOINT_SHIFT_FAIL"] = 'Could not get correct shifts'
        err_code["INVROT_FAIL"] = "inverse of rotation matrix was not found"
        err_code["BROYDENMIX_FAIL"] = 'BRMIX: very serious problems'
        err_code["DAVIDSON_FAIL"] = 'WARNING: Sub-Space-Matrix is not hermitian in DAV'
        ## FIXME: this needs to be more specific
        err_code["NBANDS_FAIL"] = 'NBANDS'
        err_code["RSPHER_FAIL"] = "ERROR RSPHER"
        err_code["ZHEGV_FAIL"] = "ZHEGV"
        err_code["DENTET_FAIL"] = "WARNING DENTET"
        err_code["REAL_OPTLAY_FAIL"] = "REAL_OPTLAY: internal error"
        err_code["ZPOTRF"] = "LAPACK: Routine ZPOTRF failed"
        err_code["FEXCF"] = "ERROR FEXCF"
        err_code["NETWORK_QUIESCED"] = "network quiesced"
        err_code["HARD_KILLED"] = "exit signals: Killed"
        err_code["INCOHERENT_POTCARS"] = "You have build up your multi-ion-type POTCAR file out of POTCAR"
        err_code["ATOMS_TOO_CLOSE"] = "The distance between some ions is very small"
        err_code["STOPCAR_EXISTS"] = "soft stop encountered"
        err_code["SUBSPACE_PSSYEVX_FAIL"] = "ERROR in subspace rotation PSSYEVX"
        err_code["LATTICE_TOO_LONG"] = "One of the lattice vectors is very long"
        super(VASPOutSignal, self).__init__(err_code, [last_file("vasp.out")])


class HitAMemberSignal(SignalDetector):
    def detect(self, dir_name):
        # Look for 'hit a member that was already found in another star'
        # in *.error
        file_names = glob.glob("%s/*.error" % dir_name)
        for file_name in file_names:
            if string_list_in_file(["hit a member that was already found in another star"],
                                          file_name, ignore_case=True):
                return set(["HIT_A_MEMBER_FAIL"])
        return set()


class WallTimeSignal(SignalDetector):

    def detect(self, dir_name):
        # Look for *.error
        file_names = glob.glob(os.path.join(dir_name, "*.error"))
        for file_name in file_names:
            if string_list_in_file(["job killed: walltime"], file_name,
                                          ignore_case=True):
                return set(["WALLTIME_EXCEEDED"])
        return set()


class DiskSpaceExceededSignal(SignalDetector):

    def detect(self, dir_name):
        # Look for *.error
        file_names = glob.glob(os.path.join(dir_name, "*.error"))
        for file_name in file_names:
            if string_list_in_file(["No space left"], file_name,
                                          ignore_case=True):
                return set(["DISK_SPACE_EXCEEDED"])
        return set()


class SegFaultSignal(SignalDetector):

    def detect(self, dir_name):
        """
        Looks through all *.error files for (fault|segmentation)

        Error in UKY looks like this:
            'forrtl: severe (174): SIGSEGV, segmentation fault occurred'
        """
        file_names = glob.glob("%s/*.error" % dir_name)
        rx = re.compile(r'(fault|segmentation)', re.IGNORECASE)
        for file_name in file_names:
            with open(file_name, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if rx.search(line) is not None:
                        return set(["SEGFAULT"])
        return set()


class VASPInputsExistSignal(SignalDetector):

    def detect(self, dir_name):
        names = [os.path.join(dir_name, last_file(x)) for x in ['POSCAR', 'INCAR', 'KPOINTS', 'POTCAR']]
        return set() if all([os.path.exists(file_name) for file_name in names]) and all([os.stat(file_name).st_size > 0 for file_name in names]) else set(["INPUTS_DONT_EXIST"])


class VASPOutputsExistSignal(SignalDetector):

    def detect(self, dir_name):
        names = [os.path.join(dir_name, last_file(x)) for x in ['OUTCAR', 'CONTCAR', 'OSZICAR', 'vasprun.xml', 'CHGCAR', 'vasp.out']]
        return set() if all([os.path.exists(file_name) for file_name in names]) and os.stat(os.path.join(dir_name, "OUTCAR")).st_size > 0 else set(["OUTPUTS_DONT_EXIST"])


class VASPStartedCompletedSignal(SignalDetectorSimple):

    def __init__(self):
        super(VASPStartedCompletedSignal, self).__init__({"VASP_HASNT_STARTED": "vasp", "VASP_HASNT_COMPLETED": "Voluntary context switches:"}, [last_file("OUTCAR")], invert_search=True)