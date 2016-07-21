import glob
import os
import re
from monty.io import zopen
from monty.os.path import zpath
from mpworks.workflows.wf_utils import last_relax

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 29, 2013'

# TODO: This is all really ugly...


def string_list_in_file(s_list, filename, ignore_case=True):
    #based on Michael's code
    """
    args ->
        s_list  (str) : a list of strings in the file
        filename (str) : is the absolute path of the file that is analyzed

    Returns the strings that matched...

    Note: this is going to be slow as mud for huge files (e.g., OUTCAR)
    Using grep via subprocess might be better, but has dependency of
    shell (i.e., non-windows)

    """
    matches = set()
    with zopen(filename, 'r') as f:
        for line in f:
            for s in s_list:
                if (ignore_case and s.lower() in line.lower()) or s in line:
                    matches.add(s)
                    if len(matches) == len(s_list):
                        return s_list

    return list(matches)

class SignalDetector(object):
    '''
    A SignalDetector is an abstract class that takes in a directory name and returns a set of Strings.
    Each String represents an error code that was detected during the run
    '''

    def detect(self, dir_name):
        #returns a set() of signals (Strings)
        raise NotImplementedError


class SignalDetectorList(list):
    '''
    Takes in a list of SignalDetectors() and provides a convenience method, detect_all(), that can merge the results of all the SignalDetectors()
    Very basic...
    '''
    def detect_all(self, dir_name):
        signals = set()
        for detector in self:
            for signal in detector.detect(dir_name):
                signals.add(signal)
        return signals

class SignalDetectorSimple(SignalDetector):
    '''
    A convenience class for defining a Signal Detector where you just want to search for the presence (or absence) of a String in a file or list of files
    Makes it easy to detect errors, for example, that are directly printed to output files
    '''
    def __init__(self, signames_targetstrings, filename_list, invert_search=False, ignore_case=True, ignore_nonexistent_file=True):
        '''

        :param signames_targetstrings: A dictionary of signal names (e.g. "ERR_1") to the target String searched for in the file ("SEVERE ERROR in calculation!")
        :param filename_list: A list of filenames to search, note that the default dir_name in detect() is now obselete
        :param invert_search: Inverts search, e.g. error is True (signal is returned) when the target String is *NOT* present
        :param ignore_case: ignore case in target String
        :param ignore_nonexistent_file: if a file in filename_list doesn't exist, move on without returning any errors
        '''
        self.signames_targetstrings = signames_targetstrings
        #generate the reverse dictionary
        self.targetstrings_signames = dict([[v, k] for k, v in self.signames_targetstrings.items()])

        self.filename_list = filename_list
        self.ignore_case = ignore_case
        self.ignore_nonexistent_file = ignore_nonexistent_file
        self.invert_search = invert_search

    def detect(self, dir_name):

        signals = set()

        for filename in self.filename_list:
            #find the strings that match in the file
            if not self.ignore_nonexistent_file or os.path.exists(zpath(os.path.join(dir_name, filename))):
                f = last_relax(os.path.join(dir_name, filename))
                errors = string_list_in_file(self.signames_targetstrings.values(), f, ignore_case=self.ignore_case)
                if self.invert_search:
                    errors_inverted = [item for item in self.targetstrings_signames.keys() if item not in errors]
                    errors = errors_inverted

                #add the signal names for those strings
                for e in errors:
                    signals.add(self.targetstrings_signames[e])
        return signals


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
        super(VASPOutSignal, self).__init__(err_code, ["vasp.out"])


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
            if string_list_in_file(["job killed: walltime", "PBS: job killed"], file_name,
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
        rx = re.compile(r'segmentation', re.IGNORECASE)
        for file_name in file_names:
            with zopen(file_name, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if rx.search(line) is not None:
                        return set(["SEGFAULT"])
        return set()


class VASPInputsExistSignal(SignalDetector):

    def detect(self, dir_name):
        names = [last_relax(os.path.join(dir_name, x)) for x in ['POSCAR', 'INCAR', 'KPOINTS', 'POTCAR']]
        return set() if all([os.path.exists(file_name) for file_name in names]) and all([os.stat(file_name).st_size > 0 for file_name in names]) else set(["INPUTS_DONT_EXIST"])


class VASPOutputsExistSignal(SignalDetector):

    def detect(self, dir_name):

        names = [last_relax(os.path.join(dir_name, x)) for x in ['OUTCAR', 'OSZICAR', 'vasprun.xml', 'vasp.out']]
        return set() if all([os.path.exists(file_name) for file_name in names]) and os.stat(names[0]).st_size > 0 else set(["OUTPUTS_DONT_EXIST"])


class VASPStartedCompletedSignal(SignalDetectorSimple):

    def __init__(self):
        super(VASPStartedCompletedSignal, self).__init__({"VASP_HASNT_STARTED": "vasp", "VASP_HASNT_COMPLETED": "Voluntary context switches:"}, ["OUTCAR"], invert_search=True)


class Relax2ExistsSignal(SignalDetector):

    def detect(self, dir_name):
        f_exists = 'relax2' in last_relax(os.path.join(dir_name, 'vasprun.xml'))
        return set() if f_exists else set(["NO_RELAX2"])
