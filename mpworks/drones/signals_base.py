import os
from pymatgen import zopen

__author__ = 'Anubhav Jain'
__copyright__ = 'Copyright 2013, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Apr 29, 2013'


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

class SignalDetector():
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
            if not self.ignore_nonexistent_file or os.path.exists(os.path.join(dir_name, filename)):
                errors = string_list_in_file(self.signames_targetstrings.values(), os.path.join(dir_name, filename), ignore_case=self.ignore_case)
                if self.invert_search:
                    errors_inverted = [item for item in self.targetstrings_signames.keys() if item not in errors]
                    errors = errors_inverted

                #add the signal names for those strings
                for e in errors:
                    signals.add(self.targetstrings_signames[e])
        return signals