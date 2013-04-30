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
        names = ['POSCAR', 'INCAR', 'KPOINTS', 'POTCAR']
        file_names = [os.path.join(dir_name, f) for f in names]
        return set() if all([os.path.exists(file_name) for file_name in file_names])  and all([os.stat(file_name).st_size > 0 for file_name in file_names]) else set(["INPUTS_DONT_EXIST"])


class VASPOutputsExistSignal(SignalDetector):

    def detect(self, dir_name):
        names = ['OUTCAR', 'CONTCAR', 'OSZICAR', 'vasprun.xml', 'CHGCAR', 'vasp.out']
        file_names = [os.path.join(dir_name, f) for f in names]
        return set() if all([os.path.exists(file_name) for file_name in file_names]) and os.stat(os.path.join(dir_name, "OUTCAR")).st_size > 0 else set(["OUTPUTS_DONT_EXIST"])


class StopcarExistsSignal(SignalDetector):

    def detect(self, dir_name):
        return set(["STOPCAR_EXISTS"]) if os.path.exists(os.path.join(dir_name, "STOPCAR")) else set()


class VASPStartedCompletedSignal(SignalDetectorSimple):

    def __init__(self):
        super(VASPStartedCompletedSignal, self).__init__({"VASP_HASNT_STARTED": "vasp", "VASP_HASNT_COMPLETED": "Voluntary context switches:"}, ["OUTCAR"], invert_search=True)


class PositiveEnergySignal(SignalDetector):

    def __init__(self, max_steps=3):
        self.max_steps = max_steps

    def detect(self, dir_name):
        oszicar_file = os.path.join(dir_name, 'OSZICAR')
        lines = []
        sre = re.compile('F= ')
        if os.path.exists(oszicar_file):
            with open(oszicar_file, 'r') as f:
                for line in f.readlines():
                    if sre.search(line) is not None:
                        lines.append(float(line.split()[2]))
            #analyze lines
            #lines array is empty
            if lines:
                #Max number of ionic steps where the energy can be positive
                l = [i for i in lines if i > 0]
                if len(l) >= self.max_steps:
                    return set(["POSITIVE_ENERGY"])

        return set()


class ChargeUnconvergedSignal(SignalDetector):

    def __init__(self, tolerance=0.7):
        self.tolerance = tolerance

    def detect(self, dir_name):
        """docstring for has_charge_converged(dir_name):"""
        # MIT cmd: grep ' F= ' -B2 OSZICAR | tail -n3 | head -n1 | awk '{ if($NF > 0.7) print}'
        # get data from OSZICAR
        """
               N       E                     dE             d eps       ncg     rms          rms(c)
        DAV:   1    -0.284125818633E+00   -0.28413E+00   -0.54399E+02  2265   0.326E+02

        DAV:   2    -0.158775256418E+01   -0.13036E+01   -0.11755E+01  3025   0.516E+01

        DAV:   3    -0.159687567244E+01   -0.91231E-02   -0.91055E-02  2585   0.356E+00

        DAV:   4    -0.159694529039E+01   -0.69618E-04   -0.69618E-04  2715   0.318E-01

        DAV:   5    -0.159694656701E+01   -0.12766E-05   -0.12766E-05  2545   0.347E-02    0.443E+00
        RMM:   6    -0.183752882276E+01   -0.24058E+00   -0.12765E-01  2300   0.536E+00    0.200E+00
        RMM:   7    -0.190323874745E+01   -0.65710E-01   -0.25050E-02  2200   0.194E+00    0.101E-01
        RMM:   8    -0.190407037625E+01   -0.83163E-03   -0.15679E-03  2200   0.446E-01    0.718E-02
        RMM:   9    -0.190451467389E+01   -0.44430E-03   -0.40973E-04  2201   0.266E-01    0.283E-02
        RMM:  10    -0.190457492968E+01   -0.60256E-04   -0.90850E-05  2133   0.950E-02    0.824E-03
        RMM:  11    -0.190458159707E+01   -0.66674E-05   -0.22968E-06  1974   0.178E-02    0.162E-03
        RMM:  12    -0.190458158078E+01    0.16289E-07   -0.84927E-07  1520   0.111E-02

           1 F= -.19045816E+01 E0= -.19047496E+01  d E =-.190458E+01  mag=     0.0000
               N       E                     dE             d eps       ncg     rms          rms(c)
        DAV:   1    -0.190493863283E+01   -0.35704E-03   -0.91506E-03  2210   0.181E+00    0.986E-02
        RMM:   2    -0.190486500926E+01    0.73624E-04   -0.34570E-05  2200   0.879E-02    0.594E-02
        RMM:   3    -0.190482736424E+01    0.37645E-04   -0.19973E-05  2241   0.584E-02    0.760E-04
        RMM:   4    -0.190482711745E+01    0.24679E-06   -0.81603E-07  1457   0.184E-02

           2 F= -.19048271E+01 E0= -.19049984E+01  d E =-.245537E-03  mag=     0.0000
               N       E                     dE             d eps       ncg     rms          rms(c)
        DAV:   1    -0.190560006333E+01   -0.77270E-03   -0.40109E-02  2200   0.379E+00    0.207E-01
        RMM:   2    -0.190527684933E+01    0.32321E-03   -0.14896E-04  2200   0.184E-01    0.125E-01
        RMM:   3    -0.190510804468E+01    0.16880E-03   -0.87792E-05  2244   0.122E-01    0.149E-03
        RMM:   4    -0.190510664241E+01    0.14023E-05   -0.31690E-06  2071   0.370E-02

           3 F= -.19051066E+01 E0= -.19052848E+01  d E =-.525062E-03  mag=     0.0000

        """

        # Be Optimistic, that's my motto

        oszicar_file = os.path.join(os.path.abspath(dir_name), 'OSZICAR')
        if os.path.exists(oszicar_file):
            with open(oszicar_file, 'r') as f:
                lines = f.read().split("\n")
            rx = re.compile('F= ')
            for i, line in enumerate(lines):
                if rx.search(line) is not None:
                    data = lines[i - 2].split()
                    #weak safety check to see line is valid
                    # 'RMM:  11    -0.190458159707E+01   -0.66674E-05   -0.22968E-06  1974   0.178E-02    0.162E-03'
                    if len(data) == 7:
                        d = float(data[-1])
                        if d >= self.tolerance:
                            return set(["CHARGE_UNCONVERGED"])
        return set()