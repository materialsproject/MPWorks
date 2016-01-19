from pymatgen.core.periodic_table import Element
from pymatgen.core.structure import Structure

def get_nat_type(struct):
    nat_type = []
    for symbol in struct.symbol_set:
        ii = 0
        for site in struct.species:
            if site == Element(symbol):
                ii += 1
        nat_type.append(ii)

    return nat_type


def get_mass_list(structure):
    mass_list = map(lambda x:Element.from_Z(x).data["Atomic mass"], structure.atomic_numbers)
    return mass_list


def get_modes_from_OUTCAR(outcar_fh, nat, mass_map):
    import sys
    import re
    from math import sqrt
    eigvals = [ 0.0 for i in range(nat*3) ]
    eigvecs = [ 0.0 for i in range(nat*3) ]
    norms   = [ 0.0 for i in range(nat*3) ]
    # mass_map = get_mass_list('POSCAR.phon')

    #
    outcar_fh.seek(0) # just in case


    while True:
        line = outcar_fh.readline()
        if not line:
            break
        #

        if "Eigenvectors and eigenvalues of the dynamical matrix" in line:
            outcar_fh.readline() # ----------------------------------------------------
            outcar_fh.readline() # empty line

            for i in range(nat*3): # all frequencies should be supplied, regardless of those requested to calculate
                outcar_fh.readline() # empty line
                p = re.search(r'^\s*(\d+).+?([\.\d]+) cm-1', outcar_fh.readline())
                eigvals[i] = float(p.group(2))
                #
                outcar_fh.readline() # X         Y         Z           dx          dy          dz
                eigvec = []
                #
                for j in range(nat):
                    tmp = outcar_fh.readline().split()

                    #
                    eigvec.append([ round(float(tmp[x])/sqrt(mass_map[j]),6) for x in range(3,6) ])
                    #
                eigvecs[i] = eigvec
                norms[i] = sqrt( sum( [abs(x)**2 for sublist in eigvec for x in sublist] ) )
            #
            return eigvals, eigvecs, norms

                #print mass

        #
    print "[get_modes_from_OUTCAR]: ERROR Couldn't find 'Eigenvectors after division by SQRT(mass)' in OUTCAR. Use 'NWRITE=3' in INCAR. Exiting..."
    sys.exit(1)


def get_epsilon_from_OUTCAR(outcar_fh):
    import re
    import sys
    epsilon = []
    #
    outcar_fh.seek(0) # just in case
    while True:
        line = outcar_fh.readline()
        if not line:
            break
        #
        if "MACROSCOPIC STATIC DIELECTRIC TENSOR" in line:
            outcar_fh.readline()
            epsilon.append([float(x) for x in outcar_fh.readline().split()])
            epsilon.append([float(x) for x in outcar_fh.readline().split()])
            epsilon.append([float(x) for x in outcar_fh.readline().split()])
            return epsilon
    #
    raise RuntimeError("[get_epsilon_from_OUTCAR]: ERROR Couldn't find dielectric tensor in OUTCAR")
    return 1


def verify_raman(fw_spec):
    from math import pi
    from numpy import linalg

    passed_vars = fw_spec['passed_vars'][0]
    max_mode_index = passed_vars[4]

    "For the 0.005 step_size calculaiton:"
    step_size = 0.005
    norm = linalg.norm(passed_vars[2][max_mode_index])
    ra = [[0.0 for x in range(3)] for y in range(3)]
    ii = 0
    for coeff in [-0.5, 0.5]:
        parent_index = -2*(2 - ii)
        previous_dir = fw_spec['_job_info'][parent_index]
        if os.path.isfile(previous_dir+"OUTCAR"):
            filename = "OUTCAR"
        else:
            filename = "OUTCAR.gz"
        with open(previous_dir+filename, 'r') as outcar_fh:
            epsilon = get_epsilon_from_OUTCAR(outcar_fh)
        for m in range(3):
            for n in range(3):
                ra[m][n] += epsilon[m][n] * coeff/step_size * norm * vol/(4.0*pi)
        ii += 1

    alpha = (ra[0][0] + ra[1][1] + ra[2][2])/3.0
    beta2 = ( (ra[0][0] - ra[1][1])**2 + (ra[0][0] - ra[2][2])**2 + (ra[1][1] - ra[2][2])**2 + 6.0 * (ra[0][1]**2 + ra[0][2]**2 + ra[1][2]**2) )/2.0
    activity = 45.0*alpha**2 + 7.0*beta2

    raman_results = passed_vars[5]
    ae = abs(raman_results[max_mode_index][2] - activity)
    are = abs(raman_results[max_mode_index][2] - activity) / raman_results[max_mode_index][2]
    d = {}
    if ae < 3 or are < 0.1:
        d['verified'] = True
    else:
        d['verified'] = False
    d['eigvalues'] = passed_vars[0]
    d['eigvectors'] = passed_vars[1]
    d['norms'] = passed_vars[2]
    d['alpha'] = raman_results[0]
    d['beta2'] = raman_results[1]
    d['activity'] = raman_results[2]

    return d
