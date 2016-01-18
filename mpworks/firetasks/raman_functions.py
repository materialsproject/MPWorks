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
