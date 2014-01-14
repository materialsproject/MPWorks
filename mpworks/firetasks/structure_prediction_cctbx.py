
if __name__ == '__main__':
    from pymatgen import read_structure
    import json
    from pymatpro.abitbx.utils import find_symmetry, to_diffraction_pattern_doc
    
    s = read_structure('prediction_symmetry.cif')
    with open('symmetrydict.json', 'w') as f:
        d = find_symmetry(s)[0]
        json.dump(d, f)
                  
                  
                  