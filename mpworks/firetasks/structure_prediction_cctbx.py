
if __name__ == '__main__':
    from pymatgen import read_structure
    from pymatgen.serializers.json_coders import pmg_dump
    import json
    from pymatpro.abitbx.utils import find_symmetry, to_diffraction_pattern_doc, mg_structure_to_cctbx_crystal_structure
    
    s = read_structure('prediction_symmetry.cif')
    d = find_symmetry(s)[0]
    d_doc = to_diffraction_pattern_doc(mg_structure_to_cctbx_crystal_structure(s))
    pmg_dump({'space_group': d, 'diffraction_pattern_doc': d_doc}, 'symmetrydict.json')
