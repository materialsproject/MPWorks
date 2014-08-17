






def update_spec_force_convergence(spec):
    fw_spec = spec
    update_set = {"ENCUT": 600, "EDIFF": 0.00005}
    fw_spec['vasp']['incar'].update(update_set)
    kpoints = spec['vasp']['kpoints']
    k = [2*k for k in kpoints['kpoints'][0]]
    fw_spec['vasp']['kpoints']['kpoints'] = [k]
    return fw_spec