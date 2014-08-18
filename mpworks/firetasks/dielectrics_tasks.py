






def update_spec_static_dielectrics_convergence(spec):
    fw_spec = spec
    update_set = {"ENCUT": 600, "EDIFF": 0.00005}
    fw_spec['vasp']['incar'].update(update_set)
    kpoints = spec['vasp']['kpoints']
    k = [2*k for k in kpoints['kpoints'][0]]
    fw_spec['vasp']['kpoints']['kpoints'] = [k]
    return fw_spec


class SetupStaticDielectricsConvergenceTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Static Dielectrics Convergence Task"

    def run_task(self, fw_spec):
        incar = fw_spec['vasp']['incar']
        update_set = {"ENCUT": 600, "EDIFF": 0.00005}
        incar.update(update_set)
        #if fw_spec['double_kmesh']:
        kpoints = fw_spec['vasp']['kpoints']
        k = [2*k for k in kpoints['kpoints'][0]]
        kpoints['kpoints'] = [k]
        return FWAction()


class SetupStaticDielectricsTask(FireTaskBase, FWSerializable):
    _fw_name = "Setup Static Dielectrics Task"

    def run_task(self, fw_spec):
        incar = Incar.from_file(zpath("INCAR"))
        incar.update({"ISIF": 2})
        incar.write_file("INCAR")
        return FWAction()