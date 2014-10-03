#!/bin/bash -login
source /global/u1/h/huck/ph_playground/virtenv_ph_playground/bin/activate
export FW_CONFIG_FILE=$FW_CONFIG_ph_playground
export DB_LOC=/global/u1/h/huck/ph_playground/config/dbs
export VENV_LOC=/global/u1/h/huck/ph_playground/virtenv_ph_playground/bin/activate
export SCRIPT_LOC=/global/u1/h/huck/ph_playground/config/scripts
cd $PBS_O_WORKDIR
num_ids_per_job=20000
start_id=$(echo "${PBS_VNODENUM}*$num_ids_per_job" | bc)
end_id=$(echo "(${PBS_VNODENUM}+1)*$num_ids_per_job" | bc)
sleep $(echo "(${PBS_VNODENUM}+1)*2" | bc)
#logfile="${PBS_O_WORKDIR}/${PBS_JOBID}_${PBS_VNODENUM}.log"
python -m mpworks.scripts.check_snl spacegroups --start $start_id --end $end_id #> $logfile 2>&1
