#!/bin/bash
#SBATCH --no-requeue
#SBATCH --job-name="aiida-1132744"
#SBATCH --get-user-env
#SBATCH --output=_scheduler-stdout.txt
#SBATCH --error=_scheduler-stderr.txt
#SBATCH --account=mr0
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --time=02:00:00


#SBATCH --account=project_465000106
#SBATCH --partition=small
#SBATCH --mem=227328


module load PrgEnv-gnu/8.2.0
module load craype-x86-milan
module load cray-libsci/21.08.1.2
module load cray-fftw/3.3.8.12
module load cray-hdf5-parallel/1.12.0.7

export OMP_NUM_THREADS=1


'srun' '-u' '-n' '128' '/projappl/project_465000028/juqiao/phase2/q-e/bin/pw.x' '-nk' '16' '-in' 'aiida.in'  > 'aiida.out' 

 

 
