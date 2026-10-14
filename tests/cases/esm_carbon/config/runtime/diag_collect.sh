#!/usr/bin/bash

# Runs once per node on a hang: node state, GPU/NIC state, per-rank process state and
# gdb backtraces of all GPU ranks plus a few CPU ranks.
out=$1/$(hostname)
mkdir -p $out && cd $out
{ date; uptime; free -g; } > node.txt
nvidia-smi > nvidia-smi.txt 2>&1
nvidia-smi -q > nvidia-smi_q.txt 2>&1
cxi_stat > cxi_stat.txt 2>&1
dmesg -T 2>&1 | tail -n 500 > dmesg.txt
ps -u $USER -Lo pid,tid,stat,wchan:32,pcpu,rss,etime,comm > ps_threads.txt

n_cpu=0
for pid in $(pgrep -u $USER -x 'icon_(gpu|cpu)'); do
  rank=$(tr '\0' '\n' < /proc/$pid/environ | sed -n 's/^SLURM_PROCID=//p')
  { cat /proc/$pid/status; echo "wchan: $(cat /proc/$pid/wchan)"; } > proc_rank${rank}.txt 2>&1
  if [[ $(readlink /proc/$pid/exe) -ef $ICON_GPU_EXE ]] || (( n_cpu++ < WATCHDOG_GDB_CPU_PER_NODE )); then
    # Read binaries through this step's mounts, the target's mount namespace is not accessible
    timeout -k 10 120 gdb -batch -nx -q -iex 'set sysroot /' -ex 'set pagination off' -ex 'info threads' \
      -ex 'thread apply all bt' "$(readlink /proc/$pid/exe)" $pid > gdb_rank${rank}.txt 2>&1 &
  fi
done
wait
