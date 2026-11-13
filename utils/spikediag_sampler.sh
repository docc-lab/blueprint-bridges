#!/usr/bin/env bash
# Tomislav-RetCtx (user 2026-11-09: "You need to figure out what is causing this... Unexplained spikes are utterly
# unacceptable"): per-node kernel sampler for the latency-spike investigation. Runs ON a node (started over ssh by
# spikediag_chain.sh), writes one block per tick to /var/tmp/spikediag.txt until /var/tmp/spikediag.stop exists or
# DURATION seconds pass. Every counter is cumulative; the analysis (spikediag_analyze.py) differences them.
#
#   @ <unix time>                                   tick start (node clock, NTP-synchronised)
#   tcp <Tcp: values of /proc/net/snmp>             header once as "tcp_h"
#   ext <TcpExt: values of /proc/net/netstat>       header once as "ext_h"
#   softnet <processed,dropped,squeezed per CPU>    /proc/net/softnet_stat, CPUs separated by ';'
#   nic <rx_packets rx_missed rx_dropped rx_fifo rx_errors tx_packets tx_dropped>   cluster NIC (10.10.1.x)
#   vx <rx_packets rx_dropped tx_packets tx_dropped>                                vxlan.calico
#   ct <insert_failed drop early_drop>              sudo conntrack -S, summed over CPUs
#   psi <cpu some> <io some> <io full> <mem some> <mem full>   total stall microseconds, /proc/pressure
#   cpu <per CPU: user nice system idle iowait irq softirq steal, ';' separated>      /proc/stat jiffies
#   pod <uid>=<nr_periods>,<nr_throttled>,<throttled_usec>,<usage_usec>,<cpu.pressure some total> ...  pod cgroups
#   vm <pgmajfault compact_stall allocstall_normal allocstall_movable thp_fault_alloc pswpin pswpout>
#   disk <dev>=<reads,read_ms,writes,write_ms,io_ms,weighted_ms> ...                 sd*/nvme* whole disks
#
# usage (on the node): spikediag_sampler.sh [DURATION_S] [INTERVAL_S]
DURATION=${1:-7200}; INTERVAL=${2:-0.5}
# Tomislav-RetCtx (2026-11-09): /var/tmp, not /dev/shm: systemd-logind (RemoveIPC=yes) deletes a user's /dev/shm files
# 10 s after the user is fully logged out, which lost three sampler files of the first instrumented run.
OUT=/var/tmp/spikediag.txt; STOP=/var/tmp/spikediag.stop
IF=$(ip -o -4 addr | awk '$4 ~ /^10\.10\.1\./{print $2; exit}')
rm -f "$STOP"
end=$(( $(date +%s) + DURATION ))
{
  echo "# node $(hostname -s) if=$IF interval=$INTERVAL started $(date -u +%FT%T.%NZ)"
  echo "tcp_h $(awk '/^Tcp:/{print; exit}' /proc/net/snmp | cut -d' ' -f2-)"
  echo "ext_h $(awk '/^TcpExt:/{print; exit}' /proc/net/netstat | cut -d' ' -f2-)"
  echo "dmesg_start $(sudo -n dmesg | tail -1 | cut -c1-200)"
} > "$OUT"
S=/sys/class/net/$IF/statistics; V=/sys/class/net/vxlan.calico/statistics
declare -A PODPID
while [ "$(date +%s)" -lt "$end" ] && [ ! -e "$STOP" ]; do
  t=$(date +%s.%N)
  {
    echo "@ $t"
    echo "tcp $(awk '/^Tcp:/{n++; if(n==2){$1=""; print; exit}}' /proc/net/snmp)"
    echo "ext $(awk '/^TcpExt:/{n++; if(n==2){$1=""; print; exit}}' /proc/net/netstat)"
    echo "softnet $(awk '{printf "%d,%d,%d;", strtonum("0x"$1), strtonum("0x"$2), strtonum("0x"$3)}' /proc/net/softnet_stat)"
    echo "nic $(cat $S/rx_packets $S/rx_missed_errors $S/rx_dropped $S/rx_fifo_errors $S/rx_errors $S/tx_packets $S/tx_dropped | tr '\n' ' ')"
    echo "vx $(cat $V/rx_packets $V/rx_dropped $V/tx_packets $V/tx_dropped 2>/dev/null | tr '\n' ' ')"
    echo "ct $(sudo -n conntrack -S 2>/dev/null | awk '{for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]=="insert_failed")f+=a[2]; if(a[1]=="drop")d+=a[2]; if(a[1]=="early_drop")e+=a[2]}} END{print f+0, d+0, e+0}')"
    echo "psi $(awk -F'total=' '/^some/{printf "%s ", $2}' /proc/pressure/cpu) $(awk -F'total=' '{printf "%s ", $2}' /proc/pressure/io) $(awk -F'total=' '{printf "%s ", $2}' /proc/pressure/memory)"
    echo "cpu $(awk '/^cpu[0-9]/{printf "%s,%s,%s,%s,%s,%s,%s,%s;", $2,$3,$4,$5,$6,$7,$8,$9}' /proc/stat)"
    printf 'pod'
    for d in /sys/fs/cgroup/kubepods.slice/kubepods-pod*.slice /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod*.slice; do
      [ -e "$d/cpu.stat" ] || continue
      u=${d##*pod}; u=${u%.slice}
      printf ' %s=%s,%s' "$u" "$(awk '/^(nr_periods|nr_throttled|throttled_usec|usage_usec) /{v[$1]=$2} END{printf "%s,%s,%s,%s", v["nr_periods"]+0, v["nr_throttled"]+0, v["throttled_usec"]+0, v["usage_usec"]+0}' $d/cpu.stat)" \
        "$(awk -F'total=' '/^some/{print $2}' $d/cpu.pressure 2>/dev/null)"
    done
    echo
    # Tomislav-RetCtx (2026-11-09): TCP counters of each POD's network namespace (the tcp/ext lines above are the host
    # namespace only, which never sees pod-to-pod traffic): any process of the pod reads its netns via /proc/<pid>/net.
    # podtcp <uid>=<RetransSegs>,<TCPTimeouts>,<TCPLossProbes>,<TCPSynRetrans>,<TCPSpuriousRTOs>,<TCPFastRetrans>,<CurrEstab>
    printf 'podtcp'
    for d in /sys/fs/cgroup/kubepods.slice/kubepods-pod*.slice /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod*.slice; do
      u=${d##*pod}; u=${u%.slice}
      p=${PODPID[$u]:-}
      if [ -z "$p" ] || [ ! -e "/proc/$p/net/snmp" ]; then
        p=$(cat $d/*/cgroup.procs 2>/dev/null | head -1); PODPID[$u]=$p
      fi
      [ -n "$p" ] || continue
      printf ' %s=%s' "$u" "$(awk '/^Tcp:/{n++; if(n==1){for(i=2;i<=NF;i++) h[$i]=i} else {r=$h["RetransSegs"]; c=$h["CurrEstab"]}}
        /^TcpExt:/{m++; if(m==1){for(i=2;i<=NF;i++) e[$i]=i} else {printf "%s,%s,%s,%s,%s,%s,%s", r, $e["TCPTimeouts"], $e["TCPLossProbes"], $e["TCPSynRetrans"], $e["TCPSpuriousRTOs"], $e["TCPFastRetrans"], c}}' \
        /proc/$p/net/snmp /proc/$p/net/netstat 2>/dev/null)"
    done
    echo
    echo "vm $(awk '/^(pgmajfault|compact_stall|allocstall_normal|allocstall_movable|thp_fault_alloc|pswpin|pswpout) /{v[$1]=$2} END{printf "%s %s %s %s %s %s %s", v["pgmajfault"]+0, v["compact_stall"]+0, v["allocstall_normal"]+0, v["allocstall_movable"]+0, v["thp_fault_alloc"]+0, v["pswpin"]+0, v["pswpout"]+0}' /proc/vmstat)"
    echo "disk $(awk '$3 ~ /^(sd[a-z]+|nvme[0-9]+n[0-9]+)$/{printf "%s=%s,%s,%s,%s,%s,%s ", $3, $4, $7, $8, $11, $13, $14}' /proc/diskstats)"
  } >> "$OUT"
  sleep "$INTERVAL"
done
{ echo "# stopped $(date -u +%FT%T.%NZ)"; echo "dmesg_end"; sudo -n dmesg -T | tail -200; } >> "$OUT"
