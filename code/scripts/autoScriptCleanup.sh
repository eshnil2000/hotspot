#!/bin/bash

rm -rf /home/ubuntu/automaton/snapshot
echo $(date) "Beginning autoLXC checkpointing" >> /home/ubuntu/automaton/system.log
lxc-checkpoint -s -n autoLXC -v -D /home/ubuntu/automaton/snapshot/
echo $(date) "Completed checkpointing" >> /home/ubuntu/automaton/system.log

#cp -f /etc/network/interfaces.d/br0.cfg /var/lib/lxc/
cp -f /home/ubuntu/.aws/credentials /var/lib/lxc/
cp -fR /home/ubuntu/automaton /var/lib/lxc/
umount /dev/xvdh

#ifconfig eth1 down
#ifconfig eth1 hw ether 06:79:b3:6b:40:77
#ifconfig br0 hw ether 06:79:b3:6b:40:77
#ifconfig eth1 up
#ifdown br0
