#!/bin/bash
while true; do sleep 2; [[ $(blkid) == *xvdh* ]] && break; done
if [[ $(blkid) == *xvdh* ]]; then echo $(date) "xvdh attached, proceeding..." >> /home/ubuntu/automaton/system.log;
			     else echo $(date) "no xvdh, can't proceed" >> /home/ubuntu/automaton/system.log; exit 0; fi

#while true; do sleep 2; ip link show dev eth1 && break; done
#if [[ $(ip link show) == *eth1* ]]; then echo $(date) "eth1 attached, proceeding..." >> /home/ubuntu/system.log;
#				    else echo $(date) "no eth1, can't proceed" >> /home/ubuntu/system.log; exit 0; fi

mount /dev/xvdh /var/lib/lxc
#mv -f /var/lib/lxc/br0.cfg /etc/network/interfaces.d/
mkdir -p /home/ubuntu/.aws/
mv -f /var/lib/lxc/credentials /home/ubuntu/.aws/
chmod 666 /home/ubuntu/.aws/credentials
cp -fR /var/lib/lxc/automaton /home/ubuntu/
rm -rf /var/lib/lxc/automaton

#ifup br0
#ifconfig eth1 down
#ifconfig eth1 hw ether 00:AA:BB:CC:DD:EE
#ifconfig br0 hw ether 00:AA:BB:CC:DD:EE
#ifconfig eth1 up

echo $(date) "Beginning autoLXC restore" >> /home/ubuntu/automaton/system.log
lxc-checkpoint -r -n autoLXC -v -D /home/ubuntu/automaton/snapshot/ >> /home/ubuntu/automaton/system.log
echo $(date) "Restore completed, cleaning up snapshot directory" >> /home/ubuntu/automaton/system.log
/usr/bin/python /home/ubuntu/automaton/src/autoPolicyController.py &
exit 0
