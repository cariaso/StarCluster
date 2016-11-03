# Copyright 2015 Michael Cariaso
#
# This file is a plugin for StarCluster.
#
# This plugin is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

from starcluster import clustersetup
from starcluster.logger import log
import boto3
import time

class RAIDPlugin(clustersetup.DefaultClusterSetup):
    """Add RAID support

    Example config:

    [plugin efs]
    SETUP_CLASS = starcluster.plugins.efs.EFSPlugin
    mount_point = /mnt/myraid

    # as in /dev/sdf and /dev/xvdf
    first_device_letter = f

    # eiether provide existing volume IDs
    #volume_ids = vol-a3e01921,vol-d5e01957

    # or setting for newly created volumes
    num_volumes = 2
    volume_size = 5
    iops = 100
    #iops = min
    #iops = max


    """
    def __init__(self, mount_point=None,
                 volume_ids=None,
                 num_volumes=None,
                 first_device_letter=None,
                 volume_size=None,
                 iops=None,
                 **kwargs):
        if mount_point is None: mount_point = '/mnt/raid'
        self.mount_point = mount_point
        if first_device_letter:
            self.first_device_letter = first_device_letter
        else:
            self.first_device_letter = 'f'
        self.volume_ids = volume_ids
        if not self.volume_ids:
            self.num_volumes = int(num_volumes)
            self.volume_size = int(volume_size)
            if iops:
                if iops.lower() == 'min':
                    self.iops = 20 * self.volume_size
                elif iops.lower() == 'max':
                    self.iops = 50 * self.volume_size
                else:
                    self.iops = int(iops)
            else:
                self.iops = 50 * self.volume_size

        super(RAIDPlugin, self).__init__(**kwargs)

    def run(self, nodes, master, user, user_shell, volumes):
        self._master = master
        self._new_security_group = master.cluster_groups[0].id


        log.info("Configuring RAID")

        # do a suitable check for lvm2
        needs_lvm2 = True
        if needs_lvm2:
            try:
                node.ssh.execute("echo 'APT::Periodic::Enable \"0\";' >> /etc/apt/apt.conf.d/10periodic")
            except Exception, e:
                print e
                log.warn(e)

            # Ubuntu 16 has a very stupid new default
            # https://github.com/geerlingguy/packer-ubuntu-1604/issues/3#issue-154560190
            try:
                log.info("killing any running apt-get")
                node.ssh.execute("killall apt-get")
                node.ssh.execute("dpkg --configure -a")
                node.ssh.execute("apt-get update")
                node.ssh.execute("apt-get upgrade")
                log.info("clean kill")
            except Exception, e:
                log.info("not a clean kill")
                print e
                log.warn(e)


            try:
                log.info("purge unattended-upgrades")
                node.ssh.execute('apt-get -y purge unattended-upgrades')
                log.info("purged unattended-upgrades")
            except Exception, e:
                log.info("purge unattended-upgrades failed")
                print e
                log.warn(e)
        

            master.ssh.execute('apt-get install -y lvm2')

        self._b3client = self._get_boto_client('ec2')

        zone = master.ssh.execute('ec2metadata --availability-zone')[0]
        firstdeviceletter = 'g'

        if self.volume_ids:
            volumeIds = self.volume_ids.split(',')
            make_volumes = False
        else:
            volumeIds = []
            make_volumes = True

        devices = []

        if make_volumes:
            for i in range(self.num_volumes):
                response = self._b3client.create_volume(
                    DryRun=False,
                    Size=self.volume_size,
                    AvailabilityZone=zone,
                    VolumeType='io1',#'gp2'
                    Iops=self.iops,
                )
                #print response
                volumeIds.append(response.get('VolumeId'))
                print 'created volume %s' % response.get('VolumeId')

        if True:
            for i, volumeId in enumerate(volumeIds):
                device = '/dev/sd%c' % chr(ord(firstdeviceletter)+i)

                print 'Waiting for volume to switch to available'
                waiter = self._b3client.get_waiter('volume_available')
                waiter.wait(VolumeIds=[volumeId])
                print 'Volume %s is available' % volumeId

                response = self._b3client.attach_volume(
                    DryRun=False,
                    VolumeId=volumeId,
                    InstanceId=master.id,
                    Device=device,
                )
                devices.append(device)
                log.info("Attaching %s to %s %s" % (volumeId, master.id, device))


                print 'Waiting for volume to switch to In Use state'
                waiter = self._b3client.get_waiter('volume_in_use')
                waiter.wait(VolumeIds=[volumeId])
                print 'Volume %s is in use' % volumeId


                print 'Waiting for volume to switch to attached state'
                while self._b3client.describe_volumes(VolumeIds=[volumeId])['Volumes'][0]['Attachments'][0]['State'] != 'attached':
                    time.sleep(3)
                print 'Volume %s is attached' % volumeId


        lvname = 'fastdisk'
        fileservername = 'fileserver'
        devname = '/dev/%s/%s' % (fileservername, lvname)

        xvnames = []
        wipe_drive = False
        if wipe_drive:
            for device in devices:
                xvname = device.replace('/sd','/xvd')
                cmd = 'parted %s -s mklabel gpt unit TB mkpart primary 0 100%%' % xvname
                print cmd
                master.ssh.execute(cmd)
                #master.ssh.execute('mkfs.ext4 %s' % xvname)
                xvnames.append(xvname)

                cmd = 'dd if=/dev/zero of=%s bs=512 count=1' % xvname
                print cmd
                master.ssh.execute(cmd)

#            pdb.set_trace()



            cmd = 'umount %s' % devname
            print cmd
            try:
                master.ssh.execute(cmd)
            except Exception, e:
                print '!!!',e

            cmd = 'dmsetup remove %s-%s' % (fileservername, lvname)
            print cmd
            try:
                master.ssh.execute(cmd)
            except Exception, e:
                print'!!!', e

            cmd = 'lvremove -f %s' % fileservername
            print cmd
            try:
                master.ssh.execute(cmd)
            except Exception, e:
                print '!!!',e


            cmd = 'pvcreate %s' % ' '.join(xvnames)
            print cmd
            master.ssh.execute(cmd)


            cmd = 'vgcreate %s %s' % (fileservername, ' '.join(xvnames))
            master.ssh.execute(cmd)

            # http://tldp.org/HOWTO/LVM-HOWTO/recipethreescsistripe.html
            #https://sysadmincasts.com/episodes/27-lvm-linear-vs-striped-logical-volumes
            numstripes = len(xvnames)
            #cmd = 'lvcreate --extents 100%%FREE --stripes %s --stripesize 256 --name %s %s' % (numstripes, lvname, fileservername)
            cmd = 'lvcreate --extents 100%%FREE --stripes %s --name %s %s' % (numstripes, lvname, fileservername)
            print cmd
            master.ssh.execute(cmd)

            cmd = 'mkfs.ext4 %s' % (devname)
            print cmd
            master.ssh.execute(cmd)


        cmd = 'mkdir -p %s' % (self.mount_point)
        print cmd
        master.ssh.execute(cmd)

        cmd = 'mount %s %s' % (devname, self.mount_point)
        print cmd
        master.ssh.execute(cmd)

        log.info('semi-done')

        #self._authorize_efs()
        #log.info("Mounting efs on all nodes")
        #for node in nodes:
        #    log.info("  Mounting efs on %s" % node)
        #    self._install_efs_on_node(node)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self._master = master
        self._new_security_group = node.cluster_groups[0].id

        #log.info("Adding %s to EFS" % node.alias)
        #self._install_efs_on_node(node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self._master = master
        log.info("No need to remove %s from RAID" % node.alias)

    def on_shutdown(self, nodes, master, user, user_shell, volumes):
        """
        This method gets executed before shutting down the cluster
        """
        self._master = master
        self._new_security_group = master.cluster_groups[0].id

        #self._deauthorize_efs()

    def _get_boto_client(self, service):
        creds = self._master.ec2.__dict__
        b3client = boto3.client(
            service,
            aws_access_key_id=creds.get('aws_access_key_id'),
            aws_secret_access_key=creds.get('aws_secret_access_key'),
            region_name=creds.get('_conn').region.name,
        )
        return b3client
