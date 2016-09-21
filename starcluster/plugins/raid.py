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


    """

    def __init__(self, mount_point=None,
                 **kwargs):
        self.mount_point = mount_point
        #self.fs_id = fs_id
        super(RAIDPlugin, self).__init__(**kwargs)

    def run(self, nodes, master, user, user_shell, volumes):
        self._master = master
        self._new_security_group = master.cluster_groups[0].id

        log.info("Configuring RAID")

        self._b3client = self._get_boto_client('ec2')

        num_volumes = 2
        volume_size = 5
        iops = volume_size * 20 #* 50
        zone = master.ssh.execute('ec2metadata --availability-zone')[0]
        firstdeviceletter = 'g'

        volumeIds = []
        devices = []

        if True:
            for i in range(num_volumes):
                response = self._b3client.create_volume(
                    DryRun=False,
                    Size=volume_size,
                    AvailabilityZone=zone,
                    VolumeType='io1',#'gp2'
                    Iops=iops,
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
 

        xvnames = []
        if True:
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


            lvname = 'fastdisk'
            fileservername = 'fileserver'
            devname = '/dev/%s/%s' % (fileservername, lvname)

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
            cmd = 'lvcreate --yes --extents 100%%FREE --stripes %s --name %s %s' % (numstripes, lvname, fileservername)
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

    # def _authorize_efs(self):

    #     self._b3client = self._get_efs_client()

    #     mount_targets = self._get_mount_targets(self.fs_id)

    #     for targetinfo in mount_targets:
    #         log.info('Authorizing EFS security group')
    #         resp = self._b3client.describe_mount_target_security_groups(
    #             MountTargetId=targetinfo.get('MountTargetId'),
    #         )
    #         oldgroups = resp['SecurityGroups']
    #         oldgroups.append(self._new_security_group)
    #         newgroups = list(set(oldgroups))

    #         self._b3client.modify_mount_target_security_groups(
    #             MountTargetId=targetinfo.get('MountTargetId'),
    #             SecurityGroups=newgroups,
    #         )

    # def _deauthorize_efs(self):

    #     self._b3client = self._get_efs_client()

    #     mount_targets = self._get_mount_targets(self.fs_id)

    #     for targetinfo in mount_targets:
    #         resp = self._b3client.describe_mount_target_security_groups(
    #             MountTargetId=targetinfo.get('MountTargetId'),
    #         )
    #         groups = resp['SecurityGroups']
    #         found_group = None
    #         try:
    #             groups.remove(self._new_security_group)
    #             found_group = True
    #         except ValueError:
    #             log.info('Expected security group is not currently associated')
    #             found_group = False

    #         if found_group:
    #             self._b3client.modify_mount_target_security_groups(
    #                 MountTargetId=targetinfo.get('MountTargetId'),
    #                 SecurityGroups=groups,
    #             )
    #             msg = 'Disassociated EFS security group %s' % (
    #                 self._new_security_group
    #             )
    #             log.info(msg)

    # def _get_mount_targets(self, fs_id):
    #     mtresponse = self._b3client.describe_mount_targets(FileSystemId=fs_id)
    #     mts = mtresponse.get('MountTargets')
    #     return mts

    # def _install_efs_on_node(self, node):
    #     if not node.ssh.path_exists(self.mount_point):
    #         node.ssh.makedirs(self.mount_point, mode=0777)
    #     zone = node.ssh.execute('ec2metadata --availability-zone')[0]
    #     region = zone[:-1]
    #     name_parts = [zone, self.fs_id, 'efs', region, 'amazonaws', 'com']
    #     efs_dns = '.'.join(name_parts)
    #     mount_info = node.ssh.execute('grep %s /proc/mounts' %
    #                                   self.mount_point, raise_on_failure=False,
    #                                   ignore_exit_status=True)
    #     log.warn('%s using sync mount' % self.mount_point)
    #     #cmd = 'mount -t nfs4 -ominorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,sync,noac,dirsync, %s:/ %s' % (efs_dns,
    #     cmd = 'mount -t nfs4 -ominorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 %s:/ %s' % (efs_dns,
    #                                                       self.mount_point)
    #     if mount_info:
    #         log.warn('%s is already a mount point' % self.mount_point)
    #         log.info(mount_info[0])
    #     else:
    #         node.ssh.execute(cmd)
