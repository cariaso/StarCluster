#!/usr/bin/env python

from base import CmdBase

class CmdListZones(CmdBase):
    """
    listzones

    List all EC2 availability zones in the current region (us-east-1 by default)
    """
    names = ['listzones', 'lz']
    def addopts(self, parser):
        opt = parser.add_option(
            "-r","--region", dest="region",
            default=None,
            help="Show all zones in a given region (see listregions)")
    def execute(self, args):
        ec2 = self.cfg.get_easy_ec2()
        ec2.list_zones(region=self.opts.region)
