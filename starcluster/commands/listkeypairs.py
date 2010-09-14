#!/usr/bin/env python

from base import CmdBase

class CmdListKeyPairs(CmdBase):
    """
    listkeypairs

    List all EC2 keypairs
    """
    names = ['listkeypairs', 'lk']
    def execute(self, args):
        ec2 = self.cfg.get_easy_ec2()
        ec2.list_keypairs()
