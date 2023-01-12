#!/usr/bin/env python

from __future__ import division
import logging
import os
import sys
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from scilifelab_epps.epp import attach_file
from genologics.entities import Process

def main(lims, args):
    currentStep = Process(lims, id=args.pid)

