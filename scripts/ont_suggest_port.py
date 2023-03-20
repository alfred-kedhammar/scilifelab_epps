#!/usr/bin/env python

from __future__ import division
import re
import sys
from ont_send_fc_to_db import get_ONT_db


DESC = """ Script for EPP "Suggest PromethION ports".
Use StatusDB to find the least used ports and populate the positions UDFs with them.
"""


def main():

    # Get database
    db = get_ONT_db()
    view = db.view("info/all_stats")
    
    # Instantiate dict for counting port usage
    ports = {}
    for c in list("123"):
        for r in list("ABCDEFGH"):
            ports[c + r] = 0

    pattern = re.compile("/\d{8}_\d{4}_([1-8][A-H])_") # Matches start of run name, capturing position as a group

    # Count port usage
    for row in view.rows:
        if re.search(pattern, row.value["TACA_run_path"]):
            position = re.search(pattern, row.value["TACA_run_path"]).groups()[0]
            ports[position] += 1

    # Print ports to stdout, starting with the least used
    ports_list = list(ports)
    ports_list.sort(key = lambda x: x[1])
    message = f'Listing ports, from least to most used: {", ".join([port[0] for port in ports_list])}'
    sys.stdout.write(message)


if __name__ == "__main__":
    main()