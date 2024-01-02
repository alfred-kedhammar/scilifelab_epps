#!/usr/bin/env python

from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

DESC = """EPP for copying run recipe
Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""


def main(lims, args):
    process = Process(lims, id=args.pid)
    # Read in run recipe file
    for outart in process.all_outputs():
        if outart.type == "ResultFile" and outart.name == "Run Recipe":
            try:
                fid = outart.files[0].id
                file_name = outart.files[0].original_location
                content = lims.get_file_contents(id=fid).read()
                if isinstance(content, bytes):
                    content = content.decode("utf-8")
            except:
                raise RuntimeError("Cannot access the run recipe file.")
            break

    with open(
        f"/srv/ngi-nas-ns/NovaSeq_data/gls_recipe_novaseq/{file_name}", "w"
    ) as sf:
        sf.write(content)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
