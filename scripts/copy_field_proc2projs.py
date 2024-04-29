#!/usr/bin/env python

DESC = """EPP script to copy user defined field from any process to associated
project/projects in Clarity LIMS. If the specifyed process handles many artifacts
associated to different projects, all these projects will get the specifyed udf.

Can be executed in the background or triggered by a user pressing a "blue button".

The script can output two different logs, where the status_changelog
contains notes with the technician, the date and changed status for each
copied status. The regular log file contains regular execution information.

Written by Maya Brandi
"""

import logging
import sys
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import CopyField, EppLogger


def main(lims, args, epp_logger):
    d_elts = []
    no_updated = 0
    incorrect_udfs = 0
    project_names = ""
    source_udfs = args.source_udf
    dest_udfs = args.dest_udf
    s_elt = Process(lims, id=args.pid)
    analytes, inf = s_elt.analytes()

    for analyte in analytes:
        for samp in analyte.samples:
            d_elts.append(samp.project)
    d_elts = list(set(d_elts))

    if args.status_changelog:
        epp_logger.prepend_old_log(args.status_changelog)

    if not dest_udfs:
        dest_udfs = source_udfs
    elif len(dest_udfs) != len(source_udfs):
        logging.error("source_udfs and dest_udfs lists of arguments are uneven.")
        sys.exit(-1)

    for d_elt in d_elts:
        if d_elt:
            project_names = " ".join([project_names, d_elt.name])
        else:
            project_names = ""
        for i in range(len(source_udfs)):
            source_udf = source_udfs[i]
            dest_udf = dest_udfs[i]
            with open(args.status_changelog, "a") as changelog_f:
                if source_udf in s_elt.udf:
                    copy_sesion = CopyField(s_elt, d_elt, source_udf, dest_udf)
                    test = copy_sesion.copy_udf(changelog_f)
                    if test:
                        no_updated = no_updated + 1
                else:
                    logging.warning(
                        f"Udf: {source_udf} in Process {s_elt.id} is undefined/blank, exiting"
                    )
                    incorrect_udfs = incorrect_udfs + 1

    if incorrect_udfs > 0:
        warn = f"Failed to update {incorrect_udfs} udf(s) due to missing/wrong source udf info."
    else:
        warn = ""

    d = {"up": no_updated, "ap": len(d_elts), "w": warn, "pr": project_names}

    abstract = ("Updated {up} udf(s). Handeled project(s): {pr} {w}").format(**d)
    print(abstract, file=sys.stderr)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    parser.add_argument(
        "--log",
        help=(
            "File name for standard log file, " "for runtime information and problems."
        ),
    )
    parser.add_argument(
        "-s",
        "--source_udf",
        type=str,
        default=None,
        nargs="*",
        help=(
            "Name(s) of the source user defined field(s) "
            "that will be copied. One or many udf-names "
            "can be given."
        ),
    )
    parser.add_argument(
        "-d",
        "--dest_udf",
        type=str,
        default=None,
        nargs="*",
        help=(
            "Name(s) of the destination user defined "
            "field(s) that will be written to. This "
            "argument is optional, if left empty "
            "the source_udf argument is used instead. "
            "Zero or many udf-names can be given. If "
            "more than zero, the numer of udfs needs "
            "to be the same as number of source_udfs"
        ),
    )
    parser.add_argument(
        "-c",
        "--status_changelog",
        help=(
            "File name for status changelog file, for "
            "concise information on who, what and when "
            "for status change events. "
            "Prepends the old changelog file by default."
        ),
    )

    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args, epp_logger)
