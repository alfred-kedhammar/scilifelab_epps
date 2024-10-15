#!/usr/bin/env python

import re
import sys
from argparse import ArgumentParser

import psycopg2
import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Project
from genologics.lims import Lims

with open("/opt/gls/clarity/users/glsai/config/genosqlrc.yaml") as f:
    config = yaml.safe_load(f)

DESC = """EPP used to validate a project
Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""

# Pre-compile regexes in global scope:
NGISAMPLE_PAT = re.compile("P[0-9]+_[0-9]+")


# Verify sample IDs
def verify_sample_ids(project_id):
    message = []
    # Query sample names with given project luid
    query = (
        "select sample.name from sample "
        "inner join project on sample.projectid=project.projectid "
        "where project.luid = %s;"
    )
    with psycopg2.connect(
        user=config["username"],
        host=config["url"],
        database=config["db"],
        password=config["password"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (project_id,))
            query_output = cursor.fetchall()

    # Validate sample name format
    for out in query_output:
        sample_id = out[0]
        if not NGISAMPLE_PAT.findall(sample_id):
            message.append(f"SAMPLE NAME WARNING: Bad sample ID format {sample_id}")
        else:
            if sample_id.split("_")[0] != project_id:
                message.append(
                    f"SAMPLE NAME WARNING: Sample ID {sample_id} does not match project ID {project_id}"
                )

    return message


def main(lims, pid):
    message = []
    project = Project(lims, id=pid)

    # Validate sample IDs
    message += verify_sample_ids(project.id)

    if message:
        print(f"No issue detected for project {pid}")
    else:
        sys.stderr.write("; ".join(message))
        sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Project ID for current Project")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid)
