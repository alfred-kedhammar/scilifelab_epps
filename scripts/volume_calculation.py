#!/usr/bin/env python

import os
import sys
from argparse import ArgumentParser

import psycopg2
import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import attach_file

DESC = """EPP for calculating volume for the OmniC protocol
Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""

factors = {
    "ng/ul": 1,
    "ug/ul": 0.001,
    "mg/ul": 0.000001,
    "ng/ml": 1000,
    "ug/ml": 1,
    "mg/ml": 0.001,
}

with open("/opt/gls/clarity/users/glsai/config/genosqlrc.yaml") as f:
    config = yaml.safe_load(f)


# Verify that inputs have necessary measurements for calculation
def verify_inputs(process, value_list):
    message = []
    for inp in process.all_inputs():
        for val in value_list:
            if not inp.udf.get(val):
                message.append(f"ERROR: Unknown {val} for sample {inp.name}.")
            elif val == "Conc. Units" and inp.udf[val].lower() not in [
                "ng/ul",
                "ug/ul",
                "mg/ul",
                "ng/ml",
                "ug/ml",
                "mg/ml",
            ]:
                message.append(
                    f"ERROR: Unsupported {val} for sample {inp.name}."
                )
    return message


# API-based method for Sample Setup
def calculate_volume_limsapi(process, use_total_lysate):
    error_messages = []
    log = []

    verify_inputs_message = verify_inputs(
        process, ["Concentration", "Conc. Units", "Amount (ng)"]
    )
    error_messages += verify_inputs_message

    for art_tuple in process.input_output_maps:
        input = art_tuple[0]["uri"]
        output = art_tuple[1]["uri"]
        if input.type == "Analyte" and output.type == "Analyte":
            if output.udf.get("Volume to take (uL)"):
                del output.udf["Volume to take (uL)"]
                output.put()
            if output.udf.get("Amount for prep (ng)"):
                if input.udf["Amount (ng)"] >= output.udf["Amount for prep (ng)"]:
                    if use_total_lysate:
                        output.udf["Volume to take (uL)"] = (
                            output.udf["Amount for prep (ng)"]
                            * 58.5
                            / input.udf["Amount (ng)"]
                        )
                        log.append(
                            "Use formula: Amount for prep (ng) x 58.5 / Total lysate (ng). Volume to take (uL) for sample {} is {}.".format(
                                output.name, round(output.udf["Volume to take (uL)"], 2)
                            )
                        )
                    else:
                        factor = factors[input.udf["Conc. Units"].lower()]
                        output.udf["Volume to take (uL)"] = (
                            output.udf["Amount for prep (ng)"]
                            / input.udf["Concentration"]
                            * factor
                        )
                        log.append(
                            "Use formula: Amount for prep (ng) / Concentration (ng/ul). Volume to take (uL) for sample {} is {}.".format(
                                output.name, round(output.udf["Volume to take (uL)"], 2)
                            )
                        )
                    output.put()
                else:
                    error_messages.append(
                        "ERROR: Amount for prep (ng) is higher than Total Amount (ng) for sample {}.".format(
                            output.name
                        )
                    )
            else:
                error_messages.append(
                    f"ERROR: Amount for prep (ng) not defined for sample {output.name}."
                )

    return error_messages, log


# Postgres-based method for Setup Workset/Plate
def calculate_volume_postgres(process):
    error_messages = []
    log = []

    connection = psycopg2.connect(
        user=config["username"],
        host=config["url"],
        database=config["db"],
        password=config["password"],
    )
    cursor = connection.cursor()

    query = (
        "select pro.processid, art.name, aus.numeric0, aus.text0 "
        "from process pro "
        "inner join processtype pt on pt.typeid=pro.typeid "
        "inner join processiotracker pit on pit.processid=pro.processid "
        "inner join artifact art on art.artifactid=pit.inputartifactid "
        "inner join outputmapping opm on opm.trackerid=pit.trackerid "
        "inner join artifact art2 on art2.artifactid=opm.outputartifactid "
        "inner join artifactudfstorage aus on aus.artifactid=art2.artifactid "
        "inner join processoutputtype pot on pot.processtypeid = pt.typeid AND pot.typeid = art2.processoutputtypeid "
        "where pt.displayname='Intermediate QC' "
        "and art2.name='Qubit Measurement' "
        "and pot.displayname = 'ResultFile' "
        "and aus.numeric0 is not NULL "
        "and aus.text0 is not NULL "
        "and art.luid='{}';"
    )

    for art_tuple in process.input_output_maps:
        input = art_tuple[0]["uri"]
        output = art_tuple[1]["uri"]
        if input.type == "Analyte" and output.type == "Analyte":
            if output.udf.get("Volume to take (uL)"):
                del output.udf["Volume to take (uL)"]
                output.put()
            cursor.execute(query.format(input.id))
            query_output = cursor.fetchall()
            if len(query_output) == 1:
                conc = query_output[0][2]
                conc_unit = query_output[0][3]
            # When there are more than 1 query results found, use the latest values
            elif len(query_output) > 1:
                conc = max(query_output, key=lambda tup: tup[0])[2]
                conc_unit = max(query_output, key=lambda tup: tup[0])[3]
            # No concentration could be found
            else:
                error_messages.append(
                    f"ERROR: No measurement found for sample {output.name}."
                )
            # Calculation
            if output.udf.get("Amount for prep (ng)"):
                if conc and conc_unit.lower() in factors.keys():
                    factor = factors[conc_unit.lower()]
                    output.udf["Volume to take (uL)"] = (
                        output.udf["Amount for prep (ng)"] / conc * factor
                    )
                    log.append(
                        "Volume to take (uL) for sample {} is {}.".format(
                            output.name, round(output.udf["Volume to take (uL)"], 2)
                        )
                    )
                    output.put()
                else:
                    error_messages.append(
                        f"ERROR: Invalid conc or conc unit for sample {output.name}."
                    )
            else:
                error_messages.append(
                    f"ERROR: Amount for prep (ng) not defined for sample {output.name}."
                )

    return error_messages, log


def main(lims, pid):
    process = Process(lims, id=pid)

    art_workflows = set()
    for inp in process.all_inputs():
        for stage in inp.workflow_stages_and_statuses:
            if stage[1] == "IN_PROGRESS":
                art_workflows.add(stage[0].workflow.name)

    if process.type.name == "Sample Setup":
        (error_messages, log) = calculate_volume_limsapi(process, use_total_lysate=True)
    elif process.type.name == "Setup Workset/Plate" and any(
        "OmniC" in i for i in art_workflows
    ):
        (error_messages, log) = calculate_volume_postgres(process)

    with open("volume_calculation.log", "w") as log_context:
        log_context.write("\n".join(error_messages + log))

    for out in process.all_outputs():
        # Attach the the log file
        if out.name == "Volume Calculation Log":
            attach_file(os.path.join(os.getcwd(), "volume_calculation.log"), out)

    if error_messages:
        sys.stderr.write("; ".join(error_messages) + "\n")
        sys.exit(2)
    else:
        print("Volume calculation completed without any error.", file=sys.stderr)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid)
