#!/usr/bin/env python

DESC = """EPP script to copy user defined field 'Reference Genome' from project
level to submitted sample level for the input artifacts of given process,
in Clarity LIMS. Can be executed in the background or triggered by a user
 pressing a "blue button".

The script outputs a regular log file that contains regular execution
information.

Error handling:
If the udf 'Reference Genome' is blank or not defined for any of the input
projects, the script will log this, and not perform any changes for that sample.


Written by Johannes Alneberg, Science for Life Laboratory, Stockholm, Sweden
"""
import logging
import sys
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import EppLogger


class Session:
    def __init__(self, process, udf, d_udf):
        self.process = process
        self.s_udf = udf  # Source udf
        self.d_udf = d_udf  # Destination udf

    def _sample_udf(self, sample):
        if self.d_udf in sample.udf:
            return sample.udf[self.d_udf]
        else:
            return "Undefined/Blank"

    def log_before_change(self, project, sample):
        logging.info(
            f"Copying from project with id: {project.id} to sample with "
            f" id: {sample.id}"
        )

    def log_after_change(self, project, saved_sample_udf):
        d = {"udf": self.d_udf, "su": saved_sample_udf, "nv": project.udf[self.s_udf]}

        logging.info("Updated Sample {udf} from {su} to {nv}.".format(**d))

    def copy_udf(self, project, sample):
        saved_sample_udf = self._sample_udf(sample)

        self.log_before_change(project, sample)

        n_udf = project.udf[self.s_udf]  # New udf
        sample.udf[self.d_udf] = n_udf
        sample.put()
        self.log_after_change(project, saved_sample_udf)

    def copy_main(self, samples):
        for sample in samples:
            project = sample.project

            self.copy_udf(project, sample)


def all_projects_for_artifacts(artifacts):
    """get all unique projects associated with a list of artifacts"""
    projects = set()
    for artifact in artifacts:
        for sample in artifact.samples:
            projects.add(sample.project)
    return list(projects)


def check_udf_is_defined(projects, udf):
    """Filter and Warn if udf is not defined for any of inputs."""
    filtered_projects = []
    incorrect_projects = []
    for project in projects:
        if udf in project.udf:
            filtered_projects.append(project)
        else:
            logging.warning(
                f"Found project with id {project.id} with {udf} "
                "undefined/blank, exiting"
            )
            incorrect_projects.append(project)
    return filtered_projects, incorrect_projects


def filter_samples(artifacts, projects):
    """All samples belonging to a project in projects"""
    projects = set(projects)
    return_samples = []
    samples = set()
    for artifact in artifacts:
        samples.update(artifact.samples)

    for sample in samples:
        if sample.project in projects:
            return_samples.append(sample)
        else:
            logging.warning(
                f"Filtered out sample {sample.name} belonging to project {sample.project.name} "
                "without udf defined"
            )
    return return_samples


def main(lims, args, epp_logger):
    pro = Process(lims, id=args.pid)
    source_udf = "Reference genome"
    destination_udf = "Reference Genome"

    artifacts = pro.all_inputs(unique=True)
    projects = all_projects_for_artifacts(artifacts)

    correct_projects, incorrect_udf = check_udf_is_defined(projects, source_udf)
    correct_samples = filter_samples(artifacts, correct_projects)

    session = Session(pro, source_udf, destination_udf)
    session.copy_main(correct_samples)

    if len(incorrect_udf) == 0:
        warning = "no projects"
    else:
        warning = f"WARNING: skipped {len(incorrect_udf)} project(s)"

    d = {"cs": len(correct_samples), "warning": warning}

    abstract = ("Updated {cs} sample(s), {warning} with incorrect udf info.").format(
        **d
    )

    print(abstract, file=sys.stderr)  # stderr will be logged and printed in GUI


if __name__ == "__main__":
    # Initialize parser with standard arguments and description

    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    parser.add_argument(
        "--log",
        help=(
            "File name for standard log file, " " for runtime information and problems."
        ),
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args, epp_logger)
