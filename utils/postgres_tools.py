import os
import sys
import yaml
import psycopg2

DESC = """Utility functions for querying the Clarity LIMS database using postgres.
"""

with open("/opt/gls/clarity/users/glsai/config/genosqlrc.yaml", "r") as f:
    config = yaml.safe_load(f)

connection = psycopg2.connect(
    user=config["username"],
    host=config["url"],
    database=config["db"],
    password=config["password"],
)
cursor = connection.cursor()


def get_measurements(input_art=None, measurement_name=None, qc_step_name=None):
    """Fetch artifact measurements, bypassing the need for an aggregate QC step."""
    pass
