import glob

from setuptools import find_packages, setup

version = "1.0"

with open("requirements.txt") as rq:
    requires = rq.readlines()

setup(
    name="scilifelab_epps",
    version=version,
    description="Collection of EPPs for the Scilifelab Stockholm node.",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
    ],
    keywords="genologics api rest",
    author="Per Kraulis",
    author_email="per.kraulis@scilifelab.se",
    maintainer="Denis Moreno",
    maintainer_email="denis.moreno@scilifelab.se",
    url="https://github.com/scilifelab/scilifelab_epps",
    license="GPLv3",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    scripts=glob.glob("scripts/*.py"),
    include_package_data=True,
    zip_safe=False,
    entry_points="""
      # -*- Entry points: -*-
      """,
)
