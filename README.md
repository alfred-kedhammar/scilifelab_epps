# scilifelab_epps
Collection of EPPs for the Scilifelab Stockholm node.

Table of contents
- [scilifelab\_epps](#scilifelab_epps)
  - [Overview](#overview)
  - [Installation](#installation)
  - [Development](#development)
    - [Automated linting](#automated-linting)
      - [GitHub Actions](#github-actions)
      - [Pre-commit](#pre-commit)
      - [VS Code automation](#vs-code-automation)
    - [Git blame suppression](#git-blame-suppression)


## Overview
The repo is installed inside a designated conda environment on a server hosting Illumina Clarity LIMS.

After installation, the scripts in `scripts/` are addded to the `bin` of the Conda environment, from which they can be called by LIMS.

## Installation
1) Create a designated Conda environment (Python >=3.10)
2) Inside the environment, run

```
pip install requirements.txt
python install setup.py
```

## Development

Run `pip install requirements-dev.txt`

### Automated linting
This repo is configured for automated linting. Linter parameters are defined in `pyproject.toml`.

As of now, we use:
- [ruff](https://docs.astral.sh/ruff/) to perform automated formatting and a variety of lint checks.
- [mypy](https://mypy.readthedocs.io/en/stable/) for static type checking and to prevent contradictory type annotation.

#### [GitHub Actions](https://docs.github.com/en/actions)
Configured in `.github/workflows/lint-code.yml`. Will test all pushed commits, but not change code or prevent merges.

#### [Pre-commit](https://pre-commit.com/)
Will prevent local commits that fail linting checks. Configured in `.pre-commit-config.yml`.

To set up pre-commit checking:
1) Run `pip install pre-commit`
2) Navigate to the repo root
3) Run `pre-commit install`

This can be disabled with `pre-commit uninstall`

#### VS Code automation
To enable automated linting in VS Code, go the the user `settings.json` and include the following lines:
```
"[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
}
```

This will run the `ruff`-mediated linting with the same parameters as the `GitHub Actions` and `pre-commit` every time VS Code is used to format the code in the repository.

To run formatting on save, include the lines:
```
"[python]": {
    "editor.formatOnSave": true,
}
```

### Git blame suppression
When a non-invasive tool is used to tidy up a lot of code, it is useful to supress the Git blame for that particular commit, so the original author can still be traced.

To do this, add the hash of the commit containing the changes to `.git-blame-ignore-revs`, headed by an explanatory comment.