from typing import List, Sequence
import re

SEMANTIC_VERSION_PATTERN = r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?"


def increment_version(line: str, search_terms: List[str]) -> str:
    if not any([s in line for s in search_terms]):
        return line

    match = re.search(SEMANTIC_VERSION_PATTERN, line)
    version = match.group(0)
    major = match.group(1)
    minor = match.group(2)
    patch = match.group(3)
    label = match.group(4)
    new_patch = str(int(patch) + 1)

    if not label:
        new_ver = f"{major}.{minor}.{new_patch}"
    else:
        new_ver = f"{major}.{minor}.{new_patch}-{label}"

    return line.replace(version, new_ver)


def process_file(file_name: str, search_terms: Sequence[str]):
    with open(file_name, "rt") as fin:
        lines = fin.readlines()

    with open(file_name, "wt") as fout:
        for line in lines:
            line = increment_version(line, search_terms)
            fout.write(line)


process_file("helm/Chart.yaml", ("version: ", "appVersion: "))
process_file("altonomy/ace/__init__.py", ("__version__ = ",))