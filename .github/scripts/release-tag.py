"""Print the release tag recorded in a commit's workspace manifest."""

import re
import subprocess
import sys
import tomllib


def release_tag(target):
    manifest = subprocess.check_output(
        ["git", "show", f"{target}:Cargo.toml"], text=True
    )
    version = tomllib.loads(manifest)["workspace"]["package"]["version"]
    number = r"(?:0|[1-9][0-9]*)"
    match = re.fullmatch(rf"({number}\.{number}\.{number})(?:-alpha\.{number})?", version)
    if match is None:
        raise ValueError("workspace version must be MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-alpha.N")

    tag = f"v{version}"
    for existing in {tag, f"v{match[1]}"}:
        result = subprocess.run(
            ["git", "rev-parse", "--quiet", "--verify", f"refs/tags/{existing}"],
            stdout=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            raise ValueError(f"{existing} already exists; bump workspace.package.version before releasing")
        if result.returncode != 1:
            raise ValueError(f"could not check whether {existing} exists")
    return tag


if __name__ == "__main__":
    try:
        print(release_tag(sys.argv[1] if len(sys.argv) > 1 else "HEAD"))
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(f"Error: {error}")
