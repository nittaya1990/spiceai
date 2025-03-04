import os
import sys

gitRef = os.getenv("GITHUB_REF")

with open(os.getenv("GITHUB_ENV"), "a") as githubEnv:
    with open("version.txt") as f:
        version = f.read()
    releaseVersion = version.strip()
    releaseNotePath = "docs/release_notes/v{}.md".format(releaseVersion)

    print("Checking if {} exists".format(releaseNotePath))
    if os.path.exists(releaseNotePath):
        print("Found {}".format(releaseNotePath))
        # Set LATEST_RELEASE to true
        githubEnv.write("LATEST_RELEASE=true\n")
    else:
        print("{} is not found".format(releaseNotePath))
    print("Release build from {}...".format(gitRef))

    githubEnv.write("REL_VERSION={}\n".format(releaseVersion))
