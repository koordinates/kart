#!/bin/bash
set -e

# Helper script to download vendor-* archives from Github CI.
# Usage ./github-download.sh {PLATFORM} {BRANCH}

if ! command -v jq >/dev/null; then
    echo "❗️This script requires 'jq' (and ideally 'hub') installed"
    exit 2
fi

REPO=koordinates/sno
WORKFLOW=build.yml

if [ -n "${1-}" ]; then
    PLATFORM=$1
elif [ "${OS}" = "Windows_NT" ]; then
    PLATFORM=Windows
else
    PLATFORM=$(uname -s)
fi
echo "Platform: ${PLATFORM}"

CURRENT_BRANCH=$(git branch --show-current)
BRANCH=${2:-${CURRENT_BRANCH:-master}}
echo "Branch: ${BRANCH}"

echo "Finding most recent CI Run for '${BRANCH}'..."
RUN_SET=$(curl -sS "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?branch=${BRANCH}&status=success&event=push")
RUN=$(jq .workflow_runs[0] <<<"$RUN_SET")
if [ -z "$RUN" ] || [ "$RUN" = "null" ]; then
    echo "❗️Couldn't find a recent successful CI build for '$BRANCH'"
    if [ "$BRANCH" != "master" ]; then
        echo "Try with '$0 ${PLATFORM} master'?"
    fi
    exit 1
fi
RUN_ID=$(jq -r .id <<< "$RUN")
echo "CI Run: ${RUN_ID}"

PLATFORM_ARTIFACT="vendor-${PLATFORM}"

echo "Finding '${PLATFORM_ARTIFACT}' artifact from CI Run..."
ARTIFACT_SET=$(curl -sS "https://api.github.com/repos/${REPO}/actions/runs/${RUN_ID}/artifacts")
ARTIFACT_URLS=$(jq -r ".artifacts | map(select(any(.name; startswith(\"${PLATFORM_ARTIFACT}\")))|.archive_download_url)[]" <<<"$ARTIFACT_SET")
ARTIFACT_IDS=$(jq -r ".artifacts | map(select(any(.name; startswith(\"${PLATFORM_ARTIFACT}\")))|.id)[]" <<<"$ARTIFACT_SET")
if [ -z "$ARTIFACT_IDS" ]; then
    echo "❗️Couldn't find a ${PLATFORM_ARTIFACT} artifact. Check $(jq -r .html_url <<< "$RUN") ?"
    exit 1
fi

#
# Github requires users to be logged in to access the artifacts API, even
# for public projects. ¯\_(ツ)_/¯
#

cd "$(dirname "$0")"
mkdir -p dist/
DOWNLOADED=false
if command -v hub >/dev/null; then
    # if 'hub' is installed and configured, use its authenticated access to the API
    if echo | hub api user > /dev/null ; then
        echo "Using 'hub' to download artifact via the Github API..."
        for URL in $ARTIFACT_URLS; do
            echo "Downloading... $URL"
            hub api "$URL" > dist/tmp.zip
            unzip -o -d dist/ dist/tmp.zip
            rm dist/tmp.zip
        done
        DOWNLOADED=true
    else
        echo -e "\n⚠️ "'`hub` failed. Run `hub api user` to setup authentication. https://hub.github.com/hub.1.html#github-oauth-authentication'
    fi
fi
if [ "$DOWNLOADED" = false ] ; then
    # build a list of direct-download browser links
    CHECK_SUITE_ID=$(jq -r .check_suite_url <<<"$RUN" | awk -F / '{print $NF}')
    echo "CI Check Suite: ${CHECK_SUITE_ID}"

    echo -e "\n⚠️  Please download the following URL in your browser; extract the ZIP; and put the ${PLATFORM_ARTIFACT}.tar.gz into $(pwd -P)/dist/"
    if [ "$PLATFORM" == "Darwin" ]; then
        echo "⚠️  On macOS Finder can be overly clever, use \`unzip {downloaded}.zip\` instead to get the ${PLATFORM_ARTIFACT}.tar.gz file."
    fi
    for ID in $ARTIFACT_IDS; do
        echo "https://github.com/${REPO}/suites/${CHECK_SUITE_ID}/artifacts/${ID}"
    done
fi
