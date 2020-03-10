#!/bin/bash
set -e

# Helper script to download vendor-* archives from Github CI.

if ! command -v jq >/dev/null; then
    echo "❗️This script requires 'jq' (and ideally 'hub') installed"
    exit 2
fi

REPO=koordinates/sno
WORKFLOW=build.yml

CURRENT_BRANCH=$(git branch --show-current)
BRANCH=${1:-${CURRENT_BRANCH:-master}}
echo "Branch: ${BRANCH}"

echo "Finding most recent CI Run for '${BRANCH}'..."
RUN_SET=$(curl -sS "https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?branch=${BRANCH}&status=success&event=push")
RUN=$(jq .workflow_runs[0] <<<"$RUN_SET")
if [ -z "$RUN" ] || [ "$RUN" = "null" ]; then
    echo "❗️Couldn't find a recent successful CI build for '$BRANCH'"
    if [ "$BRANCH" != "master" ]; then
        echo "Try with '$0 master'?"
    fi
    exit 1
fi
RUN_ID=$(jq -r .id <<< "$RUN")
echo "CI Run: ${RUN_ID}"

echo "Finding vendor-* artifacts from CI Run..."
ARTIFACT_SET=$(curl -sS "https://api.github.com/repos/${REPO}/actions/runs/${RUN_ID}/artifacts")
ARTIFACT_URLS=$(jq -r '.artifacts | map(select(any(.name; startswith("vendor-")))|.archive_download_url)[]' <<<"$ARTIFACT_SET")
ARTIFACT_IDS=$(jq -r '.artifacts | map(select(any(.name; startswith("vendor-")))|.id)[]' <<<"$ARTIFACT_SET")
if [ -z "$ARTIFACT_IDS" ]; then
    echo "❗️Couldn't find any vendor-* artifacts. Check $(jq -r .html_url <<< "$RUN") ?"
    exit 1
fi

#
# Github requires users to be logged in to access the artifacts API, even
# for public projects. ¯\_(ツ)_/¯
#

cd "$(dirname "$0")"
mkdir -p dist/
if command -v hub >/dev/null; then
    # if 'hub' is installed, use its authenticated access to the API
    echo "Using 'hub' to download artifacts via the Github API..."
    for URL in $ARTIFACT_URLS; do
        echo "Downloading... $URL"
        hub api "$URL" > dist/tmp.zip
        unzip -d dist/ dist/tmp.zip
        rm dist/tmp.zip
    done
else
    # build a list of direct-download browser links
    CHECK_SUITE_ID=$(jq -r .check_suite_url <<<"$RUN" | awk -F / '{print $NF}')
    echo "CI Check Suite: ${CHECK_SUITE_ID}"

    echo -e "\n⚠️  Please download the following URLs in your browser; extract the ZIPs; and put the vendor-*.tar.gz into $(pwd -P)/dist/"
    for ID in $ARTIFACT_IDS; do
        echo "https://github.com/${REPO}/suites/${CHECK_SUITE_ID}/artifacts/${ID}"
    done
fi
