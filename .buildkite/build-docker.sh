#!/bin/bash
set -eu

echo "--- Pulling images from build cache"

CACHE_BUILD_STAGE="${ECR_CACHE}/${BUILDKITE_PIPELINE_SLUG}:${BUILDKITE_BRANCH}.build-stage"
CACHE_RUN_STAGE="${ECR_CACHE}/${BUILDKITE_PIPELINE_SLUG}:${BUILDKITE_BRANCH}.run-stage"

if [ -n "${NO_DOCKER_CACHE-}" ] || [[ "${BUILDKITE_MESSAGE,,}" =~ \[\s*ci\s+nocache\s*\] ]]; then
    echo "Skipping because NO_DOCKER_CACHE / [ci nocache] is set"
    CACHE_FROM=""
else
    CACHE_IMAGES=(
        "${CACHE_BUILD_STAGE}"
        "${CACHE_RUN_STAGE}"
    )

    if [ "$BUILDKITE_BRANCH" != "master" ]; then
        CACHE_IMAGES+=(
            "${ECR_CACHE}/${BUILDKITE_PIPELINE_SLUG}:master.build-stage"
            "${ECR_CACHE}/${BUILDKITE_PIPELINE_SLUG}:master.run-stage"
        )
    fi

    CACHE_FROM=""
    for IMG in "${CACHE_IMAGES[@]}"; do
        docker pull "${IMG}" || true
        CACHE_FROM="${CACHE_FROM} --cache-from=${IMG}"
    done
fi

echo "--- Building build-stage image"

BUILD_TAG="${BUILDKITE_PIPELINE_SLUG}-build:${BUILDKITE_JOB_ID}"
docker build \
    --pull \
    --target build-stage \
    $CACHE_FROM \
    --tag "${BUILD_TAG}" \
    .

docker tag "${BUILD_TAG}" "${CACHE_BUILD_STAGE}"
docker push "${CACHE_BUILD_STAGE}"

echo "--- Building run-stage image"

docker build \
    --pull \
    --target run-stage \
    --cache-from "${CACHE_FROM}" \
    $CACHE_FROM \
    --tag "${IMAGE}" \
    .

docker tag "${IMAGE}" "${CACHE_RUN_STAGE}"
docker push "${IMAGE}"
docker push "${CACHE_RUN_STAGE}"

buildkite-agent meta-data set "${IMAGE_METADATA}" "${IMAGE}"
