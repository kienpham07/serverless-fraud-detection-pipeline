#!/usr/bin/env bash
# ==============================================================================
# Script: build_layer.sh
# Description: Packages Python ML dependencies (Scikit-Learn, NumPy, Joblib, Pandas)
#              into an AWS Lambda Layer zip compatible with Python 3.11 x86_64.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LAYERS_DIR="${PROJECT_ROOT}/terraform/layers"
LAYER_ZIP="${LAYERS_DIR}/sklearn-layer.zip"
BUILD_DIR="${PROJECT_ROOT}/build/layer"

DOCKER_IMAGE="public.ecr.aws/sam/build-python3.11"

echo "============================================================"
echo "          BUILDING AWS LAMBDA SCIKIT-LEARN LAYER            "
echo "============================================================"
echo "Project Root  : ${PROJECT_ROOT}"
echo "Build Dir     : ${BUILD_DIR}"
echo "Destination   : ${LAYER_ZIP}"
echo "Docker Image  : ${DOCKER_IMAGE}"
echo "------------------------------------------------------------"

# Ensure output directory exists
mkdir -p "${LAYERS_DIR}"
mkdir -p "${BUILD_DIR}"

# Clean previous build artifacts
rm -rf "${BUILD_DIR:?}"/*
rm -f "${LAYER_ZIP}"

# Check Docker availability
if ! command -v docker &> /dev/null; then
    echo "[-] Error: docker is not installed or not in PATH." >&2
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "[-] Error: docker daemon is not accessible. Please start Docker service." >&2
    exit 1
fi

echo "[+] Pulling build container image (${DOCKER_IMAGE})..."
docker pull "${DOCKER_IMAGE}"

echo "[+] Installing ML dependencies inside Lambda-compatible container..."
# Build directory structure: python/lib/python3.11/site-packages
TARGET_SITE_PACKAGES="/var/task/python/lib/python3.11/site-packages"

docker run --rm \
    -v "${BUILD_DIR}:/var/task" \
    "${DOCKER_IMAGE}" \
    bash -c "
        set -euo pipefail
        echo '[Container] Upgrading pip...'
        python3.11 -m pip install --upgrade pip

        echo '[Container] Installing locked ML libraries...'
        python3.11 -m pip install \
            --no-cache-dir \
            --target '${TARGET_SITE_PACKAGES}' \
            scikit-learn==1.9.0 \
            numpy==2.5.2 \
            joblib==1.5.3 \
            pandas==3.0.5

        echo '[Container] Stripping unnecessary files to optimize package size...'
        find '${TARGET_SITE_PACKAGES}' -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
        find '${TARGET_SITE_PACKAGES}' -type d -name 'tests' -exec rm -rf {} + 2>/dev/null || true
        find '${TARGET_SITE_PACKAGES}' -type d -name 'test' -exec rm -rf {} + 2>/dev/null || true
        find '${TARGET_SITE_PACKAGES}' -type f -name '*.pyc' -delete || true
        find '${TARGET_SITE_PACKAGES}' -type f -name '*.pyo' -delete || true
        find '${TARGET_SITE_PACKAGES}' -type f -name '*.dist-info' -exec rm -rf {} + 2>/dev/null || true
    "

echo "[+] Packaging layer into zip archive..."
cd "${BUILD_DIR}"
zip -r -q -9 "${LAYER_ZIP}" python/

ZIP_SIZE=$(du -h "${LAYER_ZIP}" | cut -f1)
ZIP_BYTES=$(stat -c%s "${LAYER_ZIP}" 2>/dev/null || stat -f%z "${LAYER_ZIP}")

echo "------------------------------------------------------------"
echo "[✔] Layer package created successfully!"
echo "    File : ${LAYER_ZIP}"
echo "    Size : ${ZIP_SIZE} (${ZIP_BYTES} bytes)"
echo "============================================================"
