#!/usr/bin/bash -l

BUILD_DIR="${BUILD_DIR:-/dev/shm/$USER/sirocco_install}"
squash_name="sirocco_venv.squashfs"

pushd "${BUILD_DIR}" >/dev/null 2>&1

uv venv --relocatable --python="$(which python)" --prompt="༄ sirocco ༄"
source .venv/bin/activate
uv sync --no-cache --link-mode=copy --compile-bytecode --active --no-editable --inexact || exit

mksquashfs .venv "${squash_name}" -no-recovery -noappend -Xcompression-level 3 || exit

popd >/dev/null 2>&1

rsync -av "${BUILD_DIR}/${squash_name}" .
