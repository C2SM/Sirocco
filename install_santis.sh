#!/usr/bin/bash -l

build_dir="/dev/shm/$USER/sirocco_install"
squash_name="sirocco_venv.squashfs"

mkdir -p ${build_dir}/.venv
uv venv --clear --relocatable --python="$(which python)" --prompt="༄ sirocco ༄" ${build_dir}/.venv
source ${build_dir}/.venv/bin/activate
uv sync --no-cache --link-mode=copy --compile-bytecode --active --no-editable --inexact || exit

mksquashfs ${build_dir}/.venv ./"${squash_name}" -no-recovery -noappend -Xcompression-level 3 || exit

rm -rf ${build_dir}
