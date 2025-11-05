#!/usr/bin/bash -l

if [ $# != 0 ]; then
    VENV="${1}/.venv"
    VENV_SQUASHFS="${1}/venv.squashfs"
    rm -rf ${VENV}
    mkdir -p ${VENV}
    ln -s ${VENV} .venv
else
    VENV=.venv
    VENV_SQUASHFS=".venv.squashfs"
fi

uv venv --relocatable --python="$(which python)" --prompt="༄ sirocco ༄"
source .venv/bin/activate
CC=$(which gcc) \
CFLAGS="-I/user-environment/env/default/include -I/user-environment/env/default/include/graphviz" \
LDFLAGS="-L/user-environment/env/default/lib -L/user-environment/env/default/lib/graphviz" \
uv sync --no-cache --link-mode=copy --compile-bytecode --active --no-editable --inexact || exit

mksquashfs ${VENV} ${VENV_SQUASHFS} -no-recovery -noappend -Xcompression-level 3 || exit

rm -rf ${VENV}

if [ -n "${1}" ]; then
    rsync -av ${VENV_SQUASHFS} .
    rm -rf ${1}
fi

