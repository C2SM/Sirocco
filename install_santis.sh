# NOTE: relocatable option not available for `uv sync`
#       so we have to mount it where it was generated

CC=$(which gcc) \
CFLAGS="-I/user-environment/env/default/include -I/user-environment/env/default/include/graphviz" \
LDFLAGS="-L/user-environment/env/default/lib -L/user-environment/env/default/lib/graphviz" \
uv sync --python="$(which python)" --no-cache --link-mode=copy

python -m compileall -j 8 -o 0 -o 1 -o 2 .venv/lib/python3.12/site-packages

mksquashfs .venv .venv.squashfs -no-recovery -noappend -Xcompression-level 3
# mv .venv .venv.save
rm -rf .venv
mkdir .venv
