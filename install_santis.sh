CC=$(which gcc) \
CFLAGS="-I/user-environment/env/default/include -I/user-environment/env/default/include/graphviz" \
LDFLAGS="-L/user-environment/env/default/lib -L/user-environment/env/default/lib/graphviz" \
uv sync --python="$(which python)" --no-cache --link-mode=copy
