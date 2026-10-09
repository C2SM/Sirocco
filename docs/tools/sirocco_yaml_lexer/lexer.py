from pygments.lexers.data import YamlLexer
from pygments.token import Name

class SiroccoYamlLexer(YamlLexer):
    name = 'Sirocco-YAML'
    aliases = ['sirocco-yaml']
    filenames = ['sirocco.yaml']

    SPECIAL_KEYS = {
      "scheduler",
      "front_depth",
      "cycles",
      "tasks",
      "data",
      # cycles
      "cycling",
      "start_date",
      "stop_date",
      "period",
      "tasks",
      "components"
      "master",
      "inputs",
      "target_cycle",
      "lag",
      "date",
      "when",
      "at",
      "before",
      "after",
      "outputs",
      "wait_on",
      "data",
      "parameters",
      # tasks
      "ROOT",
      "SIROCCO",
      "plugin",
      "computer",
      "account",
      "partition",
      "walltime",
      "nodes",
      "sockets_per_nodes",
      "gpus-per-node",
      "gres",
      "gres_flags",
      "procs_per_node",
      "cores_per_proc",
      "uenv",
      "view",
      # shell plugin
      "src",
      "command",
      # icon plugin
      "namelists",
      "yac_coupling",
      "exe",
      "path",
      "procs",
      "cpu",
      "gpu",
      "hiopy",
      "procs_per_io_node",
      "separate_io",
      "icon4py_venv",
      "gt4py_build_cache_dir",
      "compute_procs_per_node",
      "compute_weight",
      "prefetch",
      "restart",
      "streams",
      "runtime",
      # Hiopy
      "venv",
      "procs",
      # data
      "available",
      "generated",
      "path"
    }

    def get_tokens_unprocessed(self, text=None, *args, **kwargs):
        parent_tokens = super().get_tokens_unprocessed(text, *args, **kwargs)
        for index, token, value in parent_tokens:
            if token is Name.Tag and value.strip() in self.SPECIAL_KEYS:
                yield index, Name.Function, value
            else:
                yield index, token, value
