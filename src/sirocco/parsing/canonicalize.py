import functools
from typing import Any

from sirocco.parsing import _yaml_data_models as models
from sirocco.parsing import config


def str_to_paramref(value: str) -> config.ParamRef:
    match value:
        case "single":
            return config.ParamRef.SINGLE
        case "all":
            return config.ParamRef.ALL
        case _:
            raise ValueError


@functools.singledispatch
def canonicalize(self, obj: models.BaseModel) -> Any:
    raise NotImplementedError


@canonicalize.register
def canonicalize_when(obj: models._WhenBaseModel) -> config.When:
    match obj:
        case models._WhenBaseModel(before=None, after=None, at=at):
            return config.AtSpec(at=at)
        case models._WhenBaseModel(at=None, before=before, after=after):
            return config.BeforeAfterSpec(before=before, after=after)
        case None:
            return config.Always()
        case _:
            raise ValueError


@canonicalize.register
def canonicalize_input(obj: models.ConfigCycleTaskInput) -> config.CycleTaskInput:
    when = canonicalize_when(obj.when)
    parameters = {k: str_to_paramref(v) for k, v in obj.parameters.items()}
    return config.CycleTaskInput(name=obj.name, date=obj.date, lag=obj.lag, when=when, parameters=parameters)


@canonicalize.register
def canonicalize_wait_on(obj: models.ConfigCycleTaskWaitOn) -> config.CycleTaskWaitOn:
    when = canonicalize_when(obj.when)
    parameters = {k: str_to_paramref(v) for k, v in obj.parameters.items()}
    return config.CycleTaskWaitOn(name=obj.name, date=obj.date, lag=obj.lag, when=when, parameters=parameters)


@canonicalize.register
def canonicalize_output(obj: models.ConfigCycleTaskOutput) -> config.CycleTaskOutput:
    return config.CycleTaskOutput(name=obj.name)


@canonicalize.register
def canonicalize_cycle_task(obj: models.ConfigCycleTask) -> config.CycleTask:
    return config.CycleTask(
        inputs=[canonicalize(i) for i in obj.inputs],
        outputs=[canonicalize(o) for o in obj.outputs],
        wait_on=[canonicalize(w) for w in obj.wait_on],
    )


@canonicalize.register
def canonicalize_cycle(
    obj: models.ConfigCycle,
) -> config.DatedCycle | config.UndatedCycle:
    tasks = canonicalize(obj.tasks)
    if obj.start_date is obj.end_date is obj.period is None:
        return config.UndatedCycle(name=obj.name, tasks=tasks)
    return config.DatedCycle(
        name=obj.name,
        tasks=tasks,
        start_date=obj.start_date,
        end_date=obj.end_date,
        period=obj.period,
    )


@canonicalize.register
def canonicalize_cli_args(
    obj: models._CliArgsBaseModel,
) -> config.CliSignature:
    # TODO (ricoh): the ordering information should really come from the yaml file already.
    # It is crucial information to reliably format the cli call properly.
    signature: config.CliSignature = []
    if obj.flags:
        signature.extend(config.CliFlag(name=f) for f in obj.flags)
    if obj.keyword:
        signature.extend(config.CliOption(name=o, value=v) for o, v in obj.keyword.items())
    if obj.positional:
        if isinstance(obj.positional, list):
            signature.extend(config.CliArg(value=a) for a in obj.positional)
        else:
            signature.append(config.CliArg(value=obj.positional))
    if obj.source_file:
        if isinstance(obj.source_file, list):
            signature.extend(config.CliSourceFile(name=s) for s in obj.source_file)
        else:
            signature.append(config.CliSourceFile(name=obj.source_file))
    return signature


@canonicalize.register
def canonicalize_root_task(obj: models.ConfigRootTask) -> config.RootTask:
    return config.RootTask(
        host=obj.host,
        account=obj.account,
        uenv=obj.uenv,
        nodes=obj.nodes,
        walltime=obj.walltime,
    )


@canonicalize.register
def canonicalize_shell_task(obj: models.ConfigShellTask) -> config.ShellTask:
    return config.ShellTask(
        host=obj.host,
        account=obj.account,
        uenv=obj.uenv,
        nodes=obj.nodes,
        walltime=obj.walltime,
        command=obj.command,
        cli_arguments=canonicalize(obj.cli_arguments),
        src=obj.src,
    )


@canonicalize.register
def canonicalize_icon_task(obj: models.ConfigIconTask) -> config.IconTask:
    return config.IconTask(
        host=obj.host,
        account=obj.account,
        uenv=obj.uenv,
        nodes=obj.nodes,
        walltime=obj.walltime,
        namelists=obj.namelists,
    )
