from sirocco.core.tasks.icon_task.ports import IconPort
from sirocco.engines.standalone.tasks.icon_ports import special_ports


def test_special_ports_implemented():
    """Every special ICON port declared in core is implemented by the standalone engine"""
    declared = {name for name, port in IconPort.registered_ports.items() if port.is_special}
    assert declared == set(special_ports)
