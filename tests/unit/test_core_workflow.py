import datetime

from sirocco.core import workflow
from sirocco.parsing import _yaml_data_models as models


def test_workflow_cycle_dates_undated():
    config_cycle = models.ConfigCycle(undated={"tasks": []})
    assert list(workflow.Workflow.cycle_dates(config_cycle)) == [None]


def test_workflow_cycle_dates_no_period():
    start_isodate = "2000-01-01T00:00:00"
    config_cycle = models.ConfigCycle(
        dated={
            "tasks": [],
            "start_date": start_isodate,
            "end_date": "2000-02-01T00:00:00",
            "period": None,
        }
    )
    assert list(workflow.Workflow.cycle_dates(config_cycle)) == [
        datetime.datetime.fromisoformat(start_isodate)
    ]
    # TODO (ricoh): in this case 'end_date' is required for ConfigCycle but is it ever referenced?


def test_workflow_cycle_dates():
    start_isodate = "2000-01-01T00:00:00"
    mid_isodate = "2000-02-01T00:00:00"
    last_isodate = "2000-03-01T00:00:00"
    config_cycle = models.ConfigCycle(
        with_period={
            "tasks": [],
            "start_date": start_isodate,
            "end_date": "2000-03-02T00:00:00",  # end date is exclusive
            "period": "P1M",
        }
    )
    print(config_cycle)
    assert list(workflow.Workflow.cycle_dates(config_cycle)) == [
        datetime.datetime.fromisoformat(i)
        for i in [start_isodate, mid_isodate, last_isodate]
    ]
