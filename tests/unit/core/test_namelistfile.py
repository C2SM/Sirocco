import pytest

from sirocco.core.namelistfile import NamelistFile

REPEATED = "&output_nml\n a = 1\n/\n&output_nml\n a = 2\n/\n"
SINGLE = "&output_nml\n a = 1\n/\n"
ABSENT = "&run_nml\n x = 1\n/\n"


def updated_values(tmp_path, content, specs):
    path = tmp_path / "test.nml"
    path.write_text(content)
    namelist = NamelistFile(path=path)
    namelist.update_from_specs(specs)
    # Go through a dump to check what ends up in the written namelist
    namelist.dump(path)
    return [occurrence["a"] for occurrence in NamelistFile(path=path).iter_nml("output_nml")]


@pytest.mark.parametrize(
    ("content", "specs", "expected"),
    [
        pytest.param(SINGLE, {"output_nml[1]": {"a": 10}}, [10], id="index-on-single-section"),
        pytest.param(ABSENT, {"output_nml[2]": {"a": 20}, "output_nml[1]": {"a": 10}}, [10, 20], id="append-unordered"),
        pytest.param(
            REPEATED,
            {"output_nml[3]": {"b": 3}, "output_nml[1]": {"a": 10}, "output_nml": {"a": 99}},
            [10, 99, 99],
            id="no-index-updates-all-and-index-takes-precedence",
        ),
    ],
)
def test_update_from_specs(tmp_path, content, specs, expected):
    assert updated_values(tmp_path, content, specs) == expected


@pytest.mark.parametrize("section", ["output_nml[0]", "output_nml[4]"])
def test_update_from_specs_wrong_index(tmp_path, section):
    with pytest.raises(ValueError, match="index must be between 1 and 3"):
        updated_values(tmp_path, REPEATED, {section: {"a": 0}})
