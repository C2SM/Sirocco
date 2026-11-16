import pytest

from sirocco.core.namelistfile import NamelistFile

REPEATED = "&output_nml\n a = 1\n/\n&output_nml\n a = 2\n/\n&run_nml\n x = 1\n/\n"
SINGLE = "&output_nml\n a = 1\n/\n&run_nml\n x = 1\n/\n"
ABSENT = "&run_nml\n x = 1\n/\n"


def updated_values(tmp_path, content, specs, section="output_nml", key="a"):
    path = tmp_path / "test.nml"
    path.write_text(content)
    namelist = NamelistFile(path=path)
    namelist.update_from_specs(specs)
    # Go through a dump to check what ends up in the written namelist
    namelist.dump(path)
    return [occurrence[key] for occurrence in NamelistFile(path=path).iter_nml(section)]


@pytest.mark.parametrize(
    ("content", "specs", "expected"),
    [
        pytest.param(REPEATED, {"output_nml[1]": {"a": 10}}, [10, 2], id="repeated-first"),
        pytest.param(REPEATED, {"output_nml[2]": {"a": 20}}, [1, 20], id="repeated-second"),
        pytest.param(REPEATED, {"output_nml[3]": {"a": 30}}, [1, 2, 30], id="repeated-append"),
        pytest.param(REPEATED, {"output_nml": {"a": 99}}, [99, 99], id="repeated-all"),
        pytest.param(SINGLE, {"output_nml[1]": {"a": 10}}, [10], id="single-first"),
        pytest.param(SINGLE, {"output_nml[2]": {"a": 20}}, [1, 20], id="single-append"),
        pytest.param(SINGLE, {"output_nml": {"a": 99}}, [99], id="single-no-index"),
        pytest.param(ABSENT, {"output_nml[1]": {"a": 10}}, [10], id="absent-first"),
        pytest.param(ABSENT, {"output_nml": {"a": 99}}, [99], id="absent-no-index"),
        pytest.param(
            ABSENT,
            {"output_nml[1]": {"a": 10}, "output_nml[2]": {"a": 20}},
            [10, 20],
            id="absent-create-several",
        ),
        pytest.param(
            ABSENT,
            {"output_nml[2]": {"a": 20}, "output_nml[1]": {"a": 10}},
            [10, 20],
            id="absent-create-several-unordered",
        ),
        pytest.param(
            SINGLE,
            {"output_nml[3]": {"a": 30}, "output_nml[2]": {"a": 20}},
            [1, 20, 30],
            id="single-append-several-unordered",
        ),
        pytest.param(
            REPEATED,
            {"output_nml[3]": {"b": 3}, "output_nml[1]": {"a": 10}, "output_nml": {"a": 99}},
            [10, 99, 99],
            id="index-overrides-no-index-also-for-new",
        ),
    ],
)
def test_update_from_specs(tmp_path, content, specs, expected):
    assert updated_values(tmp_path, content, specs) == expected


def test_update_from_specs_keeps_other_entries(tmp_path):
    assert updated_values(tmp_path, SINGLE, {"output_nml[1]": {"b": 5}}) == [1]
    assert updated_values(tmp_path, SINGLE, {"output_nml[1]": {"b": 5}}, key="b") == [5]
    assert updated_values(tmp_path, SINGLE, {"output_nml[2]": {"a": 20}}, section="run_nml", key="x") == [1]


@pytest.mark.parametrize(
    ("content", "section"),
    [
        pytest.param(REPEATED, "output_nml[4]", id="repeated-gap"),
        pytest.param(SINGLE, "output_nml[3]", id="single-gap"),
        pytest.param(ABSENT, "output_nml[2]", id="absent-gap"),
        pytest.param(REPEATED, "output_nml[0]", id="zero-index"),
    ],
)
def test_update_from_specs_wrong_index(tmp_path, content, section):
    with pytest.raises(ValueError, match="index must be between 1 and"):
        updated_values(tmp_path, content, {section: {"a": 0}})
