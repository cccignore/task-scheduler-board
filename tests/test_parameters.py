from tests.helpers import (
    claim_task,
    create_group,
    create_task,
    get_task,
    start_task,
    update_group,
)
from app.services import resolve_parameter_chain


def _resolved(task: dict) -> list:
    return [step["resolved_parameters"] for step in task["steps"]]


def test_pure_chain_returns_independent_snapshots_and_keeps_json_falsy_values():
    snapshots = resolve_parameter_chain(
        {"value": "base", "zero": 9, "flag": True, "nullable": "base"},
        {"value": "group", "literal_blank": ""},
        [
            {"value": "step", "zero": 0, "flag": False, "nullable": None},
            {"value": "", "brand_new_blank": ""},
        ],
    )

    assert snapshots == [
        {
            "value": "step",
            "zero": 0,
            "flag": False,
            "nullable": None,
            "literal_blank": "",
        },
        {
            "value": "step",
            "zero": 0,
            "flag": False,
            "nullable": None,
            "literal_blank": "",
        },
    ]
    assert snapshots[0] is not snapshots[1]
    snapshots[1]["value"] = "mutated-test-copy"
    assert snapshots[0]["value"] == "step"
    assert "brand_new_blank" not in snapshots[0]


def test_only_exact_empty_string_skips_and_nested_snapshots_are_deep_copies():
    snapshots = resolve_parameter_chain(
        {
            "object": {"old": True},
            "array": ["old"],
            "space": "old",
        },
        {},
        [
            {"object": {}, "array": [], "space": " "},
            {
                "object": {"nested": {"value": 1}},
                "array": [{"value": 1}],
            },
            {},
        ],
    )

    assert snapshots[0] == {"object": {}, "array": [], "space": " "}
    assert snapshots[1] == snapshots[2] == {
        "object": {"nested": {"value": 1}},
        "array": [{"value": 1}],
        "space": " ",
    }

    snapshots[2]["object"]["nested"]["value"] = 2
    snapshots[2]["array"][0]["value"] = 2
    assert snapshots[1]["object"]["nested"]["value"] == 1
    assert snapshots[1]["array"][0]["value"] == 1


def test_complete_parameter_merge_boundary_matrix(client):
    group = create_group(
        client,
        "customers-east",
        {
            "sender": "group",
            "blank": "",  # L2 empty string is a literal value.
            "group_only": "G",
            "count": 0,
            "flag": False,
            "nullable": None,
            "nested": {"layer": "group"},
        },
    )
    task = create_task(
        client,
        "parameter-matrix",
        [
            {
                "name": "first",
                "overrides": {
                    "sender": "step-1",
                    "new_key": "new-1",
                    "count": 0,
                    "flag": False,
                    "nullable": None,
                },
            },
            {
                "name": "empty-means-keep-current",
                "overrides": {
                    "sender": "",
                    "new_key": "",
                    "group_only": "",
                    "base_only": "",
                },
            },
            {
                "name": "later-overrides-win",
                "overrides": {
                    "sender": "step-3",
                    "new_key": "new-3",
                    "count": 5,
                    "nested": {"layer": "step"},
                },
            },
            {"name": "sticky-with-no-override", "overrides": {}},
            {
                "name": "empty-new-key-does-not-create-it",
                "overrides": {"sender": "", "never_seen": ""},
            },
        ],
        base_parameters={
            "sender": "base",
            "base_only": "B",
            "count": 7,
            "flag": True,
            "nullable": "from-base",
            "nested": {"layer": "base", "discarded": True},
        },
        group_id=group["id"],
    )

    claimed = claim_task(client, "parameter-worker")
    assert claimed["task"]["id"] == task["id"]
    start_task(client, task["id"], "parameter-worker", claimed["claim_token"])
    resolved = _resolved(get_task(client, task["id"]))

    initial = {
        "sender": "group",
        "base_only": "B",
        "blank": "",
        "group_only": "G",
        "count": 0,
        "flag": False,
        "nullable": None,
        "nested": {"layer": "group"},
    }
    step_1 = {
        **initial,
        "sender": "step-1",
        "new_key": "new-1",
    }
    step_3 = {
        **step_1,
        "sender": "step-3",
        "new_key": "new-3",
        "count": 5,
        "nested": {"layer": "step"},
    }

    assert resolved == [step_1, step_1, step_3, step_3, step_3]
    assert "never_seen" not in resolved[-1]


def test_no_group_and_empty_dictionaries(client):
    task = create_task(
        client,
        "no-group",
        [
            {"name": "empty", "overrides": {"kept": ""}},
            {"name": "new-key", "overrides": {"added": "yes"}},
        ],
        base_parameters={"kept": "base"},
    )
    claimed = claim_task(client, "solo")
    start_task(client, task["id"], "solo", claimed["claim_token"])

    assert _resolved(get_task(client, task["id"])) == [
        {"kept": "base"},
        {"kept": "base", "added": "yes"},
    ]


def test_group_override_is_snapshotted_exactly_at_start(client):
    group = create_group(client, "snapshot-group", {"value": "at-create"})
    task = create_task(
        client,
        "snapshot-task",
        [
            {"name": "one", "overrides": {}},
            {"name": "two", "overrides": {}},
        ],
        base_parameters={"value": "base", "base_only": True},
        group_id=group["id"],
    )

    update_group(
        client,
        group["id"],
        overrides={"value": "at-start", "before_start": "visible"},
    )
    claimed = claim_task(client, "snapshot-worker")
    claim_token = claimed["claim_token"]
    start_task(client, task["id"], "snapshot-worker", claim_token)
    update_group(
        client,
        group["id"],
        overrides={"value": "after-start", "after_start": "must-not-leak"},
    )
    # An owning worker may retry start; that retry must reuse the stored
    # snapshot instead of reading the now-modified group.
    start_task(client, task["id"], "snapshot-worker", claim_token)

    expected = {
        "value": "at-start",
        "base_only": True,
        "before_start": "visible",
    }
    resolved = _resolved(get_task(client, task["id"]))
    assert resolved == [expected, expected]
    assert all("after_start" not in parameters for parameters in resolved)
