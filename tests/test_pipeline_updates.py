# pylint: disable=missing-docstring,redefined-outer-name
import time

import pytest
from testing import Testing

from pcsm import Runner


def _build_pipeline_update_scenario(scenario_name):
    STAGE_LIMIT_ITEMS_LEN = 2000
    STAGE_LIMIT_SLICE_TO = 1000
    STAGE_LIMIT_EXTRAS = 3000
    BUFBUILDER_NUM_ELEMENTS = 300
    BUFBUILDER_NUM_MODIFY = 100
    BUFBUILDER_TRUNCATE_TO = 250
    BUFBUILDER_LARGE_VAL = "X" * 20_000
    BUFBUILDER_PAD_VAL = "P" * 10_000
    BUFBUILDER_NEW_VAL = "Y" * 20_000

    embedded = {"author": "test", "version": 1, "status": "active"}

    if scenario_name == "stage_limit":
        doc = {
            "_id": 1,
            "items": [f"old_val_{i}" for i in range(STAGE_LIMIT_ITEMS_LEN)],
            "items_count": STAGE_LIMIT_ITEMS_LEN,
            "metadata": "initial",
            "meta": embedded,
        }

        massive_set = {"items_count": STAGE_LIMIT_SLICE_TO, "meta.updated": True}

        for i in range(STAGE_LIMIT_EXTRAS):
            massive_set[f"extra_field_{i}"] = "value"
        update = [
            {"$set": {"items": {"$slice": ["$items", STAGE_LIMIT_SLICE_TO]}}},
            {"$set": massive_set},
        ]

        return {"doc": doc, "update": update}

    if scenario_name == "bufbuilder":
        doc = {
            "_id": 1,
            "meta": embedded,
            "arr": [
                {
                    "d": BUFBUILDER_LARGE_VAL,
                    "pad1": BUFBUILDER_PAD_VAL,
                    "pad2": BUFBUILDER_PAD_VAL,
                    "v": i + 1,
                }
                for i in range(BUFBUILDER_NUM_ELEMENTS)
            ],
        }

        update = [
            {
                "$set": {
                    "arr": {
                        "$slice": [
                            {
                                "$map": {
                                    "input": "$arr",
                                    "as": "el",
                                    "in": {
                                        "$cond": {
                                            "if": {"$lte": ["$$el.v", BUFBUILDER_NUM_MODIFY]},
                                            "then": {
                                                "$mergeObjects": [
                                                    "$$el",
                                                    {"d": BUFBUILDER_NEW_VAL},
                                                ]
                                            },
                                            "else": "$$el",
                                        }
                                    },
                                }
                            },
                            BUFBUILDER_TRUNCATE_TO,
                        ]
                    }
                }
            },
            {"$set": {"meta.updated": True}},
        ]

        return {"doc": doc, "update": update}

    if scenario_name == "slice_zero":
        num_elements = 20
        empty_idx = 10
        truncate_to = 15

        arr = []
        for i in range(num_elements):
            sub = [] if i == empty_idx else [i * 10, i * 10 + 1]
            arr.append({"items": sub, "n": f"e{i}", "v": i})

        doc = {"_id": 1, "meta": embedded, "arr": arr}

        update = [
            {"$set": {"arr": {"$slice": ["$arr", truncate_to]}}},
            {"$set": {f"arr.{empty_idx}.items.0": 99, "meta.updated": True}},
        ]

        return {"doc": doc, "update": update}

    raise ValueError(f"unknown scenario: {scenario_name}")


def _bson_eq(a, b):
    """Order-sensitive comparison (matches BSON/MongoDB field-order semantics)"""
    if type(a) is not type(b):
        return False

    if isinstance(a, dict):
        if list(a.keys()) != list(b.keys()):
            return False
        return all(_bson_eq(a[k], b[k]) for k in a)

    if isinstance(a, list):
        return len(a) == len(b) and all(_bson_eq(x, y) for x, y in zip(a, b, strict=False))

    return a == b


def _assert_docs_equal(src_doc, dst_doc, ns):
    """Field-by-field comparison with detailed diagnostics on mismatch"""
    assert src_doc is not None, f"Source document missing in {ns}"
    assert dst_doc is not None, f"Target document missing in {ns}"

    if set(src_doc.keys()) != set(dst_doc.keys()):
        only_src = set(src_doc) - set(dst_doc)
        only_dst = set(dst_doc) - set(src_doc)
        pytest.fail(f"Document key mismatch in {ns}: only_src={only_src}, only_dst={only_dst}")

    value_errors = []

    for k in src_doc:
        if not _bson_eq(src_doc[k], dst_doc[k]):
            value_errors.append(k)

    if not value_errors:
        if list(src_doc.keys()) != list(dst_doc.keys()):
            print(f"  WARNING: {ns}: top-level field order differs (data OK)")
        return

    lines = [f"Document mismatch in {ns}, fields: {value_errors[:10]}"]
    for k in value_errors[:5]:
        sv, dv = src_doc[k], dst_doc[k]

        if isinstance(sv, list) and isinstance(dv, list) and len(sv) == len(dv):
            diff_idx = [i for i in range(len(sv)) if not _bson_eq(sv[i], dv[i])]
            lines.append(
                f"  key '{k}': {len(sv)} elems, {len(diff_idx)} differ"
                f" at indices {diff_idx[:10]}{'...' if len(diff_idx) > 10 else ''}"
            )

            for i in diff_idx[:3]:
                lines.append(f"    [{i}] src={repr(sv[i])[:200]}")
                lines.append(f"    [{i}] dst={repr(dv[i])[:200]}")
        else:
            lines.append(f"  key '{k}': src={repr(sv)[:200]}")
            lines.append(f"  key '{k}': dst={repr(dv)[:200]}")

    pytest.fail("\n".join(lines))


def _wait_for_target_doc(target, db, coll_name, doc_id, timeout=30):
    """Poll target until the document appears or timeout.

    On some MongoDB versions (notably 6.0 sharded clusters), the optime-based
    synchronization barrier in wait_for_current_optime() may return before the
    change stream events have been fully flushed to the target. This polling
    loop provides a reliable data-level verification.
    """
    for _ in range(timeout * 2):
        doc = target[db][coll_name].find_one({"_id": doc_id})

        if doc is not None:
            return doc

        time.sleep(0.5)

    return None


@pytest.mark.timeout(120)
@pytest.mark.parametrize("scenario_name", ["stage_limit", "bufbuilder", "slice_zero"])
def test_pipeline_update_regression(t: Testing, scenario_name: str):
    """Pipeline update regression: stage-limit, bufbuilder overflow, and $slice-zero"""
    db = "pipeline_test_db"
    coll_name = f"pipeline_{scenario_name}"
    scenario = _build_pipeline_update_scenario(scenario_name)
    doc = scenario["doc"]
    update = scenario["update"]

    with t.run(Runner.Phase.APPLY, wait_timeout=60):
        t.source[db][coll_name].insert_one(doc)
        t.source[db][coll_name].update_one({"_id": 1}, update)
        # Poll target BEFORE finalization to ensure the change stream has delivered
        # the events. On MongoDB 6.0 sharded clusters, the optime-based barrier in
        # wait_for_current_optime() may return before events are fully flushed.
        _wait_for_target_doc(t.target, db, coll_name, 1)

    src_doc = t.source[db][coll_name].find_one({"_id": 1})
    dst_doc = t.target[db][coll_name].find_one({"_id": 1})
    _assert_docs_equal(src_doc, dst_doc, f"{db}.{coll_name}")


def _build_dotted_field_name_scenario(scenario_name):
    """Pipeline updates of fields whose own name contains a dot (PCSM-139)"""
    # Padding keeps each update a field-level diff in the oplog; a small document
    # would be logged as a full replacement and never reach the update path.
    pad = "P" * 4000

    if scenario_name == "key_in_document":
        doc = {"_id": 1, "pad": pad, "files": {"main.ts": "v1", "b.ts": "b", "plain": "p"}}
        updates = [
            [{"$set": {"files": {"$setField": {"field": name, "input": "$files", "value": value}}}}]
            for name, value in [("main.ts", "v2"), ("new.ts", "n")]
        ]
        updates.append([{"$set": {"files": {"$unsetField": {"field": "b.ts", "input": "$files"}}}}])

        return {"doc": doc, "updates": updates}

    if scenario_name == "top_level_field":
        doc = {"_id": 1, "pad": pad, "a.b": 1, "c.d": 2}
        updates = [
            [{"$replaceWith": {"$setField": {"field": "a.b", "input": "$$ROOT", "value": 10}}}],
            [{"$replaceWith": {"$unsetField": {"field": "c.d", "input": "$$ROOT"}}}],
        ]

        return {"doc": doc, "updates": updates}

    if scenario_name == "url_key_in_array_element":
        messages = [{"captions": {"https://x.example/a.png": "a", "pad": pad}}, {"role": "user"}]
        doc = {"_id": 1, "messages": messages}
        changed = [{"captions": {"https://x.example/a.png": "a2", "pad": pad}}, {"role": "user"}]
        updates = [[{"$set": {"messages": {"$literal": changed}}}]]

        return {"doc": doc, "updates": updates}

    if scenario_name == "same_path_twice":
        # files["main.ts"] and files.main.ts are both reported as "files.main.ts"
        files = {"main.ts": "dotted", "main": {"ts": "nested", "pad": pad}}
        doc = {"_id": 1, "pad": pad, "files": files}
        changed = {"main.ts": "dotted2", "main": {"ts": "nested2", "pad": pad}}
        updates = [[{"$set": {"files": {"$literal": changed}}}]]

        return {"doc": doc, "updates": updates}

    if scenario_name == "truncated_array":
        doc = {"_id": 1, "pad": pad, "arrs": {"x.y": [1, 2, 3, 4], "pad": pad}}
        updates = [[{"$set": {"arrs": {"$literal": {"x.y": [1, 9], "pad": pad}}}}]]

        return {"doc": doc, "updates": updates}

    if scenario_name == "values_of_dotted_keys_emptied":
        functions = {
            name: {"files": {"main.ts": f"code of {name}", "README": "r"}, "pad": pad}
            for name in ["hello", "send.email", "api.v2.users"]
        }
        doc = {"_id": 1, "functions": functions}
        empty_files = {
            "$arrayToObject": {
                "$map": {
                    "input": {"$objectToArray": "$functions"},
                    "as": "e",
                    "in": {"k": "$$e.k", "v": {"$mergeObjects": ["$$e.v", {"files": {}}]}},
                }
            }
        }
        updates = [[{"$set": {"functions": empty_files}}]]

        return {"doc": doc, "updates": updates}

    raise ValueError(f"unknown scenario: {scenario_name}")


@pytest.mark.timeout(120)
@pytest.mark.parametrize(
    "scenario_name",
    [
        "key_in_document",
        "top_level_field",
        "url_key_in_array_element",
        "same_path_twice",
        "truncated_array",
        "values_of_dotted_keys_emptied",
    ],
)
def test_pipeline_update_dotted_field_names(t: Testing, scenario_name: str):
    """PCSM-139: a field name with a dot must not be replayed as a nested path"""
    if t.source.admin.command("buildInfo")["versionArray"][:2] < [6, 1]:
        pytest.skip("the source reports disambiguatedPaths only from MongoDB 6.1")

    db = "pipeline_test_db"
    coll_name = f"dotted_{scenario_name}"
    scenario = _build_dotted_field_name_scenario(scenario_name)

    with t.run(Runner.Phase.APPLY, wait_timeout=60):
        t.source[db][coll_name].insert_one(scenario["doc"])
        for update in scenario["updates"]:
            t.source[db][coll_name].update_one({"_id": 1}, update)
        _wait_for_target_doc(t.target, db, coll_name, 1)

    src_doc = t.source[db][coll_name].find_one({"_id": 1})
    dst_doc = t.target[db][coll_name].find_one({"_id": 1})
    _assert_docs_equal(src_doc, dst_doc, f"{db}.{coll_name}")
