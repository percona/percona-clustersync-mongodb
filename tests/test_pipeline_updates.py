# pylint: disable=missing-docstring,redefined-outer-name
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
    src_keys = list(src_doc.keys())
    dst_keys = list(dst_doc.keys())
    if src_keys != dst_keys:
        only_src = set(src_doc) - set(dst_doc)
        only_dst = set(dst_doc) - set(src_doc)
        pytest.fail(
            f"Document key mismatch in {ns}: only_src={only_src}, only_dst={only_dst}, "
            f"src_order={src_keys}, dst_order={dst_keys}"
        )
    value_errors = []
    for k in src_doc:
        if not _bson_eq(src_doc[k], dst_doc[k]):
            value_errors.append(k)
    if not value_errors:
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
    src_doc = t.source[db][coll_name].find_one({"_id": 1})
    dst_doc = t.target[db][coll_name].find_one({"_id": 1})
    _assert_docs_equal(src_doc, dst_doc, f"{db}.{coll_name}")
    t.compare_all()
