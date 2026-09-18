# pylint: disable=missing-docstring,redefined-outer-name
import time

import pytest
from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo import MongoClient
from testing import Testing

from pcsm import Runner

HASH_MIN = -(2**63)
HASH_SPAN = 2**64


def shard_count(client: MongoClient) -> int:
    return len(client.admin.command("listShards")["shards"])


def sorted_shards(client: MongoClient) -> list[str]:
    return sorted(s["_id"] for s in client.admin.command("listShards")["shards"])


def target_chunks(client: MongoClient, ns: str) -> list[dict]:
    """Chunks for ns on the cluster, ordered by min, read via config.chunks (uuid-keyed)."""
    coll = client["config"]["collections"].find_one({"_id": ns})
    assert coll is not None, f"{ns} not found in config.collections"
    uuid = coll["uuid"]
    return list(client["config"]["chunks"].find({"uuid": uuid}).sort("min", 1))


def _hash_bound(value) -> int:
    """Map a chunk boundary's _id to its int64 hash value (MinKey/MaxKey -> ends)."""
    if isinstance(value, MinKey):
        return HASH_MIN
    if isinstance(value, MaxKey):
        return HASH_MIN + HASH_SPAN  # exclusive upper end
    return int(value)


def shard_hash_widths(chunks: list[dict]) -> dict[str, int]:
    """Total hash-space width owned by each shard across the given chunks."""
    widths: dict[str, int] = {}
    for c in chunks:
        lo = _hash_bound(c["min"]["_id"])
        hi = _hash_bound(c["max"]["_id"])
        widths[c["shard"]] = widths.get(c["shard"], 0) + (hi - lo)
    return widths


def test_hashed_native_layout_is_balanced(t: Testing):
    """Hashed collections retain shardCollection's balanced native layout."""
    ns = "db_1.coll_1"

    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", ns, key={"_id": "hashed"})
    t.source["db_1"]["coll_1"].insert_many([{"_id": i} for i in range(50)])

    with t.run(phase=Runner.Phase.MANUAL) as r:
        r.start()
        r.wait_for_clone_completed()

        # Inspect the target layout immediately after clone, before finalize
        # (finalize can wake the balancer/auto-merger and reshape chunks).
        n = shard_count(t.target)
        chunks = target_chunks(t.target, ns)

        assert len(chunks) >= n, f"chunk count {len(chunks)} < shard count {n}"
        assert len(chunks) % n == 0, f"chunk count {len(chunks)} not a multiple of {n}"

        per_shard: dict[str, int] = {}
        for c in chunks:
            per_shard[c["shard"]] = per_shard.get(c["shard"], 0) + 1
        assert set(per_shard.values()) == {len(chunks) // n}, f"uneven chunk ownership: {per_shard}"

        # Data volume is proportional to hash-space width.
        width_per_shard = shard_hash_widths(chunks)
        total = sum(width_per_shard.values())
        ideal = total / n
        for shard, w in width_per_shard.items():
            assert abs(w - ideal) / ideal < 0.05, (
                f"shard {shard} owns {w / total:.1%} of hash space (ideal {1 / n:.1%})"
            )

    t.compare_all_sharded()


def _ownership_by_bounds(chunks: list[dict]) -> dict[tuple, str]:
    return {(repr(c["min"]["_id"]), repr(c["max"]["_id"])): c["shard"] for c in chunks}


def _setup_ranged_layout(t: Testing, ns: str, src_shards: list[str]):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", ns, key={"_id": 1})

    # Pin the manual layout below so it is stable when PCSM reads it.
    t.source["config"]["collections"].update_one({"_id": ns}, {"$set": {"noBalance": True}})

    # Split into 4 chunks, then move one range onto each non-primary shard
    # (moving to the primary is a no-op).
    for point in (0, 100, 200):
        t.source.admin.command("split", ns, middle={"_id": point})

    primary = target_chunks(t.source, ns)[0]["shard"]
    non_primary = [s for s in src_shards if s != primary]
    for i, shard in enumerate(non_primary):
        t.source.admin.command("moveChunk", ns, find={"_id": 50 + i * 100}, to=shard)

    t.source["db_1"]["coll_1"].insert_many([{"_id": i} for i in range(-50, 300, 5)])

    # Guard: source must span all shards, else the mirror assertion is trivial.
    src_owners = {c["shard"] for c in target_chunks(t.source, ns)}
    assert len(src_owners) == len(src_shards), (
        f"source layout not spread across all shards: {src_owners}"
    )


def _assert_mirrored_layout(
    src_chunks: list[dict], tgt_chunks: list[dict], src_shards: list[str], tgt_shards: list[str]
):
    pairing = dict(zip(src_shards, tgt_shards, strict=True))
    src_owner = _ownership_by_bounds(src_chunks)
    tgt_owner = _ownership_by_bounds(tgt_chunks)
    assert src_owner.keys() == tgt_owner.keys(), "target boundaries differ from source"
    for bounds, s_shard in src_owner.items():
        assert tgt_owner[bounds] == pairing[s_shard], (
            f"chunk {bounds}: target on {tgt_owner[bounds]}, "
            f"expected {pairing[s_shard]} (source {s_shard})"
        )


def test_ranged_mirror_layout(t: Testing):
    """Mirror ranged boundaries and ownership under sorted-shard pairing."""
    if shard_count(t.source) != shard_count(t.target):
        pytest.skip("mirroring requires equal shard counts (SRC_SHARDS == TGT_SHARDS)")

    ns = "db_1.coll_1"

    src_shards = sorted_shards(t.source)
    if len(src_shards) < 2:
        # Mirroring is only observable with >= 2 shards owning chunks.
        return

    _setup_ranged_layout(t, ns, src_shards)

    with t.run(phase=Runner.Phase.MANUAL) as r:
        r.start()
        r.wait_for_clone_completed()

        src_chunks = target_chunks(t.source, ns)
        tgt_chunks = target_chunks(t.target, ns)
        tgt_shards = sorted_shards(t.target)
        assert len(tgt_shards) == len(src_shards), "test requires equal shard counts"

        assert len(tgt_chunks) == len(src_chunks), (
            f"target chunk count {len(tgt_chunks)} != source {len(src_chunks)}"
        )
        _assert_mirrored_layout(src_chunks, tgt_chunks, src_shards, tgt_shards)

    t.compare_all_sharded()


def _wait_for_clone_without_failure(t: Testing, timeout: int) -> dict:
    """Fail immediately on PCSM failure rather than waiting out the clone deadline."""
    deadline = time.monotonic() + timeout
    while True:
        status = t.pcsm.status()
        assert status["state"] != "failed", f"PCSM failed during clone: {status}"
        if status["initialSync"]["cloneCompleted"]:
            return status
        assert time.monotonic() < deadline, f"clone did not complete within {timeout}s: {status}"
        time.sleep(0.5)


def _setup_presplit_layout(t: Testing, ns: str) -> list[str]:
    if not t.target.admin.command({"getParameter": 1, "enableTestCommands": 1})[
        "enableTestCommands"
    ]:
        pytest.skip("requires failCommand support on the target mongos")
    if shard_count(t.target) < 2:
        pytest.skip("presplit moveChunk requires at least two target shards")

    src_shards = sorted_shards(t.source)
    if len(src_shards) < 2:
        pytest.skip("presplit moveChunk requires at least two source shards")
    _setup_ranged_layout(t, ns, src_shards)
    return src_shards


# 24 LockTimeout is retried by every RunWithRetry caller, 11601 Interrupted only
# by moveChunk (globally it is what killOp returns).
@pytest.mark.parametrize("error_code", [24, 11601], ids=["LockTimeout", "Interrupted"])
@pytest.mark.timeout(300)
def test_presplit_retries_transient_move_chunk_error(t: Testing, error_code: int):
    ns = "db_1.coll_1"
    src_shards = _setup_presplit_layout(t, ns)
    try:
        t.target.admin.command(
            {
                "configureFailPoint": "failCommand",
                "mode": {"times": 2},
                "data": {"failCommands": ["moveChunk"], "errorCode": error_code},
            }
        )
        with t.run(phase=Runner.Phase.MANUAL, wait_timeout=90) as r:
            r.start()
            _wait_for_clone_without_failure(t, r.wait_timeout)

            # Inspect before finalize can let the balancer reshape the layout.
            src_chunks = target_chunks(t.source, ns)
            tgt_chunks = target_chunks(t.target, ns)
            tgt_shards = sorted_shards(t.target)
            assert len(tgt_chunks) == len(src_chunks), (
                f"target chunk count {len(tgt_chunks)} != source {len(src_chunks)}"
            )

            if len(src_shards) == len(tgt_shards):
                _assert_mirrored_layout(src_chunks, tgt_chunks, src_shards, tgt_shards)
            else:
                tgt_owners = {c["shard"] for c in tgt_chunks}
                assert tgt_owners == set(tgt_shards), (
                    f"target does not use all shards: {tgt_owners}"
                )
    finally:
        t.target.admin.command({"configureFailPoint": "failCommand", "mode": "off"})

    t.compare_all_sharded()


@pytest.mark.timeout(300)
def test_presplit_gives_up_and_clone_continues(t: Testing):
    ns = "db_1.coll_1"
    _setup_presplit_layout(t, ns)

    try:
        t.target.admin.command(
            {
                "configureFailPoint": "failCommand",
                "mode": "alwaysOn",
                "data": {"failCommands": ["moveChunk"], "errorCode": 24},
            }
        )
        with t.run(phase=Runner.Phase.MANUAL, wait_timeout=90) as r:
            r.start()
            status = _wait_for_clone_without_failure(t, r.wait_timeout)

            src_count = t.source["db_1"]["coll_1"].count_documents({})
            tgt_count = t.target["db_1"]["coll_1"].count_documents({})
            assert tgt_count == src_count, (
                f"{ns}: target count {tgt_count} != source {src_count}; status={status}"
            )
    finally:
        t.target.admin.command({"configureFailPoint": "failCommand", "mode": "off"})

    t.compare_all_sharded()


def test_ranged_weighted_layout(t: Testing):
    """Unequal shard counts use size-weighted placement to balance data volume."""
    n_src = shard_count(t.source)
    n_tgt = shard_count(t.target)
    if n_src == n_tgt:
        pytest.skip("weighted placement requires unequal shard counts (SRC_SHARDS != TGT_SHARDS)")

    ns = "db_1.coll_1"
    src_shards = sorted_shards(t.source)

    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", ns, key={"_id": 1})
    t.source["config"]["collections"].update_one({"_id": ns}, {"$set": {"noBalance": True}})

    # 6 chunks at [., 0, 100, 200, 300, 400, .). Insert skewed doc counts so
    # chunk sizes differ, then scatter chunks across all source shards.
    for point in (0, 100, 200, 300, 400):
        t.source.admin.command("split", ns, middle={"_id": point})

    # Heavy chunk around _id 0..100, lighter elsewhere.
    heavy = [{"_id": i} for i in range(0, 100)]
    light = (
        [{"_id": i} for i in range(-40, 0)]
        + [{"_id": i} for i in range(100, 130)]
        + [{"_id": i} for i in range(200, 230)]
        + [{"_id": i} for i in range(300, 330)]
        + [{"_id": i} for i in range(400, 430)]
    )
    t.source["db_1"]["coll_1"].insert_many(heavy + light)

    # Scatter one range onto each non-primary source shard.
    primary = target_chunks(t.source, ns)[0]["shard"]
    non_primary = [s for s in src_shards if s != primary]
    for i, shard in enumerate(non_primary):
        t.source.admin.command("moveChunk", ns, find={"_id": 150 + i * 100}, to=shard)

    src_owners = {c["shard"] for c in target_chunks(t.source, ns)}
    assert len(src_owners) == n_src, f"source layout not spread across all shards: {src_owners}"

    with t.run(phase=Runner.Phase.MANUAL) as r:
        r.start()
        r.wait_for_clone_completed()

        src_chunks = target_chunks(t.source, ns)
        tgt_chunks = target_chunks(t.target, ns)

        assert len(tgt_chunks) == len(src_chunks), (
            f"target chunk count {len(tgt_chunks)} != source {len(src_chunks)}"
        )

        tgt_owners = {c["shard"] for c in tgt_chunks}
        assert len(tgt_owners) == n_tgt, f"target does not use all shards: {tgt_owners}"

        # Largest-first placement bounds the spread by the heaviest chunk's size.
        docs_per_shard: dict[str, int] = {}
        for c in tgt_chunks:
            lo = c["min"]["_id"]
            hi = c["max"]["_id"]
            if not isinstance(hi, MaxKey):
                query = {"_id": {"$gte": lo, "$lt": hi}}
            elif not isinstance(lo, MinKey):
                query = {"_id": {"$gte": lo}}
            else:
                query = {}
            cnt = t.target["db_1"]["coll_1"].count_documents(query)
            docs_per_shard[c["shard"]] = docs_per_shard.get(c["shard"], 0) + cnt

        total = sum(docs_per_shard.values())
        ideal = total / n_tgt
        # Heaviest chunk is the 100-doc range; allow that as placement slack.
        assert max(docs_per_shard.values()) <= ideal + 100, (
            f"uneven data distribution: {docs_per_shard} (ideal {ideal:.0f})"
        )

    t.compare_all_sharded()
