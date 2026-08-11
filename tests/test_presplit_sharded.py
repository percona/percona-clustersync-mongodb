# pylint: disable=missing-docstring,redefined-outer-name
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
    """Hashed collections are not pre-split: PCSM relies on the balanced native
    layout that shardCollection produces. This guards that the target ends up
    with an even, well-distributed hashed layout after clone."""
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

        # 1. at least one chunk per shard, evenly divided across shards.
        assert len(chunks) >= n, f"chunk count {len(chunks)} < shard count {n}"
        assert len(chunks) % n == 0, f"chunk count {len(chunks)} not a multiple of {n}"

        # 2. even ownership: every shard owns the same number of chunks.
        per_shard: dict[str, int] = {}
        for c in chunks:
            per_shard[c["shard"]] = per_shard.get(c["shard"], 0) + 1
        assert set(per_shard.values()) == {len(chunks) // n}, f"uneven chunk ownership: {per_shard}"

        # 3. even data distribution: each shard owns roughly an equal share of
        # the hash space (data volume is proportional to hash width).
        width_per_shard = shard_hash_widths(chunks)
        total = sum(width_per_shard.values())
        ideal = total / n
        for shard, w in width_per_shard.items():
            assert abs(w - ideal) / ideal < 0.05, (
                f"shard {shard} owns {w / total:.1%} of hash space (ideal {1 / n:.1%})"
            )

    t.compare_all_sharded()


def _ownership_by_bounds(chunks: list[dict]) -> dict[tuple, str]:
    """Map each chunk's (min._id, max._id) bounds to its owning shard."""
    return {(repr(c["min"]["_id"]), repr(c["max"]["_id"])): c["shard"] for c in chunks}


def test_ranged_mirror_layout(t: Testing):
    """Ranged collections with equal source/target shard counts mirror the
    source chunk layout: same boundaries, and ownership paired by sorted shard
    ID (source[i] -> target[i])."""
    ns = "db_1.coll_1"

    src_shards = sorted_shards(t.source)
    if len(src_shards) < 2:
        # Mirroring is only observable with >= 2 shards owning chunks.
        return

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

    with t.run(phase=Runner.Phase.MANUAL) as r:
        r.start()
        r.wait_for_clone_completed()

        src_chunks = target_chunks(t.source, ns)
        tgt_chunks = target_chunks(t.target, ns)
        tgt_shards = sorted_shards(t.target)
        assert len(tgt_shards) == len(src_shards), "test requires equal shard counts"

        pairing = dict(zip(src_shards, tgt_shards, strict=True))

        # 1. same chunk boundaries on both sides.
        assert len(tgt_chunks) == len(src_chunks), (
            f"target chunk count {len(tgt_chunks)} != source {len(src_chunks)}"
        )

        # 2. ownership mirrors the source under sorted-shard pairing.
        src_owner = _ownership_by_bounds(src_chunks)
        tgt_owner = _ownership_by_bounds(tgt_chunks)
        assert src_owner.keys() == tgt_owner.keys(), "target boundaries differ from source"
        for bounds, s_shard in src_owner.items():
            assert tgt_owner[bounds] == pairing[s_shard], (
                f"chunk {bounds}: target on {tgt_owner[bounds]}, "
                f"expected {pairing[s_shard]} (source {s_shard})"
            )

    t.compare_all_sharded()
