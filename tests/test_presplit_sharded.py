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
