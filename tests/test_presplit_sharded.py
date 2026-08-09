# pylint: disable=missing-docstring,redefined-outer-name
import math

from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo import MongoClient
from testing import Testing

from pcsm import Runner

HASH_MIN = -(2**63)
HASH_SPAN = 2**64


def even_hash_points(num_chunks: int) -> list[int]:
    """Interior split points that divide the hash space into num_chunks equal slices."""
    return [HASH_MIN + i * (HASH_SPAN // num_chunks) for i in range(1, num_chunks)]


def hash_chunk_count(n_shards: int, src_chunks: int) -> int:
    """max(N, ceil(src/N)*N): at least one chunk per shard, rounded to a multiple of N."""
    return max(n_shards, math.ceil(src_chunks / n_shards) * n_shards)


def shard_count(client: MongoClient) -> int:
    return len(client.admin.command("listShards")["shards"])


def target_chunks(client: MongoClient, ns: str) -> list[dict]:
    """Chunks for ns on the cluster, ordered by min, read via config.chunks (uuid-keyed)."""
    coll = client["config"]["collections"].find_one({"_id": ns})
    assert coll is not None, f"{ns} not found in config.collections"
    uuid = coll["uuid"]
    return list(client["config"]["chunks"].find({"uuid": uuid}).sort("min", 1))


def _hash_bound(value, fallback: int) -> int:
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
        lo = _hash_bound(c["min"]["_id"], HASH_MIN)
        hi = _hash_bound(c["max"]["_id"], HASH_MIN + HASH_SPAN)
        widths[c["shard"]] = widths.get(c["shard"], 0) + (hi - lo)
    return widths


def test_hashed_presplit_even_layout(t: Testing):
    ns = "db_1.coll_1"

    # Force the source to have more chunks than the target would natively create,
    # so the assertion is meaningful on every target version.
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", ns, key={"_id": "hashed"})
    for point in even_hash_points(6):
        # Some points may already exist as native boundaries; ignore those.
        try:
            t.source.admin.command("split", ns, middle={"_id": Int64(point)})
        except Exception:  # pylint: disable=broad-except
            pass
    # Keep the dataset tiny so the target auto-splitter does not fire during
    # clone and reshape the chunk count we are asserting on.
    t.source["db_1"]["coll_1"].insert_many([{"_id": i} for i in range(50)])

    # The presplit sizing uses the *actual* source chunk count, which includes
    # any native boundaries, so read it back rather than assuming a value.
    src_chunks = len(target_chunks(t.source, ns))

    with t.run(phase=Runner.Phase.MANUAL) as r:
        r.start()
        r.wait_for_clone_completed()

        # Inspect the target layout immediately after clone, before finalize
        # (finalize can wake the balancer/auto-merger and reshape chunks).
        n = shard_count(t.target)
        expected_chunks = hash_chunk_count(n, src_chunks)

        chunks = target_chunks(t.target, ns)

        # 1. chunk count matches the max(N, ceil(src/N)*N) rule
        assert len(chunks) == expected_chunks, (
            f"target chunk count {len(chunks)} != expected {expected_chunks}"
        )

        # 2. even ownership: numChunks/N chunks per shard
        per_shard: dict[str, int] = {}
        for c in chunks:
            per_shard[c["shard"]] = per_shard.get(c["shard"], 0) + 1
        assert set(per_shard.values()) == {expected_chunks // n}, (
            f"uneven chunk ownership: {per_shard}"
        )

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
