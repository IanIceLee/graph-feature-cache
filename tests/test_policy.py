import pytest

from graphcache_core.policy import STRATEGIES, gain, select, choose, single_fetch_cost, LRU, LFU, GDS


def item(i, b=4, m=4, q=10.0, kc=1.0, kp=0.0):
    return dict(id=f"o{i}", b=b, m=m, q=q, q_hat=q, k_comm=kc, k_pub=kp)


def test_strategy_names():
    assert STRATEGIES == ["none", "lru", "lfu", "gds", "comm_only", "pub_only", "joint", "full_retain"]


def test_gain_modes():
    it = item(0, b=4, q=10.0, kc=2.0, kp=3.0)
    assert gain(it, 100.0, "joint") == 2.0 * 4 + 100.0 * 3.0 * 10.0
    assert gain(it, 100.0, "comm_only") == 2.0 * 4
    assert gain(it, 100.0, "pub_only") == 100.0 * 3.0 * 10.0


def test_single_fetch_cost():
    assert single_fetch_cost(4, 10.0, "source_reuse", 100.0) == 4
    assert single_fetch_cost(4, 10.0, "fresh", 100.0) == 1004


def test_select_respects_budget():
    items = [item(i, b=8, m=8, kc=i + 1) for i in range(10)]
    chosen = select(items, cap=20, lam=100.0, mode="comm_only")
    assert sum(x["m"] for x in chosen) <= 20
    assert len(chosen) > 0


def test_choose_full_and_none():
    items = [item(i) for i in range(3)]
    assert choose("none", items, 12, 100.0) == set()
    assert choose("full_retain", items, 12, 100.0) == {x["id"] for x in items}


def test_online_caches_capacity():
    for cls in (LRU, LFU):
        c = cls(8)
        c.put("a", 4)
        c.put("b", 4)
        assert c.used <= 8
        c.put("c", 4)
        assert c.used <= 8
    g = GDS(8)
    g.put("a", 4, 10)
    g.put("b", 4, 10)
    g.put("c", 4, 10)
    assert g.used <= 8
