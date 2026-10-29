#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Strategies, estimators and selectors.

Base strategies (8): none, lru, lfu, gds, comm_only, pub_only, joint, full_retain.
Component ablations (3) are `joint` with exactly one input replaced:
  no_reuse_estimate   every valid candidate gets the same expected future reuse count
  uniform_q_estimate  q_hat replaced by one global statistic from the calibration set
  density_skip_only   density-skip greedy only, no three-candidate comparison
"""
from collections import OrderedDict

STRATEGIES = ["none", "lru", "lfu", "gds", "comm_only", "pub_only", "joint", "full_retain"]
# T1 replaces unbounded retention with window_full (576 B = one full input window)
T1_STRATEGIES = ["none", "lru", "lfu", "gds", "comm_only", "pub_only", "joint", "window_full"]
ABLATIONS = ["no_reuse_estimate", "uniform_q_estimate", "density_skip_only"]
ONLINE = {"lru", "lfu", "gds"}


def single_fetch_cost(b, q_hat, rule, lam):
    """Cost of serving one object once under the active rule.

    source_reuse: a legal re-fetch is just the transfer.
    fresh:        the miss also republishes, so it pays b + lambda*q_hat.
    The same value must be used for the GDS hit-refresh and for the GDS insert."""
    return b + (lam * q_hat if rule == "fresh" else 0.0)


def gain(it, lam, mode="joint"):
    kc, kp = it["k_comm"], it["k_pub"]
    if mode in ("comm_only", "no_kpub"): kp = 0.0
    elif mode == "pub_only": kc = 0.0
    return kc * it["b"] + lam * kp * it["q_hat"]


def select(items, cap, lam, mode="joint", algorithm="three_way"):
    feas = [i for i in items if i["m"] <= cap and gain(i, lam, mode) > 0]
    if not feas: return []
    order = sorted(feas, key=lambda x: (-(gain(x, lam, mode) / x["m"]), x["m"], x["id"]))
    if algorithm == "density_skip":
        out, used = [], 0
        for it in order:
            if used + it["m"] <= cap: out.append(it); used += it["m"]
        return out

    def greedy(skip):
        out, used = [], 0
        for it in order:
            if used + it["m"] <= cap: out.append(it); used += it["m"]
            elif not skip: break
        return out
    best = min(feas, key=lambda x: (-gain(x, lam, mode), x["m"], x["id"]))
    return max([greedy(False), greedy(True), [best]],
               key=lambda c: (sum(gain(i, lam, mode) for i in c), -sum(i["m"] for i in c)))


def choose(strategy, items, cap, lam, algorithm="three_way"):
    """Returns (chosen_ids, mode, algorithm) or ('all'|'none')."""
    if strategy == "none": return set()
    if strategy == "full_retain": return {i["id"] for i in items}
    mode, alg = "joint", algorithm
    if strategy == "comm_only": mode = "comm_only"
    elif strategy == "pub_only": mode = "pub_only"
    elif strategy == "density_skip_only": alg = "density_skip"
    return {i["id"] for i in select(items, cap, lam, mode, alg)}


class LRU:
    def __init__(self, cap): self.cap = cap; self.d = OrderedDict(); self.used = 0; self.evictions = 0
    def get(self, k):
        if k in self.d: self.d.move_to_end(k); return True
        return False
    def put(self, k, m):
        ev = []
        if m > self.cap: return ev
        if k in self.d: self.used -= self.d.pop(k)
        while self.used + m > self.cap and self.d:
            kk, s = self.d.popitem(last=False); self.used -= s; ev.append(kk); self.evictions += 1
        self.d[k] = m; self.used += m
        return ev


class LFU:
    """Frequency with an optional causal seed (prefix counts); ties by (freq, seq, id)."""
    def __init__(self, cap, seed_counts=None):
        self.cap = cap; self.d = {}; self.used = 0; self.n = 0; self.evictions = 0
        self.freq = dict(seed_counts or {}); self.seq = {k: -1 for k in self.freq}
    def get(self, k):
        if k in self.d:
            self.freq[k] = self.freq.get(k, 0) + 1; self.seq[k] = self.n; self.n += 1; return True
        return False
    def put(self, k, m):
        ev = []
        if m > self.cap: return ev
        if k in self.d: self.used -= self.d.pop(k)
        while self.used + m > self.cap and self.d:
            victim = min(self.d, key=lambda x: (self.freq.get(x, 0), self.seq.get(x, 0), x))
            self.used -= self.d.pop(victim); ev.append(victim); self.evictions += 1
        self.d[k] = m; self.freq.setdefault(k, 1); self.seq[k] = self.n; self.n += 1; self.used += m
        return ev


class GDS:
    """GreedyDual-Size, H = L + c/s (refresh on hit, L = evicted H)."""
    def __init__(self, cap): self.cap = cap; self.d = {}; self.H = {}; self.used = 0; self.L = 0.0; self.evictions = 0
    def get(self, k, c, s):
        if k in self.d: self.H[k] = self.L + c / s; return True
        return False
    def put(self, k, m, c):
        ev = []
        if m > self.cap: return ev
        if k in self.d: self.used -= self.d.pop(k)
        while self.used + m > self.cap and self.d:
            victim = min(self.d, key=lambda x: (self.H.get(x, 0.0), x))
            self.L = self.H.get(victim, 0.0); self.used -= self.d.pop(victim); ev.append(victim); self.evictions += 1
        self.d[k] = m; self.H[k] = self.L + c / m; self.used += m
        return ev


def make_cache(strategy, cap, seed_counts=None):
    if strategy == "lru": return LRU(cap)
    if strategy == "lfu": return LFU(cap, seed_counts)
    if strategy == "gds": return GDS(cap)
    return None
