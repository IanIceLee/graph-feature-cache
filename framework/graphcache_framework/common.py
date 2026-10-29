#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared primitives: feature production, wire codec, digests.

A feature object is identified by (object_id, content_version). The payload is a
deterministic float32 vector derived from that identity, so a version bump always yields
different bytes -- a relabelled version that returns the old payload is detectable.
"""
import hashlib, struct
import numpy as np

MAGIC = b"JCF1"
HEADER = struct.Struct("<4sHHII")     # magic, oid_len, version, n_bytes, body_len


def sha(b):
    return hashlib.sha256(b).hexdigest()


def make_feature(oid, version, n_bytes):
    """Deterministic feature bytes for one (object, version)."""
    seed = int.from_bytes(hashlib.sha256(f"{oid}|{int(version)}".encode()).digest()[:8], "big")
    rng = np.random.default_rng(seed)
    n = max(4, n_bytes)
    return rng.standard_normal(n // 4, dtype=np.float32).tobytes()[:n_bytes]


def encode_frame(oid, version, body):
    """Real serialization step of the service: header + body."""
    ob = oid.encode()
    return HEADER.pack(MAGIC, len(ob), int(version), len(body), len(ob) + len(body)) + ob + body


def decode_frame(frame):
    """Inverse of encode_frame; rejects malformed frames instead of guessing."""
    if len(frame) < HEADER.size:
        raise ValueError("frame_too_short")
    magic, oid_len, ver, n_bytes, body_len = HEADER.unpack(frame[:HEADER.size])
    if magic != MAGIC:
        raise ValueError("bad_magic")
    if len(frame) != HEADER.size + body_len:
        raise ValueError("bad_body_len")
    off = HEADER.size
    oid = frame[off:off + oid_len].decode()
    body = frame[off + oid_len:]
    if len(body) != n_bytes:
        raise ValueError("bad_n_bytes")
    return oid, ver, body


def feature_array(body):
    return np.frombuffer(body, dtype=np.float32)


def aggregate(body):
    """Controlled consumption used where no model applies: deterministic reduction."""
    a = feature_array(body).astype(np.float64)
    return dict(n=int(a.size), sum=float(a.sum()), sumsq=float((a * a).sum()),
                first=float(a[0]) if a.size else 0.0)
