#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""The controlled publication operation, in one place so calibration and the live owner
execute exactly the same code.

Steps: encode (frame with header) -> decode round-trip check -> digest (payload + wire) ->
register (caller's sink). The feature materialization is the caller's; everything that
counts as "publication service" is here.
"""
from graphcache_framework.common import decode_frame, encode_frame, sha


def finish_publication(oid, version, body, sink=None):
    t_frame = encode_frame(oid, version, body)          # encode
    oid2, ver2, body2 = decode_frame(t_frame)           # validate the encoding
    if oid2 != oid or ver2 != version: raise ValueError("frame_roundtrip_identity")
    if body2 != body: raise ValueError("frame_roundtrip_body")
    payload_sha = sha(body2)                            # digest
    wire_sha = sha(t_frame)
    rec = dict(object_id=oid, content_version=int(version), payload_sha256=payload_sha,
               wire_sha256=wire_sha, bytes=len(body2), frame=t_frame)
    if sink is not None: sink(rec)                      # register
    return rec
