from graphcache_framework.common import make_feature, encode_frame, decode_frame, sha, aggregate


def test_make_feature_is_deterministic_and_versioned():
    a = make_feature("n1|10", 0, 32)
    b = make_feature("n1|10", 0, 32)
    c = make_feature("n1|10", 1, 32)
    assert a == b and len(a) == 32
    assert a != c


def test_frame_roundtrip():
    body = make_feature("n7|42", 3, 64)
    frame = encode_frame("n7|42", 3, body)
    oid, ver, body2 = decode_frame(frame)
    assert oid == "n7|42" and ver == 3 and body2 == body


def test_sha_matches_hashlib():
    import hashlib
    assert sha(b"abc") == hashlib.sha256(b"abc").hexdigest()


def test_aggregate_shape():
    out = aggregate(make_feature("o", 0, 16))
    assert out["n"] == 4
