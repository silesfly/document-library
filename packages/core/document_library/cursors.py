"""Bounded opaque continuation state. Decoding grants no authorization.

B owns library tuples and C owns preview/source/cache ordinal meaning. This
codec owns only the canonical v1 envelope and resource bounds, not a signature,
a session or any permission. Always reauthorize the selected real source.
"""
import base64
import binascii
import json
import math
import re

from .library_contracts import canonical_json

MAX_ENCODED = 4096
MAX_DECODED = 3072


class CursorError(ValueError):
    def __init__(self):
        super().__init__('Invalid continuation cursor')


def _kind(value):
    if not isinstance(value,str) or not re.fullmatch(r'[a-z][a-z0-9_.-]{0,31}',value):
        raise CursorError()


def _bounded(value):
    pending=[(value,0)]
    nodes=0
    while pending:
        item,depth=pending.pop();nodes+=1
        if nodes>256 or depth>8:
            raise CursorError()
        if isinstance(item,dict):
            if len(item)>64 or any(not isinstance(k,str) or not 1<=len(k)<=64 for k in item):
                raise CursorError()
            pending.extend((v,depth+1) for v in item.values())
        elif isinstance(item,list):
            if len(item)>128:
                raise CursorError()
            pending.extend((v,depth+1) for v in item)
        elif isinstance(item,str):
            if len(item.encode('utf-8'))>2048:
                raise CursorError()
        elif type(item) is int:
            if not -(2**63)<=item<2**63:
                raise CursorError()
        elif type(item) is float:
            if not math.isfinite(item):
                raise CursorError()
        elif item is not None and type(item) is not bool:
            raise CursorError()


def _pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result:
            raise CursorError()
        result[key]=value
    return result


def encode_cursor(kind, payload):
    """Encode a bounded dict payload in the canonical versioned envelope."""
    try:
        _kind(kind)
        if not isinstance(payload,dict):
            raise CursorError()
        envelope=dict(v=1,kind=kind,payload=payload)
        _bounded(envelope)
        raw=canonical_json(envelope).encode('utf-8')
        if len(raw)>MAX_DECODED:
            raise CursorError()
        token=base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')
        if len(token)>MAX_ENCODED:
            raise CursorError()
        return token
    except (TypeError,UnicodeError,RecursionError,ValueError) as error:
        if isinstance(error,CursorError):
            raise
        raise CursorError() from None


def decode_cursor(raw, *, expected_kind):
    """Return the payload; the caller still validates its own exact schema."""
    try:
        _kind(expected_kind)
        if not isinstance(raw,str) or not 1<=len(raw)<=MAX_ENCODED or not re.fullmatch('[A-Za-z0-9_-]+',raw):
            raise CursorError()
        decoded=base64.b64decode(raw+'='*((-len(raw))%4),altchars=b'-_',validate=True)
        if len(decoded)>MAX_DECODED or base64.urlsafe_b64encode(decoded).rstrip(b'=').decode()!=raw:
            raise CursorError()
        envelope=json.loads(decoded.decode('utf-8'),object_pairs_hook=_pairs)
        _bounded(envelope)
        if (not isinstance(envelope,dict) or set(envelope)!={'v','kind','payload'}
                or type(envelope['v']) is not int or envelope['v']!=1
                or envelope['kind']!=expected_kind or not isinstance(envelope['payload'],dict)
                or canonical_json(envelope).encode('utf-8')!=decoded):
            raise CursorError()
        return envelope['payload']
    except (TypeError,ValueError,UnicodeError,RecursionError,binascii.Error) as error:
        if isinstance(error,CursorError):
            raise
        raise CursorError() from None
