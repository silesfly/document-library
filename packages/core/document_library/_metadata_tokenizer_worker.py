"""Private tokenizer child. Reads only the exact manifest and three named blobs.

No weights, model runtimes, network, environment reads or diagnostic payloads.
The parent's pipe deadlines own termination/reaping. Current 0.22.1 from_str API
has separate parent evidence. Real-artifact construction remains a separate
offline check; macOS address-space enforcement is never claimed by this child.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import PurePosixPath
import resource
import stat
import struct
import sys

MAX_FRAME = 8_388_608
MAX_PROMPT = 1_048_576
MAX_IDS = 1_000_000
NAMES = frozenset(("config.json", "tokenizer.json", "tokenizer_config.json"))


class WorkerError(Exception):
    pass


def need(condition):
    if not condition:
        raise WorkerError("TOKENIZER_WORKER_REJECTED")


def unique(pairs):
    value = {}
    for key, item in pairs:
        need(key not in value)
        value[key] = item
    return value


def decode(data):
    return json.loads(data.decode("utf-8"), object_pairs_hook=unique,
                      parse_constant=lambda _: need(False))


def read_frame():
    header = sys.stdin.buffer.read(4)
    if not header:
        return None
    need(len(header) == 4)
    size, = struct.unpack("!I", header)
    need(0 < size <= MAX_FRAME)
    body = sys.stdin.buffer.read(size)
    need(len(body) == size)
    return decode(body)


def write_frame(value):
    data = json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")
    need(0 < len(data) <= MAX_FRAME)
    sys.stdout.buffer.write(struct.pack("!I", len(data)))
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


def open_directory(path):
    """Walk from / without following any path component's symlink."""
    p = PurePosixPath(path)
    need(type(path) is str and p.is_absolute() and ".." not in p.parts)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    owned = os.open("/", flags)
    try:
        for part in p.parts[1:]:
            following = os.open(part, flags, dir_fd=owned)
            os.close(owned)
            owned = following
        return owned
    except BaseException:
        os.close(owned)
        raise


def read_verified(root_fd, relative, size, digest):
    p = PurePosixPath(relative)
    need(type(relative) is str and not p.is_absolute() and p.parts
         and ".." not in p.parts and "." not in p.parts)
    need(type(size) is int and 0 < size <= 33_554_432)
    need(type(digest) is str and len(digest) == 64)
    directory = os.dup(root_fd)
    opened = None
    try:
        for part in p.parts[:-1]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                | os.O_CLOEXEC, dir_fd=directory)
            os.close(directory)
            directory = following
        opened = os.open(p.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=directory)
        before = os.fstat(opened)
        need(stat.S_ISREG(before.st_mode) and before.st_size == size)
        blocks = bytearray()
        while len(blocks) < size:
            block = os.read(opened, min(65_536, size - len(blocks)))
            need(bool(block))
            blocks.extend(block)
        need(os.read(opened, 1) == b"")
        after = os.fstat(opened)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        need(identity(before) == identity(after))
        data = bytes(blocks)
        need(hashlib.sha256(data).hexdigest() == digest)
        return data
    finally:
        if opened is not None:
            os.close(opened)
        os.close(directory)


def configure_limits(cpu_seconds, address_bytes):
    need(type(cpu_seconds) is int and 1 <= cpu_seconds <= 60)
    need(type(address_bytes) is int and 67_108_864 <= address_bytes <= 1_073_741_824)
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
    cpu = resource.getrlimit(resource.RLIMIT_CPU)
    need(cpu == (cpu_seconds, cpu_seconds))
    address_status = "NOT_VERIFIED"
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_AS, (address_bytes, address_bytes))
        need(resource.getrlimit(resource.RLIMIT_AS) == (address_bytes, address_bytes))
        address_status = "CONFIGURED_OBSERVED"
    return {"cpu_seconds": cpu_seconds, "cpu_status": "CONFIGURED_OBSERVED",
            "address_bytes": address_bytes, "address_status": address_status,
            "platform": sys.platform}


def load_tokenizer(binding, expected_profile):
    need(type(binding) is dict and type(expected_profile) is dict)
    specs = binding["artifacts"]
    need(type(specs) is list and len(specs) == 3
         and {s["name"] for s in specs} == NAMES)
    root = open_directory(binding["root"])
    try:
        manifest_spec = binding["manifest"]
        need(manifest_spec["size"] <= 1_048_576)
        manifest = decode(read_verified(root, manifest_spec["path"],
                                       manifest_spec["size"], manifest_spec["sha256"]))
        layers = manifest["layers"]
        need(type(layers) is list and len(layers) <= 4096)
        blobs = {}
        for spec in specs:
            # Name-key representation is observed in the current serving manifest.
            matches = [layer for layer in layers if type(layer) is dict
                       and layer.get("name") == spec["name"]]
            need(len(matches) == 1)
            layer = matches[0]
            need(type(layer) is dict and layer.get("name") == spec["name"]
                 and layer.get("mediaType") == "application/vnd.ollama.image.json"
                 and layer.get("digest") == "sha256:" + spec["sha256"]
                 and type(layer.get("size")) is int and layer["size"] == spec["size"])
            need(spec["path"] == "blobs/sha256-" + spec["sha256"])
            if spec["name"] != "tokenizer.json":
                need(spec["size"] <= 1_048_576)
            blobs[spec["name"]] = read_verified(root, spec["path"], spec["size"], spec["sha256"])
    finally:
        os.close(root)
    config = decode(blobs["config.json"])
    tokenizer_config = decode(blobs["tokenizer_config.json"])
    definition = decode(blobs["tokenizer.json"])
    need(type(config) is dict and type(tokenizer_config) is dict
         and type(definition) is dict)
    # Check only the two explicitly admitted implementation files, without scans.
    implementations = binding["implementation_files"]
    need(type(implementations) is list and len(implementations) == 2
         and {item["role"] for item in implementations} == {"python_module", "native_extension"})
    for item in implementations:
        path = PurePosixPath(item["path"])
        need(path.is_absolute())
        directory = open_directory(str(path.parent))
        try:
            read_verified(directory, path.name, item["size"], item["sha256"])
        finally:
            os.close(directory)
    # Construct from exactly the verified bytes. No filename reopen or downloads.
    import tokenizers
    need(tokenizers.__version__ == "0.22.1")
    actual_paths = {"python_module": tokenizers.__file__,
                    "native_extension": sys.modules["tokenizers.tokenizers"].__file__}
    need(all(actual_paths[item["role"]] == item["path"] for item in implementations))
    tokenizer = tokenizers.Tokenizer.from_str(blobs["tokenizer.json"].decode("utf-8"))
    tokenizer.no_padding()
    tokenizer.no_truncation()
    need(tokenizer.padding is None and tokenizer.truncation is None)
    vocabulary = tokenizer.get_vocab(with_added_tokens=True)
    need(type(vocabulary) is dict and 0 < len(vocabulary) <= MAX_IDS)
    need(all(type(name) is str and type(number) is int and 0 <= number < 2**32
             for name, number in vocabulary.items()))
    numbers = sorted(vocabulary.values())
    need(len(set(numbers)) == len(numbers))
    ranges = []
    for number in numbers:
        if ranges and ranges[-1][1] == number:
            ranges[-1][1] += 1
        else:
            ranges.append([number, number + 1])
    need(len(ranges) <= 4096)
    additions = definition["added_tokens"]
    need(type(additions) is list and len(additions) <= 4096)
    specials = []
    for token in additions:
        need(type(token) is dict and type(token.get("special")) is bool)
        if token["special"]:
            name, number = token["content"], token["id"]
            need(type(name) is str and 0 < len(name) <= 1024 and type(number) is int
                 and vocabulary.get(name) == number
                 and tokenizer.token_to_id(name) == number)
            specials.append([name, number])
    need(len({s[0] for s in specials}) == len(specials)
         and len({s[1] for s in specials}) == len(specials)
         and {"<|im_start|>", "<|im_end|>"} <= {s[0] for s in specials})
    # Every configured control token must be in the artifact-derived inventory.
    configured = []
    for key, value in tokenizer_config.items():
        if key.endswith("_token") and value is not None:
            need(type(value) is str)
            configured.append(value)
        if key == "model_specific_special_tokens":
            need(type(value) is dict and all(type(v) is str for v in value.values()))
            configured.extend(value.values())
    need(set(configured) <= {s[0] for s in specials})
    need(ranges == expected_profile["token_id_ranges"]
         and specials == expected_profile["special_tokens"])
    return tokenizer, {"tokenizers_version": tokenizers.__version__,
                       "from_str": "OBSERVED", "vocabulary_count": len(vocabulary),
                       "special_tokens": specials, "token_id_ranges": ranges,
                       "padding": None, "truncation": None}


def main():
    init = read_frame()
    need(type(init) is dict and init.get("op") == "init" and init.get("seq") == 0)
    profile_digest = init["profile_digest"]
    need(type(profile_digest) is str and len(profile_digest) == 64)
    limits = configure_limits(init["cpu_seconds"], init["address_bytes"])
    tokenizer, inventory = load_tokenizer(init["binding"], init["profile"])
    write_frame({"op": "ready", "seq": 0, "profile_digest": profile_digest,
                 "inventory": inventory, "limits": limits})
    sequence = 0
    while True:
        request = read_frame()
        if request is None:
            return 0
        sequence += 1
        need(sequence <= 5000 and type(request) is dict and request.get("op") == "encode"
             and request.get("seq") == sequence
             and request.get("profile_digest") == profile_digest)
        prompt = request["prompt"]
        need(type(prompt) is str and 0 < len(prompt) <= MAX_PROMPT)
        prompt_bytes = prompt.encode("utf-8")
        need(len(prompt_bytes) <= MAX_PROMPT)
        digest = hashlib.sha256(prompt_bytes).hexdigest()
        need(request.get("prompt_sha256") == digest)
        encoded = tokenizer.encode(prompt, add_special_tokens=False)
        ids = encoded.ids
        need(type(ids) is list and 0 < len(ids) <= MAX_IDS
             and all(type(n) is int and 0 <= n < 2**32 for n in ids))
        write_frame({"op": "encoded", "seq": sequence, "profile_digest": profile_digest,
                     "prompt_sha256": digest, "ids": ids})


if __name__ == "__main__":
    status = 1
    try:
        status = main()
    except BaseException:
        pass
    raise SystemExit(status)
