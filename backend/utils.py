import hashlib

BLOCKSIZE = 65536


def check_hash(filename, hash_digest):
    hasher = hashlib.new("sha256")

    with filename.open("rb") as f:
        buf = f.read(BLOCKSIZE)
        while buf:
            hasher.update(buf)
            buf = f.read(BLOCKSIZE)

    if hasher.hexdigest() != hash_digest:
        raise RuntimeError(f"Hash incorrect pour '{filename}'")
