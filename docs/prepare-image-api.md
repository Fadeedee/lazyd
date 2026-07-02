# prepare-image API

`POST /api/v1/images/prepare` prepares native EROFS rootfs layers for lazy
virtio-pmem startup. The caller passes explicit rootfs layer descriptors.
The endpoint rejects non-EROFS layers, creates per-layer sparse cache files
and bitmap files, registers external-trigger lazyd instances, and returns the
layer metadata needed by Conch.

The endpoint does not parse Conch-specific image layout, select a rootfs
manifest from an OCI index, or handle sandbox/kernel/initrd metadata. Conch is
responsible for that image-level interpretation. lazyd only consumes explicit
native EROFS layer descriptors and later fetches missing ranges from the OCI
registry.

The endpoint does not download full layer blobs. Range data is fetched later
through the existing instance `ranges/ensure` path and the FETCH data path.

## Request

```json
{
  "image_ref": "registry.example.com/ns/image:tag",
  "hosts_dir": "/etc/containerd/certs.d",
  "auth": {
    "username": "user",
    "secret": "pass"
  },
  "layers": [
    {
      "index": 0,
      "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
      "size": 4097,
      "media_type": "application/vnd.erofs.layer.v1"
    }
  ],
  "fetch": {
    "unit_bytes": 1048576
  },
  "pmem": {
    "alignment_bytes": 2097152
  }
}
```

`fetch.unit_bytes` defaults to 1 MiB and controls lazyd bitmap/range
amplification granularity. `pmem.alignment_bytes` defaults to 2 MiB and
controls `pmem_size = align_up(blob_size, alignment_bytes)`.

`layers` is required. Each entry must be a native EROFS layer descriptor chosen
by the caller. For Conch images, Conch should resolve the image index, select
the rootfs manifest, and pass only those rootfs EROFS layer descriptors to
lazyd. lazyd intentionally does not inspect `io.conch.kind` annotations.
Layer digests must use canonical `sha256:<64 lowercase hex>` form.

## Response

```json
{
  "layers": [
    {
      "index": 0,
      "sparse_path": "/var/lib/lazyd/images/sha256-1111111111111111111111111111111111111111111111111111111111111111/layer.erofs",
      "bitmap_path": "/var/lib/lazyd/images/sha256-1111111111111111111111111111111111111111111111111111111111111111/layer.erofs.bitmap",
      "blob_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
      "blob_size": 4097,
      "pmem_size": 2097152,
      "media_type": "application/vnd.erofs.layer.v1",
      "instance_id": "erofs-sha256-1111111111111111111111111111111111111111111111111111111111111111"
    }
  ]
}
```

The cache key is derived from the validated canonical layer digest. The
`instance_id` is `erofs-<cache-key>` and identifies prepared content, not an
image, layer index, VM, or sandbox. Repeated prepare calls with the same digest
and compatible size, media type, fetch unit, and cache policy reuse the same
sparse cache, bitmap, and lazyd instance even when `image_ref` or layer index
differs. Registry source and auth locate immutable content but do not change
its identity.

OCI registries that require `WWW-Authenticate: Bearer` are supported for later
metadata/range reads. lazyd obtains an anonymous or credential-backed token
from the advertised token service and retries the registry request with
`Authorization: Bearer <token>`.
