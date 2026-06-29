# prepare-image API

`POST /api/v1/images/prepare` prepares native EROFS rootfs layers for lazy
virtio-pmem startup. The endpoint reads OCI image metadata, rejects non-EROFS
layers, creates per-layer sparse cache files and bitmap files, registers
external-trigger lazyd instances, and returns the layer metadata needed by
Conch.

The endpoint does not download full layer blobs. It only reads manifest,
selected manifest when an image index is returned, image config, and layer
descriptors. Range data is fetched later through the existing instance
`ranges/ensure` path and the future FETCH data path.

## Request

```json
{
  "image_ref": "registry.example.com/ns/image:tag",
  "hosts_dir": "/etc/containerd/certs.d",
  "auth": {
    "username": "user",
    "secret": "pass"
  },
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

## Response

```json
{
  "layers": [
    {
      "index": 0,
      "sparse_path": "/var/lib/lazyd/images/sha256-layer/layer.erofs",
      "bitmap_path": "/var/lib/lazyd/images/sha256-layer/layer.erofs.bitmap",
      "blob_digest": "sha256:layer",
      "blob_size": 4097,
      "pmem_size": 2097152,
      "media_type": "application/vnd.erofs.layer.v1",
      "instance_id": "erofs-sha256-layer"
    }
  ]
}
```

The cache key is the layer digest sanitized for filesystem paths. The
`instance_id` is `erofs-<cache-key>`, so repeated prepare calls for the same
digest reuse the same sparse cache, bitmap, and lazyd instance.
