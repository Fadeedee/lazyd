# prepare-image API

`POST /api/v1/images/prepare` prepares native EROFS data for lazy
virtio-pmem startup. OCI callers pass explicit rootfs layer descriptors.
Kuasar callers pass an ordered manifest chain that accelerator describes as
one flattened EROFS image plus its canonical visible-data extent layout. The
endpoint creates sparse cache and readiness-map files, registers
external-trigger lazyd instances, and returns VMM-facing metadata.

The endpoint does not parse Conch-specific image layout, select a rootfs
manifest from an OCI index, or handle sandbox/kernel/initrd metadata. Conch is
responsible for that image-level interpretation. For Kuasar manifests,
accelerator remains responsible for manifest, chunk, store, verification, and
decryption semantics. lazyd only owns its final sparse cache and bitmap.

The endpoint does not download full layer blobs. Range data is fetched later
through the existing instance `ranges/ensure` path and the FETCH data path.

## OCI request

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
      "digest": "sha256:layer",
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

## Kuasar manifest request

```json
{
  "image_ref": "manifest://1111111111111111111111111111111111111111111111111111111111111111",
  "source": {
    "type": "kuasar-manifest",
    "manifest_keys": [
      "1111111111111111111111111111111111111111111111111111111111111111"
    ],
    "accelerator_socket": "/run/accelerator/manifest-range.sock"
  },
  "fetch": {
    "unit_bytes": 1048576
  },
  "pmem": {
    "alignment_bytes": 2097152
  }
}
```

`manifest_keys` are bare 64-character hexadecimal keys. Their order is part
of the image identity and must not be changed. `layers` must be omitted.
lazyd calls accelerator `describe` and creates one index 0 descriptor with:

```text
blob_digest = accelerator content_id
blob_size   = accelerator image_size
media_type  = application/vnd.erofs.image.v1
```

For Kuasar, `fetch.unit_bytes` remains accepted for request-schema
compatibility, but it does not define readiness or amplification granularity.
The accelerator-provided canonical data extents do: each data extent owns one
ready slot, while final visible hole and zero runs require no slot or remote
read. Both `fetch.unit_bytes` and each canonical extent must remain within the
accelerator 64 MiB range limit.

The accelerator range protocol v1 and this client are delivered together:
`describe_ok` layout metadata and its sealed layout FD are mandatory v1
content. The unpublished earlier v1 draft is not wire compatible.

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

For Kuasar, the accelerator `content_id` includes the ordered manifest keys
and image size. It is a content-level identity and never includes a VM or
sandbox ID. Multiple VMs therefore share the same lazyd cache and bitmap.

## Manual Kuasar check

Start the accelerator range service and lazyd:

```bash
manifest-ctl serve \
  --manifest-config /etc/accelerator/manifest.yaml \
  --socket /run/accelerator/manifest-range.sock

lazyd --disable-fanotify \
  --socket /run/lazyd/lazyd.sock \
  --data-socket /run/lazyd/lazyd-data.sock
```

Then send the Kuasar request above over lazyd's control UDS:

```bash
curl --unix-socket /run/lazyd/lazyd.sock \
  -H 'Content-Type: application/json' \
  --data-binary @prepare-kuasar.json \
  http://localhost/api/v1/images/prepare
```

After prepare, the returned cache file is sparse. OCI bitmap slots and Kuasar
canonical data extents initially have no ready state. A Kuasar data-plane FETCH
fetches every missing canonical data extent intersecting the requested host
page, copies each sealed staging memfd into the cache, persists the matching
ready slots, and returns the final cache FD to the VMM. The staging FD and
canonical layout FD are never exposed to the VMM.

OCI registries that require `WWW-Authenticate: Bearer` are supported for later
metadata/range reads. lazyd obtains an anonymous or credential-backed token
from the advertised token service and retries the registry request with
`Authorization: Bearer <token>`.
