# External Range API

`lazyd` supports two per-instance trigger modes:

- `fanotify` (the default): lazyd marks `target_path` and serves host VFS
  permission events itself.
- `external`: lazyd does not mark `target_path`; an external fault handler
  explicitly asks lazyd to make byte ranges ready.

The `external` mode is intended for a VMM UFFD handler. It lets the handler
keep ownership of HVA page faults while lazyd remains responsible for remote
range fetches, bitmap state, and sparse EROFS cache writes.

## Register an external instance

```http
PUT /api/v1/instances/{id}
Content-Type: application/json

{
  "target_path": "/var/lib/conch/snapshots/abc/layer0.erofs",
  "blob": {
    "digest": "sha256:...",
    "size": 134300000,
    "media_type": "application/vnd.erofs.layer.v1"
  },
  "source": {
    "type": "oci-registry",
    "image_ref": "registry.example.com/ns/image:tag",
    "hosts_dir": "/etc/containerd/certs.d"
  },
  "fetch": {
    "unit_bytes": 1048576
  },
  "trigger_mode": "external"
}
```

`fetch.unit_bytes` defaults to 1 MiB and must be a multiple of 1 MiB. The
bitmap stores readiness in 1 MiB slots; fetch units can span multiple slots.

## Ensure a range

```http
POST /api/v1/instances/{id}/ranges/ensure
Content-Type: application/json

{
  "offset": 1048576,
  "len": 4096
}
```

A `204 No Content` response means the requested range is within `blob.size`,
the required fetch unit has been written to `target_path`, and the matching
bitmap slots have been marked ready. Callers can immediately `pread` the
requested bytes after a successful response.

The endpoint is idempotent. It rejects overflowing or out-of-blob ranges with
`400 Bad Request` and returns `404 Not Found` for an unknown instance.

## UFFD caller contract

The UFFD handler maps a host fault address to an image offset before calling
this API. It must not ask lazyd to fetch the aligned padding after the real
EROFS blob:

```text
offset < blob_size                 -> ranges/ensure, pread, UFFDIO_COPY
blob_size <= offset < pmem_size    -> UFFDIO_ZEROPAGE
```

`pmem_size` and its alignment belong to the VMM/Conch layer. lazyd only owns
the real image range `[0, blob_size)`.
