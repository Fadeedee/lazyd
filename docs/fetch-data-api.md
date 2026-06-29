# FETCH data plane

The lazy pmem data plane uses a Unix Domain Socket with `SOCK_SEQPACKET`.
Each packet contains one JSON header. Successful `fetch` responses carry one
cache file descriptor with `SCM_RIGHTS`.

The control socket remains HTTP-over-UDS. The data socket is separate because
ordinary HTTP request/response bodies cannot carry file descriptors.

## Socket

Default path:

```text
/run/lazyd/lazyd-data.sock
```

Override with:

```bash
lazyd --data-socket /run/lazyd/lazyd-data.sock
```

or:

```bash
LAZYD_DATA_SOCKET=/run/lazyd/lazyd-data.sock lazyd
```

## FETCH request

```json
{
  "protocol_version": 1,
  "request_id": "req-1",
  "op": "fetch",
  "instance_id": "erofs-sha256-layer",
  "pos": 0,
  "len": 4096
}
```

`pos` and `len` must be host page aligned. The instance must already be
registered through the control plane, usually by `prepare-image`.

## FETCH success

```json
{
  "protocol_version": 1,
  "request_id": "req-1",
  "op": "fetch_ok",
  "ranges": [
    { "off": 0, "len": 1048576, "dev_off": 0 }
  ]
}
```

The packet carries the sparse EROFS cache file descriptor through
`SCM_RIGHTS`. MVP successful FETCH responses always include this fd.

`off`, `len`, and `dev_off` are page aligned. `dev_off` is the offset
StratoVirt should use when mapping the returned fd into its lazy pmem HVA.
The returned range is the complete page-aligned ready range after
`fetch.unit_bytes` amplification, so it can be larger than the requested fault
page. StratoVirt should validate all returned ranges before mapping and then
map and wake every returned range.

## Error

```json
{
  "protocol_version": 1,
  "request_id": "req-1",
  "op": "error",
  "code": 400,
  "msg": "fetch off and len must be page-aligned"
}
```

## Blob tail page

FETCH may cover the final page that crosses `blob_size`. lazyd only fetches
real bytes from the OCI blob and leaves `[blob_size, page_end)` as zeroes in
the sparse cache. Ranges fully beyond `round_up(blob_size, page_size)` are
padding and must be handled by StratoVirt, not lazyd.

## Future extension

`PROBE` is intentionally not implemented in MVP. It can be added later for
warm-cache startup optimization.
