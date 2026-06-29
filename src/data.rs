use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub protocol_version: u32,
    pub request_id: String,
    pub op: FetchOp,
    pub instance_id: String,
    pub pos: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchOp {
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DataResponse {
    FetchOk {
        protocol_version: u32,
        request_id: String,
        ranges: Vec<FetchRange>,
    },
    Error {
        protocol_version: u32,
        request_id: String,
        code: u16,
        msg: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRange {
    pub off: u64,
    pub len: u64,
    pub dev_off: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_request_schema_is_stable() {
        let request = FetchRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            op: FetchOp::Fetch,
            instance_id: "erofs-sha256-layer".to_string(),
            pos: 0,
            len: 4096,
        };

        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "fetch",
                "instance_id": "erofs-sha256-layer",
                "pos": 0,
                "len": 4096
            })
        );
    }

    #[test]
    fn fetch_ok_response_schema_is_stable() {
        let response = DataResponse::FetchOk {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            ranges: vec![FetchRange {
                off: 0,
                len: 4096,
                dev_off: 0,
            }],
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "fetch_ok",
                "ranges": [
                    { "off": 0, "len": 4096, "dev_off": 0 }
                ]
            })
        );
    }

    #[test]
    fn error_response_schema_is_stable() {
        let response = DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            code: 400,
            msg: "bad request".to_string(),
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "error",
                "code": 400,
                "msg": "bad request"
            })
        );
    }
}
