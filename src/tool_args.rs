use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::{
    ArrayValidation, InstanceType, NumberValidation, Schema, SchemaObject, StringValidation,
    SubschemaValidation,
};
use serde::{Deserialize, Deserializer};
use uuid::Uuid;

use crate::config;

pub const HEARTBEAT_MAX_INTERVAL_SECONDS: u64 = 24 * 60 * 60;
pub const HEARTBEAT_MAX_WAIT_SECONDS: u64 = 25;

fn deserialize_optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug)]
pub struct SessionId(String);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        config::validate_session_id(&value).map_err(serde::de::Error::custom)?;
        Ok(Self(value))
    }
}

impl JsonSchema for SessionId {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "SessionId".to_owned()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        session_id_schema()
    }
}

#[derive(Clone, Debug)]
pub struct CommandArgv(Vec<String>);

impl CommandArgv {
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CommandArgv {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Vec::<String>::deserialize(deserializer)?;
        if value.is_empty() {
            return Err(serde::de::Error::custom(
                "command must contain at least one argv entry",
            ));
        }
        Ok(Self(value))
    }
}

impl JsonSchema for CommandArgv {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "CommandArgv".to_owned()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        SchemaObject {
            instance_type: Some(InstanceType::Array.into()),
            array: Some(Box::new(ArrayValidation {
                items: Some(generator.subschema_for::<String>().into()),
                min_items: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatName(String);

impl HeartbeatName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for HeartbeatName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() {
            return Err(serde::de::Error::custom("heartbeat name must not be empty"));
        }
        if value.len() > 64 {
            return Err(serde::de::Error::custom(
                "heartbeat name must be at most 64 bytes",
            ));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(serde::de::Error::custom(
                "heartbeat name may contain only ASCII letters, digits, '.', '_' and '-'",
            ));
        }
        Ok(Self(value))
    }
}

impl JsonSchema for HeartbeatName {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "HeartbeatName".to_owned()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        constrained_string_schema(1, 64, "^[A-Za-z0-9._-]+$")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HeartbeatInterval(u64);

impl HeartbeatInterval {
    pub fn seconds(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for HeartbeatInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if !(1..=HEARTBEAT_MAX_INTERVAL_SECONDS).contains(&value) {
            return Err(serde::de::Error::custom(format!(
                "interval_seconds must be between 1 and {HEARTBEAT_MAX_INTERVAL_SECONDS}"
            )));
        }
        Ok(Self(value))
    }
}

impl JsonSchema for HeartbeatInterval {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "HeartbeatInterval".to_owned()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        bounded_u64_schema(1, HEARTBEAT_MAX_INTERVAL_SECONDS)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HeartbeatWait(u64);

impl HeartbeatWait {
    pub fn seconds(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for HeartbeatWait {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if !(1..=HEARTBEAT_MAX_WAIT_SECONDS).contains(&value) {
            return Err(serde::de::Error::custom(format!(
                "max_wait_seconds must be between 1 and {HEARTBEAT_MAX_WAIT_SECONDS}"
            )));
        }
        Ok(Self(value))
    }
}

impl JsonSchema for HeartbeatWait {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "HeartbeatWait".to_owned()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        bounded_u64_schema(1, HEARTBEAT_MAX_WAIT_SECONDS)
    }
}

fn session_id_schema() -> Schema {
    let mut schema = constrained_string_schema(1, 64, "^[A-Za-z0-9._-]+$").into_object();
    schema.subschemas = Some(Box::new(SubschemaValidation {
        not: Some(Box::new(
            SchemaObject {
                enum_values: Some(vec![
                    serde_json::Value::String(".".to_owned()),
                    serde_json::Value::String("..".to_owned()),
                ]),
                ..Default::default()
            }
            .into(),
        )),
        ..Default::default()
    }));
    schema.into()
}

fn constrained_string_schema(min_length: u32, max_length: u32, pattern: &str) -> Schema {
    SchemaObject {
        instance_type: Some(InstanceType::String.into()),
        string: Some(Box::new(StringValidation {
            min_length: Some(min_length),
            max_length: Some(max_length),
            pattern: Some(pattern.to_owned()),
        })),
        ..Default::default()
    }
    .into()
}

fn bounded_u64_schema(minimum: u64, maximum: u64) -> Schema {
    SchemaObject {
        instance_type: Some(InstanceType::Integer.into()),
        format: Some("uint64".to_owned()),
        number: Some(Box::new(NumberValidation {
            minimum: Some(minimum as f64),
            maximum: Some(maximum as f64),
            ..Default::default()
        })),
        ..Default::default()
    }
    .into()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionInfoArgs {
    pub session_id: SessionId,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileArgs {
    pub session_id: SessionId,
    pub path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetImageArgs {
    pub session_id: SessionId,
    /// Path to a PNG, JPEG, GIF, WebP, BMP, TIFF, or AVIF image.
    pub path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListDirectoryArgs {
    pub session_id: SessionId,
    pub path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteFileArgs {
    pub session_id: SessionId,
    pub path: String,
    pub content: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecuteArgs {
    pub session_id: SessionId,
    pub command: CommandArgv,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub cwd: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartCommandArgs {
    pub session_id: SessionId,
    pub command: CommandArgv,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub cwd: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PollJobArgs {
    pub session_id: SessionId,
    pub job_id: Uuid,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StopJobArgs {
    pub session_id: SessionId,
    pub job_id: Uuid,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatStartArgs {
    pub session_id: SessionId,
    pub interval_seconds: HeartbeatInterval,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub name: Option<HeartbeatName>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatWaitArgs {
    pub session_id: SessionId,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub name: Option<HeartbeatName>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub max_wait_seconds: Option<HeartbeatWait>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatStatusArgs {
    pub session_id: SessionId,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub name: Option<HeartbeatName>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatStopArgs {
    pub session_id: SessionId,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub name: Option<HeartbeatName>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WithoutSandboxArgs {
    pub session_id: SessionId,
    pub command: CommandArgv,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub cwd: Option<String>,
}
