//! # DynamoDB Event models
//!
//! Models for the DynamoDB event entrypoint.
//!
//! We cannot use the models provided by the AWS SDK for Rust, as they do not
//! implement the `serde::Serialize` and `serde::Deserialize` traits.

use crate::{
    model::{Event, Product},
    Error,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize, Debug)]
pub struct DynamoDBEvent {
    #[serde(rename = "Records")]
    pub records: Vec<DynamoDBRecord>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DynamoDBRecord {
    #[serde(rename = "awsRegion")]
    pub aws_region: String,

    #[serde(rename = "dynamodb")]
    pub dynamodb: DynamoDBStreamRecord,

    #[serde(rename = "eventID")]
    pub event_id: String,

    #[serde(rename = "eventName")]
    pub event_name: String,

    #[serde(rename = "eventSource")]
    pub event_source: String,

    #[serde(rename = "eventSourceARN")]
    pub event_source_arn: String,

    #[serde(rename = "eventVersion")]
    pub event_version: String,
}

impl TryFrom<&DynamoDBRecord> for Event {
    type Error = Error;

    /// Try converting a DynamoDB record to an event.
    fn try_from(value: &DynamoDBRecord) -> Result<Self, Self::Error> {
        match value.event_name.as_str() {
            "INSERT" => {
                let product = (&value.dynamodb.new_image).try_into()?;
                Ok(Event::Created { product })
            }
            "MODIFY" => {
                let old = (&value.dynamodb.old_image).try_into()?;
                let new = (&value.dynamodb.new_image).try_into()?;
                Ok(Event::Updated { old, new })
            }
            "REMOVE" => {
                let product = (&value.dynamodb.old_image).try_into()?;
                Ok(Event::Deleted { product })
            }
            _ => Err(Error::InternalError("Unknown event type")),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DynamoDBStreamRecord {
    #[serde(rename = "ApproximateCreationDateTime", default)]
    pub approximate_creation_date_time: Option<f64>,

    #[serde(rename = "Keys", default)]
    pub keys: HashMap<String, AttributeValue>,

    #[serde(rename = "NewImage", default)]
    pub new_image: HashMap<String, AttributeValue>,

    #[serde(rename = "OldImage", default)]
    pub old_image: HashMap<String, AttributeValue>,

    #[serde(rename = "SequenceNumber")]
    pub sequence_number: String,

    #[serde(rename = "SizeBytes")]
    pub size_bytes: f64,

    #[serde(rename = "StreamViewType")]
    pub stream_view_type: String,
}

/// Attribute Value
///
/// This is a copy of the `AttributeValue` struct from the AWS SDK for Rust,
/// but without blob and `is_`-prefixed methods.
/// See https://docs.rs/aws-sdk-dynamodb/0.0.22-alpha/aws_sdk_dynamodb/model/enum.AttributeValue.html
#[derive(Deserialize, Serialize, Debug)]
pub enum AttributeValue {
    // B(Blob),
    Bool(bool),
    // Bs(Vec<Blob>),
    L(Vec<AttributeValue>),
    M(HashMap<String, AttributeValue>),
    N(String),
    Ns(Vec<String>),
    Null(bool),
    S(String),
    Ss(Vec<String>),
}

impl AttributeValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AttributeValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn as_l(&self) -> Option<&Vec<AttributeValue>> {
        match self {
            AttributeValue::L(l) => Some(l),
            _ => None,
        }
    }
    pub fn as_m(&self) -> Option<&HashMap<String, AttributeValue>> {
        match self {
            AttributeValue::M(m) => Some(m),
            _ => None,
        }
    }
    pub fn as_n(&self) -> Option<f64> {
        match self {
            AttributeValue::N(n) => n.parse::<f64>().ok(),
            _ => None,
        }
    }
    pub fn as_ns(&self) -> Vec<f64> {
        match self {
            AttributeValue::Ns(ns) => ns.iter().filter_map(|n| n.parse::<f64>().ok()).collect(),
            _ => Default::default(),
        }
    }
    pub fn as_null(&self) -> Option<bool> {
        match self {
            AttributeValue::Null(null) => Some(*null),
            _ => None,
        }
    }
    pub fn as_s(&self) -> Option<&str> {
        match self {
            AttributeValue::S(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_ss(&self) -> Vec<String> {
        match self {
            AttributeValue::Ss(ss) => ss.to_owned(),
            _ => Default::default(),
        }
    }
}

impl TryFrom<&HashMap<String, AttributeValue>> for Product {
    type Error = Error;

    /// Try to convert a DynamoDB item into a Product
    ///
    /// This could fail as the DynamoDB item might be missing some fields.
    /// Two ways of casing the fields, as it seems to be different for me
    /// than for the example one
    fn try_from(value: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        Ok(Product {
            id: {
                let found = match value.get("Id") {
                    Some(id) => id,
                    None => value
                        .get("id")
                        .ok_or(Error::InternalError("Missing id in lambda"))?,
                };

                found
                    .as_s()
                    .ok_or(Error::InternalError("id is not a string"))?
                    .to_string()
            },
            name: {
                let found = match value.get("Name") {
                    Some(id) => id,
                    None => value
                        .get("name")
                        .ok_or(Error::InternalError("Missing name in lambda"))?,
                };

                found
                    .as_s()
                    .ok_or(Error::InternalError("name is not a string"))?
                    .to_string()
            },
            price: {
                let found = match value.get("Price") {
                    Some(v) => v,
                    None => value
                        .get("price")
                        .ok_or(Error::InternalError("Missing price in lambda"))?,
                };
                found
                    .as_n()
                    .ok_or(Error::InternalError("price is not a number"))?
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_ddb_event() -> DynamoDBEvent {
        let data = r#"
        {
            "Records": [
              {
                "eventID": "1",
                "eventVersion": "1.0",
                "dynamodb": {
                  "Keys": {
                    "Id": {
                      "S": "101"
                    }
                  },
                  "NewImage": {
                    "Id": {
                        "S": "101"
                    },
                    "Name": {
                      "S": "new-item"
                    },
                    "Price": {
                      "N": "10.5"
                    }
                  },
                  "StreamViewType": "NEW_AND_OLD_IMAGES",
                  "SequenceNumber": "111",
                  "SizeBytes": 26
                },
                "awsRegion": "us-west-2",
                "eventName": "INSERT",
                "eventSourceARN": "someARN",
                "eventSource": "aws:dynamodb"
              },
              {
                "eventID": "2",
                "eventVersion": "1.0",
                "dynamodb": {
                  "OldImage": {
                    "Id": {
                      "S": "102"
                    },
                    "Name": {
                      "S": "new-item2"
                    },
                    "Price": {
                      "N": "20.5"
                    }
                  },
                  "SequenceNumber": "222",
                  "Keys": {
                    "Id": {
                      "S": "102"
                    }
                  },
                  "SizeBytes": 59,
                  "NewImage": {
                    "Id": {
                        "S": "102"
                    },
                    "Name": { "S": "new-item2" },
                    "Price": {
                      "N": "30.5"
                    }
                  },
                  "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "awsRegion": "us-west-2",
                "eventName": "MODIFY",
                "eventSourceARN": "someARN",
                "eventSource": "aws:dynamodb"
            }]
        }"#;

        let event: DynamoDBEvent = serde_json::from_str(data).unwrap();

        event
    }

    // to restore the id, add this in NewImage:   "Id":  { "S": "0F7ylDuZdSWz77F9"}
    fn get_ddb_event_with_unparseable_id() -> DynamoDBEvent {
        let data = r#"
    { "Records": [{ 
        "eventID": "923c4e9082935f91618487d93856d306",
        "eventVersion": "1.1" ,

        "awsRegion": "eu-central-1",
        "dynamodb": { 
            "Keys": {
                "Id": {"S": "0F7ylDuZdSWz77F9"}
            },
            "NewImage": {
                "Price": {"N":"148.82767174148367"} , 
                "Name": {"S": "3Ow8LNjBsGj60ecw" }            }, 
            "ApproximateCreationDateTime": 1637075657.0,
        "OldImage": {}, 
        "SequenceNumber": "100000000009615304022", 
        "SizeBytes": 71.0, 
        "StreamViewType": "NEW_AND_OLD_IMAGES" 
       }, 
        "eventSourceARN": "arn:aws:dynamodb:eu-central-1:961051767741:table/rust-products-Table-VNYFY0FE9HRT/stream/2021-11-16T14:43:56.931",
        "eventName": "INSERT", 
        "eventSource": "aws:dynamodb"       }] 
    }"#;

        let event: DynamoDBEvent = serde_json::from_str(data).unwrap();

        event
    }
    #[test]
    fn test_deserialize_version_1_1() {
        let event = get_ddb_event_with_unparseable_id();

        assert_eq!(event.records.len(), 1);
        assert_eq!(event.records[0].event_name, "INSERT");
        assert_eq!(
            event.records[0]
                .dynamodb
                .new_image
                .get("Name")
                .unwrap()
                .as_s(),
            Some("3Ow8LNjBsGj60ecw")
        );
    }

    #[test]
    fn test_deserialize_v_1_0() {
        let event = get_ddb_event();

        assert_eq!(event.records.len(), 2);
        assert_eq!(event.records[0].event_name, "INSERT");
        assert_eq!(
            event.records[0]
                .dynamodb
                .new_image
                .get("Name")
                .unwrap()
                .as_s(),
            Some("new-item")
        );
        assert_eq!(event.records[1].event_name, "MODIFY");
        assert_eq!(
            event.records[1]
                .dynamodb
                .old_image
                .get("Name")
                .unwrap()
                .as_s(),
            Some("new-item2")
        );
    }

    #[test]
    fn test_dynamodb_into_event() {
        let ddb_event = get_ddb_event();

        let events = ddb_event // duplicat from production code
            .records
            .iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<Event>, _>>()
            .unwrap();

        assert_eq!(events.len(), 2);
        match &events[0] {
            Event::Created { product } => {
                assert_eq!(product.id, "101");
                assert_eq!(product.name, "new-item");
                assert_eq!(product.price, 10.5);
            }
            _ => {
                assert!(false)
            }
        };
        match &events[1] {
            Event::Updated { new, old } => {
                assert_eq!(new.id, "102");
                assert_eq!(new.name, "new-item2");
                assert_eq!(new.price, 30.5);
                assert_eq!(old.id, "102");
                assert_eq!(old.name, "new-item2");
                assert_eq!(old.price, 20.5);
            }
            _ => {
                assert!(false)
            }
        };
    }

    #[test]
    fn test_dynamodb_into_product() {
        let ddb_event = get_ddb_event();

        let product: Product = (&ddb_event.records[0].dynamodb.new_image)
            .try_into()
            .unwrap();

        assert_eq!(product.id, "101");
        assert_eq!(product.name, "new-item");
        assert_eq!(product.price, 10.5);
    }
}
