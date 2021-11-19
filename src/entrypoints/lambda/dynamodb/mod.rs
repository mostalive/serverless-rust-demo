use crate::{domain, event_bus::EventBus, Error, Event};
use lambda_runtime::Context;
use rayon::prelude::*;
use tracing::{info, instrument};



pub mod model;

/**
 * Parse outside of DynamoDBEvent, but not yet the records
 * This allows us to report errors with the incoming events' JSON syntax
 * Once it is a DynamoDbEvent, the fieldnames have been rewritten, which makes debugging harder.
 */
#[instrument(skip(event_bus, event))]
pub async fn handle_events(
    event_bus: &dyn EventBus<E = Event>,
    event: serde_json::Value,
    _: Context,
) -> Result<(), Error> {
   handle_events_unboxed(event_bus, event).await
}

pub async fn handle_events_unboxed(
    event_bus: &dyn EventBus<E = Event>,
    event: serde_json::Value,
) -> Result<(), Error> {
    info!("Handle events");
    info!("Transform events");
    let events = json_to_ddb_event_structs(event.clone()); // cloning not optimal, but still cheaper than printing
    match events {
        Err(err) => {
            return Err(err);
        }
        Ok(evs) => {
         let result = dispatch_events(event_bus, evs).await;
         return result;
      }
    }
}

fn json_to_ddb_event_structs(event: serde_json::Value) -> Result<Vec<Event>, Error> {
   let result =
        serde_json::from_value(event.clone()).map(|ddb_event| parse_ddb_events(ddb_event))
        .map_err(|e|
          {
          let incoming_event = serde_json::to_string_pretty(&event).unwrap();
          let message = format!("Error parsing dynamo db events:\n{}\nReceived Event Json:\n{}", e, incoming_event);

          Error::ClientError("Error parsing json") });
    result?
}

fn parse_ddb_events(ddb_event: model::DynamoDBEvent) -> Result<Vec<Event>, Error> {
    return ddb_event
        .records
        .par_iter()
        .map(|r| r.try_into())
        .collect::<Result<Vec<Event>, _>>();
}

/// Parse events from DynamoDB Streams and dispatch to event bus
#[instrument(skip(event_bus, events))]
pub async fn dispatch_events(
    event_bus: &dyn EventBus<E = Event>,
    events: Vec<Event>,
) -> Result<(), Error> {
    info!("Dispatching {} events", events.len());
    domain::send_events(event_bus, &events).await?;
    info!("Done dispatching events");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // to restore the id, add this in NewImage:   "Id":  { "S": "0F7ylDuZdSWz77F9"}
    fn ddb_event_with_missing_id() -> model::DynamoDBEvent {
        let data = serde_json::json!(
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
            "eventSourceARN": "arn:aws:dynamodb:eu-central-1:your_aws_id:table/rust-products-Table-VNYFY0FE9HRT/stream/2021-11-16T14:43:56.931",
            "eventName": "INSERT",
            "eventSource": "aws:dynamodb"       }]
        });

        let event: model::DynamoDBEvent = serde_json::from_value(data).unwrap();
        event
    }
    fn v1_1_insert_then_remove_events() -> model::DynamoDBEvent {
        let event_json = serde_json::json!(
         {
             "Records": [
                 {
                     "eventVersion": "1.1",
                     "eventSourceARN": "arn",
                     "eventSource": "aws:dynamodb",
                     "eventName": "INSERT",
                     "eventID": "d69bccd67495b56b4f20e519affc36a3",
                     "dynamodb": {
                         "StreamViewType": "NEW_AND_OLD_IMAGES",
                         "SizeBytes": 70,
                         "SequenceNumber": "100000000015685215244",
                         "NewImage": {
                             "price": {
                                 "N": "39.2717623435658"
                             },
                             "name": {
                                 "S": "yOMOpvOushHuresH"
                             },
                             "id": {
                                 "S": "fy4HHRVQnwUEhfbP"
                             }
                         },
                         "Keys": {
                             "id": {
                                 "S": "fy4HHRVQnwUEhfbP"
                             }
                         },
                         "ApproximateCreationDateTime": 1637143367.0},
                     "awsRegion": "eu-central-1"
                 },
                 {
                     "eventVersion": "1.1",
                     "eventSourceARN": "arn",
                     "eventSource": "aws:dynamodb",
                     "eventName": "REMOVE",
                     "eventID": "16d28ae84dd7c907550d1c96b0bff53f",
                     "dynamodb": {
                         "StreamViewType": "NEW_AND_OLD_IMAGES",
                         "SizeBytes": 70,
                         "SequenceNumber": "200000000015685215680",
                         "OldImage": {
                             "price": {
                                 "N": "39.2717623435658"
                             },
                             "name": {
                                 "S": "yOMOpvOushHuresH"
                             },
                             "id": {
                                 "S": "fy4HHRVQnwUEhfbP"
                             }
                         },
                         "Keys": {
                             "id": {
                                 "S": "fy4HHRVQnwUEhfbP"
                             }
                         },
                         "ApproximateCreationDateTime": 1637143368.0
                     },
                     "awsRegion": "eu-central-1"
                 }
             ]
         }
        );

        let event: model::DynamoDBEvent = serde_json::from_value(event_json).unwrap();
        event
    }
    #[test]
    fn version_1_1_to_product_created_event_fails_when_id_missing() {
        let ddb_event = ddb_event_with_missing_id();
        let result_events = parse_ddb_events(ddb_event);

        match result_events {
            Err(err) => {
                let message = format!("{}", err);
                assert_eq!(message, "InternalError: Missing id in lambda");
            }
            _ => {
                panic!("Expected parsing to fail with missing ID");
            }
        }
    }

    #[test]
    fn can_parse_v1_1_insert_event_product_id() {
        let ddb_event = v1_1_insert_then_remove_events();
        let result_events = parse_ddb_events(ddb_event);

        match result_events {
            Err(err) => {
                panic!("Expected parsing to succeed but got: {}", err);
            }
            Ok(events) => {
                let created: &Event = &events[0];
                match created {
                    Event::Created { product } => {
                        assert_eq!(product.id, "fy4HHRVQnwUEhfbP");
                    }
                    _ => {
                        panic!("Expected Created event, but was something else");
                    }
                }
            }
        }
    }
    #[test]
    fn can_parse_v1_1_remove_event_product_id() {
        let ddb_event = v1_1_insert_then_remove_events();
        let result_events = parse_ddb_events(ddb_event);

        match result_events {
            Err(err) => {
                panic!("Expected parsing to succeed but got: {}", err);
            }
            Ok(events) => {
                let created: &Event = &events[1];
                match created {
                    Event::Deleted { product } => {
                        assert_eq!(product.id, "fy4HHRVQnwUEhfbP");
                        assert_eq!(product.name, "yOMOpvOushHuresH");
                        assert_eq!(product.price, 39.2717623435658);
                    }
                    v => {
                        panic!("Expected Deleted event, but was something else:\n{:#?}", v);
                    }
                }
            }
        }
    }
}
