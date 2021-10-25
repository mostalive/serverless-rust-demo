use super::{AllResponse, Store};
use crate::{Error, Product};
use async_trait::async_trait;
use aws_sdk_dynamodb::{model::AttributeValue, Client};
use std::collections::HashMap;
use std::str;
use tracing::instrument;

pub struct DynamoDBStore<C> {
    client: Client<C>,
    table_name: String,
}

enum ValueType {
    N,
    S,
}

impl<C> DynamoDBStore<C>
where C: aws_smithy_client::bounds::SmithyConnector,
{
    pub fn new(client: Client<C>, table_name: &str) -> DynamoDBStore<C> {
        DynamoDBStore {
            client,
            table_name: table_name.to_owned(),
        }
    }
}

trait ProductDynamoDBStoreExt {
    fn from_dynamodb(value: HashMap<String, AttributeValue>) -> Result<Product, Error>;
    fn to_dynamodb(&self) -> HashMap<String, AttributeValue>;
}

impl ProductDynamoDBStoreExt for Product {
    fn from_dynamodb(value: HashMap<String, AttributeValue>) -> Result<Product, Error> {
        Ok(Product {
            id: get_key("id", ValueType::S, &value)?,
            name: get_key("name", ValueType::S, &value)?,
            price: get_key("price", ValueType::N, &value)?.parse::<f64>()?,
        })
    }

    fn to_dynamodb(&self) -> HashMap<String, AttributeValue> {
        let mut retval = HashMap::new();
        retval.insert("id".to_owned(), AttributeValue::S(self.id.clone()));
        retval.insert("name".to_owned(), AttributeValue::S(self.name.clone()));
        retval.insert(
            "price".to_owned(),
            AttributeValue::N(format!("{:}", self.price)),
        );

        retval
    }
}

fn get_key(
    key: &str,
    t: ValueType,
    item: &HashMap<String, AttributeValue>,
) -> Result<String, Error> {
    let v = item
        .get(key)
        .ok_or_else(|| Error::InternalError(format!("Missing '{}'", key)))?;

    Ok(match t {
        ValueType::N => v.as_n()?.to_owned(),
        ValueType::S => v.as_s()?.to_owned(),
    })
}

#[async_trait]
impl<C> Store for DynamoDBStore<C>
where C: aws_smithy_client::bounds::SmithyConnector,
{
    #[instrument(skip(self))]
    // Get all items
    async fn all(&self, next: Option<&str>) -> Result<AllResponse, Error> {
        // Scan DynamoDB table
        let mut req = self.client.scan().table_name(&self.table_name);
        req = if let Some(next) = next {
            req.exclusive_start_key("id", AttributeValue::S(next.to_owned()))
        } else {
            req
        };
        let res = req.send().await?;

        // Build response
        let products = match res.items {
            Some(items) => items
                .into_iter()
                .map(Product::from_dynamodb)
                .collect::<Result<Vec<Product>, Error>>()?,
            None => Vec::default(),
        };
        let next = res
            .last_evaluated_key
            .map(|m| get_key("id", ValueType::S, &m).unwrap());
        Ok(AllResponse { products, next })
    }
    // Get item
    async fn get(&self, id: &str) -> Result<Option<Product>, Error> {
        let res = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("id", AttributeValue::S(id.to_owned()))
            .send()
            .await?;
        Ok(match res.item {
            Some(item) => Some(Product::from_dynamodb(item)?),
            None => None,
        })
    }
    // Create or update an item
    async fn put(&self, product: &Product) -> Result<(), Error> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(product.to_dynamodb()))
            .send()
            .await?;

        Ok(())
    }
    // Delete item
    async fn delete(&self, id: &str) -> Result<(), Error> {
        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key("id", AttributeValue::S(id.to_owned()))
            .send()
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use aws_sdk_dynamodb::{Client, Config, Credentials, Region};
    use aws_smithy_client::test_connection::TestConnection;
    use aws_smithy_http::body::SdkBody;

    // Config for mocking DynamoDB
    async fn get_mock_config() -> Config {
        let cfg = aws_config::from_env()
            .region(Region::new("eu-west-1"))
            .credentials_provider(Credentials::from_keys("accesskey", "privatekey", None))
            .load().await;

        Config::new(&cfg)
    }

    #[tokio::test]
    async fn test_all_empty() -> Result<(), Error> {
        // GIVEN a DynamoDBStore with no items
        let conn = TestConnection::new(vec![(
            http::Request::new(SdkBody::from("{\"TableName\": \"test\"}")),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from("{\"Items\": []}"))
                .unwrap(),
        )]);
        let client = Client::from_conf_conn(get_mock_config().await, conn);    
        let store = DynamoDBStore::new(client, "test");

        // WHEN getting all items
        let res = store.all(None).await?;

        // THEN the response is empty
        assert_eq!(res.products.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_all() -> Result<(), Error> {
        // GIVEN a DynamoDBStore with one item
        let conn = TestConnection::new(vec![(
            http::Request::new(SdkBody::from("{\"TableName\": \"test\"}")),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from("{\"Items\": [{\"id\": {\"S\": \"1\"}, \"name\": {\"S\": \"test1\"}, \"price\": {\"N\": \"1.0\"}}]}"))
                .unwrap(),
        )]);
        let client = Client::from_conf_conn(get_mock_config().await, conn);    
        let store = DynamoDBStore::new(client, "test");

        // WHEN getting all items
        let res = store.all(None).await?;

        // THEN the response has one item
        assert_eq!(res.products.len(), 1);
        // AND the item has the correct id
        assert_eq!(res.products[0].id, "1");
        // AND the item has the correct name
        assert_eq!(res.products[0].name, "test1");
        // AND the item has the correct price
        assert_eq!(res.products[0].price, 1.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_all_next() -> Result<(), Error> {
        // GIVEN a DynamoDBStore with a last evaluated key
        let conn = TestConnection::new(vec![(
            http::Request::new(SdkBody::from("{\"TableName\": \"test\"}")),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from("{\"Items\": [], \"LastEvaluatedKey\": {\"id\": {\"S\": \"1\"}}}"))
                .unwrap(),
        )]);
        let client = Client::from_conf_conn(get_mock_config().await, conn);    
        let store = DynamoDBStore::new(client, "test");

        // WHEN getting all items
        let res = store.all(None).await?;

        // THEN the response has a next key
        assert_eq!(res.next, Some("1".to_string()));

        Ok(())
    }
}
