#[macro_use]
extern crate rbatis;
extern crate rbdc;

use std::{error::Error, time::Duration};

use rbatis::{Rbatis, sql::PageRequest};
use rbdc_oracle::{driver::OracleDriver, options::OracleConnectOptions};
use rdkafka::{producer::{BaseProducer, BaseRecord, Producer}, ClientConfig};
use serde::{Deserialize, Serialize};
use tinytemplate::TinyTemplate;



#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub msisdn: String,
    pub name: String,
    pub segment: String,
    pub balance: f32
}

crud!(Customer {});
impl_select_page!(Customer{select_page() =>"
     if !sql.contains('count'):
       `order by msisdn desc`"});

static TEMPLATE : &'static str = "Hello ${name} , your mobile ${msisdnn} , earned ${half_balance} points for Amla elolal service call *911# to run";

#[derive(Serialize)]
struct Context {
    name: String,
    msisdn: String,
    half_balance: f32
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let mut rb = Rbatis::new();

    let mut tt = TinyTemplate::new();
    tt.add_template("offer", TEMPLATE)?;

    rb.init_opt(
        OracleDriver {},
        OracleConnectOptions {
            username: "MOSTAFA".to_string(), 
            password: "P00sswd".to_string(),
            connect_string: "//10.237.71.79/orcl_pdb".to_string(),
        },
    )
    .expect("rbatis link database fail");

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Producer creation error");
    
    let page_size = 10;

    for page in 0..5 {
        let data = 
            Customer::select_page(&mut rb, &PageRequest::new(page, page_size))
            .await
            .unwrap();

        for record in data.records.iter() {
            let context = Context {
                name: record.name.clone(),
                msisdn: record.msisdn.clone(),
                half_balance: record.balance * 0.5
            };
            let rendered = tt.render("offer", &context)?;
            
            producer.send(
                BaseRecord::to("offer_message")
                    .payload(&rendered)
                    .key(&record.msisdn),
            ).expect("Failed to enqueue");

        }
    }

    // Poll at regular intervals to process all the asynchronous delivery events.
    for _ in 0..10 {
        producer.poll(Duration::from_millis(100));
    }

    // And/or flush the producer before dropping it.
    producer.flush(Duration::from_secs(1));

    Ok(())
}
