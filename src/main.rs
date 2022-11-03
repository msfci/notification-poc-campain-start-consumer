#[macro_use]
extern crate rbatis;
extern crate rbdc;

use std::{error::Error, time::{Duration, Instant}, sync::{Mutex, Arc}, thread, vec};

use rbatis::{Rbatis};
use rbdc_oracle::{driver::OracleDriver, options::OracleConnectOptions};
use rdkafka::{producer::{FutureProducer, FutureRecord, self}, ClientConfig};
use serde::{Deserialize, Serialize};
use tinytemplate::TinyTemplate;
use rayon::prelude::*;
use tokio::{runtime::Runtime, spawn};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub msisdn: Option<String>,
    pub name: Option<String>,
    pub segment: Option<String>,
    pub balance: Option<String>
}

crud!(Customer {});

#[py_sql(
    "`SELECT * from CUSTOMER` 
    ` ORDER BY msisdn OFFSET ${page_no} ROWS FETCH NEXT ${page_size} ROWS ONLY`")]
async fn select_page(rb: &mut dyn rbatis::executor::Executor, page_no:u64, page_size:u64) -> std::result::Result<Vec<Customer>, rbdc::Error> {impled!()}

static TEMPLATE : &'static str = "Hello {name}, your mobile {msisdn}, earned {half_balance} points for Amla elolal service call *911# to run";

#[derive(Serialize)]
struct Context {
    name: String,
    msisdn: String,
    half_balance: f32
}

async fn produce(producer: &FutureProducer, topic_name: &str, message: &str, key: &str) {
    let _delivery_status = producer
        .send(
            FutureRecord::to(topic_name)
                .payload(&format!("{}", message))
                .key(&format!("{}", key)),
                Duration::from_secs(0),
        )
        .await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let mut rb = Rbatis::new();
    rayon::ThreadPoolBuilder::new().num_threads(100).build_global().unwrap();
    let start = Instant::now();
    
    rb.init_opt(
        OracleDriver {},
        OracleConnectOptions {
            username: "MOSTAFA".to_string(), 
            password: "P00sswd".to_string(),
            connect_string: "//10.237.71.79/orcl_pdb".to_string(),
        },
    )
    .expect("rbatis link database fail");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Producer creation error");

    let count = 1000000;
    let page_size = 10000;

    let no_of_pages = count / page_size;

    (0..no_of_pages).into_par_iter().for_each(|page| {
        let mut rb = rb.clone();
        let rt = Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let data = handle.block_on(
            select_page(&mut rb, page*page_size, page_size)
        ).unwrap();
    
        data.into_iter()
        .for_each( |customer| {
            let producer = producer.clone();

            let mut tt = TinyTemplate::new();
            tt.add_template("offer", TEMPLATE).unwrap();
            
            let context = Context {
                name: customer.name.clone().unwrap(),
                msisdn: customer.msisdn.clone().unwrap(),
                half_balance: customer.balance.clone().unwrap().parse::<f32>().unwrap() * 0.5
            };

            let rendered = tt.render("offer", &context).unwrap();

            println!("Message: {}", rendered);
    
            rayon::spawn(move || {
                let rt = Runtime::new().unwrap();
                let handle = rt.handle().clone();
                handle.block_on(
                    produce(&producer, "offer_message", &rendered, &customer.msisdn.clone().unwrap())
                );
            });
            // produce(producer, "offer_message", &rendered, &customer.msisdn.clone().unwrap()).await;
        });
    });
    let duration = start.elapsed();

    println!("Time elapsed in expensive_function() is: {:?}", duration);
    Ok(())
}
