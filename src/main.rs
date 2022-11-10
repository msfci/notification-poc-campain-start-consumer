#[macro_use]
extern crate rbatis;
extern crate rbdc;

mod connection;
mod model;
mod producer;
mod db_service;
mod kafka_producer;


use std::{error::Error, time::{Instant}};

use futures::{stream::FuturesUnordered, future};
use rbatis::Rbatis;
use rbdc_oracle::{driver::OracleDriver, options::OracleConnectOptions};
use rdkafka::{ClientConfig, consumer::{StreamConsumer, Consumer}};
use serde::{Serialize};
use tinytemplate::TinyTemplate;
use ::futures::{TryStreamExt};

use crate::{connection::Connection, db_service::DbService, kafka_producer::KafkaProducer};

static TEMPLATE : &'static str = "Hello {name}, your mobile {msisdn}, earned {half_balance} points for Amla elolal service call *911# to run";

#[derive(Serialize)]
struct Context {
    name: String,
    msisdn: String,
    half_balance: f32
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let db_service = DbService::init("", "", "");

    let kafka_producer = KafkaProducer::init(db_service);

    db_service.get_all();

    kafka_producer.produce(1000);









    let start = Instant::now();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "start_campain_consumer_group")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");
    
    let topic = "campain_start_topic";

    consumer.subscribe(&vec![topic])
        .expect("Can't subscribe to specified topic");

    let count = 1000000;
    let page_size = 10000;

    let no_of_pages = count / page_size;

    let stream_processor = consumer.stream().try_for_each(|_| async move {
        println!("recieved ne campain");
        let db_connection = Connection::init("MOSTAFA", "P00sswd", "//10.237.71.79/orcl_pdb");

        for page in 0..no_of_pages {
            let local_rb = db_connection.rb.clone();

            tokio::spawn(async move {
                // let data = connection::select_page(&mut RB, page*page_size, page_size).await.unwrap();
                println!("started new batch");
                let data = connection::select_page_raw(&local_rb, page*page_size, page_size).await.unwrap();
                
                let mut handels = vec![];

                data
                .iter()
                .for_each(move |customer| { 
                    handels.push(async move {
                        // tokio::spawn(async move {
                        // let tasks: Vec<_> = customers_chunck
                        //     .iter()
                        //     .map(|customer| async move {
                                let mut tt = TinyTemplate::new();
                                tt.add_template("offer", TEMPLATE).unwrap();
                                
                                let context = Context {
                                    name: customer.name.clone().unwrap(),
                                    msisdn: customer.msisdn.clone().unwrap(),
                                    half_balance: customer.balance.clone().unwrap().parse::<f32>().unwrap() * 0.5
                                };

                                let rendered = tt.render("offer", &context).unwrap();

                                // println!("Message: {}", rendered);
                        
                                let producer = producer::Producer::init();
                                
                                let customer_msisdn = customer.msisdn.clone().unwrap();
                                // tokio::task::spawn_blocking(|| async move {
                                // println!("producing new message {}", &customer_msisdn);
                                
                                let produce_future = producer.produce("offer_message_new", &rendered, &customer_msisdn);
                                match produce_future.await {
                                    Ok(delivery) => println!("Sent: {:?}", delivery),
                                    Err((e, _)) => println!("Error: {:?}", e),
                                }
                            // }).collect();
                        //Ok(())
                    });
                });

                for handle in handels {
                    handle.await
                }
            });
                    
        }
        Ok(())
    });

    stream_processor.await.expect("stream processing failed");

    let duration = start.elapsed();

    println!("Time elapsed is: {:?}", duration);
    Ok(())
}
