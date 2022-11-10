use tinytemplate::TinyTemplate;

use crate::db_service::DbService;

pub struct KafkaProducer {
    db_service: DbService
}

impl KafkaProducer {
    pub fn init(db_service: DbService) -> KafkaProducer {
        KafkaProducer {
            db_service: db_service
        }
    }

    pub fn produce(&self, num_of_threads: i32) {
        let count = self.db_service.get_page_count();

        for page in 0..count {
            tokio::spawn(async move {
                println!("started new batch");
                let data = self.db_service.get_customers_by_page(page);
                
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
    }
}