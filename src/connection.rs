use rbatis::Rbatis;
use rbdc_oracle::{options::OracleConnectOptions, driver::OracleDriver};
use rbs::to_value;

use crate::model::Customer;

crud!(Customer {});

pub struct Connection {
    pub rb: Rbatis
}

impl Connection {
    pub fn init(username: &str, password: &str, url: &str) -> Connection{
        let rb = Rbatis::new();
        rb.init_opt(
            OracleDriver {},
            OracleConnectOptions {
                username: username.to_string(), 
                password: password.to_string(),
                connect_string: url.to_string(),
            },
        ).expect("rbatis link database fail");

        Connection { rb: rb }
    }

    
}

#[py_sql(
    "`SELECT * from CUSTOMER` 
    ` ORDER BY msisdn OFFSET ${page_no} ROWS FETCH NEXT ${page_size} ROWS ONLY`")]
pub async fn select_page(rb: &mut dyn rbatis::executor::Executor, page_no:u64, page_size:u64) -> std::result::Result<Vec<Customer>, rbdc::Error> {impled!()}

pub async fn select_page_raw(rb:&Rbatis, page_no:u64, page_size:u64) -> std::result::Result<Vec<Customer>, rbdc::Error> {
    rb
    .fetch_decode("SELECT * from CUSTOMER 
    ORDER BY msisdn OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", vec![to_value!(page_no), to_value!(page_size)])
    .await
}