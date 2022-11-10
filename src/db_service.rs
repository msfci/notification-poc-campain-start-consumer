use rayon::vec;

use crate::{connection::Connection, model::Customer};

pub struct DbService {
    pub db_connection: Connection
}

impl DbService {
    pub fn init(username: &str, password: &str, url: &str) -> DbService{
        DbService {
            db_connection: Connection::init("MOSTAFA", "P00sswd", "//10.237.71.79/orcl_pdb")
        }
    }

    pub fn get_customers_by_page(&self, page_id: i32) -> Vec<Customer> {
        self.db_connection.rb.clone();

        vec![]
    }

    pub fn get_all(&self) {

    }

    pub fn get_page_count(&self) -> i64 {
        return 1000000;
    }
}