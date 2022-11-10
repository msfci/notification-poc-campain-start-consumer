use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub msisdn: Option<String>,
    pub name: Option<String>,
    pub segment: Option<String>,
    pub balance: Option<String>
}
