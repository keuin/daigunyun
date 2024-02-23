use std::fs;
use std::path::Path;

use anyhow::anyhow;
use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Root {
    pub listen: String,
    pub fields: Vec<Field>,
    pub relations: Vec<Relation>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Field {
    pub id: String,
    #[serde(default = "distinct_default")]
    pub distinct: bool,
}

fn distinct_default() -> bool {
    false
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Relation {
    pub name: String,
    pub connect: String,
    pub table_name: String,
    pub fields: Vec<RelationField>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RelationField {
    pub id: String,
    pub query: String,
}

pub fn read_file<P: AsRef<Path>>(path: P) -> Result<Root, anyhow::Error> {
    let s = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(why) => {
            return Err(anyhow!("failed to read toml file: {}", why));
        }
    };
    match toml::from_str(&*s) {
        Ok(r) => Ok(r),
        Err(why) => Err(anyhow!("failed to decode toml file: {}", why)),
    }
}