use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use axum::{Json, Router, routing::get};
use axum::extract::{Query, State};
use futures_util::TryStreamExt;
use serde_derive::{Deserialize, Serialize};
use sqlx::{Column, Row, SqlitePool};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

mod config;

#[derive(Clone)]
struct AppState {
    pub config: config::Root,
    pub connections: Vec<RelationReader>,
    pub field_relations: HashMap<String, Vec<RelationReader>>,
    pub fields: HashMap<String, config::Field>,
}

#[derive(Clone)]
struct RelationReader {
    cfg: config::Relation,
    db: SqlitePool,
}

impl RelationReader {
    pub async fn from_config(cfg: &config::Relation) -> Result<RelationReader, Error> {
        if cfg.fields.is_empty() {
            return Err(anyhow!("relation does not have any field"));
        }
        match SqlitePool::connect(&cfg.connect).await {
            Ok(p) => Ok(RelationReader {
                cfg: cfg.clone(),
                db: p,
            }),
            Err(why) => {
                Err(anyhow!("failed to connect to sqlite database `{}` for relation {}: {}",
                    &cfg.connect, &cfg.name, why))
            }
        }
    }
}

impl RelationReader {
    pub async fn query(&mut self, field: &String, value: &String) -> Result<HashMap<String, String>, Error> {
        let fields = self.cfg.fields.iter().map(|r| {
            return format!("({}) as {}", r.query, r.id);
        }).reduce(|acc, s| acc + "," + &*s)
            .expect("relation does not have fields");
        let sql = format!(
            "select {fields} from {table_name} where {field} = ?",
            table_name = &self.cfg.table_name,
        );
        debug!("SQL: {}", sql);
        let mut rows = sqlx::query(&*sql).bind(value).fetch(&self.db);
        let mut rr: HashMap<String, String> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            for c in row.columns() {
                let name = String::from(c.name());
                let value: String = match row.try_get(&*name) {
                    Ok(v) => v,
                    Err(why) => {
                        return Err(anyhow!("failed to get field `{}` when querying relation {}: {}",
                        field, self.cfg.name, why));
                    }
                };
                rr.insert(name, value);
            }
        }
        Ok(rr)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    debug!("debug log is enabled");

    let cfg = config::read_file("config.toml")
        .expect("error loading config file");

    let field_names = get_fields(&cfg).expect("error reading fields");

    let mut all_relation_names = HashSet::new();
    for r in &cfg.relations {
        if !all_relation_names.insert(&r.name) {
            panic!("duplicate relation name `{r}`", r = r.name);
        }
        for f in &r.fields {
            if !field_names.contains(&f.id) {
                panic!("undeclared field `{}` used in relation `{}`, you have to declare it in `fields`",
                       f.id, r.name);
            }
        }
    }


    let mut relations = Vec::new();
    for r in &cfg.relations {
        relations.push(match RelationReader::from_config(r).await {
            Ok(v) => v,
            Err(why) => panic!("error loading relation `{}`: {}", r.name, why),
        });
    }

    let mut field_relations: HashMap<String, Vec<RelationReader>> = HashMap::new();
    for r in &relations {
        for f in &r.cfg.fields {
            if !field_relations.contains_key(&f.id) {
                field_relations.insert(f.id.clone(), Vec::new());
            }
            field_relations.get_mut(&f.id).unwrap().push(r.clone());
        }
    }

    let fields: HashMap<String, config::Field> = cfg.fields.iter().map(|f| (f.id.clone(), f.clone())).collect();

    let endpoint = cfg.listen.clone();

    let state = Arc::new(AppState {
        config: cfg,
        connections: relations,
        field_relations,
        fields,
    });

    // build our application with a single route
    let app = Router::new()
        .route("/query", get(query))
        .with_state(state);

    // run our app with hyper, listening globally on port 3000
    info!("starting HTTP server on {}", endpoint);
    let listener = tokio::net::TcpListener::bind(endpoint).await
        .expect("error binding tcp socket");
    axum::serve(listener, app).await
        .expect("error running http server");
}

#[derive(Eq, PartialEq, Clone)]
#[derive(Hash)]
struct RelationFieldValue {
    relation: String,
    field: String,
    value: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QueryResponse {
    pub success: bool,
    pub message: String,
    pub data: BTreeMap<String, Vec<String>>,
}


async fn query(
    State(state): State<Arc<AppState>>,
    Query(args): Query<HashMap<String, String>>,
) -> Json<QueryResponse> {
    // Input: known field values
    // Output: all reachable field values
    //
    // Program:
    //   populate unvisited field value set with separate initial conditions
    //   for all (field, value) in unvisited field value set:
    //     for all relation containing this field:
    //       mark (relation, field, value) as visited
    //       get all other field values from relation with field, for each (field2, value2),
    //       add all unvisited combinations (relation, field2, value2) to the unvisited field value set
    //       stop if the unvisited set is empty, or iteration count exceeds limit

    let mut unvisited = HashSet::new();
    let mut visited = HashSet::new();
    let known_field_values = args;
    // for the first query, we use even non-distinct fields as query condition
    for (field, value) in known_field_values {
        let relations = match state.field_relations.get(&field) {
            Some(v) => v,
            None => {
                return Json(QueryResponse {
                    success: false,
                    message: format!("no relation has field `{}`", &field),
                    data: Default::default(),
                });
            }
        };
        for r in relations {
            unvisited.insert(RelationFieldValue {
                relation: r.cfg.name.clone(),
                field: field.clone(),
                value: value.clone(),
            });
        }
    }
    // TODO make this configurable
    const MAX_DEPTH: i32 = 10;
    let mut depth = 0;
    let mut depth_limit_exceeded = false;
    let mut all_result: HashMap<_, HashSet<String>> = HashMap::new();
    while !unvisited.is_empty() {
        if depth > MAX_DEPTH {
            depth_limit_exceeded = true;
            break;
        }
        // visit a cloned snapshot, updates will be reflected at once in the next loop round
        for task in unvisited.clone() {
            let (field, value) = (&task.field, &task.value);
            let mut relations = match state.field_relations.get(field) {
                Some(v) => (*v).clone(),
                None => {
                    return Json(QueryResponse {
                        success: false,
                        message: format!("no relation has field `{}`", field),
                        data: Default::default(),
                    });
                }
            };
            for rel in relations.iter_mut() {
                info!("visit: relation {}, field {}, value {}", rel.cfg.name, field, value);
                // ensure every (relation, field, value) is visited only once
                if !visited.insert(RelationFieldValue {
                    relation: rel.cfg.name.clone(),
                    field: field.clone(),
                    value: value.clone(),
                }) {
                    continue;
                }
                let result = match rel.query(field, value).await {
                    Ok(v) => v,
                    Err(why) => {
                        return Json(QueryResponse {
                            success: false,
                            message: format!("failed to query relation `{r}` with field `{f}`, value `{v}`: {why}",
                                             r = &rel.cfg.name, f = field, v = value),
                            data: Default::default(),
                        });
                    }
                };
                for (field, value) in result {
                    if let Some(set) = all_result.get_mut(&field) {
                        set.insert(value.clone());
                    } else {
                        let mut s = HashSet::new();
                        s.insert(value.clone());
                        all_result.insert(field.clone(), s);
                    }
                    let v = RelationFieldValue {
                        relation: rel.cfg.name.clone(),
                        field: field.clone(),
                        value,
                    };
                    // skip non-distinct fields to prevent generating irrelevant results
                    if !state.fields.get(&field).expect("missing field info").distinct {
                        continue;
                    }
                    if visited.contains(&v) {
                        // don't re-add visited values
                        continue;
                    }
                    unvisited.insert(v);
                }
                unvisited.remove(&RelationFieldValue {
                    relation: rel.cfg.name.clone(),
                    field: field.clone(),
                    value: value.clone(),
                });
            }
        }
        depth += 1;
    }
    // use BTree map to keep the result ordered
    let all_result: BTreeMap<String, Vec<String>> = all_result.iter().map(|(k, v)| {
        let mut v: Vec<String> = v.iter().map(|v| v.clone()).collect();
        v.sort();
        (k.clone(), v)
    }).collect();
    Json(QueryResponse {
        success: true,
        message: String::from(if depth_limit_exceeded { "depth length limit exceeded" } else { "" }),
        data: all_result,
    })
}


fn get_fields(cfg: &config::Root) -> Result<HashSet<String>, anyhow::Error> {
    let mut s: HashSet<String> = HashSet::new();
    for field in &cfg.fields {
        if !s.insert(field.id.clone()) {
            return Err(anyhow!("duplicate field `{:?}`", &field.id));
        }
    }
    Ok(s)
}