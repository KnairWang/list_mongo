use anyhow::Result;
use mongodb::{
    bson::Document,
    options::{ClientOptions, Tls, TlsOptions},
    Client, Database,
};
use serde::Serialize;

const EXCLUDED_DB: [&str; 3] = ["admin", "config", "local"];

#[derive(Serialize)]
struct DatabaseInfo {
    name: String,
    collections: Vec<CollectionInfo>,
}

#[derive(Serialize)]
struct CollectionInfo {
    name: String,
    indexes: Vec<String>,
    doc_count: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = connect_to_db().await?;
    let mut result = get_databases(&client).await?;
    let handles = result.iter_mut().map(|db_info| {
        let client = client.clone();
        async move {
            let db = client.database(&db_info.name);
            db_info.collections = get_collections(&db).await?;
            let handles = db_info.collections.iter_mut().map(|coll_info| {
                let db = db.clone();
                async move {
                    let coll = db.collection::<Document>(&coll_info.name);
                    coll_info.doc_count = coll.estimated_document_count().await?;
                    let indexes = coll.list_index_names().await?;
                    coll_info.indexes = indexes;
                    anyhow::Ok(())
                }
            });
            futures::future::join_all(handles).await;
            anyhow::Ok(())
        }
    });
    futures::future::join_all(handles).await;

    Ok(())
}

async fn connect_to_db() -> Result<Client> {
    let uri = "mongodb://127.0.0.1:27017";
    let mut client_options = ClientOptions::parse(uri).await?;
    client_options.tls = Some(Tls::Enabled(
        TlsOptions::builder()
            .allow_invalid_certificates(Some(true))
            .build(),
    ));

    let client = Client::with_options(client_options)?;

    Ok(client)
}

async fn get_databases(client: &Client) -> Result<Vec<DatabaseInfo>> {
    let dbs = client.list_database_names().await?;
    let mut result = dbs
        .into_iter()
        .filter(|db| !EXCLUDED_DB.contains(&db.as_str()))
        .map(|db| DatabaseInfo {
            name: db,
            collections: Vec::new(),
        })
        .collect::<Vec<_>>();
    result.sort_by(|entry1, entry2| entry1.name.cmp(&entry2.name));
    Ok(result)
}

async fn get_collections(db: &Database) -> Result<Vec<CollectionInfo>> {
    let collection_names = db.list_collection_names().await?;
    let mut result = collection_names
        .into_iter()
        .map(|coll| CollectionInfo {
            name: coll,
            doc_count: 0,
            indexes: Vec::new(),
        })
        .collect::<Vec<_>>();
    result.sort_by(|e1, e2| e1.name.cmp(&e2.name));
    Ok(result)
}
