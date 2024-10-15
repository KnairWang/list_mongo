use std::{io::Write, sync::LazyLock};

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mongodb::{
    bson::Document,
    options::{ClientOptions, Tls, TlsOptions},
    Client, Database,
};
use serde::Serialize;

const EXCLUDED_DB: [&str; 3] = ["admin", "config", "local"];

static STYLE_OVERALL: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template("{bar:40.green/yellow} {prefix} {pos:>4}/{len:4}").unwrap()
});

static STYLE_DB: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "    {spinner:.green} {prefix} {msg} {bar:20.green/white} {pos:>4}/{len:4}",
    )
    .unwrap()
});

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
    let mp = MultiProgress::new();
    let client = connect_to_db().await?;
    let mut result = get_databases(&client).await?;

    let main_pb = mp.add(
        ProgressBar::new(result.len() as u64)
            .with_prefix("overall")
            .with_style(STYLE_OVERALL.clone()),
    );

    let handles = result.iter_mut().map(|db_info| {
        let client = client.clone();
        let mp = &mp;
        let main_pb = &main_pb;
        let pb = mp.add(
            ProgressBar::new(0)
                .with_prefix(db_info.name.clone())
                .with_style(STYLE_DB.clone()),
        );
        async move {
            let db = client.database(&db_info.name);
            db_info.collections = get_collections(&db).await?;
            pb.set_length(db_info.collections.len() as u64);
            let handles = db_info.collections.iter_mut().map(|coll_info| {
                let db = db.clone();
                let pb = &pb;
                async move {
                    let coll = db.collection::<Document>(&coll_info.name);
                    coll_info.doc_count = coll.estimated_document_count().await?;
                    let indexes = coll.list_index_names().await?;
                    coll_info.indexes = indexes;
                    pb.tick();
                    anyhow::Ok(())
                }
            });
            futures::future::join_all(handles).await;
            pb.finish_and_clear();
            main_pb.tick();
            anyhow::Ok(())
        }
    });
    futures::future::join_all(handles).await;
    main_pb.finish();

    let mut fp = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("./result.json")?;
    let output = serde_json::to_string_pretty(&result)?;
    fp.write_all(output.as_bytes())?;
    fp.flush()?;

    Ok(())
}

async fn connect_to_db() -> Result<Client> {
    let uri = std::env::args()
        .nth(1)
        .expect("MongoDB URI should be first arg");
    let mut client_options = ClientOptions::parse(uri).await?;
    client_options.tls = Some(Tls::Enabled(
        TlsOptions::builder()
            .allow_invalid_certificates(Some(true))
            .build(),
    ));
    println!("{:?}", client_options);

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
