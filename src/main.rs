use osu_db::replay::Replay;
use radix_trie::Trie;
use std::{
    collections::{HashSet, HashMap},
    env,
    fs::File, fs,
    io::{prelude::*, BufReader, Read},
    iter::FromIterator,
    sync::{Arc, Mutex},
    path::Path,
    vec::Vec};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use rayon::prelude::*;
use dotenv::dotenv;
use mongodb::{options::{ClientOptions, FindOptions, FindOneAndReplaceOptions}, sync::Client, bson::doc, bson};
use rusoto_s3::{S3Client, S3, PutObjectRequest};
use rusoto_core::{Region, request::HttpClient};
use futures::executor;
use rusoto_credential::StaticProvider;

#[derive(Serialize, Deserialize)]
struct JsonReplay {
    pub mode: i32,
    pub version: i32,
    pub beatmap_hash: String,
    pub player_name: String,
    pub replay_hash: String,
    pub count_300: i16, pub count_100: i16, pub count_50: i16,
    pub count_geki: i16, pub count_katsu: i16, pub count_miss: i16,
    pub score: i32, pub max_combo: i16, pub perfect_combo: bool,
    pub mods: i32,
    pub timestamp: String,
    pub sha256: String
}

#[tokio::main]
async fn main() {
    let allowed_usernames: HashSet<&str> = HashSet::from_iter(vec! ["Akane Butterfly", "minhducsun2002", "61ph3r" ]);
    let mut cached= Arc::new(Mutex::new(HashMap::new()));

    {
        println!("Reading hashes from cache file...");
        let file = File::open(".cache");

        if let Ok(file) = file {
            let buffer : HashMap<String, String> = BufReader::new(file)
                .lines()
                .map(|line| line.unwrap())
                .map(|line| {
                    let split: Vec<&str> = line.split(" = ").collect();
                    (split[0].to_owned(), split[1].to_owned())
                })
                .collect();
            let len = buffer.len();
            cached = Arc::new(Mutex::new(buffer));
            println!("Populated {} entries", len);
        };
    }

    dotenv().ok();
    let credentials = StaticProvider::new_minimal(
        env::var("S3_KEY_ID").unwrap(),
        env::var("S3_KEY").unwrap()
    );
    let s3client = S3Client::new_with(
        HttpClient::new().expect("Failed to create HTTP client for S3"),
        credentials,
        Region::Custom {
            name: "backblaze".to_owned(),
            endpoint: env::var("S3_ENDPOINT").unwrap()
        }
    );

    let path = env::args_os().nth(1).unwrap();
    println!("Downloading replay list...");

    let db_uri = env::var("MONGODB_URI").unwrap();
    let db_name = env::var("MONGODB_REPLAY_DB").unwrap();
    let db_collection = env::var("MONGODB_REPLAY_COLLECTION").unwrap();

    let client = Client::with_options(ClientOptions::parse(&db_uri).unwrap()).unwrap();
    let collection = client.database(&db_name).collection::<JsonReplay>(&db_collection);

    let replays = collection.find(doc! {}, FindOptions::builder().build()).unwrap();
    let results: Vec<Result<JsonReplay, _>> = replays.collect();
    let hashes: Vec<(String, bool)> = results.iter()
        .map(|res| (res.as_ref().unwrap().sha256.to_owned(), true))
        .collect();
    let trie = Trie::from_iter(hashes);
    println!("{} replays found", results.len());
    println!("Scanning for files in {:?}", path);

    let files : Vec<_> = fs::read_dir(&Path::new(&path)).unwrap().map(|f| f.unwrap().path()).collect();

    println!("Computing SHA256 hashes for files...");

    files.par_iter().for_each(|path| {
        let cached = Arc::clone(&cached);
        let mut hashes = cached.lock().unwrap();

        let full_path = path.clone().as_os_str().to_str().unwrap().to_owned();
        if !hashes.contains_key(&full_path)
        {
            let mut buffer : Vec<u8> = Vec::new();
            File::read_to_end(&mut File::open(path.clone()).unwrap(), &mut buffer).unwrap();
            let hash = hex::encode(Sha256::digest(&buffer));
            hashes.insert(full_path, hash);
            println!("Computing hash for file {:?}", path.clone());
        }
    });

    let readonly_hashes : HashMap<String, String> = cached.lock().unwrap().iter()
        .map(|tuple| (tuple.0.to_owned(), tuple.1.to_owned()))
        .collect();

    println!("Scanning for new files...");
    files.iter().for_each(|path| {
        let full_path = path.clone().as_os_str().to_str().unwrap().to_owned();

        let hash = readonly_hashes.get(&full_path).unwrap().to_owned();

        if trie.get(&hash) == None {
            println!("=> New replay found : {:?}", path.clone());
            let mut buffer : Vec<u8> = Vec::new();
            File::read_to_end(&mut File::open(path.clone()).unwrap(), &mut buffer).unwrap();
            let replay = Replay::from_file(path).unwrap();
            let json_replay = JsonReplay {
                mode: replay.mode as i32,
                version: replay.version as i32,
                beatmap_hash: replay.beatmap_hash.clone().unwrap(),
                player_name: replay.player_name.to_owned().unwrap(),
                replay_hash: replay.replay_hash.unwrap(),

                count_300: replay.count_300 as i16,
                count_100: replay.count_100 as i16,
                count_50: replay.count_50 as i16,

                count_geki: replay.count_geki as i16,
                count_katsu: replay.count_katsu as i16,
                count_miss: replay.count_miss as i16,

                score: replay.score as i32, max_combo: replay.max_combo as i16, perfect_combo: replay.perfect_combo,

                mods: replay.mods.bits() as i32, timestamp: replay.timestamp.to_rfc3339() + "Z",
                sha256: hash.to_owned(),
            };
            let file_name = replay.timestamp.to_rfc3339() + " - " + replay.player_name.unwrap().as_ref() + ".osr";
            let username = json_replay.player_name.to_owned();
            if !(username.is_empty() || (allowed_usernames.contains(&username as &str))) {
                return;
            }

            println!("   Uploading as {}/{}", json_replay.beatmap_hash, file_name);
            let future_result = s3client.put_object(
                PutObjectRequest {
                    bucket: env::var("S3_BUCKET_NAME").unwrap(),
                    key: json_replay.beatmap_hash.to_owned() + "/" + &file_name,
                    body: Some(buffer.into()),
                    ..Default::default()
                }
            );
            let result = executor::block_on(future_result);

            match result {
                Ok(_) => {
                    let result = collection.find_one_and_replace(
                        doc! { "sha256": hash },
                        json_replay,
                        FindOneAndReplaceOptions::builder().upsert(true).build()
                    );
                    match result {
                        Ok(_) => { println!("   Uploaded document to MongoDB instance.") },
                        Err(err) => { eprintln!("{:?}", err); }
                    }
                    println!("   Done.")
                },
                Err(error) => { eprintln!("{:?}", error); }
            };
        }
    });

    let hashes : Vec<String> = readonly_hashes
        .iter()
        .map(|entry| entry.0.to_owned() + " = " + entry.1)
        .collect();

    fs::write(".cache", hashes.join("\n")).unwrap();
}