// Zotify tagger
// Ari Rios <me@aririos.com>
// License: MIT
//!
//! Adds additional metadata (e.g. popularity, upstream links, explicit flag)
#![feature(closure_lifetime_binder)]

use anyhow::{Context, Result};
use async_scoped::{Scope, TokioScope};
use clap::{Parser, ValueEnum};
use dotenvy;
use ffmpeg_next::{
    Rational, Stream, codec, encoder,
    format::{self, context::Input},
    media,
};
use log::{debug, error, info, warn};
use rand::Rng;
use rspotify::{
    ClientCredsSpotify, ClientError, Credentials,
    http::HttpError,
    model::{ArtistId, Restriction, SimplifiedAlbum, SimplifiedArtist, TrackId, TrackLink, Type},
    prelude::*,
};
use serde::{Deserialize, Serialize};
// use sqlite::Connection;
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    sync::{Arc, Mutex, RwLock, mpsc},
    thread,
};
use std::{env, time::Duration};
use std::{
    fmt::Debug,
    fs::{self, DirEntry},
};
use std::{fmt::Display, io::Error};
use std::{path::PathBuf, time::Instant};
use strum::{EnumDiscriminants, EnumMessage};
use tokio::{self, sync::mpsc as async_mpsc};

#[derive(Parser, Debug)]
struct Args {
    /// Dry run (don't write to disk)
    #[arg(short, long)]
    dry_run: bool,
    #[arg(short, long, value_enum)]
    tag: Vec<TagMarker>,
    /// Load cache from disk (./tags_by_track.json). To reset the cache, simply delete the file
    #[arg(short, long)]
    use_cache: bool,
    /// Whether to overwrite existing tags on audio files
    #[arg(short, long)]
    overwrite_tags: bool,
}

/// ContextOrStream is used to abstract over metadata assigned to a container
/// or to a specific stream inside that container.
enum ContextOrStream<'a> {
    Context(&'a Input),
    Stream(&'a Stream<'a>),
}

enum InsertResult {
    Found,
    NotFound,
    Dup((String, PathBuf)),
    Error,
}

/// insert_song_path will insert a [PathBuf] matching a given [TrackId] into paths_by_track_id.
/// `id` is the TrackId as a [String].
/// `song_result_wrapped` is the [Result] of the song file search.
/// `paths_by_track_id` is passed directly.
/// `album_folder` is the [Result] of the album folder search.
fn insert_song_path(
    id: String,
    song_result_wrapped: &Result<DirEntry, Error>,
    paths_by_track_id: Arc<Mutex<HashMap<TrackId, PathBuf>>>,
    album_folder: &Vec<Result<DirEntry, Error>>,
) -> Result<InsertResult> {
    // trace!(
    //     "insert_song_path(id: {id:?}, song_result_wrapped: {song_result_wrapped:?}, paths_by_track_id: {paths_by_track_id:?}, album_folder: {album_folder:?})"
    // );
    let result_type;
    match song_result_wrapped {
        Ok(song_result) => {
            let prev_value = paths_by_track_id
                .lock()
                .expect("Poisoned lock")
                .insert(TrackId::from_id(id.clone())?, song_result.path());
            if let Some(prev_value) = prev_value {
                result_type = InsertResult::Dup((id, prev_value));
            } else {
                result_type = InsertResult::Found;
            }
        }
        Err(e) => {
            result_type = InsertResult::Error;
            error!("Error on retrieving song path at album_folder {album_folder:?}: {e}");
        }
    }

    Ok(result_type)
}

fn get_all_paths(base_path: String) -> Result<Vec<Vec<Result<DirEntry, Error>>>> {
    Ok(fs::read_dir(base_path)?
        .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
        .flat_map(|artist_folder| fs::read_dir(artist_folder.as_ref().unwrap().path()))
        .flatten()
        .flat_map(|album_folder| fs::read_dir(album_folder.unwrap().path()))
        .map(|album_folder| album_folder.collect::<Vec<_>>())
        .collect())
}

struct PopulateCounters {
    found_counter: i32,
    not_found_counter: i32,
    error_counter: i32,
    dup_counter: i32,
}

fn populate_paths(
    all_songs: Vec<Vec<Result<DirEntry, Error>>>,
    paths_by_track_id: &Arc<Mutex<HashMap<TrackId, PathBuf>>>,
) -> Result<PopulateCounters> {
    let mut found_counter = 0;
    let mut not_found_counter = 0;
    let mut error_counter = 0;
    let mut dup_counter = 0;
    for album_folder in all_songs {
        let song_ids_file = album_folder
            .iter()
            .find(|entry| entry.as_ref().unwrap().file_name() == ".song_ids");
        if let Some(file) = song_ids_file {
            let song_ids_str = fs::read_to_string(file.as_ref().unwrap().path())?;
            let song_ids: Vec<Vec<String>> = if !song_ids_str.is_empty() {
                song_ids_str
                    .lines()
                    .map(|line| line.split('\t').map(|s| s.to_owned()).collect::<Vec<_>>())
                    .collect()
            } else {
                continue;
            };
            for id in song_ids {
                let song = album_folder
                    .iter()
                    .find(|entry| *entry.as_ref().unwrap().file_name() == **id.get(4).unwrap());
                let insert_result;
                match song {
                    Some(song_result_wrapped) => {
                        insert_result = insert_song_path(
                            id.get(0).unwrap().to_string(),
                            song_result_wrapped,
                            Arc::clone(&paths_by_track_id),
                            &album_folder,
                        )?;
                    }
                    None => {
                        // Try again with base_path prefix
                        let song = album_folder.iter().find(|entry| {
                            *entry.as_ref().unwrap().path().as_os_str() == **id.get(4).unwrap()
                        });
                        match song {
                            Some(song_result_wrapped) => {
                                insert_result = insert_song_path(
                                    id.get(0).unwrap().to_string(),
                                    song_result_wrapped,
                                    Arc::clone(&paths_by_track_id),
                                    &album_folder,
                                )?;
                            }
                            None => {
                                insert_result = InsertResult::NotFound;
                                info!("No song found matching song_id at {id:?}");
                            }
                        }
                    }
                }
                match insert_result {
                    InsertResult::Found => found_counter += 1,
                    InsertResult::NotFound => not_found_counter += 1,
                    InsertResult::Dup((key, prev_value)) => {
                        debug!("prev_value for {key} was {}", prev_value.display());
                        found_counter += 1;
                        dup_counter += 1
                    }
                    InsertResult::Error => error_counter += 1,
                }
            }
        } else {
            info!(
                "No .song_ids file found for album folder {:?}, skipping",
                album_folder
            )
        }
    }
    Ok(PopulateCounters {
        found_counter,
        not_found_counter,
        error_counter,
        dup_counter,
    })
}

#[derive(Debug, EnumDiscriminants, EnumMessage, Serialize, Deserialize, Clone)]
#[strum_discriminants(derive(ValueEnum, Serialize, Deserialize, Hash, PartialOrd, Ord))]
#[strum_discriminants(name(TagMarker))]
enum Tag {
    #[strum(message = "album")]
    Album(SimplifiedAlbum),
    #[strum(message = "artist")]
    Artists(Vec<SimplifiedArtist>),
    #[strum(message = "markets")]
    AvailableMarkets(Vec<String>),
    #[strum(message = "disc_number")]
    DiscNumber(i32),
    #[strum(message = "duration")]
    Duration(Duration),
    #[strum(message = "explicit")]
    Explicit(bool),
    #[strum(message = "external_ids")]
    ExternalIds(HashMap<String, String>),
    #[strum(message = "external_urls")]
    ExternalUrls(HashMap<String, String>),
    #[strum(message = "href")]
    Href(Option<String>),
    #[strum(message = "id")]
    Id(Option<TrackId<'static>>),
    #[strum(message = "is_local")]
    IsLocal(bool),
    #[strum(message = "is_playable")]
    IsPlayable(Option<bool>),
    #[strum(message = "linked_from")]
    LinkedFrom(Option<TrackLink>),
    #[strum(message = "restrictions")]
    Restrictions(Option<Restriction>),
    #[strum(message = "name")]
    Name(String),
    #[strum(message = "popularity")]
    Popularity(u32),
    #[strum(message = "preview_url")]
    PreviewUrl(Option<String>),
    #[strum(message = "track_number")]
    TrackNumber(u32),
    #[strum(message = "type")]
    Type(Type),
    #[strum(message = "genre")]
    Genre(Vec<String>), // this is an artist-level tag not track-level
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sep = ", ";
        let output = match &self {
            &Tag::Album(album) => &album.name.clone(),
            &Tag::Artists(artists) => &artists
                .iter()
                .map(|artist: &SimplifiedArtist| artist.name.clone())
                .collect::<Vec<_>>()
                .join(sep),
            &Tag::AvailableMarkets(markets) => &markets.join(sep),
            &Tag::DiscNumber(disc_num) => &disc_num.to_string(),
            &Tag::Duration(duration) => &duration.as_secs().to_string(),
            &Tag::Explicit(explicit) => &explicit.to_string(),
            &Tag::ExternalIds(external_ids) => &external_ids
                .iter()
                .map(|(kind, id)| kind.to_owned() + ": " + id)
                .collect::<Vec<_>>()
                .join(";"),
            &Tag::ExternalUrls(external_urls) => &external_urls
                .iter()
                .map(|(kind, url)| kind.to_owned() + ": " + url)
                .collect::<Vec<_>>()
                .join(";"),
            &Tag::Genre(genres) => &genres.join(sep),
            &Tag::Href(href) => &href.clone().unwrap_or("".to_string()),
            &Tag::Id(_id) => todo!(),
            &Tag::IsLocal(is_local) => &is_local.to_string(),
            &Tag::IsPlayable(is_playable) => &is_playable.unwrap_or(false).to_string(),
            &Tag::LinkedFrom(_linked_from) => todo!(),
            &Tag::Name(name) => &name.clone(),
            &Tag::Popularity(popularity) => &popularity.to_string(),
            &Tag::PreviewUrl(preview_url) => &preview_url.clone().unwrap_or("".to_string()),
            &Tag::Restrictions(_restrictions) => todo!(),
            &Tag::TrackNumber(track_number) => &track_number.to_string(),
            &Tag::Type(type_param) => &type_param.to_string(),
        };
        write!(f, "{}", output)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TagsByTrack {
    tags: HashMap<TrackId<'static>, HashMap<TagMarker, Tag>>,
}

type PathsByTrackId = HashMap<TrackId<'static>, PathBuf>;
type GenresByArtist = HashMap<ArtistId<'static>, Vec<String>>;

async fn backoff_429<T>(res: Result<Vec<T>, ClientError>) -> Result<Vec<T>, ClientError> {
    match res {
        Ok(data) => Ok(data),
        Err(ClientError::Http(ref http_error)) => match &**http_error {
            HttpError::StatusCode(response) => {
                let code = response.status();
                let headers = response.headers();
                if code.as_u16() == 429 {
                    let retry_after = headers.get("Retry-After");
                    if let Some(retry_after) = retry_after {
                        warn!(
                            "Got 429: retry-after {:?}",
                            retry_after
                                .to_str()
                                .unwrap_or("")
                                .parse::<i64>()
                                .unwrap_or(-1)
                        );
                        tokio::time::sleep(Duration::from_secs(
                            retry_after.to_str().unwrap().parse().unwrap(),
                        ))
                        .await;
                        Box::pin(backoff_429::<T>(res)).await
                    } else {
                        panic!("Got 429 but no Retry-After");
                    }
                } else {
                    panic!("Unknown HTTP status code: {code}");
                }
            }
            other_error => panic!("Unknown http error: {other_error}"),
        },
        Err(e) => panic!("Unknown client error: {e}"),
    }
}

async fn random_delay(num_paths: u64, initial_delay_multiplier: Option<u64>) {
    // Try to prevent 429s by waiting a while before the first request
    let rand_millis =
        rand::rng().random_range(0..(num_paths * initial_delay_multiplier.unwrap_or(10)));
    tokio::time::sleep(Duration::from_millis(rand_millis)).await;
}

struct Completion(usize);

async fn create_genre_task<const CHUNK_SIZE: usize>(
    spotify: Arc<ClientCredsSpotify>,
    genres_by_artist: Arc<Mutex<HashMap<ArtistId<'_>, Vec<String>>>>,
    tags_by_track: Arc<RwLock<TagsByTrack>>,
    num_paths: u64,
    path_chunk: Vec<(TrackId<'static>, PathBuf)>,
    initial_delay_multiplier: Option<u64>,
    progress_tx: tokio::sync::mpsc::Sender<Completion>,
) -> () {
    let chunk_len = path_chunk.len();
    random_delay(num_paths, initial_delay_multiplier).await;
    let res = spotify
        .tracks(path_chunk.into_iter().map(|(track, _)| track.clone()), None)
        .await;
    let res = backoff_429(res).await.unwrap();
    let mut artists_by_track: HashMap<TrackId, Vec<ArtistId>> = HashMap::new();
    for track in res {
        let id = track.id.context("while getting track ID").unwrap();
        let artists = track.artists.clone();
        artists_by_track.insert(
            id,
            artists
                .into_iter()
                .map(|artist| artist.id.unwrap())
                .collect(),
        );
    }
    let mut artists_by_track_orig = artists_by_track.clone();
    let artists_len = artists_by_track
        .iter()
        .fold(0, |acc, (_, artists)| acc + artists.len());
    let artist_chunks: Vec<Vec<(TrackId<'static>, Vec<ArtistId<'static>>)>> = chunk_hashmap::<CHUNK_SIZE, TrackId, Vec<ArtistId>>(
        artists_by_track,
        Some(artists_len),
        // FIXME: investigate eliminating lifetimes here
        Some(Box::new(for <'a, 'b, 'c>
            |(track, artists): &'a (TrackId<'b>, Vec<ArtistId<'c>>)| -> Vec<(TrackId<'b>, Vec<ArtistId<'c>>)> {
                artists.into_iter().map(|artist|
                    (track.clone(), std::iter::once(artist.clone()).collect()))
                    .collect()
                })
            )
    );
    let artist_chunks: Vec<Vec<Vec<ArtistId<'_>>>> = artist_chunks
        .into_iter()
        .map(|chunk| chunk.into_iter().map(|(_, artists)| artists).collect())
        .collect();
    for artist_chunk in artist_chunks {
        if artist_chunk.len() > 0 {
            let res = spotify
                .artists(
                    artist_chunk
                        .into_iter()
                        .flatten()
                        .collect::<Vec<ArtistId>>(),
                )
                .await;
            // .context("while getting Spotify artists list")
            // .unwrap();
            let res = backoff_429(res).await.unwrap();
            for artist in res {
                genres_by_artist
                    .lock()
                    .unwrap()
                    .insert(artist.id, artist.genres);
            }
        }
    }
    for (artist, genres) in genres_by_artist.lock().unwrap().iter() {
        artists_by_track_orig.retain(|track, artists| {
            if artists.contains(&artist) {
                tags_by_track
                    // .lock()
                    .write()
                    .unwrap()
                    .tags
                    .entry(track.clone())
                    .and_modify(|tags| {
                        tags.entry(TagMarker::Genre)
                            .and_modify(|tag| match tag {
                                Tag::Genre(existing) => existing.append(&mut genres.clone()),
                                _ => unreachable!(),
                            })
                            .or_insert(Tag::Genre(genres.to_vec()));
                    })
                    .or_insert(HashMap::from([(
                        TagMarker::Genre,
                        Tag::Genre(genres.clone()),
                    )]));
                if artists.len() == 1 {
                    false
                } else if artists.len() > 1 {
                    let artist_idx = artists.iter().position(|art| *art == *artist);
                    if let Some(idx) = artist_idx {
                        artists.remove(idx);
                    } else {
                        error!("Artist shouldn't have been removed already: {artist:?}");
                    }
                    true
                } else {
                    error!("Artist should've been removed by now: {artist:?}");
                    false
                }
            } else {
                true
            }
        });
    }
    if artists_by_track_orig.len() != 0 {
        error!("Artists without matching track: {artists_by_track_orig:?}");
    }
    progress_tx.send(Completion(chunk_len)).await.unwrap();
}

async fn create_track_tag_task<const CHUNK_SIZE: usize>(
    spotify: Arc<ClientCredsSpotify>,
    tags_by_track: Arc<RwLock<TagsByTrack>>,
    num_paths: u64,
    tags: Vec<TagMarker>,
    path_chunk: Vec<(TrackId<'static>, PathBuf)>,
    initial_delay_multiplier: Option<u64>,
    progress_tx: tokio::sync::mpsc::Sender<Completion>,
) -> () {
    let chunk_len = path_chunk.len();
    random_delay(num_paths, initial_delay_multiplier).await;
    let res = spotify
        .tracks(path_chunk.into_iter().map(|(track, _)| track.clone()), None)
        .await;
    let res = backoff_429(res).await.unwrap();
    for track in res {
        let id = track.id.clone().context("while getting track ID").unwrap();

        for tag_marker in &tags {
            let track = track.clone();
            let property = match tag_marker {
                TagMarker::Album => Tag::Album(track.album),
                TagMarker::Artists => Tag::Artists(track.artists),
                TagMarker::AvailableMarkets => Tag::AvailableMarkets(track.available_markets),
                TagMarker::DiscNumber => Tag::DiscNumber(track.disc_number),
                TagMarker::Duration => {
                    Tag::Duration(Duration::from_secs(track.duration.num_seconds() as u64))
                }
                TagMarker::Explicit => Tag::Explicit(track.explicit),
                TagMarker::ExternalIds => Tag::ExternalIds(track.external_ids),
                TagMarker::ExternalUrls => Tag::ExternalUrls(track.external_urls),
                TagMarker::Genre => panic!(
                    "create_track_tag_task does not support genre tagging, use create_genre_task for that"
                ),
                TagMarker::Href => Tag::Href(track.href),
                TagMarker::Id => Tag::Id(Some(id.clone())),
                TagMarker::IsLocal => Tag::IsLocal(track.is_local),
                TagMarker::IsPlayable => Tag::IsPlayable(track.is_playable),
                TagMarker::LinkedFrom => Tag::LinkedFrom(track.linked_from),
                TagMarker::Name => Tag::Name(track.name),
                TagMarker::Popularity => Tag::Popularity(track.popularity),
                TagMarker::PreviewUrl => Tag::PreviewUrl(track.preview_url),
                TagMarker::Restrictions => Tag::Restrictions(track.restrictions),
                TagMarker::TrackNumber => Tag::TrackNumber(track.track_number),
                TagMarker::Type => Tag::Type(track.r#type),
            };
            debug!("thread at {id} about to lock tags_by_track");
            // FIXME: deadlock with only one chunk
            tags_by_track
                // .lock()
                .write()
                .expect("Poisoned lock")
                .tags
                .entry(id.clone())
                .and_modify(|stored_tags| {
                    stored_tags.entry(*tag_marker).insert_entry(property);
                })
                .or_insert(HashMap::from_iter(vec![]));
            debug!("thread at {id} releasing lock");
        }
    }
    progress_tx.send(Completion(chunk_len)).await.unwrap();
}

/// chunk_hashmap partitions a [HashMap] into `N` chunks, with the remainder in the final chunk.
/// `U` and `V` are the types of HashMap's keys and values, respectively.
/// `map` is the HashMap to chunk.
/// `total_len` is the total length of the HashMap if chunking should be based on something other than `map.len()`
/// (such as if the values are [Vec]s), otherwise None.
/// `map_values` is a closure that is passed to [Iterator::flat_map] on the Vec<(U, V)> representation of the HashMap
/// before chunking occurs if the values need to be remapped somehow, such as if, again, the values are [Vec]s,
/// and you want the chunks to flatten those Vecs; otherwise, pass None::<fn(&(U, V)) -> Vec<(U, V)>>.
fn chunk_hashmap<const N: usize, U: Clone, V: Clone>(
    map: HashMap<U, V>,
    total_len: Option<usize>,
    map_values: Option<impl FnMut(&(U, V)) -> Vec<(U, V)>>,
) -> Vec<Vec<(U, V)>> {
    let len = total_len.unwrap_or(map.len());
    let num_chunks = (len as f64 / N as f64).ceil() as usize;
    let mut iter_as_vec = map.into_iter().collect::<Vec<(U, V)>>();
    if let Some(value_mapper) = map_values {
        iter_as_vec = iter_as_vec
            .iter()
            .flat_map(value_mapper)
            .collect::<Vec<(U, V)>>();
    }
    let iter_as_chunks: (&[[(U, V); N]], &[(U, V)]) = iter_as_vec.as_chunks::<N>();
    (0..num_chunks)
        .map(|i| {
            if num_chunks == 1 {
                if len < N {
                    iter_as_chunks.1.to_vec()
                } else {
                    iter_as_chunks.0[i].to_vec()
                }
            } else {
                if i < num_chunks - 1 {
                    iter_as_chunks.0[i].to_vec()
                } else {
                    iter_as_chunks.1.to_vec()
                }
            }
        })
        .collect()
}

#[tokio::main]
async fn get_tags_from_spotify(
    args: &Args,
    paths_by_track_id: Arc<Mutex<PathsByTrackId>>,
    tags_by_track: Arc<RwLock<TagsByTrack>>,
    genres_by_artist: Arc<Mutex<GenresByArtist>>,
) -> Result<()> {
    let spotify_creds = Credentials::from_env().unwrap();

    let spotify = Arc::new(ClientCredsSpotify::new(spotify_creds));

    spotify.request_token().await?;
    const CHUNK_SIZE: usize = rspotify::DEFAULT_PAGINATION_CHUNKS as usize;
    let num_paths = paths_by_track_id.lock().expect("Poisoned lock").len() as u64;
    let path_chunks = chunk_hashmap::<CHUNK_SIZE, TrackId, PathBuf>(
        paths_by_track_id.lock().expect("Poisoned lock").clone(),
        None,
        // FIXME: investigate eliminating the lifetimes here
        None::<for<'a, 'b> fn(&'a (TrackId<'b>, PathBuf)) -> Vec<(TrackId<'b>, PathBuf)>>,
    );

    let (_, outputs) = TokioScope::scope_and_block(|scope: &mut Scope<'_, (), _>| {
        let (progress_tx, mut progress_rx) = async_mpsc::channel(32);
        let mut progress_total: f64 = 0.0;
        for path_chunk in path_chunks.into_iter() {
            if path_chunk.len() > 0 {
                // FIXME: combine create_{genre,track_tag}_task and pass one vec with all tags requested
                for tag in &args.tag {
                    let spotify = spotify.clone();
                    match tag {
                        TagMarker::Genre => {
                            scope.spawn(create_genre_task::<CHUNK_SIZE>(
                                spotify,
                                Arc::clone(&genres_by_artist),
                                Arc::clone(&tags_by_track),
                                num_paths,
                                path_chunk.clone(),
                                None,
                                progress_tx.clone(),
                            ));
                        }
                        other_marker => scope.spawn(create_track_tag_task::<CHUNK_SIZE>(
                            spotify,
                            Arc::clone(&tags_by_track),
                            num_paths,
                            vec![*other_marker],
                            path_chunk.clone(),
                            None,
                            progress_tx.clone(),
                        )),
                    }
                }
            }
        }
        scope.spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                progress_total += progress.0 as f64 / num_paths as f64;
                println!("Progress: {:.2}%", progress_total * 100.0);
                // FIXME: divide progress by total number of tags being retrieved
            }
        });
    });

    for output in outputs {
        output?
    }

    let tags_json = serde_json::to_string(
        &*tags_by_track
            // .lock()
            .read()
            .expect("Poisoned lock"),
    )?;

    if args.use_cache {
        fs::write("tags_by_track.json", tags_json)?;
    }

    Ok(())
}

fn write_tags(
    tags_by_track: Arc<RwLock<TagsByTrack>>,
    paths_to_tag: Arc<Mutex<PathsByTrackId>>,
    use_cache: bool,
    overwrite_tags: bool,
) -> Result<()> {
    let tags_lock = tags_by_track
        // .lock()
        .read()
        .expect("Poisoned lock");
    let tags_len = tags_lock.tags.len();
    println!("Writing tags to {tags_len} audio files...");

    ffmpeg_next::init()?;
    thread::scope(|scope| {
        let mut progress_total: f64 = 0.0;
        let mut handles = vec![];
        let (progress_tx, progress_rx) = mpsc::channel();
        for (track, tags) in tags_lock.tags.iter() {
            let progress_tx = progress_tx.clone();
            let paths_to_tag = Arc::clone(&paths_to_tag);
            handles.push(scope.spawn(move || {
                let paths = paths_to_tag.lock().expect("Poisoned lock");
                let Some(path) = paths.get(track) else {
                    if !use_cache {
                        warn!("Expected to find track {track} in paths_to_tag, skipping...");
                    }
                    return;
                };
                info!("Processing file {}", path.display());
                let mut ictx = format::input(path).unwrap();
                let context_or_stream = if ictx.metadata().iter().count() != 0 {
                    ContextOrStream::Context(&ictx)
                } else {
                    ContextOrStream::Stream(&ictx.streams().best(media::Type::Audio).unwrap())
                };
                let mut temp_path = path.clone();
                temp_path.set_extension(
                    path.extension().unwrap().to_string_lossy().into_owned() + ".tmp",
                );
                let mut octx = format::output_as(&temp_path, "ogg").unwrap();
                let mut stream_mapping: Vec<i32> = vec![0; ictx.nb_streams() as _];
                let mut ist_time_bases = vec![Rational(0, 1); ictx.nb_streams() as _];
                let mut ost_index = 0;
                for (ist_index, ist) in ictx.streams().enumerate() {
                    let ist_medium = ist.parameters().medium();
                    if ist_medium != media::Type::Audio {
                        stream_mapping[ist_index] = -1;
                        continue;
                    }
                    stream_mapping[ist_index] = ost_index;
                    ist_time_bases[ist_index] = ist.time_base();
                    ost_index += 1;
                    let mut ost = octx.add_stream(encoder::find(codec::Id::OPUS)).unwrap();
                    ost.set_parameters(ist.parameters());
                    unsafe {
                        (*ost.parameters().as_mut_ptr()).codec_tag = 0;
                    }
                }
                let mut metadata_changed = false;
                for (_tag_marker, tag) in tags {
                    let tag_name = tag.get_message().expect("Tag without message");
                    let tag_contents = tag.to_string();
                    match context_or_stream {
                        ContextOrStream::Context(ictx) => {
                            let mut octx_metadata = ictx.metadata().to_owned();
                            if !overwrite_tags && octx_metadata.get(tag_name).is_some() {
                                continue;
                            }
                            octx_metadata.set(tag_name, &tag_contents);
                            octx.set_metadata(octx_metadata);
                            metadata_changed = true;
                        }
                        ContextOrStream::Stream(input) => {
                            let mut output = octx
                                .streams_mut()
                                .find(|s| {
                                    codec::context::Context::from_parameters(s.parameters())
                                        .unwrap()
                                        .medium()
                                        == media::Type::Audio
                                })
                                .unwrap();
                            let mut output_metadata = input.metadata().to_owned();
                            if !overwrite_tags && output_metadata.get(tag_name).is_some() {
                                continue;
                            }
                            output_metadata.set(tag_name, &tag_contents);
                            output.set_metadata(output_metadata);
                            metadata_changed = true;
                        }
                    }
                }

                if !metadata_changed {
                    fs::remove_file(temp_path).unwrap();
                    progress_tx.send(Completion(1)).unwrap();
                    return;
                }

                octx.write_header().unwrap();

                for (stream, mut packet) in ictx.packets() {
                    let ist_index = stream.index();
                    let ost_index = stream_mapping[ist_index];
                    if ost_index < 0 {
                        continue;
                    }
                    let ost = octx.stream(ost_index as _).unwrap();
                    packet.rescale_ts(ist_time_bases[ist_index], ost.time_base());
                    packet.set_position(-1);
                    packet.set_stream(ost_index as _);
                    match packet.write_interleaved(&mut octx) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Failed to write audio file {}: {e}", path.display())
                        }
                    }
                }

                octx.write_trailer().unwrap();

                fs::remove_file(path).unwrap();
                fs::rename(temp_path, path).unwrap();
                progress_tx.send(Completion(1)).unwrap()
            }));
        }
        drop(progress_tx); // need to drop all tx to make progress_rx return Err
        let start = Instant::now();
        let mut elapsed_time; // unit is ms
        let mut eta; // unit is s
        for progress in progress_rx {
            progress_total += progress.0 as f64 / tags_len as f64;
            elapsed_time = start.elapsed().as_millis();
            eta = ((1.0 / progress_total) * elapsed_time as f64 - elapsed_time as f64) / 1000.0;
            println!(
                "Progress: {:.2}%, ETA: {:.0}m{:.0}s, ",
                progress_total * 100.0,
                eta / 60.0 + 1.0,
                eta % 60.0
            );
        }
        for handle in handles {
            if let Err(e) = handle.join() {
                println!("Error from tag writing thread: {:?}", e);
            }
        }
    });
    println!("Finished!");

    Ok(())
}

fn cleanup_tags(tags_by_track: Arc<RwLock<TagsByTrack>>) {
    for (_track, tags) in tags_by_track
        // .lock()
        .write()
        .expect("Poisoned lock")
        .tags
        .iter_mut()
    {
        tags.retain(|_marker, tag| match tag {
            Tag::Genre(genres) => {
                if genres.is_empty() {
                    false
                } else {
                    genres.sort();
                    genres.dedup();
                    true
                }
            }
            _ => true,
        });
    }
}

fn handle_dry_run(tags_by_track: Arc<RwLock<TagsByTrack>>) -> Result<()> {
    let tags = tags_by_track
        // .lock()
        .read()
        .expect("Poisoned lock");
    let tracks_total = tags.tags.len();
    let found_tags_total = tags.tags.len();
    let empty_tags_total = tracks_total - found_tags_total;
    println!("Dry run: found tags for {} tracks", found_tags_total);
    if empty_tags_total > 0 {
        println!("...but not {} tracks", empty_tags_total);
    }
    println!("Sample data:");
    let tags_vec: Vec<_> = tags.tags.iter().take(25).collect();
    for (track, tag) in tags_vec {
        println!("{track}: {tag:?}");
    }
    Ok(())
}

fn query_and_write_tags(
    args: &Args,
    tags_by_track: Arc<RwLock<TagsByTrack>>,
    paths_to_query: Arc<Mutex<PathsByTrackId>>,
    genres_by_artist: Arc<Mutex<GenresByArtist>>,
) -> Result<()> {
    get_tags_from_spotify(
        &args,
        Arc::clone(&paths_to_query),
        Arc::clone(&tags_by_track),
        genres_by_artist,
    )?;
    cleanup_tags(Arc::clone(&tags_by_track));
    if args.dry_run {
        return handle_dry_run(Arc::clone(&tags_by_track));
    }
    write_tags(
        Arc::clone(&tags_by_track),
        Arc::clone(&paths_to_query),
        args.use_cache,
        args.overwrite_tags,
    )
}

fn main() -> Result<()> {
    console_subscriber::init();
    // Handle background panics in threads or futures
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    env_logger::init();
    dotenvy::dotenv()?;

    let mut args = Args::parse();

    let Ok(base_path) = env::var("BASE_PATH") else {
        panic!("Pass zotify download directory as BASE_PATH environment var");
    };
    println!("Getting folders in {base_path}");
    let paths_by_track_id: Arc<Mutex<PathsByTrackId>> = Arc::new(Mutex::new(HashMap::new()));
    let all_songs = get_all_paths(base_path)?;

    println!("Processing folders...");
    let PopulateCounters {
        found_counter,
        not_found_counter,
        error_counter,
        dup_counter,
    } = populate_paths(all_songs, &paths_by_track_id)?;

    println!("{found_counter} tracks found successfully");
    println!("...including {dup_counter} duplicates");
    println!("...excluding {not_found_counter} tracks not found");
    println!("...and {error_counter} errors");

    // enum Backend {
    //     Spotify(Credentials),
    //     Annas(Connection),
    // }

    // let Ok(sqlite_db_path) = env::var("CLEAN_DB_PATH") else {
    //     panic!("Pass `spotify_clean.sqlite3` from Anna's as CLEAN_DB_PATH environment var");
    // };

    // sqlite::open(env::var("CLEAN_DB_PATH")?);

    let method = if args.use_cache { "disk" } else { "Spotify" };
    println!("Grabbing tags from {method}...");

    // Only needed for genre tagging otherwise unused
    let genres_by_artist: Arc<Mutex<HashMap<ArtistId, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let tags_by_track = if args.use_cache
        && let Ok(mut tags_json_from_disk) = File::open("tags_by_track.json")
    {
        let mut tags_str = String::new();
        tags_json_from_disk.read_to_string(&mut tags_str)?;
        Arc::new(RwLock::new(
            serde_json::from_str(&tags_str).context("while serializing tags_by_track.json")?,
        ))
    } else {
        Arc::new(RwLock::new(TagsByTrack {
            tags: HashMap::new(),
        }))
    };

    args.tag.sort();

    let backend = "Spotify";

    if args.use_cache {
        let paths_to_query = Arc::new(Mutex::new(
            paths_by_track_id.lock().expect("Poisoned lock").clone(),
        ));
        {
            let tags_lock = tags_by_track
                // .lock()
                .write()
                .expect("Poisoned lock");

            paths_to_query
                .lock()
                .expect("Poisoned lock")
                .retain(|track_id, _| {
                    let Some(track_tags_map) = tags_lock.tags.get(track_id) else {
                        return true;
                    };
                    let mut track_tags = track_tags_map.keys().cloned().collect::<Vec<_>>();
                    track_tags.sort();
                    track_tags != args.tag
                });
        }
        let paths_diff_len = paths_to_query.lock().expect("Poisoned lock").len();
        if paths_diff_len > 0 {
            println!(
                "Filling in gaps for {} songs from {backend}...",
                paths_diff_len
            );
            query_and_write_tags(
                &args,
                Arc::clone(&tags_by_track),
                paths_to_query,
                genres_by_artist,
            )
        } else {
            println!("All tags found in cache!");
            write_tags(
                Arc::clone(&tags_by_track),
                paths_to_query,
                args.use_cache,
                args.overwrite_tags,
            )
        }
    } else {
        query_and_write_tags(
            &args,
            Arc::clone(&tags_by_track),
            paths_by_track_id,
            genres_by_artist,
        )
    }
}

// #[cfg(test)]
// mod test {
//     use rspotify::model::SimplifiedAlbum;

//     use crate::Tag;

//     #[test]
//     fn tag_outputs() {
//         // let inputs = vec![Tag::Album()]
//     }
// }
