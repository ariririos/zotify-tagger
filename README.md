# zotify-tagger
Adds additional metadata from the parent service to already-downloaded files.
The `.song_ids` files that Zotify leaves behind are used to look up metadata.
The API only currently assigns genres to artists, not individual songs. This is the same thing Zotify would do
with `MD_SAVE_GENRES` and `MD_ALLGENRES` enabled and `MD_GENREDELIMITER=","`. Track-level data is used for all other tags.

# Configuation
Create an API key on the [Dashboard](https://developer.spotify.com/dashboard).

In .env or environment:
```
RSPOTIFY_CLIENT_ID={your client ID}
RSPOTIFY_CLIENT_SECRET={your client secret}
BASE_PATH={wherever you pointed Zotify at}
```

# Usage
`zotify-tagger`

# Building
This uses #![feature(closure_lifetime_binder)].
