Remuxing with ffmpeg isn't strictly necessary but it does make it easier to adapt to other input/output formats.

The genre-fetching code (starting with the line `genre_tasks.push(tokio::spawn(async move {`) uses a random sleep
duration based on the total number of songs to try to prevent 429s. I didn't do much testing on this as most of the time
is spent writing the genres to disk.

The genre-writing code (starting `thread::scope(|scope| {`) probably doesn't benefit all that much from multithreading
since you quickly run into a CPU iowait situation. It might be beneficial to work in a tmpfs and then copy the outputted songs
over in larger batches. 

Right now this just overwrites the genres already present with upstream genres, which might be problematic if you 
use something like Musicbrainz Picard to get song-level data rather than "official" artist-level data. For large music
libraries I've found that Musicbrainz typically only has genres for a small fraction of your music though.
