import parquet
import json
import glob
import pandas as pd


users = pd.read_parquet("./out/users")
print(users)

artists = pd.read_parquet("./out/artists")
print(artists)

songs = pd.read_parquet("./out/songs")
print(songs)

times = pd.read_parquet("./out/times")
print(times)

songplays = pd.read_parquet("./out/songplays")
print(songplays)
