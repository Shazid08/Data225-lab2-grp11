import numpy as np
import pandas as pd
from ast import literal_eval
import ast

credits_data = pd.read_csv("credits.csv")
keywords_data = pd.read_csv("keywords.csv")
links_data = pd.read_csv("links.csv")
ratings_data = pd.read_csv("ratings_small.csv")

movies_metadata = pd.read_csv("movies_metadata.csv", low_memory=False)

# Convert id column values to integer
movies_metadata["id"] = pd.to_numeric(movies_metadata['id'], errors='coerce', downcast="integer")

# Removing tt from imdb_id and converting to int
movies_metadata["imdb_id"] = movies_metadata['imdb_id'].str[2:]
movies_metadata["imdb_id"] = pd.to_numeric(movies_metadata['imdb_id'], errors='coerce', downcast="integer")

# Drop null values in id column
movies_metadata.dropna(subset=["id"], inplace=True)

merged_data = movies_metadata.merge(credits_data, on=["id"], how="left")
merged_data = merged_data.merge(keywords_data, on=["id"], how="left")

merged_data.drop(["imdb_id"], axis=1).merge(links_data, left_on="id", right_on="movieId", how="inner")

merged_data = merged_data.merge(links_data, left_on="imdb_id", right_on="imdbId", how="inner")

merged_data_shape = merged_data.shape

merged_data.drop_duplicates().shape

merged_data.dropna(subset=["cast", "crew", "keywords", "popularity"], inplace=True)

merged_data['budget'] = pd.to_numeric(merged_data['budget'], errors='coerce')
merged_data['budget'] = merged_data['budget'].replace(0, np.nan)

merged_data['revenue'] = merged_data['revenue'].replace(0, np.nan)

# Modify ratings to drop userId & timestamp as we don't need them
ratings_data.drop(columns=['userId', 'timestamp'], inplace=True)
# Instead of keeping every rating, use average rating based on movieId
ratings_data = ratings_data.groupby('movieId')['rating'].mean().reset_index()

ratings_data['rating'] = ratings_data['rating'].round(1)

merged_data = merged_data.merge(ratings_data, left_on="id", right_on="movieId", how="left")
merged_data.drop(columns=['movieId'], inplace=True)

merged_data['genres'] = merged_data['genres'].fillna('[]').apply(literal_eval).apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['belongs_to_collection'] = merged_data['belongs_to_collection'].fillna("[]").apply(ast.literal_eval).apply(lambda x: x if isinstance(x, dict) else np.nan)

merged_data['production_companies'] = merged_data['production_companies'].apply(ast.literal_eval)
merged_data['production_companies'] = merged_data['production_companies'].fillna("[]").apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['production_countries'] = merged_data['production_countries'].fillna('[]').apply(ast.literal_eval)
merged_data['production_countries'] = merged_data['production_countries'].apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['spoken_languages'] = merged_data['spoken_languages'].apply(ast.literal_eval)
merged_data['spoken_languages'] = merged_data['spoken_languages'].fillna('[]').apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['keywords'] = merged_data['keywords'].apply(literal_eval)
merged_data['keywords'] = merged_data['keywords'].apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['cast'] = merged_data['cast'].apply(literal_eval)
merged_data['crew'] = merged_data['crew'].apply(literal_eval)
merged_data['crew'] = merged_data['crew'].apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['cast'] = merged_data['cast'].apply(lambda x: [i for i in x] if isinstance(x, list) else [])

merged_data['cast'] = merged_data['cast'].apply(lambda x: x[:3] if len(x) >= 3 else x)
merged_data['crew'] = merged_data['crew'].apply(lambda x: x[:3] if len(x) >= 3 else x)

displayed_crew = merged_data['crew']

merged_data.to_csv('merged_data_modified.csv', index=False)
