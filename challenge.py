# Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import time
from config import db_password
# Define file directory
file_dir = 'C:/Users/JC/Desktop/UC Berkeley/Movies-ETL'
# Read Wikipedia JSON file and save the data to a new variable.
with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
# Kaggle data into dataframe
kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}/ratings.csv')
# Create a list comprehension with the filter expression

wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie] # Filter out TV shows
# Create a dataFrame
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head()
def clean_movie(movie):
    movie = dict(movie) #Create a non-destructive copy
    alt_titles = {}
    # Combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # Merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie
# Clean wiki_movies and put it into another df
clean_movies = [clean_movie(movie) for movie in wiki_movies]
wiki_movies_df = pd.DataFrame(clean_movies)
# Tell Python to treat our regular expression characters as a raw string of text by putting an r before the quotes.
# Drop any duplicate IMDb IDs
# Only the IMDb ID must be considered so by using the subset argument, set inplace equal to "True"
wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
print(len(wiki_movies_df))
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
print(len(wiki_movies_df))
wiki_movies_df.head()
# Remove Nulls
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
wiki_movies_df.head()
# Convert and Parse the Data
box_office = wiki_movies_df['Box office'].dropna()
# to determine which is not a string
def is_not_a_string(x):
    return type(x) != str
box_office[box_office.map(lambda x: type(x) != str)]
# Make a separator string
box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
# Create variables and set them as finished regular expression
# 'r' for the escape characters to remain
form_one = r'\$\d+\.?\d*\s*[mb]illion'
form_two = r'\$\d{1,3}(?:,\d{3})+'
# Create two Booleans and select box office values that do not match
matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
box_office[~matches_form_one & ~matches_form_two]
# “Million” is sometimes misspelled as “millon.” Make the second “i” optional with a quotation mark
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
# Comma or period as a thousands separator
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
# Search for strings that have $ signs and hypens, replacing them with just $ signs
box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
# Use the expressions to extract only the strings that match
# Make a regular expression that captures data when it matches either form_one or form_two with an f-string
def parse_dollars(s):
    # If s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # If it is in $###.# million form
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        
        return value

    # If it is in $###.# billion form
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        
        return value

    # If it is in $###,###,### form
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        
        return value

    # otherwise, return NaN
    else:
        return np.nan
# Extract from the box_office 
# Then apply parse_dollars to the first column
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
# Drop the Box Office column
wiki_movies_df.drop('Box office', axis=1, inplace=True)
# Create a variable named 'Budget'
budget = wiki_movies_df['Budget'].dropna()
# Convert any lists to strings
budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
# Then remove any values between a dollar sign and a hyphen
budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
# Create two Boolean Series called matches_form_one and matches_form_two, 
# and then select the budget values that don’t match either
matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
budget[~matches_form_one & ~matches_form_two]
# Remove the citation references
budget = budget.str.replace(r'\[\d+\]\s*', '')
budget[~matches_form_one & ~matches_form_two]
# Parse the budget
wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
# Drop the original Budget column
wiki_movies_df.drop('Budget', axis=1, inplace=True)
# Create another variable
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
# Parse each form
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'
# Set the infer_datetime_format option to True
wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
# Parse Running Time
running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
# Only extract digits and allow for both possible patterns
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
# This new DataFrame is all strings. Convert them to numeric values. Use the to_numeric() method and set the errors argument to 'coerce' 
# Coercing the errors will turn the empty strings into Not a Number (NaN), then we can use fillna() to change all the NaNs to zeros
running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
# Apply a function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero, and save the output to wiki_movies_df
wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
# Drop Running time from the dataset
wiki_movies_df.drop('Running time', axis=1, inplace=True)
# Remove unnecessary data
kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
# Drop the adult column
kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
# Creates the Boolean column we want and assign it back to video
kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
# Using the to_numeric(), make sure the errors= argument is set to 'raise' 
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
# Convert release_date to to_datetime()
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
# Assign it to the timestamp column
ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
# Merge by IMDb ID
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
  
# Get the index
movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index
# Drop the row
movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
# Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
# Fill missing data for columns and drop the redundant column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)
# Columns that decided to fill in zeros
fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
movies_df
# Reorder the columns
movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
# Rename the columns to be more consistent
    
movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)
# Use a groupby on the “movieId” and “rating” columns and take the count for each group
# Rename the 'userId' column to 'count'
# Pivot this data so that movieId is the index, the columns will be all the rating values, and the rows will be the counts for each rating value
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
# Rename the columns 
rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
# Left merge
movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
# Fill 0 for all non values
movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
# Make a connection string
"postgres://[user]:[password]@[location]:[port]/[database]"
# Make a connection string for the local server
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movies_challenge"
# Create the database engine
engine = create_engine(db_string)
# Save the movies_df DataFrame to a SQL table
movies_df.to_sql(name='challenge', con=engine)
# Reimport the CSV using the chunksize= parameter in read_csv()
# For loop. Append data into new rows in the SQL table
rows_imported = 0
# get the start_time
start_time = time.time()
for data in pd.read_csv(f'{file_dir}/data/ratings.csv', chunksize=1000000):
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    data.to_sql(name='ratings', con=engine, if_exists='append')
    rows_imported += len(data)

    
    print(f'Done. {time.time() - start_time} total seconds elapsed')       
