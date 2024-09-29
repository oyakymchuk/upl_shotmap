import os
import logging
import pandas as pd

from datetime import date
from requests import get
from json import loads as json_loads


# set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def extract_matches(unique_tournament: int, season_id: int):
    """Extract matches general data from SofaScore API.

    :param int unique_tournament: SofaScore unique_tournament id.
    :param int season_id: SofaScore season id.
    :return list matches_list: list of JSON (dict) objects where
        each list item is a unique match.
    """

    matches_list = []
    page_counter = 0

    logging.info("----- Matches Data: Extract step started -----")

    while True:
        api_url = f"https://api.sofascore.com/api/v1/unique-tournament/{unique_tournament}/season/{season_id}/events/last/{page_counter}"
        try:
            logging.info(f"Going for tournament {unique_tournament} season {season_id} page {page_counter} matches...")
            response = get(api_url)
        except Exception as e:
            logging.error(e)

        if response.status_code == 200:
            logging.info(f"Page {page_counter} exists and successfully loaded.")
            response_json = json_loads(response.text)
            matches = response_json["events"]
            logging.info(f"On page {page_counter} there are {len(matches)} matches.")
            matches_list += matches
            page_counter += 1
        else:
            logging.info(f"Page {page_counter} does not exis. Loop through pages is finished.")
            break

    return matches_list


def transform_matches(matches_list: list):
    """Transform list of dicts to tabular format pandas DataFrame.

    :param list matches_list: result of extract_matches method.
    :return pd.DataFrame df: transformed tabular format data.
    """

    logging.info("----- Matches Data: Transform step started -----")

    df = pd.json_normalize(matches_list)

    # extract only needed columns
    columns_extract = [
        'id',
        'homeTeam.name',
        'awayTeam.name',
        'homeTeam.id',
        'awayTeam.id',
        'startTimestamp',
        'tournament.uniqueTournament.name',
        'tournament.uniqueTournament.id',
        'season.name',
        'season.year',
        'season.id',
        'homeScore.normaltime',
        'awayScore.normaltime',
        'status.type',
        'roundInfo.round',
        'roundInfo.cupRoundType'
    ]
    df = df.loc[:, columns_extract]

    # make columns more readable
    columns_rename_map = {
        'id': 'id',
        'homeTeam.name': 'home_team_name',
        'awayTeam.name': 'away_team_name',
        'homeTeam.id': 'home_team_id',
        'awayTeam.id': 'away_team_id',
        'startTimestamp': 'date',
        'tournament.uniqueTournament.name': 'tournament_name',
        'tournament.uniqueTournament.id': 'tournament_id',
        'season.name': 'season_name',
        'season.year': 'season_year',
        'season.id': 'season_id',
        'homeScore.normaltime': 'home_scored',
        'awayScore.normaltime': 'away_scored',
        'status.type': 'status',
        'roundInfo.round': 'round',
        'roundInfo.cupRoundType': 'pre_season_cup'
    }
    df = df.rename(columns_rename_map, axis=1)

    # only finished matches are considered
    df = df.loc[df['status'] == 'finished', :]
    df = df.drop(['status'], axis=1)

    # trasformations
    df['date'] = pd.to_datetime(df['date'], unit='s').dt.date
    df['home_scored'] = df['home_scored'].astype('Int64')
    df['away_scored'] = df['away_scored'].astype('Int64')
    df['pre_season_cup'] = df['pre_season_cup'].fillna(0)
    df.loc[df['pre_season_cup'] > 0, ['pre_season_cup']] = 1
    df['pre_season_cup'] = df['pre_season_cup'].astype('Int64')

    df = df.sort_values(['date'], ascending=True)

    logging.info('Data was successfully transformed to tabular format.')

    return df


def load_matches(matches_df: pd.DataFrame):
    """Save DataFrame as .csv file into relative location: './data/matches/'.

    :param pd.DataFrame matches_df: prepared DataFrame to save
    """

    logging.info("----- Matches Data: Load step started -----")

    output_location = './data/matches'
    output_filename = f'upl_matches.csv'
    output_path = output_location + '/' + output_filename

    if not os.path.exists(output_location):
        logging.info(f'Output location does not exist. It will be created: {output_location}.')
        os.makedirs(output_location)

    try:
        matches_df.to_csv(output_path, index=False)
        logging.info(f'Data was successfully saved to {output_path}')
    except Exception as e:
        logging.info('Problems with saving to csv:')
        raise e
