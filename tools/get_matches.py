from config import *
from matches_etl import extract_matches, transform_matches, load_matches

matches_list = extract_matches(
    unique_tournament=UPL_UNIQUE_TOURNAMENT_ID,
    season_id=UPL_SEASONS_ID['24/25']
)

prepared_matches = transform_matches(matches_list=matches_list)

load_matches(prepared_matches)
