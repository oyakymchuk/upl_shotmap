from config import *
from matches_etl import extract_matches

matches_json = extract_matches(
    unique_tournament=UPL_UNIQUE_TOURNAMENT_ID,
    season_id=UPL_SEASONS_ID['24/25']
)

print(matches_json)
