import json
import requests
import logging


# set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def extract_matches(unique_tournament: int, season_id: int):
    """Extract matches general data from SofaScore API.

    :param int unique_tournament: SofaScore unique_tournament id.
    :param int season_id: SofaScore season id.
    :return list matches_list: list of JSON (dict) objects where each list item is a unique match.
    """

    matches_list = []
    page_counter = 0

    while True:
        api_url = f"https://api.sofascore.com/api/v1/unique-tournament/{unique_tournament}/season/{season_id}/events/last/{page_counter}"
        try:
            logging.info(f"Going for tournament {unique_tournament} season {season_id} page {page_counter} matches...")
            response = requests.get(api_url)
        except Exception as e:
            logging.error(e)

        if response.status_code == 200:
            logging.info(f"Page {page_counter} exists and successfully loaded.")
            response_json = json.loads(response.text)
            matches = response_json["events"]
            logging.info(f"On page {page_counter} there are {len(matches)} matches.")
            matches_list.append(matches)
            page_counter += 1
        else:
            logging.info(f"Page {page_counter} does not exis. Loop through pages is finished.")
            break

    return matches_list
