from typing import List

import requests
import pandas as pd


GBFS_FEEDS = [
    "https://mds.bird.co/gbfs/v2/public/los-angeles/free_bike_status.json",
    "https://s3.amazonaws.com/lyft-lastmile-production-iad/lbs/lax/free_bike_status.json",  # noqa
    "https://gbfs.spin.pm/api/gbfs/v2_2/los_angeles/free_bike_status"
]


def free_bike_status_to_df(feeds: List[str] = None) -> pd.DataFrame:
    out_df = pd.DataFrame()
    if feeds is None:
        feeds = GBFS_FEEDS

    for feed in feeds:
        try:
            resp = requests.get(feed)
        except Exception as err:
            print(f"Error on connect: {str(err)} for feed {feed}")

        if resp.status_code != 200:
            print(f"Got non-200 for URL {feed}, got: {resp.text}")

            continue

        bikes_list = resp.json().get("data", {}).get("bikes", None)
        if bikes_list is None:
            print(f"Could not extract bike list for: {feed}")
            continue

        _df = pd.DataFrame(bikes_list)
        out_df = pd.concat([out_df, _df])

    out_df.reset_index()

    return out_df
