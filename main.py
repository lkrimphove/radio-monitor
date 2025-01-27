import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

CURRENTLY_PLAYING_ENDPOINT = 'https://www.antenne.de/api/metadata/now'
RELEVANT_DATA = ['artist', 'title', 'isrc', 'starttime', 'mountpoint']

if 'LOG_LVL' in os.environ:
    LOG_LVL = os.environ.get('LOG_LVL')
else:
    LOG_LVL = logging.INFO

if 'REFRESH_RATE' in os.environ:
    REFRESH_RATE = os.environ.get('REFRESH_RATE')
else:
    REFRESH_RATE = 90  # seconds

if 'MAX_BATCH' in os.environ:
    MAX_BATCH = os.environ.get('MAX_BATCH')
else:
    MAX_BATCH = 1000

if 'ROOT_PATH' in os.environ:
    ROOT_PATH = os.environ.get('ROOT_PATH')
else:
    ROOT_PATH = ""

logger = logging.getLogger()
logging.basicConfig(level=LOG_LVL,
                    filename=f'{ROOT_PATH}app.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%m-%y %H:%M:%S')
logging.getLogger().addHandler(logging.StreamHandler())

logger.info(f'LOG_LVL: {LOG_LVL}')
logger.info(f'REFRESH_RATE: {REFRESH_RATE}')
logger.info(f'MAX_BATCH: {MAX_BATCH}')
logger.info(f'ROOT_PATH: {ROOT_PATH}')


records = []

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)


def read_relevant_stations():
    # read in stations to monitor
    # only songs from stations listed in relevant_stations.txt will be tracked
    # leave file empty to monitor every station
    file_path = f'{ROOT_PATH}relevant_stations.txt'

    relevant_stations = []
    if os.path.isfile(file_path):
        with open(file_path) as f:
            for line in f.readlines():
                relevant_stations.append(line.strip())
        logger.debug(f'Relevant stations: {relevant_stations}')

    return relevant_stations


def get_station_data():
    try:
        # retrieve metadata of every song currently playing on every station
        response = session.get(url=CURRENTLY_PLAYING_ENDPOINT)
        if response.status_code == 200:
            return response.json()['data']
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

    return []


def write_to_parquet():
    if len(records) < 0:
        return

    logger.info(f'Starting to write {len(records)} new songs')

    now = datetime.now()
    file_name = f'{now.date()}.parquet'
    directory = f'{ROOT_PATH}gathered_data/{now.year}/{now.month}'
    file_path = f'{directory}/{file_name}'

    df = pd.DataFrame(records)
    df = df.rename(columns={"mountpoint": "station"})

    if not os.path.isdir(directory):
        os.makedirs(directory)

    if not os.path.isfile(file_path):
        df.to_parquet(file_path, engine='fastparquet')
    else:
        df.to_parquet(file_path, engine='fastparquet', append=True)

    logger.info(f'Finished to write {len(records)} new songs')


def seconds_since_midnight():
    now = datetime.now() - timedelta(seconds=REFRESH_RATE)
    return (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()


def signal_handler(signal, frame):
    logger.info('Got exit signal')
    write_to_parquet()
    logger.info('Stop')
    sys.exit(0)


def main():
    global records
    logger.info('Start')

    signal.signal(signal.SIGINT, signal_handler)

    relevant_stations = read_relevant_stations()

    playing = {}
    while True:
        # retrieve all songs currently playing on relevant stations
        stations = get_station_data()
        if len(relevant_stations) > 0:
            stations = list(filter(lambda d: d['mountpoint'] in relevant_stations, stations))
        currently_playing = {}
        for station in stations:
            if station['class'] == 'Music':
                currently_playing[station['mountpoint']] = ({key: station[key] for key in RELEVANT_DATA})
        logger.debug(f'Currently playing: {currently_playing}')

        # filter for tracks that have recently started playing
        new_songs = {}
        for station in currently_playing:
            if station not in playing:
                new_songs[station] = currently_playing[station]
            elif currently_playing[station]['isrc'] is not None:
                if currently_playing[station]['isrc'] != playing[station]['isrc']:
                    new_songs[station] = currently_playing[station]
                    continue
            else:
                for key in RELEVANT_DATA:
                    if currently_playing[station][key] != playing[station][key]:
                        new_songs[station] = currently_playing[station]
                        continue
        playing = currently_playing
        logger.info(f'{len(new_songs)} new songs')
        logger.debug(f'New songs playing: {new_songs}')

        for station in new_songs:
            # store all new songs
            records.append(new_songs[station])

        if seconds_since_midnight() <= REFRESH_RATE or len(records) > 1000:
            write_to_parquet()
            records = []

        time.sleep(REFRESH_RATE)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
