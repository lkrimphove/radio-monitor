import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

CURRENTLY_PLAYING_ENDPOINT = 'https://www.antenne.de/api/metadata/now'
RELEVANT_DATA = ['artist', 'title', 'isrc', 'starttime', 'mountpoint']
REFRESH_RATE = 90  # seconds

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG,
                    filename='app.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%m-%y %H:%M:%S')

records = []


def read_relevant_stations():
    # read in stations to monitor
    # only songs from stations listed in relevant_stations.txt will be tracked
    # leave file empty to monitor every station
    file_path = 'relevant_stations.txt'

    relevant_stations = []
    if os.path.isfile(file_path):
        with open(file_path) as f:
            for line in f.readlines():
                relevant_stations.append(line.strip())
        logger.debug(f'Relevant stations: {relevant_stations}')

    return relevant_stations


def get_station_data():
    # retrieve metadata of every song currently playing on every station
    response = requests.get(url=CURRENTLY_PLAYING_ENDPOINT)
    if response.status_code == 200:
        return response.json()['data']
    return None


def write_to_parquet():
    if len(records) < 0:
        return

    logger.debug(f'Starting to write {len(records)} new songs')

    now = datetime.now()
    file_name = f'{now.date()}.parquet'
    directory = f'gathered_data/{now.year}/{now.month}'
    file_path = f'{directory}/{file_name}'

    df = pd.DataFrame(records)
    df = df.rename(columns={"mountpoint": "station"})

    if not os.path.isdir(directory):
        os.makedirs(directory)

    if not os.path.isfile(file_path):
        df.to_parquet(file_path, engine='fastparquet')
    else:
        df.to_parquet(file_path, engine='fastparquet', append=True)

    logger.debug(f'Finished to write {len(records)} new songs')


def seconds_since_midnight():
    now = datetime.now() - timedelta(seconds=REFRESH_RATE)
    return (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()


def signal_handler(signal, frame):
    logger.info('Got exit signal')
    write_to_parquet()
    logger.info('Stop')
    sys.exit(0)


if __name__ == '__main__':
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
        logger.debug(f'New songs playing: {new_songs}')

        for station in new_songs:
            # store all new songs
            records.append(new_songs[station])

        if seconds_since_midnight() < REFRESH_RATE or len(records) > 10000:
            write_to_parquet(records)
            records = []

        time.sleep(REFRESH_RATE)
