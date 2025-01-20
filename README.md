# Radio Monitor

This Python script collects metadata for songs currently playing on Antenne Radio stations, but you should be able to adapt this to your local radio stations easily.
It saves the data in a structured format (Parquet). It monitors specified radio stations and logs song details such as artist, title, ISRC and start time.

## Requirements

- Python 3.8+
- `pandas`
- `requests`
- `urllib3`
- `fastparquet`

Install the required packages using:

```bash
pip install -r requirments.txt
```

## Usage

### Environment Variables
Set the following environment variables to configure the script:

- `LOG_LVL`: Logging level (`INFO`, `DEBUG`, etc.). Default is `INFO`.
- `REFRESH_RATE`: Interval in seconds to fetch new data. Default is `90`.
- `MAX_BATCH`: Maximum number of records before writing to a file. Default is `1000`.
- `ROOT_PATH`: Path to store logs and output files. Default is current directory.

### Customizing Stations
Specify stations to monitor in a text file named `relevant_stations.txt` in the root path. List each station on a new line. If the file is empty, all stations are monitored.

### Logging
Logs are written to `app.log` and the console. Adjust the logging level via the `LOG_LVL` environment variable.

### Data Storage
Data is stored in a hierarchical directory structure under `gathered_data`, organized by year and month. Files are saved in Parquet format.

---

Enjoy tracking your favorite radio stations!

---


If you like my work please consider supporting me by buying me a coffee:

[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/lkrimphove)
