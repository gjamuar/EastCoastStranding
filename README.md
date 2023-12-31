## Setup environment

```shell
conda create --name eastcoaststranding python=3.10.12
conda activate eastcoaststranding
pip install -r requirements.txt
```

## How to run 

```shell
$ python vessels.py -h
usage: vessels.py [-h] [-ws WHALE_SDATE] [-we WHALE_EDATE] [-vs VESSEL_SDATE] [-ve VESSEL_EDATE]

            Stranded whales and Vessels Traffic data on Map!
        ---------------------------------------------------------
        This program displays Stranded whales and Vessels Traffic
        in north-east coast during selected time window.
        

options:
  -h, --help            show this help message and exit
  -ws WHALE_SDATE, --whale_sdate WHALE_SDATE
                        Start date for stranded whale time window in YYYY-MM-DD format
  -we WHALE_EDATE, --whale_edate WHALE_EDATE
                        End date for stranded whale time window in YYYY-MM-DD format
  -vs VESSEL_SDATE, --vessel_sdate VESSEL_SDATE
                        Start date for vessel traffic time window in YYYY-MM-DD format
  -ve VESSEL_EDATE, --vessel_edate VESSEL_EDATE
                        End date for vessel traffic time window in YYYY-MM-DD format

$ python vessels.py -ws 2022-12-01 -we 2023-03-28 -vs 2022-12-01 -ve 2023-03-28
```

### Inlcude boat with name

File vessels_include.py which has included VESSELS_NAME list. 
Add new vessel names here. 
Also, I have a flag VESSEL_NAME_FILTER , If you want to turn off this filter, make it False.

`vessels_include.py` : https://github.com/gjamuar/EastCoastStranding/blob/main/vessels_include.py

This script has a function filter_vessels_by_name  which is used while reading the vessel files in read_vessel_files . Here is the change:
https://github.com/gjamuar/EastCoastStranding/blob/9c666ca2d5a46e8d7b44856343d0a58f4b02c8b2/excludeboatwithcargovesseltypenewcsv7.py#L152
https://github.com/gjamuar/EastCoastStranding/blob/9c666ca2d5a46e8d7b44856343d0a58f4b02c8b2/vessels.py#L65
