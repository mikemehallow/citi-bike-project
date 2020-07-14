import pandas as pd
import numpy as np

def process_monthly_trips(zip_file_path):

    # Read compressed csv files
    trips = pd.read_csv(zip_file_path, compression='zip')

    # Replace all spaces with underscores in column names
    trips.columns = [x.replace(' ', '_') for x in trips.columns]

    # Round start and stop times to nearest second
    trips['starttime'] = pd.to_datetime(trips.starttime)
    trips['stoptime'] = pd.to_datetime(trips.stoptime)
    
    trips = trips.rename(columns={'starttime': 'start_time', 'stoptime': 'stop_time', 'bikeid': 'bike_id'})
    
    return trips


def fill_missing_ops(trips, ops):
    trips['prev_end_station_id'] = trips.sort_values(by='start_time').groupby('bike_id')['end_station_id'].shift()
    trips['prev_end_station_name'] = trips.sort_values(by='start_time').groupby('bike_id')['end_station_name'].shift()
    trips['prev_stop_time'] = trips.sort_values(by='start_time').groupby('bike_id')['stop_time'].shift()

    trips['station_match'] = trips.start_station_id == trips.prev_end_station_id
    
    missing = trips[(~trips.prev_end_station_id.isna()) & (~trips.station_match)].copy()
    missing['time_gap'] = missing.start_time - missing.prev_stop_time
    missing['missing_start_time'] = (missing.prev_stop_time + missing.time_gap/2)
    missing['missing_stop_time'] = (missing.prev_stop_time + missing.time_gap/2) + pd.Timedelta(1, 'ms')    

    # Create dataframe of simulated data
    sim_starts = missing[['missing_start_time', 'prev_end_station_id', 'prev_end_station_name', 'bike_id']]
    sim_stops = missing[['missing_stop_time', 'start_station_id', 'start_station_name', 'bike_id']]

    sim_starts = sim_starts.rename(columns={'missing_start_time': 'op_time', 'prev_end_station_id': 'station_id', 'prev_end_station_name': 'station_name'})
    sim_stops = sim_stops.rename(columns={'missing_stop_time': 'op_time', 'start_station_id': 'station_id', 'start_station_name': 'station_name'})

    sim_starts['op_type'] = 'departure'
    sim_stops['op_type'] = 'arrival'

    sim_starts['net_bikes'] = -1
    sim_stops['net_bikes'] = 1

    sim_ops = pd.concat([sim_starts, sim_stops]).sort_values(by='op_time')
    
    # Append simulated trips data to ops data and refer to new dataframe as ops_full
    ops = pd.concat([sim_ops, ops]).sort_values(by='op_time')

    return ops

def convert_trips_to_ops(trips):
    
    trips = trips.rename(columns={'tripduration': 'trip_duration', 
                                  'starttime': 'start_time',
                                  'stoptime': 'stop_time',
                                  'bikeid': 'bike_id',
                                  'usertype': 'user_type'})

    starts = trips[['start_time', 'start_station_id', 'start_station_name', 'bike_id']]
    stops = trips[['stop_time', 'end_station_id', 'end_station_name', 'bike_id']]

    starts = starts.rename(columns={'start_time': 'op_time', 'start_station_id': 'station_id', 'start_station_name': 'station_name'})
    stops = stops.rename(columns={'stop_time': 'op_time', 'end_station_id': 'station_id', 'end_station_name': 'station_name'})

    starts['op_type'] = 'departure'
    stops['op_type'] = 'arrival'

    starts['net_bikes'] = -1
    stops['net_bikes'] = 1

    ops = pd.concat([starts, stops]).sort_values(by='op_time')
    
    return ops


def insert_status_updates(stations, ops):
    
    # Transform station updates data into ops data format
    station_cols = ['lastCommunicationTime', 'id', 'stationName', 'availableBikes']
    status_updates = stations[station_cols].rename(columns={'lastCommunicationTime': 'op_time', 
                                                            'id': 'station_id', 
                                                            'stationName': 'station_name',
                                                            'availableBikes': 'available_bikes'})

    status_updates['op_time'] = pd.to_datetime(status_updates['op_time'])
    status_updates['op_type'] = 'status'
    status_updates['net_bikes'] = 0
    status_updates['bike_id'] = np.nan

    # Merge status updates
    ops = pd.concat([ops, status_updates]).sort_values(by='op_time')
    
    return ops