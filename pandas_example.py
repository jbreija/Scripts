#!/usr/bin/python
# DESCRIPTION: Retrieves telemetry data from Hadoop/Hive, performs some data transformations then uploads results to S3
# DEPENDENCIES: Requires Hortonworks ODBC Driver for Apache Hive (v2.1.7) https://hortonworks.com/downloads/

import boto3
import requests
import logging
import json
import os
import io
import psutil
import pandas as pd
import datetime as dt
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from pyhive import hive
import ast
import time
from geopy.distance import vincenty


class ingestData():

    # LOGGING
    LOG_FILE = os.getcwd() + "/logs"
    if not os.path.exists(LOG_FILE):
        os.makedirs(LOG_FILE)
    LOG_FILE = LOG_FILE + "/" + dt.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d %H_%M_%S') + ".log"
    logFormatter = logging.Formatter("%(levelname)s %(asctime)s %(processName)s %(message)s")
    fileHandler = logging.FileHandler("{0}".format(LOG_FILE))
    fileHandler.setFormatter(logFormatter)
    rootLogger = logging.getLogger()
    rootLogger.addHandler(fileHandler)
    rootLogger.setLevel(logging.INFO)
    rootLogger.addHandler(logging.StreamHandler())

    # CLASS CONSTANTS
    with open(os.getcwd() + '/.config', 'r') as_config:
        cfg = ast.literal_eval(_config.read())
    HV_USER = cfg['HV_USER']
    HV_PW = cfg['HV_PW']
    HADOOP_PROD = "hostname"
    HADOOP_SECONDARY = "hostname"
    HADOOP_QUERY_TEST = "select droid_id, logged_at, latitude, longitude from DB_TABLE_NAME.TELEMETRY limit 500000"
    S3_USER = cfg['S3_USER']
    S3_KEY = cfg['S3_KEY']
    S3_BUCKET = "test-bucket"
    S3_ENDPOINT = "https://" + S3_BUCKET + "@domain.com"
    S3_FILENAME = "__trips_" + dt.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S') + '.csv'
    NOTIFICATION_ENDPOINT = ""
    TIMER = time.time()

    # CITY CENTROIDS
    data = [
        ['CITY',"LAT","LON"],
        ['TBD', 1000.0, 1000.0],
        ['DETROIT',42.33,-83.04],
        ['PHOENIX',33.44,-112.07],
        ['ATLANTA',33.75,-84.38],
        ['MIAMI',25.76,-80.19],
        ['WASHINGTON_DC',38.89,-77.03],
        ['CHICAGO',41.87,-87.62],
        ['SAN_FRANCISCO',37.75,-122.49],
        ['NASHVILLE',36.16,-86.78],
        ['BOSTON', 42.35, -71.05],
        ['LOS_ANGELES', 34.04, -118.24],
        ['DENVER', 39.73, -105.00],
        ['SEATTLE', 47.60, -122.33],
        ['LAS_VEGAS', 36.16, -115.14],
        ['SALT_LAKE_CITY', 40.76, -111.88],
        ['CHARLOTTE', 35.22, -80.84],
        ['AUSTIN', 30.26, -97.74],
        ['SACRAMENTO', 38.64, -121.38],
        ['SAVANNAH', 32.05, -81.12],
        ['JACKSONVILLE', 30.28, -81.45],
        ['MEMPHIS', 35.12, -90.09],
        ['KANSAS_CITY', 35.12, -90.09],
        ['MINNEAPOLIS', 44.97, -93.27],
        ['BUFFALO', 42.87, -78.88],
        ['NEW YORK', 41.23, -73.59]
    ]
    df_city = pd.DataFrame(data[1:],columns=data[0])
    CITY_COORDS = df_city.values[None, :, 1:].astype(float)


    # PROPOSED LOCATIONS FOR ANALYSIS
    df_loc = pd.read_csv("Random_city_locations.csv")
    coords = df_loc[['Lat', 'Lon']]
    LOC_COORDS = coords.values[None, :, 1:].astype(float)

    # CIRCULAR BOUNDING BOX RADII
    RADII = [
        (.25, 'quart_mile'),
        (.50, 'half_mile'),
        (1, 'one_mile'),
        (2, 'two_mile'),
    ]

    def __init__(self, start_date, num_days):

        # INSTANCE CONSTANTS
        HADOOP_QUERY = "select droid_id, logged_at, latitude, longitude from DB_TABLE_NAME.TELEMETRY" \
                     " where logged_at between '" + str(dt.datetime.strptime(start_date, '%Y-%m-%d')) + "' and '" + str((dt.datetime.strptime(start_date, '%Y-%m-%d') +
                    dt.timedelta(days=num_days)).strftime("%Y-%m-%d %H:%M:%S")) + "'"

        global OUTPUT_DIR
        OUTPUT_DIR = os.getcwd() + "/results/" + str(start_date)
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        try:

            # Step 1. Get the data
            df = self.hv_connect(HADOOP_QUERY)
            #df = pd.read_csv(OUTPUT_DIR + "/PRACTICE.csv")

            # Step 2. Transform the data
            df = self.get_weekday(df)
            df = self.get_city(df)
            df = self.bin_timeslots(df)
            #self.analyze_location(df, start_date)
            #self.save_raw_data(df, start_date)
            df = self.count_trips(df, start_date, num_days)
            df2 = self.group_timeslots(df, start_date, num_days)

            # Step 3. Graph the data
            im = self.plot_trips(df, start_date)

            # Step 4. Upload results to S3
            self.s3_upload(df, df2, im)

            # Step 5. Send a completion notification
            self.send_notification()

            # Step 6. Bonus Level: Plot GPS readings
            # Tunnel warp to R land

        except Exception as e:
            logging.info(str(e))

    def hv_connect(self, HV_QUERY_DAYS):
        logging.info("Step 1. Retrieve telemetry data from Hive")
        self.TIMER = time.time()
        conn = hive.Connection(host=self.HADOOP_PRIMARY, username=self.HV_USER, auth='LDAP', password=self.HV_PW)
        cursor = conn.cursor()
        cursor.execute(HV_QUERY_DAYS)
        df = pd.DataFrame(cursor.fetchall())

        # # Add column names to Data Frame (df)
        cols = []
        for row in cursor.description:
            cols.append(row[0])
        df.columns = cols
        cursor.close()

        df.to_csv(OUTPUT_DIR + "/RAW_DATA.csv")
        self.log_df(df)
        return df

    def get_weekday(self, df):
        logging.info("Step 2.1 For each date in the telemetry data readings, determine day of week.")
        self.TIMER = time.time()
        df['logged_at'] = pd.to_datetime(df["logged_at"])
        df['Day'] = df['logged_at'].dt.weekday_name
        self.log_df(df)
        return df

    def get_city(self, df):
        logging.info("Step 2.2 Resolve the city name based off the lat/lon coordinates")
        self.TIMER = time.time()
        coords = df[['latitude', 'longitude']]
       _coords = coords.values[:, None].astype(float)
        df['City'] = self.df_city.CITY.iloc[(np.abs(_coords - self.CITY_COORDS) <= 2).all(2).argmax(1)].values
        self.log_df(df)
        return df

    def bin_timeslots(self, df):
        logging.info("Step 2.3 Divide each of the seven days of week into 10-minute bins.")
        self.TIMER = time.time()
        df['Time'] = pd.to_timedelta(df['logged_at'])
        df['Time'] = ((df['Time'].dt.total_seconds() - (df['Time'].dt.days * 86400)) /600)+1
        df['Time'] = df['Time'].apply(np.floor)
        df['Time'] = pd.to_datetime(df['Time'] * 600, unit='s')
        df['Time'] = df['Time'].dt.strftime('%H:%M:%S')
        self.log_df(df)
        return df

    def analyze_location(self, df, week):
        logging.info("Step 2.4 For each potential station location, record all GPS coordinates within 0.25, 0.5 and 1 mile radii.")
        self.TIMER = time.time()
        df2 = df.copy()
        df3 = df2[df2['City'] == 'CITY_NAME']
        df3.reset_index(inplace=True, drop=True)
        df_all = pd.merge(df3.assign(key=0), self.df_loc.assign(key=0), on='key').drop('key', axis=1)
        df_all['MILES'] = df_all.apply((lambda row: vincenty((row['latitude'], row['longitude']),(row['Lat'], row['Lon'])).miles),axis=1)
        closest = df_all.loc[df_all.groupby(["logged_at", 'latitude', 'longitude'])["MILES"].idxmin()]
        df_withlocs = df3.merge(closest,on=["logged_at", 'latitude', 'longitude'],suffixes=('', '_cl')).drop(['Lat', 'Lon'], axis=1)

        for dist, column in self.RADII:
            locs = df_withlocs['Street'].copy()
            locs[df_withlocs['MILES'] > dist] = np.nan
            df3[column] = locs

        df3.to_csv(OUTPUT_DIR + "/Location_Analysis_" + str(week) + ".csv")
        self.log_df(df3)

    def save_raw_data(self, df, week):
        logging.info("Step 2.5 Save a .csv file of raw telemetry data for each city.")
        df.groupby('City').apply(lambda x: x.to_csv(OUTPUT_DIR + "/data_{}_".format(x.name.lower()) + str(week) + ".csv"))

    def count_trips(self, df, week, days):
        logging.info("Step 2.6 For each telemetry reading corresponding to the day of week / time of day distribution,"
                     "add one observation to the bin, omitting duplications from the same droid")
        self.TIMER = time.time()
        df = df.drop_duplicates(subset=['droid_id', 'Time'], keep='first')
        df = df.groupby(['City', 'Day', 'Time'])[['Time']].count()
        df = df.rename(columns={'Time': 'Active Droids'})
        df.reset_index(inplace=True)
        df.loc[:, 'Source'] = "Hadoop_" + str(week) + "_" + str(days) + " days"
        df.to_csv(OUTPUT_DIR + "/10min_timeslots_" + str(week) + ".csv")
        self.log_df(df)
        return df

    def group_timeslots(self, df, week, days):
        logging.info("Step 2.7 Associate each telemetry reading according to Time Of Day (TOD) distributions, "
                     "Note that in this proposed framework the day begins and ends at 3:30 AM")
        self.TIMER = time.time()
        df2 = df.copy()
        df2['Time'] = pd.to_timedelta(df2['Time'])
        df2['Time'] = ((df2['Time'].dt.total_seconds() - (df2['Time'].dt.days * 86400)) / 600) + 1
        df2 = df2.groupby([pd.cut(df2['Time'], bins=[0, 21, 39, 57, 93, 111, 131, 144], labels=["Night", "Late Night", "AM Peak", "Mid-day", "PM Peak", "Evening", "Night2"]), 'Day', 'City'])['Active Droids']
        df2 = df2.sum().to_frame().reset_index()
        df2['Active Droids'] = df2['Active Droids'].astype(np.float64)
        df2['Time'] = df2['Time'].replace({'Night2': 'Night'})
        df2 = df2.groupby(['City', 'Day', 'Time']).agg({'Active Droids': ['sum']})
        df2 = df2.reset_index()
        df2.columns = df2.columns.droplevel(1)
        avg = {"Night": 36, "Late Night": 18, "AM Peak": 18, "Mid-day": 36, "PM Peak": 18, "Evening": 18}
        df2['Active Droids'] /= df2['Time'].map(avg)
        df2.loc[:, 'Source'] = "Hadoop_" + str(week) + "_" + str(days) + " days"
        df2.to_csv(OUTPUT_DIR + "/TOD_timeslots_" + str(week) + ".csv")
        self.log_df(df2)
        return df2

    def plot_trips(self, df, week):
        logging.info("Step 3 Visualize activity by time, day and city")
        #df2 = df.copy()
        #df2.set_index(['City', 'Time'], inplace=True)
        df['Time'] = pd.to_datetime(df['Time'])
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        cities = ["WASHINGTON_DC", "LOS_ANGELES", "SAN_FRANCISCO", "DETROIT", "BOSTON", "PHOENIX", "CHICAGO", "ATLANTA"]
        #cities =  np.unique(df["City"])
        fig, axes = plt.subplots(nrows=len(days), figsize=(13, 8), sharex="all")

        # loop over days (one could use groupby here, but that would lead to days unsorted)
        for i, day in enumerate(days):
            ddf = df[df["Day"] == day].sort_values("Time")
            for city in cities:
                dddf = ddf[ddf["City"] == city]
                axes[i].plot(dddf["Time"], dddf["Active Droids"], label=city)
            axes[i].margins(x=0)
            axes[i].set_title(day)

        fmt = matplotlib.dates.DateFormatter("%H:%M")
        axes[-1].xaxis.set_major_formatter(fmt)
        axes[0].legend(bbox_to_anchor=(1.02, 1))
        axes[0].set_title('Active droids in top 8 most active cities by time and weekday, week of ' + str(week))
        fig.subplots_adjust(left=0.05, bottom=0.05, top=0.95, right=0.85, hspace=0.8)
        plt.savefig(os.getcwd() + "/results/" + str(week) + "/Plot_by_time_day_city_" + str(week) + ".png", format='png')
        self.log_df(df)
        return plt

    def s3_upload(self, df, df2, im):
        logging.info("Step 4 Upload results to S3")
        buf = io.StringIO()
        buf2 = io.StringIO()
        df.to_csv(buf)
        s3_conn = boto3.client('s3', endpoint_url=self.S3_ENDPOINT, aws_access_key_id=self.S3_USER, aws_secret_access_key=self.S3_KEY)
        s3_conn.put_object(Body=buf.getvalue(), Bucket=self.S3_BUCKET, Key=self.S3_FILENAME)

        df2.to_csv(buf2)
        s3_conn.put_object(Body=buf2.getvalue(), Bucket=self.S3_BUCKET, Key="TOD_" + self.S3_FILENAME)

        buf = io.BytesIO()
        im.savefig(buf, format='png')
        s3_conn.put_object(Body=buf.getvalue(), Bucket=self.S3_BUCKET, Key="Plot_by_time_day_city.png")
        buf.close()

    def send_notification(self):
        logging.info("Step 5 Communicate results")
        flag=""
        try:
            r= requests.post(self.NOTIFICATION_ENDPOINT, data=json.dumps(flag), timeout=5)
            logging.info(str(r.status_code))
        except Exception as e:
            logging.error(str(e))

    def log_df(self, df):
        with pd.option_context('display.width', 1000, 'display.max_rows', 200):
            process = psutil.Process(os.getpid())
            logging.info("Step Runtime: " + self.run_time(time.time() - self.TIMER))
            logging.info("Step Data Frame Memory Usage: " + str(((df.memory_usage(index=True).sum()/1024)/1024)) + " Mb's")
            logging.info("Total Process Memory Usage: " + str(((process.memory_info().rss / 1024) / 1024)) + " Mb's\n")
            self.fileHandler.setFormatter("")
            logging.info(df)
            self.fileHandler.setFormatter(self.logFormatter)

    def run_time(self, seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "%02d:%02d:%02d" % (h, m, s)

if __name__ == "__main__":
    ingestData('2018-02-18', 7) #start_date, num_days