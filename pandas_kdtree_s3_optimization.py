#!/usr/bin/env python

import io
import os
import boto3
import pandas as pd
import logging
import pickle
import utm
import datetime as dt
import time

from os import listdir
from config import settings
from scipy.spatial import cKDTree
from multiprocessing import Pool, cpu_count
from crypto import security


class util:
    S3_USER = settings['S3_info']['S3_USER']
    S3_SECRET_KEY = settings['S3_info']['SECRET_KEY']
    S3_BUCKET = settings['S3_info']['S3_BUCKET']
    S3_ENDPOINT = settings['S3_info']['S3_ENDPOINT']
    LOG_FILE_PATH = settings['directories']['Log_file_path']

    PROXIMITY_RADII = [200, 400, 800, 1600]  # Meters

    crypt = security.PasswordHash
    key = os.environ['PROJECT_KEY']
    secure = crypt(key)
    decrypted_key = secure.decrypt(S3_SECRET_KEY)

    # Setup Logging system
    def start_log(self):
        if not os.path.exists(self.LOG_FILE_PATH):
            os.makedirs(self.LOG_FILE_PATH)
        log_file = os.path.join(self.LOG_FILE_PATH, dt.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d %H_%M_%S')
                                + "_" + settings['logging']['log_file_name']+".log")
        file_handler = logging.FileHandler("{0}".format(log_file))
        log_formatter = logging.Formatter("%(levelname)s %(asctime)s %(processName)s %(message)s")
        file_handler.setFormatter(log_formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        level = getattr(logging, settings['logging']['level'])
        root_logger.setLevel(level)
        root_logger.addHandler(logging.StreamHandler())

    def s3_load_trees(self, folder=settings['directories']['current_tree_folder'], end_day=dt.date.today()):
        """
        This function loads all tree files in specified S3 bucket and saves them locally inside the current_tree folder.
        If file already exists on disk, will be omitted from download
        :param folder: The folder that the tree files will be saved into
        :param end_day: The day to start loading 13 weeks of data from
        :return: Dictionary of file path and actual tree file
        """

        try:
            if isinstance(end_day, str):
                end_day = dt.datetime.strptime(end_day, '%Y-%m-%d').date()
            elif end_day is None:
                end_day = dt.date.today()
        except ValueError:
            raise InvalidDateFormatException
        logging.info('Update End Day: ' + str(end_day))

        weekday = end_day.weekday()
        delta = dt.timedelta(days=weekday, weeks=settings['util_config']['number_of_weeks'])
        start_week = end_day - delta
        logging.info('Update Start day: ' + str(start_week))

        logging.info('Loading spatial trees into memory from S3')
        file_list = self.s3_query('.tree')

        dir_names = []
        for item in file_list:
            if item[:1] != '/':
                dir_names.append(os.path.dirname(item))
        dir_names = list(sorted(set(dir_names), reverse=True))

        # get list of weeks to add, list of weeks to remove based on date
        needed_weeks, obsolete_weeks = evaluate_dates(dir_names, start_week, end_day)

        # get all tree files under needed weeks in s3
        all_files = get_all_files(file_list, dir_names)

        # sorting list of all PROJECT trees and removing directory items
        all_files = filter_all_files(all_files)

        # adding all needed weeks to list of files to download list
        files_to_download = get_files_to_download(needed_weeks, all_files)

        # obtain list of trees already inside folder
        try:
            os.mkdir(folder)
        except OSError:
            pass
        tree_folder = [f for f in listdir(os.path.join(folder, os.path.curdir))]

        # remove items already in disk from files to download list
        files_to_download = filter_download_list(files_to_download, tree_folder)

        # remove items on disk that are obsolete
        files_to_delete = get_delete_file_list(obsolete_weeks, tree_folder)
        delete_files(files_to_delete, folder)

        downloaded_files = self.s3_download(files_to_download, '.tree', folder)
        logging.info(downloaded_files)
        return downloaded_files

    def parse_incoming_file(self, request):
        """
        Parses and validates input excel file and then returns information about which tree file to load
        :param request: http request containing the .xlsx file containing addresses to score
        :return: df: Data frame of excel sheet, input_trees: Dictionary mapping utm zone to tree, needed_zones: UTM zones needed to calculate scores
        """
        # Determine file type
        if '.xlsx' in request.files['address_to_score'].filename:
            xlsx_stream = request.files['address_to_score'].stream.read()
            df = pd.read_excel(io.BytesIO(xlsx_stream))
        else:
            return 'Please input a .xlsx file. Current file is in a different format'

        # Check for missing required columns
        required_columns = ['city', 'latitude', 'longitude', 'num_chargers', 'provider']
        df.rename(columns=lambda x: x.lower(), inplace=True)
        column_names = list(df.columns.values)
        missing = []
        for req in required_columns:
            if req not in column_names:
                missing.append(req)
        if len(missing) is not 0:
            logging.info('Missing following columns: ' + missing)
            raise MissingColumnException(missing)

        # Setup count columns
        for rad in self.PROXIMITY_RADII:
            df['num_pts%i' % rad] = 0

        df = df.fillna(-1)

        # Convert input coordinates to  UTM
        df = self.calc_utm(df)

        # Make spatial trees for each of the input UTM zones
        utm_zones = df.z.unique()
        needed_zones = []
        input_trees = {}
        for each_utm_zone in utm_zones:
            needed_zones.append(each_utm_zone)
            utm_zone_coords = df[df.z == each_utm_zone]
            utm_tree = cKDTree(utm_zone_coords[['x', 'y']])

            # Store the results in a dictionary mapping between UTM zones and tree objects
            input_trees[str(each_utm_zone)] = utm_tree

        # return the dictionary of spatial trees for the input coordinates
        return df, input_trees, needed_zones

    def multiprocess_query(self, df, input_trees, PROJECT_trees):
        """
        Queries the input file against all relevant PROJECT tree data to generate number of points within certain radii
        :param df: Generated Data frame of input file from parser
        :param input_trees: Dictionary mapping utm zone to tree
        :param PROJECT_trees: Relevant tree data corresponding to only the needed UTM zones
        :return: Data frame with additional columns corresponding to points within different radii
        """

        # Use max available processors - 1
        pool = Pool(processes=cpu_count() - 1)
        pool.daemon = True
        results = {}
        counter = 0

        # Figure out which trees to query based on input trees
        for k, v in input_trees.items():
            input_tuple = (k, v)
            for key, value in PROJECT_trees.items():
                PROJECT_tuple = (key, value)

                # If the input UTM zone is in the spatial tree filename
                string = 'utm_' + str(input_tuple[0]) + '.tree'
                if string in PROJECT_tuple[0]:
                    # Start multiple processes for all trees matching that UTM zone
                    results[counter] = pool.apply_async(query_trees, args=(df, input_tuple, PROJECT_tuple))
                    counter += 1

        # Wait for all processes to finish before proceeding
        pool.close()
        pool.join()

        # Concatenate the results
        df_all = input_trees
        if results:
            df_all = pd.concat(
                {item: result.get() for item, result in results.items() if isinstance(result.get(), pd.DataFrame)},
                ignore_index=True)
            df_all = df_all.dropna(axis=1, how='all')

        col_names = list(df_all.columns)

        # Make list of columns to not group by
        dont_group = ['num_pts200', 'num_pts400', 'num_pts800', 'num_pts1600', 'x', 'y', 'z', 'zl']

        # Remove above columns from current columns
        col_names = [x for x in col_names if x not in dont_group]

        # Group by col_names, and sum the count columns
        df = df_all.groupby(by=col_names)[['num_pts200', 'num_pts400', 'num_pts800', 'num_pts1600']].sum()
        df = df.reset_index()

        if not df.empty:
            return df
        else:
            return results

    def score_locations(self, df):
        """
        Generates a score for each location
        :param df: Original data frame
        :return: Data frame with additional columns for score with chargers and without chargers
        """
        # Convert city name to title case
        df['city'] = df['city'].str.replace("_", " ")
        df['city'] = df['city'].str.title()

        # Make list of cities
        cities = df['city'].unique().tolist()
        dfs = []
        for each_city in range(len(cities)):
            # Calculate score for that city
            city_column = calc_score(df[df['city'] == cities[each_city]])
            dfs.append(city_column)

        # Merge results
        if len(dfs) > 1:
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = dfs[0]
        return df

    def rank_locations(self, df):
        """
        Generates a ranking for each location ranging from lowest, low, mid, high, highest
        :param df: Original data frame
        :return: Data frame with additional column for ranking
        """
        cities = df['city'].unique()
        df['ranking'] = ''
        num_categories = 5
        for each_city in cities:

            city_column = df[df['city'] == str(each_city)]
            max_score = city_column.score_nochargers.max()
            min_score = city_column.score_nochargers.min()
            score_ranges = (max_score - min_score) / num_categories

            for index, row in city_column.iterrows():
                if min_score <= row['score_nochargers'] < min_score + score_ranges:
                    df['ranking'].iloc[index] = 'Lowest'
                elif min_score + score_ranges <= row['score_nochargers'] < min_score + score_ranges * 2:
                    df['ranking'].iloc[index] = 'Low'
                elif min_score + score_ranges * 2 <= row['score_nochargers'] <= min_score + score_ranges * 3:
                    df['ranking'].iloc[index] = 'Mid'
                elif min_score + score_ranges * 3 <= row['score_nochargers'] <= min_score + score_ranges * 4:
                    df['ranking'].iloc[index] = 'High'
                elif min_score + score_ranges * 4 <= row['score_nochargers'] <= max_score:
                    df['ranking'].iloc[index] = 'Highest'
                else:
                    df['ranking'].iloc[index] = 'error'
        df = df.sort_values(by='score_nochargers', ascending=False)
        return df

    def s3_query(self, s3_path):
        """
        Generates a list of all files containing the substring specified
        :param s3_path: substring to query for
        :return: a list of all tree files
        """
        s3 = boto3.resource('s3', endpoint_url=self.S3_ENDPOINT, aws_access_key_id=self.S3_USER, aws_secret_access_key=self.decrypted_key)

        bucket = s3.Bucket(self.S3_BUCKET)
        files = []

        for object in bucket.objects.all():
            if s3_path in object.key:
                files.append(object.key)
        return files

    def s3_download(self, file_list, file_type, folder):
        """
        Downloads all files based on the file list and saves them into a specified folder
        :param file_list: list of files to be downloaded
        :param file_type: substring indicating type of file to be downloaded
        :param folder: folder to download to
        :return: Dictionary mapping of file to file path
        """
        new_dictionary = {}
        s3_conn = boto3.client('s3', endpoint_url=self.S3_ENDPOINT, aws_access_key_id=self.S3_USER,
                               aws_secret_access_key=self.decrypted_key)
        try:
            os.mkdir(folder)
        except OSError:
            pass
        for each_file in file_list:

            obj = s3_conn.get_object(Bucket=self.S3_BUCKET, Key=each_file)
            body = obj['Body']
            if file_type == '.tree':
                var = pickle.loads(body.read())

                with open(os.path.join(folder, os.path.basename(each_file)), 'wb') as new_file:
                    pickle.dump(var, new_file, protocol=pickle.HIGHEST_PROTOCOL)
                    new_file.close()
                    new_dictionary[str(each_file)] = os.path.join(folder, os.path.basename(each_file))
        if file_type == '.tree':
            return new_dictionary

    def load_needed_trees(self, needed_zones, folder=settings['directories']['current_tree_folder']):
        tree_dict = {}
        for zone in needed_zones:
            for filename in os.listdir(folder):
                if filename.endswith('{}.tree'.format(zone)):
                    with open(os.path.join(folder, filename), 'rb') as f:
                        var = pickle.load(f)
                        tree_dict[os.path.basename(filename)] = var
        return tree_dict

    def calc_utm(self, df):
        logging.info('Step 2 Convert the lat/lon to UTM')
        df[['x', 'y', 'z', 'zl']] = df.apply(lambda x: pd.Series(utm.from_latlon(x.latitude, x.longitude)), axis=1)
        return df

    def replace_null(self, df):
        null_rows = df.index[df['num_chargers'] == -1]
        for x in null_rows:
            df.loc[x, 'num_chargers'] = 'N/A'
            df.loc[x, 'score_chargers'] = 'N/A'
        return df


class MissingColumnException(Exception):
    pass


class InvalidDateFormatException(Exception):
    pass


def calc_score(df):
    df.num_pts1600 = df.num_pts1600 - df.num_pts800
    df.num_pts800 = df.num_pts800 - df.num_pts400
    df.num_pts400 = df.num_pts400 - df.num_pts200
    m200 = df.num_pts200.max()
    m400 = df.num_pts400.max()
    m800 = df.num_pts800.max()
    m1600 = df.num_pts1600.max()
    mchargers = df.num_chargers.max()
    df['score_nochargers'] = df.apply(score_no_charge, args=(m200, m400, m800, m1600), axis=1)
    df['score_chargers'] = df.apply(score_charge, args=(m200, m400, m800, m1600, mchargers), axis=1)

    return df.replace(pd.np.nan, 0.0, regex=True)


def query_trees(df, input_tuple, PROJECT_tuple):
    # Get the row numbers in the input file
    rows = df[df.z == int(input_tuple[0])]

    # For each radius
    for rad in util.PROXIMITY_RADII:

        # Have the input tree query the PROJECT tree
        results = input_tuple[1].query_ball_tree(PROJECT_tuple[1], r=rad)

        # Update the proper rows in the input file with the counts
        df['num_pts%i' % rad].iloc[rows.index] = pd.Series([len(i) for i in results], index=rows.index)
    return df


def score_no_charge(row, m2, m4, m8, m16):
    if row.num_pts200 + row.num_pts400 + row.num_pts800 + row.num_pts1600 != 0:
        return round(100. * (
                0.4 * row.num_pts200 / m2 + 0.3 * row.num_pts400 / m4 + 0.2 * row.num_pts800 / m8 + 0.1 * row.num_pts1600 / m16),
                     0)
    else:
        return 0


def score_charge(row, m2, m4, m8, m16, mc):
    if row.num_pts200 + row.num_pts400 + row.num_pts800 + row.num_pts1600 != 0 and mc != 0:
        return round((100. * (
                0.25 * row.num_pts200 / m2 + 0.2 * row.num_pts400 / m4 + 0.15 * row.num_pts800 / m8 + 0.1 * row.num_pts1600 / m16 + 0.3 * row.num_chargers / mc)),
                     0)
    else:
        return 0


def evaluate_dates(dir_names, start_week, end_day):
    needed_weeks = []
    obsolete_weeks = []
    for each_week in dir_names:
        date_object = dt.datetime.strptime(each_week[-10:], '%Y-%m-%d').date()
        if start_week <= date_object <= end_day:
            needed_weeks.append(each_week)
        else:
            obsolete_weeks.append(str(date_object))
    return needed_weeks, obsolete_weeks


def get_all_files(file_list, dir_names):
    all_files = []
    for each_file in file_list:
        for each_dir in dir_names:
            if each_dir in each_file:
                all_files.append(each_file)
    return all_files


def filter_all_files(all_files):
    all_files = sorted(all_files, reverse=True)
    all_files = [x for x in all_files if not x.startswith('/')]
    return all_files


def get_files_to_download(needed_weeks, all_files):
    files_to_download = []
    for needed_week in needed_weeks:
        list_of_files = [x for x in all_files if needed_week in x]
        files_to_download = files_to_download + list_of_files
    return files_to_download


def filter_download_list(files_to_download, tree_folder):
    for files_on_disk in tree_folder:
        files_to_download = [x for x in files_to_download if files_on_disk not in x]
        logging.info(files_to_download)
    return files_to_download


def get_delete_file_list(obsolete_weeks, tree_folder):
    files_to_delete = []
    for obsolete_week in obsolete_weeks:
        list_of_files = [x for x in tree_folder if obsolete_week in x]
        files_to_delete = files_to_delete + list_of_files
    return files_to_delete


def delete_files(files_to_delete, folder):
    for delete_file in files_to_delete:
        logging.info('Removing: ' + delete_file)
        os.remove(os.path.join(folder, delete_file))
