#!/usr/bin/env python

import boto3
import logging
import os
import io
import time
import pandas as pd
import numpy as np
import pickle
import sys
from multiprocessing import Pool, cpu_count
from queue import Queue
from scipy.spatial import cKDTree
import datetime as dt
from config import settings
from crypto import security

# Config settings
S3_USER = settings['S3_info']['S3_USER']
S3_SECRET_KEY = settings['S3_info']['SECRET_KEY']
S3_BUCKET = settings['S3_info']['S3_BUCKET']
S3_ENDPOINT = settings['S3_info']['S3_ENDPOINT']
LOG_FILE_PATH = settings['logging']['Log_file_path']
S3_PATH_FOR_DOWNLOADING_project_DATA = settings['S3_info']['S3_PATH_FOR_DOWNLOADING_project_DATA']

crypt = security.PasswordHash
key = os.environ['project_KEY']
secure = crypt(key)
decrypted_key = secure.decrypt(S3_SECRET_KEY)


def start_log():
    """
    Setup Logging system
    :return: None
    """
    if not os.path.exists(LOG_FILE_PATH):
        os.makedirs(LOG_FILE_PATH)
    log_file = LOG_FILE_PATH + "/" + dt.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d %H_%M_%S') + ".log"
    file_handler = logging.FileHandler("{0}".format(log_file))
    log_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                  "%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(logging.StreamHandler())


def multi_processor(input_data, func_name, num_loops):
    """
    multi_process processing
    :param input_data: a data frame
    :param func_name: the function which is going to be run
    :param num_loops: number of loops is required for multiple processing
    :return: a dictionary
    """
    try:
        if len(input_data.index) > cpu_count() * num_loops:
            combined_data = np.array_split(input_data, (cpu_count() - 1) * num_loops)
        else:
            combined_data = []
            combined_data.append(input_data)

        pool = Pool(processes=cpu_count() - 1)
        pool.daemon = True
        results = {}

        q = Queue()
        for d in combined_data:
            q.put(d)

        del combined_data
        del input_data
        for x in range(int(q.qsize())):
            results[x] = pool.apply_async(func_name, args=(q.get(), x))

        pool.close()
        pool.join()

        df_all = pd.concat(
            {item: result.get() for item, result in results.items() if isinstance(result.get(), pd.DataFrame)},
            ignore_index=True)

        if not df_all.empty:
            return df_all
        else:
            return results

    except Exception as e:
        logging.error("Multiprocessing failed.\n" + str(e))
        sys.exit(0)


def upload_object_to_s3(object_to_save, s3_path):
    """
    upload df or KdTree to S3
    :param object_to_save: a csv or tree file
    :param s3_path: a string
    :return: none
    """
    logging.info("Uploading S3 file " + str(s3_path))
    try:
        s3_conn = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_USER,
                               aws_secret_access_key=decrypted_key)
        if isinstance(object_to_save, pd.DataFrame):
            buf = io.StringIO()
            object_to_save.to_csv(buf, index=False)
            s3_conn.put_object(Body=buf.getvalue(), Bucket=S3_BUCKET, Key=s3_path)

        if isinstance(object_to_save, cKDTree):
            buf = io.BytesIO()
            pickle.dump(object_to_save, buf)
            s3_conn.put_object(Body=buf.getvalue(), Bucket=S3_BUCKET, Key=s3_path)
    except Exception as e:
        logging.error("Uploading to S3 file failed. \n" + str(e))
        sys.exit(0)


def s3_query(file_type):
    """
    given the file type, find the file list in S3
    :param file_type: a csv or tree file
    :return: the files downloaded from S3
    """
    try:
        s3 = boto3.resource('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_USER, aws_secret_access_key=decrypted_key)
        bucket = s3.Bucket(S3_BUCKET)
        files = []

        for object in bucket.objects.all():
            if file_type in object.key:
                files.append(object.key)
        return files
    except Exception as e:
        logging.error("Finding the file list in S3 failed. \n" + str(e))
        sys.exit(0)


def download_object_from_s3(file_list, file_type):
    """
    download data (csv or tree) from S3
    :param file_list: a list of files
    :param file_type: could be csv or tree
    :return: a object list if input file is csv, or return a dictionary list if input is tree
    """
    try:
        object_list = []
        object_dictionary = {}
        s3_conn = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_USER,
                               aws_secret_access_key=decrypted_key)

        for each_file in file_list:

            obj = s3_conn.get_object(Bucket=S3_BUCKET, Key=each_file)
            body = obj['Body']
            if file_type == ".csv":
                csv_string = body.read().decode('utf-8')
                df = pd.read_csv(io.StringIO(csv_string))
                object_list.append(df)
            elif file_type == ".tree":
                var = pickle.loads(body.read())
                object_dictionary[str(each_file)] = var

        if file_type == ".csv":
            return object_list
        elif file_type == ".tree":
            return object_dictionary

    except Exception as e:
        logging.error("Downloading from S3 file failed. \n" + str(e))
        sys.exit(0)


def download_and_merge_project_data_from_s3(num_weeks):
    """
    download project data from s3 and merge them as one file
    :param num_weeks: date
    :return: a data frame contains merged project data
    """
    logging.info("Retrieving last " + str(num_weeks) + " weeks of project results from S3")
    try:
        files_to_download = s3_query(S3_PATH_FOR_DOWNLOADING_project_DATA)
        files_to_download.sort(reverse=True)
        files_to_download = files_to_download[:num_weeks]
        downloaded_files = download_object_from_s3(files_to_download, ".csv")

        df = pd.concat(downloaded_files, ignore_index=True)
        df = df.groupby(['City', 'Day', 'Time'])[['active droids']].sum()
        df = df.reset_index()

        # Don't forget to divide the sum
        df['active droids'] = df['active droids'] / num_weeks
        df.loc[:, 'Source'] = "project_Hadoop"
        return df
    except Exception as e:
        logging.error("Retrieving last " + str(num_weeks) + " weeks of project results from S3 failed. \n" + str(e))
        sys.exit(0)


def run_time(seconds):
    """
    format time to hh:mm:ss
    :param seconds: number of seconds
    :return: a formatted time
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)


def save_csv(df, folder_name, file_name, start_date):
    """
    save raw data to local folder
    :param df: input
    :param folder_name: specific folder name
    :param file_name: specific file name
    :param start_date: start date of the data
    :return: none
    """
    logging.info("Saving this file: " + folder_name + start_date + ".csv")
    if not os.path.exists(folder_name + start_date):
        os.makedirs(folder_name + start_date)

    path = '{}{}/RAW_DATA_'.format(folder_name, start_date)
    df.to_csv(path + file_name + ".csv", index=False)
