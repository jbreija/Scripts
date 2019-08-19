#!/usr/bin/python
# ROS Camera Processor
# Description: Automation tool for mass extraction of images and video from any number of ROS camera topics or bag files.

import logging
import argparse
import subprocess
import time
import os
import re
from Queue import Queue
from multiprocessing import Pool, cpu_count
import cv2
from cv_bridge import CvBridge
import rosbag
import datetime

# PARAMETERS
    # Required
parser = argparse.ArgumentParser(description='bag-parser - Pull metatdata from multiple ROS bag files at once')
parser.add_argument('-b', '--bag_file', help='Required: Name of bag file.', required=True)
parser.add_argument('-c', '--camera_topic', nargs='+', help='Required: Name of camera topic(s), Use "all" for all', required=True)
    # Semi-Optional
parser.add_argument('-v', '--video', action='store_true', help='SExtract video. -v or -i must be specified.')
parser.add_argument('-i', '--image', action='store_true', help='Extract image. -v or -i must be specified.')
    # Optional
parser.add_argument('-o', '--output_dir', help='Optional: Directory to save results.')
parser.add_argument('-p', '--path', action='store_true', help='Optional: Same as -a but puts all results into same camera folders')
args = parser.parse_args()

# LOGGING
LOG_FILE = "/home/user/image_2_video.log"
logFormatter = logging.Formatter("%(levelname)s %(asctime)s %(processName)s %(message)s")
fileHandler = logging.FileHandler("{0}".format(LOG_FILE))
fileHandler.setFormatter(logFormatter)
ch = logging.StreamHandler()
rootLogger = logging.getLogger()
rootLogger.addHandler(fileHandler)
rootLogger.addHandler(ch)
rootLogger.setLevel(logging.INFO)

""" MAIN - Check the parameters to determine what to process"""
def main():
    # logging info
    main_start = time.time()
    logging.info('Main start')

    # Check for -p parameter to determine if we're processing multiple bags or just one
    if args.path:
        multi_process(args.bag_file)
    else:
        process_camera(args.bag_file, "1", "1")

    # End of main()
    run_time = secTime(time.time() - main_start)
    logging.info('Total runtime: ' + run_time)
    return

""" Multiprocesses all bag files in input directory if -p parameter specified"""
def multi_process(bag_file):

    # Make a list of all other bag files in the same directory as the input bag
    bag_folder = str(os.path.dirname(bag_file)) + "/"
    bag_list = [f for f in os.listdir(bag_folder) if ".bag" in f]

    # Add the list of bags to a processing queue
    q = Queue()
    total_data = 0
    for bag in bag_list:
        q.put(bag)
        total_data += os.stat(str(os.path.dirname(bag_file)) + "/" + bag).st_size
    total_data = str((((total_data / 1024) / 1024) / 1024))

    # Start a new process for every bag in the queue
    num_bags = q.qsize()
    if num_bags < cpu_count():
        num_cpu = num_bags
        pool = Pool(processes=num_bags)
    else:
        num_cpu = cpu_count()
        pool = Pool(processes=cpu_count())
    pool.daemon = True

    logging.info('Multiprocessing ' + str(num_bags) + ' new bags (' + total_data + "Gb) with " + str(num_cpu) + " processors is commencing")
    for x in range(int(q.qsize())):
        pool.apply_async(process_camera, args=(str(bag_folder + q.get()), str(x + 1), str(num_bags)))

    # Wait for sub-processes to finish before proceeding

    pool.close()
    pool.join()

    if args.path:
        tmp_folders = reconstruct_path()
    return

""" Processes a single bag file for camera images"""
def process_camera(bag_name, counter, num_bags):
    try:
        # Assign a bag reader object to the bag file path
        bag_object = rosbag.Bag(bag_name, "r")
        # Make camera topic sub-directories under bag name directory for output prep

        output_folders = setup_output_directory(bag_object)
        # Extract the camera images to the output folders
        extract_images(bag_object, output_folders)
        bag_object.close()
        # If -v parameter specified then make videos from images
        if not args.path:
            if args.video:
                create_vid(output_folders)
                # If -i parameter not specified then images are deleted after video is made
                if not args.image:
                    delete_images(output_folders)

    except Exception as e:
        logging.error('Crash ' + bag_name + ' ' + str(e))

""" Extracts images from cameras"""
def extract_images(bag_obj, output_folders):

    # output folders is a list of dictionary mappings from topics to output folders
    topics = output_folders.keys()

    # setup list of dictionary entries for each topic.
    img_counter = []
    for top in topics:
        img_counter.append({'topic': top, "tot_img": bag_obj.get_message_count(top), "cur_img":0})

    # For every image msg in the specified camera topic...
    bridge = CvBridge()
    for topic, msg, t in bag_obj.read_messages(topics=topics):
        # Convert ROS img msg to openCV2
        cv_img = bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")

        # Get the correct output folder for this camera topic
        o_dir = output_folders.get(topic)

        # Retrieve the counter from the camera counter array
        results = (item for item in img_counter if item["topic"] == topic).next()
        count = results['cur_img'] + 1

        # Write the image.
        cv2.imwrite(os.path.join(o_dir, 'frame{:05d}_{}.png'.format(count, results['tot_img'])), cv_img)
        logging.info('frame{:06d}_{}.png '.format(count, results['tot_img']) + str(os.path.basename(bag_obj.filename)) + " " + str(topic))

        # Update the counter in the topic dictionary
        for d in img_counter:
            d.update(("cur_img", count) for k, v in d.iteritems() if v == topic)

""" Creates videos from images if -v parameter specified"""
def create_vid(output_folders):

    # For every camera topic directory in the bag folder, run mencoder in the command line to reconstruct the video
    for x in output_folders:
        cmd = "mencoder \"mf://" + output_folders[x] + "/*.png\" -mf type=png:fps=10 -o " + output_folders[
           x] + "/output.mp4 -speed 1 -ofps 10 -ovc x264 -x264encopts preset=veryslow:tune=film:crf=30:frameref=10:fast_pskip=0:threads=auto"
        subprocess.check_output(cmd, shell=True)

def get_hostname(bag_obj):
    for topic, msg, time in bag_obj.read_messages(topics='/metadata'):
        hostname = eval('msg.hostname')
        break
    return hostname

def get_camera_topics(bag_obj):
    cam_topic = []
    for top in bag_obj.get_type_and_topic_info()[1].keys():
        if "image_raw" in top and "grey" not in top:
            cam_topic.append(top)
    return cam_topic

""" Determines output directory structure. Every bag gets a folder with subfolders for each camera topic."""
def setup_output_directory(bag_obj):

    # Create folder for each camera topic specified in script parameters
    outDirs = {}
    hostname = get_hostname(bag_obj)
    for topic in args.camera_topic:
        if topic == "all":
            cam_topics = get_camera_topics(bag_obj)
            for top in cam_topics:
                outDirs[top] = create_folders(bag_obj, top, hostname)
        else:
            outDirs[topic] = create_folders(bag_obj, topic, hostname)
    return outDirs

""" Creates output folders"""
def create_folders(bag, top, hostname):
    # Construct the folder path string
    output_dir = str(re.search('^(.*)\/', (str(args.output_dir) + hostname + "/" + str(os.path.basename(bag.filename)[:-4]) + str(top))).group(1))

    # Make the folder if it doesn't already exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

""" Deletes images after video is created if -v parameter is used without -i"""
def delete_images(output_folders):
    for f in output_folders:
        cmd = "rm -r {}/*.png".format(output_folders[f])
        subprocess.check_output(cmd, shell=True)

""" Formats integer seconds into 00:00:00 format"""
def secTime(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

""" Creates pseudo /tmp directory using sym links for reorganizing images in chronological order"""
def reconstruct_path():
    bag_obj = rosbag.Bag(args.bag_file, "r")
    hostname = get_hostname(bag_obj)
    baglist = []
    cam_topics = []

    for arg in args.camera_topic:
        if arg == "all":
            cam_topics = get_camera_topics(bag_obj)
        else:
            cam_topics.append(str(re.search('^(.*)\/', (str(arg))).group(1)))

    firstbag = str(args.bag_file)
    count = 0
    for f in sorted(os.listdir(args.output_dir + "/" + hostname)):
        baglist.append(str(f))
        if count == 0:
            firstbag = str(os.path.dirname(args.bag_file)) + "/" + str(f) + ".bag"
        count += 1

    first_bag = rosbag.Bag(firstbag, "r")
    path_start = bag_obj.get_start_time()
    for topic, msg, time in first_bag.read_messages(topics='/metadata'):
        hostname = eval('msg.hostname')
        break
    first_bag.close()

    path_folder = args.output_dir + "path_" + hostname + "_" + datetime.datetime.fromtimestamp(int(path_start)).strftime('%Y_%m_%d_%H_%M_%S')

    newlist = sorted(cam_topics)
    for topic in newlist:
        count = 1
        if not os.path.exists(path_folder + "/" + topic[:-9]):
            os.makedirs(path_folder + "/" + topic[:-9])
        for bag in baglist:
            image_path = args.output_dir + hostname + "/" + bag + topic[:-9]
            for i in sorted(os.listdir(image_path)):
                cmd = "ln -sf " + image_path + "/" + i + " " + path_folder + topic[:-9] + '/frame{:05d}.png'.format(count)
                subprocess.check_output(cmd, shell=True)
                count += 1
        output_folder = str(path_folder + topic[:-9])
        cmd = "mencoder \"mf://" + output_folder + "*.png\" -mf type=png:fps=10 -o " + output_folder + "output.mp4 -speed 1 -ofps 10 -ovc x264 -x264encopts preset=veryslow:tune=film:crf=30:frameref=10:fast_pskip=0:threads=auto"
        subprocess.check_output(cmd, shell=True)

""" Program begin. Calls main()"""
if __name__ == '__main__':
    main()