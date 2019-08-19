# vid_aud_2_text.py v1.0 Video Scraper - By Jason Breijak

# Input is a video file and output is a .csv file for all of the displayed text and spoken speech in video.
# Results are saved to results.csv, all intermediate data is stored in /output folder


import speech_recognition as sr
import wave
#import moviepy.editor as mp
import subprocess
import re
import csv
import itertools
import os, os.path
import fnmatch
import timeit
import sys

# LOG FILE: uncomment line below if you want to redirect output to a logfile
# sys.stdout = open('logfile.txt', 'a')

runStart = timeit.default_timer() #used to keep track of program run time

# Input Variables:
videosource = 'inputvideo3.avi' # Name of video file input. Program will automatically detect and convert source video type to .avi
shutterRate = 5 # Number of seconds between still frame images. Used for OCR
sampleRate = 15 # Number of seconds between audio clips. Used for Speech Recognition. Recommended max = 30, else might hang.

# check to see if video source file exists and throw error if not
checkSrc = "{}{}".format("./", videosource)
if not os.path.isfile(checkSrc):
    error = "{}{}".format("Error: no such input video file ", videosource)
    sys.exit(error)


# Beginning of function declarations. Skip to end to see main function where program begins.
def TimeToSecond(time):
    (h, m, s) = time.split(':')
    result = int(h) * 3600 + int(m) * 60 + int(s)
    return result

def SecondToTime(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

def AudToAudClip():

    # Both of these variables are used in the loop below. Setting their initial values here.
    endSample = sampleRate
    startSample = endSample - sampleRate

    statusUpd = 'Extracting ~{0} audio clips from video source (Please wait, see /output dir for status)...'
    statusUpd = statusUpd.format(round(sampleCount))
    print(statusUpd)
    for i in range(int(sampleCount)):

        audClip = 'output/audClip{0}.wav'
        audClip = audClip.format(i + 1)

        win = wave.open("output/allAudio.wav", 'rb')
        wout = wave.open(audClip, 'wb')

        t0, t1 = startSample, endSample  # cut audio between start and end time
        s0, s1 = int(t0 * win.getframerate()), int(t1 * win.getframerate())

        win.readframes(s0)  # discard
        frames = win.readframes(s1 - s0)

        wout.setparams(win.getparams())
        wout.writeframes(frames)

        win.close()
        wout.close()

        endSample = sampleRate * (i + 2)
        startSample = endSample - sampleRate

    audCount = len(fnmatch.filter(os.listdir("output/"), "audClip*"))
    statusUpd = 'Success!, {0} audio clips created'
    statusUpd = statusUpd.format(audCount)
    print(statusUpd)
    return

def VidToAud(filename):
    import subprocess

    #clear any previous data
    clearFile = "rm output/allAudio.wav 2> /dev/null"
    subprocess.call(clearFile, shell=True)

    command = "ffmpeg -i %s -loglevel panic -ab 160k -ac 2 -ar 44100 -vn output/allAudio.wav"  % (filename)
    subprocess.call(command, shell=True)
    return

def VidLength(filename):

    #this function calls ffprobe in the commmand prompt which displays the video length embedded in a paragraph. A regex
    #is then used to extract the time value

    import subprocess
    result = subprocess.Popen(["ffprobe", filename],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    probeResult = ""


    for x in result.stdout.readlines():
        # the line with the Video length time has "Duration" written next to it so we're searching for that line
        x = x.decode("utf-8")

        if re.search("Duration", x):
        #if "Duration" in x:
            probeResult = x
        # here's the regex to retrieve the time format from the line with "Duration"
    rgx_filter = re.compile('(\d\d:\d\d:\d\d)')
    mfilter = re.search(rgx_filter, probeResult)
    time = mfilter.group(1)
    return time

def VidToPic(filename, shutterRate):
    import subprocess

    # clear any previous img data
    clearFile = "rm output/imgClip* 2> /dev/null"
    subprocess.call(clearFile, shell=True)

    # Status update of program execution outputted
    imgCount = (videoSeconds / shutterRate) + 1
    imgCount = round(imgCount)
    statusUpd = 'Extracting ~{0} still frames from video source (Please wait, see /output dir for status)...'
    statusUpd = statusUpd.format(imgCount)
    print(statusUpd)

    # I originally anticipated having to run a command that extracts a frame and using python to loop through it but found
    # that ffmpeg's -vf argument can extract frames at regularly specified intervals like this:
    command = 'ffmpeg -i {0} -loglevel panic -vf fps=1/{1} -qscale:v 2 output/imgClip{2}.png'
    command = command.format(filename, shutterRate, '%01d')
    subprocess.call(command, shell=True)

    imgCount = len(fnmatch.filter(os.listdir("output/"), "imgClip*"))
    statusUpd = 'Success!, {0} Images created'
    statusUpd = statusUpd.format(imgCount)
    print(statusUpd)
    return

def writeCSV(inputFile, res_type):
    #Used for OCR only. STT csv write is local to that function, would like to merge the calls but wasn't top priority

    #count number of files matching the inputFile e.g. audClip, txtClip, enhClip etc....
    txtCount = len(fnmatch.filter(os.listdir("output/"), inputFile + "*"))

    #Do this loop for the number of files we have
    for i in range(txtCount):
        txtClip = "output/" + inputFile + "{0}.txt"
        txtClip = txtClip.format(i + 1)
        with open(txtClip, 'r') as in_file:
            # Strip the lines from the txt file
            stripped = (line.strip() for line in in_file if line)

            # Merge them into an iteratable group
            grouped = list(itertools.zip_longest(*[stripped] * 1))



            with open(results_csv, "a") as outfile:
                time = i * shutterRate
                endtime = time + shutterRate
                writer = csv.writer(outfile, delimiter=',', dialect='excel-tab')

                outputlines = [videosource, videoSeconds, res_type, ocrTimeTrack[i], txtClip, time, endtime]
                outputlines.extend(grouped)
                writer.writerow(outputlines)


    return

def SpeechRec(filename):
    VidToAud(filename)
    AudToAudClip()
    audCount = len(fnmatch.filter(os.listdir("output/"), "audClip*"))
    srSource = "google"  # Speech Recognition API to use (until we build and train one locally)

    from os import path
    for i in range(audCount):

        # https://github.com/Uberi/speech_recognition/blob/master/examples/audio_transcribe.py

        audClip = 'output/audClip{}.wav'
        audClip = audClip.format(i+1)


        AUDIO_FILE = path.join(path.dirname(path.realpath(__file__)), audClip)

        statusUpd = 'Processing audio clip {} of {} for speech recognition'
        statusUpd = statusUpd.format(i+1, audCount)
        print (statusUpd)

        # sr object is imported from Speech_Recognition package imported on line 1
        r = sr.Recognizer()
        with sr.AudioFile(AUDIO_FILE) as source:

            try:
                sttRunStart = timeit.default_timer()

                # read the entire audio file
                audio = r.record(source)

                with open(results_csv, "a") as outfile:
                    time = i * sampleRate
                    endtime = time + sampleRate
                    writer = csv.writer(outfile, delimiter=',', dialect='excel-tab')

                    #r.recognize_google is where the call to Google's speech recognition API is made
                    sttResults = r.recognize_google(audio)
                    sttRunTime = "{:10.2f}".format(timeit.default_timer() - sttRunStart)

                    outputlines = [videosource, videoSeconds, "stt", sttRunTime, audClip, time, endtime, sttResults]


                    writer.writerow(outputlines)

            except sr.UnknownValueError:
                print("Could not understand audio")
            except sr.RequestError as e:
                print("Unable to connecto to API; {0}".format(e))

    # OTHER API'S ARE LISTED BELOW BUT REQUIRE API KEY, Sphinx, Wit, Bing, Houndify, IBM.
    # for testing purposes, we're just using the default API key
    # to use another API key, use `r.recognize_google(audio, key="GOOGLE_SPEECH_RECOGNITION_API_KEY")`

    # # recognize speech using Sphinx
    # try:
    #     print("Sphinx thinks you said " + r.recognize_sphinx(audio))
    # except sr.UnknownValueError:
    #     print("Sphinx could not understand audio")
    # except sr.RequestError as e:
    #     print("Sphinx error; {0}".format(e))

    # # recognize speech using Wit.ai
    # WIT_AI_KEY = "INSERT WIT.AI API KEY HERE"  # Wit.ai keys are 32-character uppercase alphanumeric strings
    # try:
    #     print("Wit.ai thinks you said " + r.recognize_wit(audio, key=WIT_AI_KEY))
    # except sr.UnknownValueError:
    #     print("Wit.ai could not understand audio")
    # except sr.RequestError as e:
    #     print("Could not request results from Wit.ai service; {0}".format(e))

    # # recognize speech using Microsoft Bing Voice Recognition
    # BING_KEY = "INSERT BING API KEY HERE"  # Microsoft Bing Voice Recognition API keys 32-character lowercase hexadecimal strings
    # try:
    #     print("Microsoft Bing Voice Recognition thinks you said " + r.recognize_bing(audio, key=BING_KEY))
    # except sr.UnknownValueError:
    #     print("Microsoft Bing Voice Recognition could not understand audio")
    # except sr.RequestError as e:
    #     print("Could not request results from Microsoft Bing Voice Recognition service; {0}".format(e))

    # # recognize speech using Houndify
    # HOUNDIFY_CLIENT_ID = "INSERT HOUNDIFY CLIENT ID HERE"  # Houndify client IDs are Base64-encoded strings
    # HOUNDIFY_CLIENT_KEY = "INSERT HOUNDIFY CLIENT KEY HERE"  # Houndify client keys are Base64-encoded strings
    # try:
    #     print("Houndify thinks you said " + r.recognize_houndify(audio, client_id=HOUNDIFY_CLIENT_ID,
    #                                                              client_key=HOUNDIFY_CLIENT_KEY))
    # except sr.UnknownValueError:
    #     print("Houndify could not understand audio")
    # except sr.RequestError as e:
    #     print("Could not request results from Houndify service; {0}".format(e))

    # # recognize speech using IBM Speech to Text
    # IBM_USERNAME = "INSERT IBM SPEECH TO TEXT USERNAME HERE"  # IBM Speech to Text usernames are strings of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
    # IBM_PASSWORD = "INSERT IBM SPEECH TO TEXT PASSWORD HERE"  # IBM Speech to Text passwords are mixed-case alphanumeric strings
    # try:
    #     print(
    #     "IBM Speech to Text thinks you said " + r.recognize_ibm(audio, username=IBM_USERNAME, password=IBM_PASSWORD))
    # except sr.UnknownValueError:
    #     print("IBM Speech to Text could not understand audio")
    # except sr.RequestError as e:
    #     print("Could not request results from IBM Speech to Text service; {0}".format(e))

    return

def ParallelProc(filename):

    # This function used for sending parallel calls to 3rd party Speech Recognition API's
    from multiprocessing import Pool
    pool = Pool()
    result1 = pool.apply_async(solve1, [A])    # evaluate "solve1(A)" asynchronously
    result2 = pool.apply_async(solve2, [B])    # evaluate "solve2(B)" asynchronously
    answer1 = result1.get(timeout=10)
    answer2 = result2.get(timeout=10)

def VidToVid(filename):

    #clear any previous data
    clearFile = "rm output/VidToAvi.avi 2> /dev/null"
    subprocess.call(clearFile, shell=True)
    vidRunStart = timeit.default_timer()

    command = "{}{}{}".format("ffmpeg -i ", filename, " -ar 44100 -b 1024k ./output/VidToAvi.avi  2> /dev/null")
    subprocess.call(command, shell=True)
    videosource = "./output/VidToAvi.avi"
    vidRunTime = "{:10.2f}".format(timeit.default_timer() - vidRunStart)
    print("{}{}{}".format("Success!, Video source converted to .avi and saved at ./output/VidToAvi.avi\n Took ", vidRunTime, " seconds"))
    return

def ocr(filename):

    VidToPic(videosource, shutterRate)

    frameCount = len(fnmatch.filter(os.listdir("output/"), 'imgClip*'))
    #ocrTimeTrack = []
    for i in range(frameCount):
        ocrRunStart = timeit.default_timer()

        command = 'tesseract output/imgClip{0}.png output/txtClip{0} 2> /dev/null'
        command = command.format(i+1)
        subprocess.call(command, shell=True)

        ocrRunEnd = timeit.default_timer()
        ocrTimeTrack.append("{:10.2f}".format(ocrRunEnd - ocrRunStart))
        statusUpd = 'Processing image clip {} of {} for Optical Character Recognition'
        statusUpd = statusUpd.format(i + 1, frameCount)
        print(statusUpd)

    # Calls function writeCSV that loops through all text files with the given argument and writes to .csv
    writeCSV("txtClip", "ocr")

    # Empty the list results for re-using in enhanced images
    del ocrTimeTrack[:]

    # Enhancing images for OCR prep
    # Define parameters for image sharpening here by using ImageMagick's convert command
    for i in range(frameCount):
        ocrRunStart = timeit.default_timer()

        command = 'convert output/imgClip{0}.png -sharpen 0x10 -threshold 55% output/enhClip{0}.png'
        command = command.format(i+1)
        subprocess.call(command, shell=True)

        command = 'tesseract output/enhClip{0}.png output/enhTxtClip{0} 2> /dev/null'
        command = command.format(i+1)
        subprocess.call(command, shell=True)

        ocrRunEnd = timeit.default_timer()
        ocrTimeTrack.append("{:10.2f}".format(ocrRunEnd - ocrRunStart))
        statusUpd = 'Processing enhanced image clip {} of {} for Optical Character Recognition'
        statusUpd = statusUpd.format(i + 1, frameCount)
        print (statusUpd)

    # Write enhanced image OCR results to csv
    writeCSV("enhTxtClip", "ocr-enh")

    return

def delete_line_csv(pattern):
    data = open(results_csv).readlines()

    i = 0
    for line in data:
        temp = line
        if pattern in 5:
            data.pop(i)
        i += 1

    open(results_csv, "w").write("".join(data))

# MAIN CODE
if __name__ == '__main__':

    # this program originally only supported .wav and .avi video formats so I had logic to check file type and convert
    # to .avi. Uncomment the 3 lines below if you're not able to read from video source

    # if ".avi" not in checkSrc:
    #     print("Detected non .avi input source, video source must be of RIFF file format (.avi or.wav), converting now, may take a few minutes")
    #     VidToVid(videosource)


    # Determine 0 for results{0}.csv filename
    csvCount = len(fnmatch.filter(os.listdir("."), "*.csv"))  # count number of .csv files in source directory
    results_csv = 'results{0}.csv'.format(csvCount)  # Creates new results{csvCount}.csv file instead of overwriting/appending
    ocrTimeTrack = []

    # Calculate some values (features)
    videoLength = VidLength(videosource)
    videoSeconds = TimeToSecond(videoLength)
    videoTime = SecondToTime(videoSeconds)
    sampleCount = videoSeconds / sampleRate
    imgrCount = (videoSeconds / shutterRate) + 1

    # Print column headers in results{0}.csv
    with open(results_csv, 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(
            ['Video_Name', 'Video_Length', 'Result_Type', 'Time_Taken', 'Clip_Source', 'Clip_Start', 'Clip_End',
             'Result'])


    # Print a status update
    print("{}{}".format("Video Source: ", videosource))
    print("{}{}{}{}{}".format("Video Length: ", videoLength, " (", videoSeconds, " seconds)"))
    print("{}{}{}{}{}{}{}{}".format(videoSeconds, "/", shutterRate, " = ~", round(imgrCount),
                                    " still frames to create at a shutter rate of ", shutterRate, " seconds (OCR)"))
    print("{}{}{}{}{}{}{}{}".format(videoSeconds, "/", sampleRate, " = ~", round(sampleCount),
                                    " audio clips to create at a sample rate of ", sampleRate,
                                    " seconds (Speech Recognition)\n"))


    # TRACE THESE 2 CALLS IF YOU WANT TO UNDERSTAND THE FLOW OF THIS PROGRAM. 99% OF ACTION IS HERE
    ocr(videosource)
    SpeechRec(videosource)


    # Program is done at this point, Now just print status below.

    frameCount = len(fnmatch.filter(os.listdir("output/"), "imgClip*"))
    audCount = len(fnmatch.filter(os.listdir("output/"), "audClip*"))
    runEnd = timeit.default_timer()
    runTime = runEnd - runStart
    runTime = "{:10.2f}".format(runTime)

    print ("~~~~~~~~~~~~~~DONE!~~~~~~~~~~")
    print ("~~~~~~~~~vScrape RESULTS:~~~~~~~~~~\n")

    print ("{}{}".format("Runtime was: ", runTime))
    print ("{}{}{}{}".format(frameCount, " images were extracted from video source at a shutter rate of ", shutterRate, " seconds"))
    print ("{}{}{}{}".format(audCount, " audio clips were extracted from video source at a shutter rate of ", sampleRate, " seconds"))
    print ("{}".format("OCR and STT results saved to results.csv"))