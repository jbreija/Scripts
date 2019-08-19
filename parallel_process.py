import subprocess
from multiprocessing import Pool, cpu_count

def multi_processor(function_name):

    # Use a regex to make a list of full paths for audio files in /some/directory
    # You could also just pass in a list of audio files as a parameter to this function
    file_list = []
    file_list = str(subprocess.check_output("find ./some/directory -type f -iname \"*a_string_in_your_aud_file_name*\" ",shell=True)).split('\\n')
    file_list = sorted(file_list)

    # Test, comment out two lines above and put 3 strings in the list so your_function should run three times with 3 processors in parallel
    file_list.append("test1")
    file_list.append("test2")
    file_list.append("test3")

    # Use max number of system processors - 1
    pool = Pool(processes=cpu_count()-1)
    pool.daemon = True

    results = {}
    # for every audio file in the file list, start a new process
    for aud_file in file_list:
        results[aud_file] = pool.apply_async(your_function, args=("arg1", "arg2"))

    # Wait for all processes to finish before proceeding
    pool.close()
    pool.join()

    # Results and any errors are returned
    return {your_function: result.get() for your_function, result in results.items()}


def your_function(arg1, arg2):
    try:
        print("put your stuff in this function")
        your_results = ""
        return your_results
    except Exception as e:
        return str(e)


if __name__ == "__main__":
    multi_processor("your_function")