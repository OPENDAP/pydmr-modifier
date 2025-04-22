import time


def output_errlog(msg):
    """
    error logging function - write a message to a date-stamped file.

    This function will create a date-stamped log file and record the message there
    or append the message to an existing file with the current day's date.

    :param msg:
    :return:
    """
    errlogfile = "logs/pydmrError" + time.strftime("-%m.%d.%Y") + ".log"
    with open(errlogfile, "a") as f:
        f.write(msg)
