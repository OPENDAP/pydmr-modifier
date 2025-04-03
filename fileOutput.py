import datetime

local_path = ""
status_path = ""

def write(path, content):
    with open(path, 'w') as f:
        f.write(content)
        f.close()

def create_status():
    global status_path
    date = datetime.datetime.now()
    status_path = f"logs/status_{date.strftime('%m%d%Y_%H%M')}.log"
    with open(status_path, 'w') as f:
        f.write(f"Status of Pydmr run on {date.strftime('%m/%d/%Y')}\n")
        f.close()

def update_status(data):
    global status_path
    with open(status_path, 'a') as f:
        f.write(data)
        f.close()

def create_summary(ccid):
    global local_path
    local_path = f"logs/{ccid}_summary.txt"
    with open(local_path, 'w') as f:
        f.write(f"Summary of granules in Collection {ccid}\n")
        f.close()


def update_summary(data):
    global local_path
    count, text = format_data(data)
    if count != 0:
        with open(local_path, 'a') as f:
            f.write(text)
            f.close()


def format_data(data):
    top = ""
    bottom = ""

    year = ""
    cnt = ""

    count = 1
    for tup in data:
        if count < 13:
            top += f"{format_cell(convert_month(tup[0]))}"
            bottom += f"{format_cell(tup[1])}"
        else:
            year = tup[0]
            cnt = tup[1]
        count += 1

    msg = f"{year}: {cnt}\n{thick_border()}{top}{thin_border()}{bottom}\n{thick_border()}"
    return cnt, msg


def convert_month(data):
    month = ""
    match data:
        case 1:
            month = "Jan"
        case 2:
            month = "Feb"
        case 3:
            month = "Mar"
        case 4:
            month = "Apr"
        case 5:
            month = "May"
        case 6:
            month = "Jun"
        case 7:
            month = "Jul"
        case 8:
            month = "Aug"
        case 9:
            month = "Sep"
        case 10:
            month = "Oct"
        case 11:
            month = "Nov"
        case 12:
            month = "Dec"
    return month


def format_cell(data):
    length = len(f"{data}")
    txt = "|"
    odd = True
    if length % 2 != 0:
        odd = False

    split = 0
    if odd:
        split = (6 - length) / 2
    else:
        split = (5 - length) / 2

    x = 0
    while x < split:
        txt += " "
        x += 1

    txt += f"{data}"

    x = 0
    while x < split:
        txt += " "
        x += 1

    if not odd: txt += " |"
    else: txt += "|"

    return txt


def thick_border():
    return "+------++------++------++------++------++------++------++------++------++------++------++------+\n"

def thin_border():
    return "\n|------||------||------||------||------||------||------||------||------||------||------||------|\n"

