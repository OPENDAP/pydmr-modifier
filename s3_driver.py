
import configparser
import datetime
import os
import shutil

import regex as re
import boto3
import opendap_cmr
import fileOutput as out

import earthaccess
# from earthaccess import Auth, DataGranules #, Store

verbose = True
nasa_s3 = ""
open_s3 = ""
template = ""
replace = ""
auth = earthaccess.Auth()
limit = 0


def load_config():
    print("Loading config: ") if verbose else ''
    parser = configparser.RawConfigParser()
    config_filepath = r'config.txt'
    parser.read(config_filepath)

    global nasa_s3
    nasa_s3 = parser.get("s3", "ns3")
    print("\tnasa_s3: " + nasa_s3) if verbose else ''

    global open_s3
    open_s3 = parser.get("s3", "os3")
    print("\topen_s3: " + open_s3) if verbose else ''

    global template
    template = parser.get("s3", "tp")
    print("\ttemplate: " + template) if verbose else ''

    global replace
    replace = parser.get("s3", "rp")
    print("\treplace: " + replace) if verbose else ''


def query_cmr(ccid: str, max = -1) -> list:
    """
    Queries CMR for a list of granule urls.
    For the CCID, get a list of granule IDs and use those to find the S3 URLs that
    provide direct access to the granules. The function will return a list of URLs.
    :param ccid: CMR Collection Concept ID
    :param max: Instead of returning all the urls, return only the first 'max' number of urls.
    :return: A list of URLs
    """
    print("Starting query_cmr with url: " + ccid) if verbose else ''

    # granules is a list of granule IDs, the request to CMR is limited to max return values.
    # although if the page size is larger, the function will get that many values from CMR.
    granules = opendap_cmr.get_collection_granule_ids(ccid, max)
    num_gran = len(granules)
    print("# granules: " + str(num_gran))
    print("max: " + str(max)) if max != -1 else ''
    cur_num = 0
    url_list = []
    for granule_id in granules:
        # print(f"\ngranule: {granule_id}") if verbose else ''
        urls = opendap_cmr.get_related_urls_from_granule_id(ccid, granule_id)
        # print(f"# urls: {len(urls)}") if verbose else ''
        for url in urls:
            # print(f"\turl: {urls[url]}") if verbose else ''
            if urls[url].startswith("s3://"):
                url_list.append(urls[url])
                break   # only add the first s3 url

        cur_num += 1
        print_progress(cur_num, num_gran)
        if len(url_list) == max:
            break
    print("\n")
    return url_list


def query_earthaccess(ccid, year, month):
    """
    Args:
        ccid:
        year:
        month:

    Returns:

    """
    # print("Starting query_earthaccess with CCID: " + ccid) if verbose else ''
    url_list = []

    results = earthaccess.search_data(
        concept_id=ccid,
        temporal=(f"{year}-{month}",f"{year}-{month}"),
        cloud_hosted=True
    )

    for result in results:
        for url in result.data_links():
            if "opendap" in url and url.endswith(".html"):
                url = url.replace(".html", "")

            # hack to get the DMR++
            url = f"{url}.dmrpp"
            url_list.append(url)

    return url_list


def query_s3(s3_url):
    """
        /!\ currently not used /!\
        Query a s3 bucket for all the dmrpp files it contains.
    """

    print("Starting query_s3 with url: " + s3_url) if verbose else ''

    # pseudocode
    # make the s3 boto3 client using the s3_url ( ?? maybe the region, access id, and access key ?? )
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    s3 = boto3.client('s3')
        # client(service_name, region_name=None, api_version=None, use_ssl=True,
        #   verify=None, endpoint_url=None, aws_access_key_id=None,
        #   aws_secret_access_key=None, aws_session_token=None, config=None)
    # use s3 client to query the s3 bucket
    # need the bucket name and prefix
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
    response = s3.list_objects_v2(Bucket=open_s3)

    files = []
    # print out the list of responses
    for obj in response['Contents']:
        print("\t", obj['Key'], obj['Size'])
        # check if the response ends with a dmrpp ext.
        if obj['Key'].endswith(".dmrpp"):
            files.append(obj['Key'])
        # if so add to a list

    print("exiting query_s3") if verbose else ''
    # return the list of dmrpp file urls
    return files


def download_file_from_s3(url, local_file_path):
    """
        Downloads a file from an S3 bucket.
        Args:
            url (str): The url of the file in s3.
            local_file_path (str): The path to save the downloaded file locally.
    """
    try:
        session = auth.get_session()
        with session.get(
                url,
                stream=True,
                allow_redirects=True,
        ) as r:
            r.raise_for_status()
            with open(local_file_path, "wb") as f:
                # This is to cap memory usage for large files at 1MB per write to disk per thread
                # https://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content
                shutil.copyfileobj(r.raw, f, length=1024 * 1024)
    except BaseException as e:
        print(f"Error while downloading the file {local_file_path}")
        print(e)
        return False

    return True


def replace_template(path, url):
    contents = ""
    with open(path, 'r') as f:
        contents = f.read()
        f.close()

    contents = re.sub(replace, url, contents)

    with open(path, 'w') as f:
        f.write(contents)
        f.close()


def copy_file_to_s3(local_file_path, s3_bucket_name, s3_file_name):
    """Copies a local file to an S3 bucket.
    Args:
        local_file_path (str): The path to the local file.
        s3_bucket_name (str): The name of the S3 bucket.
        s3_file_name (str): The s3 file name for the uploaded file in S3.
    """

    s3 = boto3.client('s3')
    s3.upload_file(local_file_path, s3_bucket_name, s3_file_name)


def delete_file(path):
    """ Deletes a file.
    Args:
        path: path of the file to delete.

    Returns: NA
    """

    if os.path.exists(path):
        os.remove(path)
    else:
        print("The file does not exist: " + path)


def test_url(url, ccid):
    # print(f"\turl: {url}") if verbose else ''
    local_path, file = build_urls(url, ccid)
    dacc = ccid.partition("-")[2]

    if download_file_from_s3(url, local_path):
        replace_template(local_path, url)
        file_name = f"{dacc}/{ccid}/{file}"
        copy_file_to_s3(local_path, open_s3, file_name)
        delete_file(local_path)


def print_progress(amount, total):
    """
    outputs the progress bar to the terminal
    :param amount:
    :param total:
    :return:
    """
    percent = amount * 100 / total
    msg = "\t\t\t" + str(round(percent, 2)) + "% [ " + str(amount) + " / " + str(total) + " ] "
    print(msg, end="\r", flush=True) if verbose else ''


def build_urls(url, ccid):
    # url_parts =  ('', 's3://', ('podaac_bucket', '/', 'a/very/long/object/name/for/a/file.ext'))
    # url_parts = url.partition("s3://")[2].partition('/')

    # bucket = url_parts[0]
    # file = url_parts[2]

    dacc = ccid.partition("-")[2]
    file = url.split("/")[-1]

    local_path = "Imports/" + dacc
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    local_path = local_path + "/" + file

    return local_path, file


def load_ccid_list(ifile):
    ccids = []
    with open(ifile, 'r') as f:
        for line in f:
            ccids.append(line.strip())
        f.close()

    x = 1
    for ccid in ccids:
        out.create_summary(ccid)
        out.update_status(f"\t[{x} of {len(ccids)}] {ccid} - Started: {datetime.datetime.now().strftime('%H:%M - %m/%d/%Y')}")
        process_ccid(ccid)
        out.update_status(f" - Completed: {datetime.datetime.now().strftime('%H:%M - %m/%d/%Y')}\n")


def process_ccid(ccid):
    print(f"[==============================]\nccid: {ccid}") if verbose else ''
    cur_year = datetime.date.today().year
    outlist = []
    total = 0
    for year in range(1970, cur_year + 1):
        print(f"\t{year}: ") if verbose else ''
        for month in range(1, 13):
            url_list = query_earthaccess(ccid, year, month)
            print(f"\t\t{month} - urls: {len(url_list)}") if verbose and len(url_list) > 0 else '.'
            outlist.append((month, len(url_list)))
            total += len(url_list)

            x = 0
            for url in url_list:
                test_url(url, ccid, year, month)
                print_progress(x, len(url_list))
                x += 1
                if limit != -1 and x > limit:
                    break

            print(".") if verbose and len(url_list) > 0 else ''

        outlist.append((year, total))
        out.update_summary(outlist)
        outlist.clear()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Query CMR and get information about Providers with Collections "
                                                 "accessible using OPeNDAP.")

    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true",
                        default=False)
    parser.add_argument("-t", "--test", help="test mode, caps max number of granule urls to 10",
                        action="store_true", default=False)

    group = parser.add_mutually_exclusive_group(required=True)  # only one option in 'group' is allowed at a time
    group.add_argument("-c", "--ccid", help="ccid to send to CMR")
    group.add_argument("-i", "--input", help="path to file containing list of CCIDs")

    args = parser.parse_args()

    # first we authenticate with NASA EDL
    auth.login(strategy="netrc")
    # are we authenticated?
    print("Authenticated: " + str(auth.authenticated)) if verbose else ''


    # pseudocode
    # call load_config(...) to set ns3 and os3
    load_config()
    global limit
    if args.test:
        limit = 10
    else:
        limit = -1

    out.create_status()

    if args.input:
        print(f"file: {args.input}") if verbose else ''
        load_ccid_list(args.input)
    else:
        out.create_summary(args.ccid)
        out.update_status(f"\t{args.ccid} - Started: {datetime.datetime.now().strftime('%H:%M - %m/%d/%Y')}")
        process_ccid(args.ccid)
        out.update_status(f" - Completed: {datetime.datetime.now().strftime('%H:%M - %m/%d/%Y')}\n")


if __name__ == "__main__":
    main()
