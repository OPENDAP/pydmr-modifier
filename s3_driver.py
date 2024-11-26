
import configparser
import os

import regex as re
import boto3
import cmr

verbose = True
nasa_s3 = ""
open_s3 = ""
template = ""
replace = ""

def load_config():
    print("Loading config: ") if verbose else ''
    parser = configparser.RawConfigParser()
    config_filepath = r'config.txt'
    parser.read(config_filepath)

    global nasa_s3
    nasa_s3 = parser.get("s3", "ns3")
    print("\tnasa_s3: " + nasa_s3)

    global open_s3
    open_s3 = parser.get("s3", "os3")
    print("\topen_s3: " + open_s3)

    global template
    template = parser.get("s3", "tp")
    print("\ttemplate: " + template)

    global replace
    replace = parser.get("s3", "rp")
    print("\treplace: " + replace)


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
    granules = cmr.get_collection_granule_ids(ccid, max)
    num_gran = len(granules)
    print("# granules: " + str(num_gran))
    print("max: " + str(max)) if max != -1 else ''
    cur_num = 0
    url_list = []
    for granule_id in granules:
        # print(f"\ngranule: {granule_id}") if verbose else ''
        urls = cmr.get_related_urls_from_granule_id(ccid, granule_id)
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


def query_s3(s3_url):
    """Query a s3 bucket for all the dmrpp files it contains."""

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


def download_file_from_s3(s3_bucket_name, s3_file_name, local_file_path):
    """Downloads a file from an S3 bucket.

    Args:
        s3_bucket_name (str): The name of the S3 bucket.
        s3_file_name (str): The file name of the file in S3.
        local_file_path (str): The path to save the downloaded file locally.
    """
    print("\t\tDownloading: " + s3_file_name) if verbose else ''
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket_name, s3_file_name, local_file_path)


def replace_template(path, url):
    print("Starting Transform: " + path) if verbose else ''
    # pseudocode
    # read file into memory from Imports
    # search the file for the template string
    # replace any instant of templates with the replacement string
    # write file to Exports
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
    print(f"\turl: {url}") if verbose else ''
    bucket, file, request_url, local_path = build_urls(url)

    download_file_from_s3(bucket, file, local_path)
    replace_template(local_path, url)
    copy_file_to_s3(local_path, open_s3, ccid + "/" + request_url)
    delete_file(local_path)


def print_progress(amount, total):
    """
    outputs the progress bar to the terminal
    :param amount:
    :param total:
    :return:
    """
    percent = amount * 100 / total
    msg = "\t" + str(round(percent, 2)) + "% [ " + str(amount) + " / " + str(total) + " ] "
    print(msg, end="\r", flush=True)


def build_urls(url):
    # url_parts =  ('', 's3://', ('podaac_bucket', '/', 'a/very/long/object/name/for/a/file.ext'))
    url_parts = url.partition("s3://")[2].partition('/')

    bucket = url_parts[0]
    file = url_parts[2]

    request_url = url + ".dmrpp"
    local_path = "Imports/" + bucket + "/" + file + ".dmrpp"

    return bucket, file, request_url, local_path


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Query CMR and get information about Providers with Collections "
                                                 "accessible using OPeNDAP.")

    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true",
                        default=False)
    parser.add_argument("-c", "--ccid", help="ccid to send to CMR")
    parser.add_argument("-t", "--test", help="test mode, caps max number of granule urls to 10",
                        action="store_true", default=False)

    args = parser.parse_args()

    print("ccid: " + args.ccid)
    # pseudocode
    # call load_config(...) to set ns3 and os3
    load_config()
    if args.test:
        url_list = query_cmr(args.ccid, 10)
    else:
        url_list = query_cmr(args.ccid)

    print(f"# urls: {len(url_list)}") if verbose else ''

    for url in url_list:
        test_url(url, args.ccid)

    """
    # foreach loop
    for file in files:
        grab url from list              | (done)
        download file viva boto3        | (done) (unable to test)
        regex swap the template         | (done) (unable to test)
        upload to s3 bucket viva boto3  | (done) (tested using opendap_s3 files, untested with nasa files)
        delete file                     | (done) (untested)
    """


if __name__ == "__main__":
    main()
