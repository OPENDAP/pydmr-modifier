
import configparser
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


def query_cmr(ccid):
    print("Starting query_cmr with url: " + ccid) if verbose else ''

    granules = cmr.get_collection_granules(ccid)
    print("# granules: " + str(len(granules)))
    # x = 0
    url_list = []
    for granule in granules:
        # print("granule: " + granule + " - " + granules[granule])
        urls = cmr.get_related_urls(ccid, granules[granule])
        # print("# urls: " + str(len(urls)))
        for url in urls:
            # print("\turl: " + url + " - " + urls[url])
            if url == "URL2":
                url_list.append(urls[url])
                print(".", end="", flush=True)
        # print("\n")
        # x += 1
        # if x == 5:
        #    break
    print("\n")
    return url_list


def query_s3(s3_url):
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
        # check if the responce ends with a dmrpp ext.
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


def transform(path):
    print("Starting Transform: " + path) if verbose else ''
    # pseudocode
    # read file into memory from Imports
    # search the file for the template string
    # replace any instant of templates with the replacement string
    # write file to Exports


def copy_file_to_s3(local_file_path, s3_bucket_name, s3_file_name):
    """Copies a local file to an S3 bucket.

    Args:
        local_file_path (str): The path to the local file.
        s3_bucket_name (str): The name of the S3 bucket.
        s3_file_name (str): The s3 file name for the uploaded file in S3.
    """

    s3 = boto3.client('s3')
    s3.upload_file(local_file_path, s3_bucket_name, s3_file_name)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Query CMR and get information about Providers with Collections "
                                                 "accessible using OPeNDAP.")

    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true", default=False)
    parser.add_argument("-c", "--ccid", help="ccid to send to CMR")

    args = parser.parse_args()

    print("ccid: " + args.ccid)
    # pseudocode
    # call load_config(...) to set ns3 and os3
    load_config()
    url_list = query_cmr(args.ccid)
    print("# urls: " + str(len(url_list)))
    # call query_s3(...) w/ ns3 and save list of urls into list/vector of strings
    """
    files = query_s3(nasa_s3)
    # foreach loop
    for file in files:
        print("\t" + file)
        # grab url from list
        sfile = file.replace('/', '.')
        # download file from nasa s3 and write to Imports
        download_file_from_s3(nasa_s3, file, "Imports/"+sfile)
        # pass file to transform(...)
            # regex find template string in file
            # replace with new string in file
            # write file to Exports
            # pass file to writs_s3(...)
                # copies file from Exports to opendap s3
    """


if __name__ == "__main__":
    main()
