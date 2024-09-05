
import configparser
import boto3

nasa_s3 = ""
open_s3 = ""
template = ""
replace = ""

def load_config():
    parser = configparser.RawConfigParser()
    config_filepath = r'config.txt'
    parser.read(config_filepath)

    global nasa_s3
    nasa_s3 = parser.get("s3", "ns3")
    print("source dir: " + nasa_s3)

    global open_s3
    open_s3 = parser.get("s3", "os3")
    print("source dir: " + open_s3)

    global template
    template = parser.get("s3", "tp")
    print("source dir: " + template)

    global replace
    replace = parser.get("s3", "rp")
    print("source dir: " + replace)


def query_s3(s3_url):
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
    response = s3.list_objects_v2(Bucket='your-bucket-name', Prefix='images/')

    # print out the list of responses
    for obj in response['Contents']:
        print(obj['Key'], obj['Size'])
        # check if the responce ends with a dmrpp ext.
        # if so add to a list

    # return the list of dmrpp file urls


def main():
    # pseudocode
    # call load_config(...) to set ns3 and os3
    load_config()
    # call query_s3(...) w/ ns3 and save list of urls into list/vector of strings
    query_s3(nasa_s3)
    # foreach loop
        # grab url from list
        # download file from nasa s3 and write to Imports
        # pass file to transform(...)
            # regex find template string in file
            # replace with new string in file
            # write file to Exports
            # pass file to writs_s3(...)
                # copies file from Exports to opendap s3


if __name__ == "__main__":
    main()
