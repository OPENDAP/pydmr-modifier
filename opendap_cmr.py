"""
Access information about data in NASA's EarthData Cloud system using the
CMR Web API.
"""
import errLog
# from typing import Dict, Any, Set

import requests
import threading
from concurrent.futures import ThreadPoolExecutor


"""
set 'verbose'' in main(), etc., and it affects various functions
"""
verbose: bool = False


class CMRException(Exception):
    """When CMR returns an error"""

    def __init__(self, status, message="No error message given"):
        self.status = status
        self.message = message

    def __str__(self):
        return f'CMR Exception HTTP status: {self.status} - {self.message}'


"""
These are the response processors used by 'process_response()'. They extract
various things from the JSON and return a dictionary.
"""


def is_entry_feed(json_resp: dict) -> bool:
    """
    Does this JSON object have the 'entry' key within a 'feed' key?
    This function is used to protect various response processors
    from responses that contain no entries or are malformed.
    """
    return (len(json_resp) > 0
            and "feed" in json_resp.keys()
            and isinstance(json_resp["feed"], dict)
            and "entry" in json_resp["feed"].keys())


def is_item_feed(json_resp: dict) -> bool:
    """
    Does this JSON object have the 'meta' key within a 'items' key/array?
    This function is used to protect various response processors
    from responses that contain no items or are malformed.
    """
    return (len(json_resp) > 0
            and "items" in json_resp.keys()
            and isinstance(json_resp["items"], list)
            and len(json_resp["items"]) > 0
            and "meta" in json_resp["items"][0])


def is_meta_item(json_resp: dict) -> bool:
    """
    Does this JSON object have the 'meta' key that contains concept-id and native-id keys?
    This function is used to protect various response processors
    from responses that contain no entries or are malformed.

    This function processes the return information from a granules.umm_json request.
    """
    return (len(json_resp) > 0 and "meta" in json_resp.keys()
            and isinstance(json_resp["meta"], dict)
            and "concept-id" in json_resp["meta"].keys()
            and "native-id" in json_resp["meta"].keys())


def is_granule_item(json_resp: dict) -> bool:
    """
    Does this JSON object have the 'RelatedUrls' key within a 'umm' key?
    This function is used to protect various response processors
    from responses that contain no entries or are malformed.

    This function processes the return information from a granules.umm_json request.
    """
    return (len(json_resp) > 0
            and "umm" in json_resp.keys()
            and isinstance(json_resp["umm"], dict)
            and "RelatedUrls" in json_resp["umm"].keys())


def collection_granules_dict(json_resp: dict) -> dict:
    """
    This function processes the return information from a granules.json request.
    Do not use it for a granules.umm_json request.

    :param json_resp: CMR JSON response
    :return: A dictionary with the Granule id indexing the granule title and the producer granule id ,
    or a dictionary with the Granule id indexing the granule title.
    :rtype: dict
    """
    if not is_entry_feed(json_resp):
        return {}

    dict_resp = {}
    # Look for the entry id, title, producer_granule_id and OPeNDAP link. Build a
    for entry in json_resp["feed"]["entry"]:
        if "producer_granule_id" in entry:  # some granule records lack "producer_granule_id". jhrg 9/4/22
            dict_resp[entry["id"]] = (entry["title"], entry["producer_granule_id"])
        else:
            dict_resp[entry["id"]] = (entry["title"],)

    return dict_resp


def collection_granule_and_url_dict(json_resp: dict) -> dict:
    """
    This function processes the return information from a granules.json request.
    Do not use it for a granules.umm_json request.

    :param json_resp: CMR JSON response
    :return: A dictionary with the Granule id indexing the granule title and OPeNDAP URL
    :rtype: dict
    :deprecated: jhrg 1/23/23
    """
    if not is_entry_feed(json_resp):
        return {}

    dict_resp = {}
    # Look for the entry id, title, and OPeNDAP link.
    for entry in json_resp["feed"]["entry"]:
        if len(entry.keys() & ("id", "title", "links")) == 3:
            # check for the OPeNDAP URL in the 'links' array
            for link in entry["links"]:
                if "title" in link and link["title"].find("OPeNDAP") == 0:
                    dict_resp[entry["id"]] = (entry["title"], link["href"])
                    break

    return dict_resp


def provider_collections_dict(json_resp):
    """
    Extract collection IDs and Titles from CMR JSON. Optionally get the granule count.

    :param json_resp: CMR JSON response
    :return: The provider collection IDs and title in a dictionary
    :rtype: dict
    """
    if not is_entry_feed(json_resp):
        return {}

    dict_resp = {}
    for entry in json_resp["feed"]["entry"]:
        if "granule_count" in entry:
            dict_resp[entry["id"]] = (entry["granule_count"], entry["title"])
        else:
            dict_resp[entry["id"]] = (entry["title"])

    return dict_resp


def provider_id(json_resp: dict) -> set:
    """
    Extract Provider IDs from CMR JSON.

    The JSON passed to this function is an Array of 'items' each of which holds
    a dictionary with a single key 'meta'. The value of the 'meta' key is itself
    a dictionary that holds lots of info, including the provider-id key-value pair.

    :param json_resp: CMR JSON response
    :returns: The provider ids in a set
    :rtype: set
    """
    if not is_item_feed(json_resp):
        return set()

    resp = set()
    for item in json_resp["items"]:
        if "provider-id" in item["meta"]:
            resp.add(item["meta"]["provider-id"])

    return resp


def granule_data_url_dict(json_resp: dict) -> dict:
    """
    Extract Related URLs from CMR JSON UMM.

    This function processes the return information from a granules.umm_json request.
    Do not use it for a granules.json request.

    Only http URLs that are NOT marked with Subtype 'OPENDAP DATA' are returned. This
    has been added (jhrg 5/4/23) so that the ask_cmr.py -r option will work, returning
    the underlying URL to the data. Access to the data using that URL will nominally
    require auth using TEA or S3 signing.

    :param json_resp: CMR JSON UMM response
    :returns: The granule UR related URL info in a dictionary. Only Type 'GET DATA'
        or 'USE SERVICE API' without Subtype 'OPENDAP DATA' type URLs are included.
        Each is indexed using 'URL1', ..., 'URLn.' The dictionaries look like:
        {'URL1': 's3://podaac/metopb_00588_eps_o_250_2101_ovw.l2.nc',
         'URL2': 'https://archive/250_2101_ovw.l2.nc'}
    :rtype: dict
    """
    # Check json_resp as above but for items, etc. jhrg 10/11/22
    if "items" not in json_resp.keys():
        return {}

    dict_resp = {}
    i = 1
    for item in json_resp["items"]:
        if not is_granule_item(item):
            continue
        for r_url in item["umm"]["RelatedUrls"]:
            if "Type" not in r_url or "URL" not in r_url:
                continue
            if ("Type" in r_url
                    and r_url["Type"] in ('GET DATA', 'USE SERVICE API', 'EXTENDED METADATA')
                    and "Subtype" not in r_url):
                dict_resp[f'URL{i}'] = (r_url["URL"])
                i += 1

    return dict_resp


def granule_json(json_resp: dict) -> dict:
    """
    This is an identity response for a granules.umm_json request.
    :return: The granule JSON from CMR JSON UMM response.
    """
    return json_resp


def granule_ur_dict(json_resp: dict) -> dict:
    """
    Extract Related URLs from CMR JSON UMM.

    This function processes the return information from a granules.umm_json request.
    Do not use it for a granules.json request.

    Modified so that only URLs with the 'Subtype' 'OPENDAP DATA' are returned.
    jhrg 1/23/23

    :param json_resp: CMR JSON UMM response
    :returns: The granule UR related URL info in a dictionary. Only Type 'GET DATA'
        or 'USE SERVICE API' with Subtype 'OPENDAP DATA' type URLs are included.
        Each is indexed using 'URL1', ..., 'URLn.' The dictionaries look like:
        {'URL1': 's3://podaac/metopb_00588_eps_o_250_2101_ovw.l2.nc',
         'URL2': 'https://archive/250_2101_ovw.l2.nc'}
    :rtype: dict
    """
    # Check json_resp as above but for items, etc. jhrg 10/11/22
    if "items" not in json_resp.keys():
        return {}

    dict_resp = {}
    i = 1
    for item in json_resp["items"]:
        if not is_granule_item(item):
            continue
        for r_url in item["umm"]["RelatedUrls"]:
            if "Type" not in r_url or "URL" not in r_url:
                continue
            if "Type" in r_url and r_url["Type"] in ('GET DATA', 'USE SERVICE API') \
                    and "Subtype" in r_url and r_url["Subtype"] == 'OPENDAP DATA':
                dict_resp[f'URL{i}'] = (r_url["URL"])
                i += 1

    return dict_resp


def granule_ur_dict_2(json_resp: dict) -> dict:
    """
    Extract Related URLs from CMR JSON UMM. This version returns a dictionary with
    an ID, Title and URL like {ID : (Title, URL)}.

    This function processes the return information from a granules.umm_json request.
    Do not use it for a granules.json request.

    Modified so that only URLs with the 'Subtype' 'OPENDAP DATA' are returned.
    The response should use the 'concept-id' for ID, native-id for the Title
    and 'URL' for the URL.
    jhrg 1/23/23

    :param json_resp: CMR JSON UMM response
    :returns: The granule UR related URL info in a dictionary. Only Type 'GET DATA'
        or 'USE SERVICE API' with Subtype 'OPENDAP DATA' type URLs are included.
        Each is indexed using the granule concept ID and looks like:
        {'G2081588885-POCLOUD': ('ascat_20121029_010001_metopb_00588_eps_o_250_2101_ovw.l2',
                                 'https://opendap.../podaac/metopb_00588_eps_o_250_2101_ovw.l2.nc')}
    :rtype: dict
    """
    # Check json_resp as above but for items, etc. jhrg 10/11/22
    if "items" not in json_resp.keys():
        return {}

    dict_resp = {}
    for item in json_resp["items"]:
        if not (is_meta_item(item) and is_granule_item(item)):
            continue
        if "concept-id" not in item["meta"].keys() or "native-id" not in item["meta"].keys():
            continue
        concept_id = item["meta"]["concept-id"]
        native_id = item["meta"]["native-id"]
        for r_url in item["umm"]["RelatedUrls"]:
            if "Type" not in r_url or "URL" not in r_url:
                continue
            if "Type" in r_url and r_url["Type"] in ('GET DATA', 'USE SERVICE API') \
                    and "Subtype" in r_url and r_url["Subtype"] == 'OPENDAP DATA':
                dict_resp[concept_id] = (native_id, r_url["URL"])

    return dict_resp


def merge_dict(dict1: dict, dict2: dict) -> dict:
    """
    Merge dictionaries, preserve key order
    See https://www.geeksforgeeks.org/python-merging-two-dictionaries/

    :param dict1:
    :param dict2:
    :returns: The dict1, modified so the entries in dict2 have been appended
    :rtype: dict
    """
    # silently bail
    if not (type(dict1) is dict and type(dict2) is dict):
        raise TypeError("Both arguments to cmr.merge() must be dictionaries.")

    # If there is nothing in dict1, return dict2.
    if len(dict1) == 0:
        return dict2

    for i in dict2.keys():
        dict1[i] = dict2[i]

    return dict1


def convert(a: list) -> dict:
    """
    Convert and array of 2N things to a dictionary of N entries
    See https://www.geeksforgeeks.org/python-convert-a-list-to-dictionary/

    :param a: The List/Array to convert
    :return: The resulting dictionary
    :rtype: dict
    """
    it = iter(a)
    res_dct = dict(zip(it, it))
    return res_dct


# TODO Make a 'returns a set' version of this to avoid the 'function with two
#  return types' confusion. jhrg 7/6/24
def process_request(cmr_query_url: str, response_processor: callable(dict), session: object, page_size=10,
                    page_num=0) -> dict:
    """
    The generic part of a CMR request. Make the request, print some stuff
    and return the number of entries. The page_size parameter is there so that paged responses
    can be handled. By default, CMR returns 10 entry items per page.

    :param cmr_query_url: The whole URL, query params and all
    :param response_processor: A function that will process the returned json response
    :param session: A requests package session object
    :param page_size: The number of entries per page from CMR. The default is the CMR default value.
    :param page_num: Return an explicit page of the query response. If not given, gets all the pages
    :returns: A dictionary of entries
    :rtype: dict or set
    """
    page = 1 if page_num == 0 else page_num
    entries_dict = {}
    entries_set = set()
    try:
        while True:
            # By default, requests uses cookies, supports OAuth2 and reads username and password
            # from a ~/.netrc file.
            r = session.get(f'{cmr_query_url}&page_num={page}&page_size={page_size}')
            page += 1  # if page_num was explicitly set, this is not needed

            print("-", end="", flush=True) if verbose else ''
            if verbose > 0:
                print(f'CMR Query URL: {cmr_query_url}')
                print(f'Status code: {r.status_code}')

            if r.status_code != 200:
                # JSON returned on error: {'errors': ['Collection-concept-id [ECCO Ocean ...']}
                raise CMRException(r.status_code, r.json()["errors"][0])

            json_resp = r.json()
            if "feed" in json_resp and "entry" in json_resp["feed"]:  # 'feed' is for the json response
                entries_num = len(json_resp["feed"]["entry"])
            elif "items" in json_resp:  # 'items' is for json_umm
                entries_num = len(json_resp["items"])
            else:
                raise CMRException(200, "cmr.process_request does not know how to decode the response")

            if entries_num > 0:
                entries_page = response_processor(json_resp)  # The response_processor() is passed in
                if type(entries_page) is dict:
                    entries_dict = merge_dict(entries_dict, entries_page)  # merge is smart if entries is empty
                elif type(entries_page) is set:
                    entries_set.update(entries_page)

            if page_num != 0 or entries_num < page_size:
                break

    except requests.exceptions.ConnectionError:
        err = "/////////////////////////////////////////////////////\n"
        err += "ConnectionError : cmr.py::process_request() - " + cmr_query_url + "\n"
        errLog.output_errlog(err)
    except requests.exceptions.JSONDecodeError:
        err = "/////////////////////////////////////////////////////\n"
        err += "JSONDecodeError : cmr.py::process_request() - " + cmr_query_url + "\n"
        errLog.output_errlog(err)

    if len(entries_dict) > 0:
        return entries_dict
    elif len(entries_set) > 0:
        return entries_set
    else:
        return {}


def process_request_list(cmr_query_url: str, response_processor: callable(list), session: object,
                         num_responses = -1, page_size=10, page_num=0) -> list:
    """
    Query CMR and return a list of results.

    The generic part of a CMR request. Make the request, print some stuff
    and return a list of results. The page_size parameter is there so that paged responses
    can be handled. By default, CMR returns 10 entry items per page.

    :param cmr_query_url: The whole URL, query params and all.
    :param response_processor: A function that will process the returned json response returning
    results in a list.
    :param session: A requests package session object.
    :param num_responses: The number of responses to get. If not given, gets all the responses.
    :param page_size: The number of entries per page from CMR. The default is the CMR default value.
    :param page_num: Return an explicit page of the query response. If not given, gets all the pages.

    :returns: A dictionary of entries
    :rtype: list
    """
    # Ensure that if the caller wants a specific page, that page is returned.
    # OOtherwise, get all the pages.
    page = 1 if page_num == 0 else page_num
    # Ensure that if the caller wants fewer responses than the page size, only
    # that number of responses is retrieved
    if num_responses > -1 and page_size > num_responses:
        page_size = num_responses

    entries = []
    try:
        while True:
            # By default, requests uses cookies, supports OAuth2 and reads username and password
            # from a ~/.netrc file.
            r = session.get(f'{cmr_query_url}&page_num={page}&page_size={page_size}')
            page += 1  # if page_num was explicitly set, this is not needed

            print("-", end="", flush=True) if verbose else ''
            if verbose > 0:
                print(f'CMR Query URL: {cmr_query_url}')
                print(f'Status code: {r.status_code}')

            if r.status_code != 200:
                # JSON returned on error: {'errors': ['Collection-concept-id [ECCO Ocean ...']}
                raise CMRException(r.status_code, r.json()["errors"][0])

            json_resp = r.json()
            if "feed" in json_resp and "entry" in json_resp["feed"]:  # 'feed' is for the json response
                entries_num = len(json_resp["feed"]["entry"])
            elif "items" in json_resp:  # 'items' is for json_umm
                entries_num = len(json_resp["items"])
            else:
                raise CMRException(200, "cmr.process_request does not know how to decode the response")

            if entries_num > 0:
                entries_page = response_processor(json_resp)  # The response_processor() is passed in
                entries = entries + entries_page

            if page_num != 0 or entries_num < page_size or (num_responses > -1 and len(entries) >= num_responses):
                break

    except requests.exceptions.ConnectionError:
        err = "/////////////////////////////////////////////////////\n"
        err += "ConnectionError : cmr.py::process_request() - " + cmr_query_url + "\n"
        errLog.output_errlog(err)
    except requests.exceptions.JSONDecodeError:
        err = "/////////////////////////////////////////////////////\n"
        err += "JSONDecodeError : cmr.py::process_request() - " + cmr_query_url + "\n"
        errLog.output_errlog(err)

    return entries


""" Used to ensure that each thread has its own session for the HTTP Requests package """
thread_local = threading.local()


def get_session() -> object:
    """
    With 'thread_local' above, get a new session object for each thread. Reuse session
    for existing threads. The Requests Session object is not multi-thread safe.
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def get_collection_granules_umm_first_last(ccid: str, json_processor=granule_ur_dict_2, pretty=False,
                                           service='cmr.earthdata.nasa.gov') -> dict:
    """
    This method uses the granules.umm_json_v1_4 response and finds the first and
    last granules for a collection using the 'items' response from CMR.

    :param ccid The Collection Concept ID
    :param json_processor A function to parse the JSON from CMR
    :param pretty Ask CMR to return a 'pretty' JSON response
    :param service Which instance of CMR to query
    :return: Return a dictionary that is the result of merging two dicts structured
    like {ID1 : (Title1, URL1), ID2 : (Title2, URL2)} where ID is the granule ID.
    """
    pretty = '&pretty=true' if pretty else ''

    # by default, CMR returns results with "sort_key = +start_date" returning the oldest granule
    cmr_query_url = f'https://{service}/search/granules.umm_json_v1_4?collection_concept_id={ccid}{pretty}'
    oldest_dict = process_request(cmr_query_url, json_processor, get_session(), page_size=1, page_num=1)

    # Use "-start-date" to get the newest granule
    sort_key = '&sort_key=-start_date'
    cmr_query_url = f'{cmr_query_url}{sort_key}'
    newest_dict = process_request(cmr_query_url, json_processor, get_session(), page_size=1, page_num=1)

    if len(newest_dict) != 1 and len(oldest_dict) != 1:
        raise CMRException(500, f"Expected at least one response item from CMR, got {len(newest_dict)+len(oldest_dict)}"
                                f" while asking about {ccid}, even though has_opendap_url was true for the collection.")

    # Use host_patterns to see if the URL(s) is/are in the dictionaries. Maybe. jhrg 1/25/23
    return merge_dict(oldest_dict, newest_dict)


def get_provider_collections(provider: str, opendap=False, pretty=False, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Get all the OPeNDAP-enabled collections for a given provider. This uses the UMM-S record to get the
    OPeNDAP-enabled collections. See get_provider_opendap_collections_brutishly() for a
    method that tests the URLs themselves.

    The return value is a dictionary of the collection concept IDs and titles. For example,
    {'C2036877686-POCLOUD': 'MetOp-A ASCAT Level 2 12.5-km Ocean Surface Wind Vector Climate ...',
    'C2036877806-POCLOUD': 'GHRSST L3C hourly America Region sub-skin Sea Surface Temperature v1.0...',
    'C2036878103-POCLOUD': 'GHRSST Level 4 RAMSSA_9km Australian Regional Foundation Sea Surface  ...'}

    :param provider: The string ID for a given EDC provider (e.g., ORNL_CLOUD)
    :param opendap: If true, return only the collections with OPeNDAP URLS
    :param pretty: request a 'pretty' version of the response from the service. default False
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :returns: A dictionary of CCIDs and titles.
    """
    pretty = '&pretty=true' if pretty else ''
    opendap = '&has_opendap_url=true' if opendap else ''
    cmr_query_url = f'https://{service}/search/collections.json?provider={provider}{opendap}{pretty}'
    return process_request(cmr_query_url, provider_collections_dict, get_session(), page_size=500)


def collection_has_opendap(ccid: str, cloud_prefix="https://opendap.earthdata.nasa.gov/",
                           json_processor=granule_ur_dict_2, service='cmr.earthdata.nasa.gov') -> tuple:
    """
    For a CCID, check that the first granule has an OPeNDAP URL. This returns a tuple
    of the CCID, true/false if the URL is in the cloud, and it also returns the URL.
    The URL _may_ be to granule for an on-prem server which is a useful check to see
    that this code, and the contents of CMR are truthful.

    :param ccid: The collection concept ID
    :param cloud_prefix: Is this a URL to a collection in the cloud?
    :param json_processor: Use this function to process the returned JSON
    :param service: Use this endpoint for CMR

    :return: A tuple of (ccid, True|False) - True if an OPeNDAP is present
    """
    # by default, CMR returns results with "sort_key = +start_date" returning the oldest granule
    cmr_query_url = f'https://{service}/search/granules.umm_json_v1_4?collection_concept_id={ccid}'
    oldest_dict = process_request(cmr_query_url, json_processor, get_session(), page_size=1, page_num=1)

    if len(oldest_dict) != 1:
        return ccid, False, ""    # Empty URL if there is none.
    else:
        first_key = next(iter(oldest_dict.keys()))
        url = oldest_dict[first_key][1]  # The value is a tuple, the second element of which is the URL
        if url.startswith(cloud_prefix):
            return ccid, True, url
        else:
            return ccid, False, url


def get_provider_opendap_collections_brutishly(provider: str, workers=64, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Get all the collections for a given provider that have OPeNDAP URLs.

    This function does not use the UMM-S record but instead performs a
    brute-force (in parallel) search of all the collections of a provider
    and returns the CCID of all of those where the first granule has an
    OPeNDAP URL. We don't include the test for the last URL since there
    might be a collection with only one URL.

    The method collection_has_opendap() is used to test each collection
    using the granules.umm_json_v1_4 response and the function collection_has_opendap()
    to determine what is and is not OPeNDAP-enabled.

    :param provider: The string ID for a given EDC provider (e.g., ORNL_CLOUD)
    :param workers: Use this many threads when asking CMR about granules. I set this at 64 by trial and error.
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :returns: A dictionary
    """
    cmr_query_url = f'https://{service}/search/collections.json?provider={provider}'
    all_collections = process_request(cmr_query_url, provider_collections_dict, get_session(), page_size=500)

    ccids = list(all_collections.keys())

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Use 'partial' to curry collection_has_opendap() if using optional parameters. jhrg 6/30/24
        results = executor.map(collection_has_opendap, ccids)

    ccids_opendap = {key: (value2, value3) for key, value2, value3 in results}

    return ccids_opendap


def get_provider_opendap_collections_uum_s(provider: str, workers=64, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Using the UUM-S records, get all the collections for a given provider that have OPeNDAP URLs.

    The method collection_has_opendap() is used to test each collection using
    the granules.umm_json_v1_4 response and the function collection_has_opendap()
    to determine what is and is not OPeNDAP-enabled.

    :param provider: The string ID for a given EDC provider (e.g., ORNL_CLOUD)
    :param workers: Use this many threads when asking CMR about granules. I set this at 64 by trial and error.
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :returns: A dictionary
    """

    opendap = '&has_opendap_url=true'

    cmr_query_url = f'https://{service}/search/collections.json?provider={provider}{opendap}'
    umm_s_collections = process_request(cmr_query_url, provider_collections_dict, get_session(), page_size=500)

    ccids = list(umm_s_collections.keys())

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Use 'partial' to curry collection_has_opendap() if using optional parameters. jhrg 6/30/24
        results = executor.map(collection_has_opendap, ccids)

    ccids_opendap = {key: (value2, value3) for key, value2, value3 in results}

    return ccids_opendap


def get_collection_entry(ccid: str, pretty=False, count=False, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Get the collection entry given a concept id.

    :param ccid: The string Collection (Concept) Id
    :param pretty: request a 'pretty' version of the response from the service. default False
    :param count: request the granule count for the collection
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :returns:The collection JSON object
    """
    pretty = '&pretty=true' if pretty else ''
    collection_count = '&include_granule_counts=true' if count else ''
    cmr_query_url = f'https://{service}/search/collections.json?concept_id={ccid}{collection_count}{pretty}'
    return process_request(cmr_query_url, provider_collections_dict, get_session(), page_num=1)


def get_related_urls(ccid: str, granule_ur: str, pretty=False, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Search for a granules RelatedUrls using the collection concept id and granule ur.
    This provides a way to go from the REST form of a URL that the OPeNDAP server typically
    receives and the URLs that can be used to directly access data (and thus the DMR++
    if the data are in S3 and OPeNDAP-enabled).

    :returns: A dictionary that holds all the RelatedUrls that have Type 'GET DATA' or 'USE SERVICE DATA.'
    """
    pretty = '&pretty=true' if pretty else ''
    cmr_query_url = f'https://{service}/search/granules.umm_json_v1_4?collection_concept_id={ccid}&granule_ur={granule_ur}{pretty}'
    return process_request(cmr_query_url, granule_data_url_dict, get_session(), page_num=1)


def get_cmr_json(ccid: str, granule_ur: str, pretty=False, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Ask for the CMR JSON object for the given 'REST' path.
    :param ccid: The string Collection (Concept) ID
    :param granule_ur: The granule name
    :param pretty: request a 'pretty' version of the response from the service. default False
    :param service: The URL of the service to query. default cmr.earthdata.nasa.gov
    :returns: The CMR JSON object
    """
    pretty = '&pretty=true' if pretty else ''
    cmr_query_url = f'https://{service}/search/granules.umm_json_v1_4?collection_concept_id={ccid}&granule_ur={granule_ur}{pretty}'
    return process_request(cmr_query_url, granule_json, get_session(), page_num=1)


def get_collection_granules(ccid: str, pretty=False, service='cmr.earthdata.nasa.gov', descending=False) -> dict:
    """
    Get granules for a collection

    :param ccid: The string Collection (Concept) Id
    :param pretty: request a 'pretty' version of the response from the service. default False
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :param descending: If true, get the granules in newest first order, else oldest granule is first
    :returns: The collection JSON object
    """
    pretty = '&pretty=true' if pretty else ''
    sort_key = '&sort_key=-start_date' if descending else ''
    cmr_query_url = f'https://{service}/search/granules.json?collection_concept_id={ccid}{pretty}{sort_key}'
    return process_request(cmr_query_url, collection_granules_dict, get_session(), page_size=500)


def collection_granules_list(json_resp: dict) -> list:
    """
    This function processes the return information from a granules.json request.
    Do not use it for a granules.umm_json request.

    :param json_resp: CMR JSON response
    :return: A list with the Granule IDs extracted from a CMR response.
    :rtype: list
    """
    if not is_entry_feed(json_resp):
        return []

    resp = []
    # Look for the entry id aka the granule ID
    for entry in json_resp["feed"]["entry"]:
        if "id" in entry:  # some granule records lack "producer_granule_id". jhrg 9/4/22
            resp += [entry["id"]]

    return resp


def get_collection_granule_ids(ccid: str, num = -1, descending=False,  service='cmr.earthdata.nasa.gov') -> list:
    """
    Get granule IDs for a collection

    :param ccid: The string Collection Concept ID
    :param num: Limit the number of granule IDs returned. Default is -1, which returns all granule IDs.
    :param descending: If true, get the granules in newest first order, else oldest granule is first
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)

    :returns: The collection's Granule IDs, in a list
    """
    sort_key = '&sort_key=-start_date' if descending else ''
    cmr_query_url = f'https://{service}/search/granules.json?collection_concept_id={ccid}{sort_key}'
    return process_request_list(cmr_query_url, collection_granules_list, get_session(), num, page_size=500)


def get_related_urls_from_granule_id(ccid: str, granule_id: str, service='cmr.earthdata.nasa.gov') -> dict:
    """
    Search for a granules RelatedUrls using the collection concept id and granule ur.
    This provides a way to go from the REST form of a URL that the OPeNDAP server typically
    receives and the URLs that can be used to directly access data (and thus the DMR++
    if the data are in S3 and OPeNDAP-enabled).

    :returns: A dictionary that holds all the RelatedUrls that have Type 'GET DATA' or 'USE SERVICE DATA.'
    """
    cmr_query_url = f'https://{service}/search/granules.umm_json?collection_concept_id={ccid}&concept_id={granule_id}'
    return process_request(cmr_query_url, granule_data_url_dict, get_session(), page_num=1)

def get_collection_granules_temporal(ccid: str, time_range: str, pretty=False, service='cmr.earthdata.nasa.gov',
                                     descending=False) -> dict:
    """
    Get granules that fall within a time range for a collection

    :param ccid: The string Collection (Concept) Id
    :param time_range: date range to limit granule query e.g., '2000-01-01T10:00:00Z,2010-03-10T12:00:00Z'
    :param pretty: request a 'pretty' version of the response from the service. default False
    :param service: The URL of the service to query (default cmr.earthdata.nasa.gov)
    :param descending: If true, get the granules in newest first order, else oldest granule is first
    :returns: The collection JSON object
    """
    temporal = f'&temporal={time_range}'
    pretty = '&pretty=true' if pretty else ''
    sort_key = '&sort_key=-start_date' if descending else ''
    cmr_query_url = f'https://{service}/search/granules.json?collection_concept_id={ccid}{pretty}{sort_key}{temporal}'
    return process_request(cmr_query_url, collection_granules_dict, get_session(), page_size=500)


def decompose_resty_url(url: str, pretty=False) -> dict:
    """
    Extract the collection concept id and granule ur. Use this information to
    get the actual URLs that lead to the data. If a 'provider - collection name'
    URL is used, this will result in an error stating that the collection
    concept id does not exist.

    :param url: The URL to parse
    :param pretty: Ask CMR to return a JSON UMM document for humans
    :returns: A dictionary of the URLs, indexed as 'URL1', ..., 'URLn.'
    """
    url_pieces = url.split('/')[3:]
    url_dict = convert(url_pieces)  # convert the array to a dictionary
    print(f'URL parts: {url_dict}') if verbose else ''

    items = get_related_urls(url_dict['collections'], url_dict['granules'], pretty=pretty)
    print(f'Data URLs: {items}') if verbose else ''
    return items
