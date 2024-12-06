import shutil
from earthaccess import Auth, DataGranules, Store

"""
Code to test using earthaccess for our own nefarious goals.

jhrg 12/06/24
"""


def example():
    """Experiment with earthaccess. jhrg 12/06/24"""
    query = DataGranules().concept_id("C2208422957-POCLOUD").bounding_box(-134.7, 54.9, -100.9, 69.2)
    print(f"Granule hits: {query.hits()}")
    cloud_granules = query.get(10)

    # is this a cloud hosted data granule?
    if cloud_granules[0].cloud_hosted:
        print(f"# Let's pretty print this: {cloud_granules[0]}")
        print(f"URL: {cloud_granules[0].data_links()}")

    # login
    auth = Auth()
    auth.login(strategy="netrc", persist=True)
    store = Store(auth)

    # If we get an error with direct_access=True, most likely is because
    # we are running this code outside the us-west-2 region.
    try:
        files = store.get(cloud_granules[0:4], local_path="./data/demo-POCLOUD")
    except Exception as e:
        print(f"Error: {e}, we are probably not using this code in the Amazon cloud. Trying external links...")
        # There is hope, even if we are not in the Amazon cloud we can still get the data
        files = store.get(cloud_granules[0:4], access="external", local_path="./data/demo-POCLOUD")


def example_hacked():
    """Experiment with earthaccess some more. jhrg 12/06/24"""
    query = DataGranules().concept_id("C2208422957-POCLOUD").bounding_box(-134.7, 54.9, -100.9, 69.2)
    print(f"Granule hits: {query.hits()}")
    cloud_granules = query.get(10)

    # is this a cloud hosted data granule?
    if cloud_granules[0].cloud_hosted:
        print(f"# Let's pretty print this: {cloud_granules[0]}")
        print(f"URL: {cloud_granules[0].data_links()}")

    # login
    auth = Auth()
    auth.login(strategy="netrc", persist=True)

    url = cloud_granules[0].data_links()[0]
    print(f"URL: {url}")
    if "opendap" in url and url.endswith(".html"):
        url = url.replace(".html", "")

    # hack to get the DMR++
    url = f"{url}.dmrpp"

    local_filename = url.split("/")[-1]
    path = f"./data/{local_filename}"

    try:
        session = auth.get_session()
        with session.get(
                url,
                stream=True,
                allow_redirects=True,
        ) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                # This is to cap memory usage for large files at 1MB per write to disk per thread
                # https://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content
                shutil.copyfileobj(r.raw, f, length=1024 * 1024)
    except Exception:
        print(f"Error while downloading the file {local_filename}")
        raise Exception


example_hacked()
