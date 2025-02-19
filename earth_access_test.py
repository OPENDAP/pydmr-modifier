import shutil
import earthaccess
from datetime import date
from earthaccess import Auth, DataGranules, Store

import fileOutput as out

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
    query = DataGranules().concept_id("C2208422957-POCLOUD")
    print(f"Granule hits: {query.hits()}")
    cloud_granules = query.get(11)


    # is this a cloud hosted data granule?
    if cloud_granules[0].cloud_hosted:
        #print(f"# Let's pretty print this: {cloud_granules[0]}")
        print(f"Granule URL: {cloud_granules[0].data_links()}")

    # login
    auth = Auth()
    auth.login(strategy="netrc", persist=True)

    url = cloud_granules[0].data_links()[0]
    if "opendap" in url and url.endswith(".html"):
        url = url.replace(".html", "")

    # hack to get the DMR++
    url = f"{url}.dmrpp"
    print(f"Modified URL: {url}")

    local_filename = url.split("/")[-1]
    path = f"./Imports/{local_filename}"

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

# example_hacked()

def example_hacked_v2():
    """
    messing around with new earthaccess API calls
    Returns: nada (atm)

    """
    ccid = "C2036877806-POCLOUD"
    # ccid = "C2208422957-POCLOUD"
    total = 0
    utotal = 0
    year_total = 0
    url_total = 0
    cur_year = date.today().year
    out.create_summary(ccid)
    outlist = []
    for year in range(1970, cur_year + 1):
        for month in range(1, 13):
            results = earthaccess.search_data(
                concept_id=ccid,
                temporal=(f"{year}-{month}",f"{year}-{month}"),
                cloud_hosted=True
            )  # we can use an inner loop for months, and each month will have a variable number of results.
            print(f"\t{year}-{month} results: {len(results)}", end=" ") if len(results) > 0 else ''

            out_month = (month, len(results))
            outlist.append(out_month)

            year_total += len(results)

            url_list = []
            for result in results:
                for url in result.data_links():
                    if "opendap" in url and url.endswith(".html"):
                        url = url.replace(".html", "")

                    # hack to get the DMR++
                    url = f"{url}.dmrpp"
                    url_list.append(url)

            print(f"=> {len(url_list)}") if len(url_list) > 0 else ''
            url_total += len(url_list)
        print(f"{year} results: {year_total} => {url_total}")

        out_year = (year, year_total)
        outlist.append(out_year)
        out.update_summary(outlist)

        total += year_total
        utotal += url_total
        year_total = 0
        url_total = 0
        outlist.clear()
        # results = earthaccess.search_data(concept_id="C2208422957-POCLOUD", cloud_hosted=True)
        # granule_urls = [g.data_links() for g in results]
    print(f"total results: {total} => {utotal}")


if __name__ == "__main__":
    # example_hacked()
    example_hacked_v2()