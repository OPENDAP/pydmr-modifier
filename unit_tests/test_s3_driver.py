import os
import tempfile
import unittest
from unittest.mock import patch  # For mocking external dependencies
import s3_driver as s3

#unused
granuleA = {"Collection": {'Version': '1.0', 'ShortName': 'GOES16-SST-OSISAF-L3C-v1.0'},
            "Spatial coverage": {'HorizontalSpatialDomain': {'Geometry': {'BoundingRectangles': [{'WestBoundingCoordinate': -135, 'SouthBoundingCoordinate': -60, 'EastBoundingCoordinate': -15, 'NorthBoundingCoordinate': 60}]}}},
            "Temporal coverage": {'RangeDateTime': {'EndingDateTime': '2020-01-01T00:30:00.000Z', 'BeginningDateTime': '2019-12-31T23:30:00.000Z'}},
            "Size(MB)": 10.1276216506958,
            "Data": ['https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/GOES16-SST-OSISAF-L3C-v1.0/2019/365/20200101000000-OSISAF-L3C_GHRSST-SSTsubskin-GOES16-ssteqc_goes16_20200101_000000-v02.0-fv01.0.nc']
            }
#unusewd
granuleB = {"Collection": {'Version': '1.0', 'ShortName': 'GOES16-SST-OSISAF-L3C-v1.0'},
            "Spatial coverage": {'HorizontalSpatialDomain': {'Geometry': {'BoundingRectangles': [{'WestBoundingCoordinate': -135, 'SouthBoundingCoordinate': -60, 'EastBoundingCoordinate': -15, 'NorthBoundingCoordinate': 60}]}}},
            "Temporal coverage": {'RangeDateTime': {'EndingDateTime': '2020-01-01T01:30:00.000Z', 'BeginningDateTime': '2020-01-01T00:30:00.000Z'}},
            "Size(MB)": 10.250887870788574,
            "Data": ['https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/GOES16-SST-OSISAF-L3C-v1.0/2020/001/20200101010000-OSISAF-L3C_GHRSST-SSTsubskin-GOES16-ssteqc_goes16_20200101_010000-v02.0-fv01.0.nc']
            }

#unused
mock_data_granules = [granuleA, granuleB]

def mock_earthaccess_search(concept_id, temporal, cloud_hosted):
    return mock_data_granules


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

    def setUp(self):
        # Create a temporary file for each test
        self.temp_file = tempfile.NamedTemporaryFile(dir=self.temp_dir, delete=False)
        self.addCleanup(self.temp_file.close)  # Ensure file is closed
        self.addCleanup(os.remove, self.temp_file.name)  # Ensure file is deleted

    # could not get to work, SBL 4-15-25
    # @patch('earthaccess.search_data', mock_earthaccess_search)
    # def test_query_earthaccess_list(self):
    #    s3.verbose = False
    #    url_list =  s3.query_earthaccess("C##########-DACC", 2020, 1)
    #    for url in url_list:
    #        print(url)
    #    self.assertEqual(url_list[0], "...")

    def test_load_config(self):
        s3.verbose = False
        s3.load_config()
        self.assertEqual(s3.replace, "OPeNDAP_DMRpp_DATA_ACCESS_URL")

    def test_load_config_false(self):
        s3.verbose = False
        s3.load_config()
        self.assertNotEqual(s3.replace, "OPeND@P_DMRpp_DATA_ACCESS_URL")

    def test_replace_template(self):
        test_file = "Test: Failure"

        with open(self.temp_file.name, 'w') as f:
            f.write(test_file)
            f.close()

        s3.replace = "Failure"
        s3.replace_template(self.temp_file.name, "Success")

        content = ""
        with open(self.temp_file.name, 'r') as f:
            content = f.read()
            f.close()
        self.assertEqual(content, "Test: Success")

    def test_replace_template_false(self):
        test_file = "Test: Failure"

        with open(self.temp_file.name, 'w') as f:
            f.write(test_file)
            f.close()

        s3.replace = "Failure"
        s3.replace_template(self.temp_file.name, "Successful")

        content = ""
        with open(self.temp_file.name, 'r') as f:
            content = f.read()
            f.close()
        self.assertNotEqual(content, "Test: Success")

    def test_build_urls(self):
        url = "http://bucket_name/a/very/long/object/name/for/a/dmrrp-file.ext"
        ccid = "C##########-DACC"

        local_path, file = s3.build_urls(url, ccid)

        self.assertEqual(local_path, "Imports/DACC/dmrrp-file.ext")
        self.assertEqual(file, "dmrrp-file.ext")

    def test_build_urls_false(self):
        url = "http://bucket_name/a/very/long/object/name/for/a/dmr-file.ext"
        ccid = "C##########-DAAC"

        local_path, file = s3.build_urls(url, ccid)

        self.assertNotEqual(local_path, "Imports/DACC/dmrrp-file.ext")
        self.assertNotEqual(file, "dmrrp-file.ext")


if __name__ == '__main__':
    unittest.main()
