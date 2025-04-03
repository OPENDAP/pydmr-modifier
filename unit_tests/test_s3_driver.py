import os
import tempfile
import unittest
import s3_driver as s3
from fileOutput import local_path


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

    def setUp(self):
        # Create a temporary file for each test
        self.temp_file = tempfile.NamedTemporaryFile(dir=self.temp_dir, delete=False)
        self.addCleanup(self.temp_file.close)  # Ensure file is closed
        self.addCleanup(os.remove, self.temp_file.name)  # Ensure file is deleted

    def test_load_config(self):
        s3.verbose = False
        s3.load_config()
        self.assertEqual(s3.replace, "OPeNDAP_DMRpp_DATA_ACCESS_URL")

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
        self.assertEqual(content, "Test: Success")  # add assertion here

    def test_build_urls(self):
        url = "http://bucket_name/a/very/long/object/name/for/a/dmrrp-file.ext"
        ccid = "C##########-DACC"

        local_path, file = s3.build_urls(url, ccid)

        self.assertEqual(local_path, "Imports/DACC/dmrrp-file.ext")
        self.assertEqual(file, "dmrrp-file.ext")

if __name__ == '__main__':
    unittest.main()
