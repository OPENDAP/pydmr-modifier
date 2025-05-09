
import unittest
import cmr


# Unit tests
class TestJsonProcCMR(unittest.TestCase):

    def test_valid_response_with_producer_granule_id(self):
        """Test with a valid JSON response including 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "title": "Granule 1", "producer_granule_id": "PG123"},
                    {"id": "G124", "title": "Granule 2", "producer_granule_id": "PG124"}
                ]
            }
        }
        expected = {
            "G123": ("Granule 1", "PG123"),
            "G124": ("Granule 2", "PG124")
        }
        self.assertEqual(cmr.collection_granules_dict(json_resp), expected)

    def test_valid_response_without_producer_granule_id(self):
        """Test with a valid JSON response missing 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "title": "Granule 1"},
                    {"id": "G124", "title": "Granule 2"}
                ]
            }
        }
        expected = {
            "G123": ("Granule 1",),
            "G124": ("Granule 2",)
        }
        self.assertEqual(cmr.collection_granules_dict(json_resp), expected)

    def test_empty_feed(self):
        """Test with an empty 'feed'"""
        json_resp = {"feed": {"entry": []}}
        self.assertEqual(cmr.collection_granules_dict(json_resp), {})

    def test_no_feed_key(self):
        """Test when the JSON response lacks 'feed' key"""
        json_resp = {"not_feed": {"entry": []}}
        self.assertEqual(cmr.collection_granules_dict(json_resp), {})

    def test_no_entry_key(self):
        """Test when the JSON response lacks 'entry' key in 'feed'"""
        json_resp = {"feed": {"not_entry": []}}
        self.assertEqual(cmr.collection_granules_dict(json_resp), {})

    def test_empty_json(self):
        """Test with an empty JSON object"""
        json_resp = {}
        self.assertEqual(cmr.collection_granules_dict(json_resp), {})

    def test_none_input(self):
        """Test when None is passed as input"""
        with self.assertRaises(TypeError):
            cmr.collection_granules_dict(None)

    def test_mixed_entries(self):
        """Test with a mix of entries, some with and some without 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "title": "Granule 1", "producer_granule_id": "PG123"},
                    {"id": "G124", "title": "Granule 2"}
                ]
            }
        }
        expected = {
            "G123": ("Granule 1", "PG123"),
            "G124": ("Granule 2",)
        }
        self.assertEqual(cmr.collection_granules_dict(json_resp), expected)


    def test_valid_response_with_opendap_links(self):
        """Test with a valid JSON response including OPeNDAP links"""
        json_resp = {
            "feed": {
                "entry": [
                    {
                        "id": "G123",
                        "title": "Granule 1",
                        "links": [{"title": "OPeNDAP URL", "href": "http://example.com/opendap1"}]
                    },
                    {
                        "id": "G124",
                        "title": "Granule 2",
                        "links": [{"title": "OPeNDAP URL", "href": "http://example.com/opendap2"}]
                    }
                ]
            }
        }
        expected = {
            "G123": ("Granule 1", "http://example.com/opendap1"),
            "G124": ("Granule 2", "http://example.com/opendap2")
        }
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_missing_opendap_link(self):
        """Test with entries missing OPeNDAP links"""
        json_resp = {
            "feed": {
                "entry": [
                    {
                        "id": "G123",
                        "title": "Granule 1",
                        "links": [{"title": "Non-OPeNDAP Link", "href": "http://example.com/non-opendap"}]
                    }
                ]
            }
        }
        expected = {}
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_mixed_valid_and_invalid_entries(self):
        """Test with a mix of entries, some with and some without OPeNDAP links"""
        json_resp = {
            "feed": {
                "entry": [
                    {
                        "id": "G123",
                        "title": "Granule 1",
                        "links": [{"title": "OPeNDAP URL", "href": "http://example.com/opendap1"}]
                    },
                    {
                        "id": "G124",
                        "title": "Granule 2",
                        "links": [{"title": "Non-OPeNDAP Link", "href": "http://example.com/non-opendap"}]
                    }
                ]
            }
        }
        expected = {
            "G123": ("Granule 1", "http://example.com/opendap1")
        }
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_no_links_key(self):
        """Test with an entry missing the 'links' key"""
        json_resp = {
            "feed": {
                "entry": [
                    {
                        "id": "G123",
                        "title": "Granule 1"
                    }
                ]
            }
        }
        expected = {}
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_empty_feed(self):
        """Test with an empty 'feed'"""
        json_resp = {"feed": {"entry": []}}
        expected = {}
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_no_feed_key(self):
        """Test when the JSON response lacks the 'feed' key"""
        json_resp = {"not_feed": {"entry": []}}
        expected = {}
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_empty_json(self):
        """Test with an empty JSON object"""
        json_resp = {}
        expected = {}
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

    def test_none_input(self):
        """Test when None is passed as input"""
        with self.assertRaises(TypeError):
            cmr.collection_granule_and_url_dict(None)

    def test_valid_entry_with_multiple_links(self):
        """Test with an entry containing multiple links, including OPeNDAP"""
        json_resp = {
            "feed": {
                "entry": [
                    {
                        "id": "G123",
                        "title": "Granule 1",
                        "links": [
                            {"title": "Other Link", "href": "http://example.com/other"},
                            {"title": "OPeNDAP URL", "href": "http://example.com/opendap1"}
                        ]
                    }
                ]
            }
        }
        expected = {
            "G123": ("Granule 1", "http://example.com/opendap1")
        }
        self.assertEqual(cmr.collection_granule_and_url_dict(json_resp), expected)

# Run the tests
if __name__ == "__main__":
    unittest.main()
