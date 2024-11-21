import unittest
import cmr


# Unit test class
class TestCollectionGranulesList(unittest.TestCase):

    def test_valid_response_with_producer_granule_id(self):
        """Test with a valid JSON response including 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "producer_granule_id": "PG123"},
                    {"id": "G124", "producer_granule_id": "PG124"}
                ]
            }
        }
        expected = ["G123", "G124"]
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_missing_producer_granule_id(self):
        """Test with entries missing 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123"},
                    {"id": "G124"}
                ]
            }
        }
        expected = []
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_mixed_valid_and_invalid_entries(self):
        """Test with a mix of entries, some with and some without 'producer_granule_id'"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "producer_granule_id": "PG123"},
                    {"id": "G124"}
                ]
            }
        }
        expected = ["G123"]
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_no_feed_key(self):
        """Test when the JSON response lacks the 'feed' key"""
        json_resp = {"not_feed": {"entry": []}}
        expected = []
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_no_entry_key(self):
        """Test when the JSON response lacks 'entry' key in 'feed'"""
        json_resp = {"feed": {"not_entry": []}}
        expected = []
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_empty_feed(self):
        """Test with an empty 'feed'"""
        json_resp = {"feed": {"entry": []}}
        expected = []
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_empty_json(self):
        """Test with an empty JSON object"""
        json_resp = {}
        expected = []
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_none_input(self):
        """Test when None is passed as input"""
        with self.assertRaises(TypeError):
            cmr.collection_granules_list(None)

    def test_valid_response_with_additional_keys(self):
        """Test with a valid response containing additional keys in the entries"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "producer_granule_id": "PG123", "extra": "value"},
                    {"id": "G124", "producer_granule_id": "PG124", "extra": "value"}
                ]
            }
        }
        expected = ["G123", "G124"]
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

    def test_duplicate_granule_ids(self):
        """Test with duplicate granule IDs in the response"""
        json_resp = {
            "feed": {
                "entry": [
                    {"id": "G123", "producer_granule_id": "PG123"},
                    {"id": "G123", "producer_granule_id": "PG123"}
                ]
            }
        }
        expected = ["G123", "G123"]
        self.assertEqual(cmr.collection_granules_list(json_resp), expected)

# Run the tests
if __name__ == "__main__":
    unittest.main()
