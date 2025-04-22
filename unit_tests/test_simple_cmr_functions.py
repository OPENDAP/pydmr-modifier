import unittest
import cmr


class TestSimpleCMR(unittest.TestCase):

    def test_valid_feed_with_entry(self):
        """Test with a valid JSON having 'feed' and 'entry' keys"""
        json_resp = {"feed": {"entry": {"id": 1, "title": "Sample"}}}
        self.assertTrue(cmr.is_entry_feed(json_resp))

    def test_valid_feed_without_entry(self):
        """Test with a valid JSON having 'feed' but no 'entry' key"""
        json_resp = {"feed": {"not_entry": {"id": 1}}}
        self.assertFalse(cmr.is_entry_feed(json_resp))

    def test_no_feed_key(self):
        """Test with a JSON that does not contain 'feed' key"""
        json_resp = {"no_feed": {"entry": {"id": 1}}}
        self.assertFalse(cmr.is_entry_feed(json_resp))

    def test_empty_json(self):
        """Test with an empty JSON object"""
        json_resp = {}
        self.assertFalse(cmr.is_entry_feed(json_resp))

    def test_none_json(self):
        """Test with None as input"""
        json_resp = None
        with self.assertRaises(TypeError):
            cmr.is_entry_feed(json_resp)

    def test_feed_key_not_a_dict(self):
        """Test with 'feed' key not being a dictionary"""
        json_resp = {"feed": "not_a_dict"}
        self.assertFalse(cmr.is_entry_feed(json_resp))

    def test_feed_entry_empty(self):
        """Test with 'feed' containing an empty 'entry' key"""
        json_resp = {"feed": {"entry": {}}}
        self.assertTrue(cmr.is_entry_feed(json_resp))

    def test_feed_entry_missing(self):
        """Test with 'feed' missing the 'entry' key"""
        json_resp = {"feed": {}}
        self.assertFalse(cmr.is_entry_feed(json_resp))

    def test_valid_item_feed(self):
        """Test with a valid JSON having 'items' with 'meta' key"""
        json_resp = {"items": [{"meta": {"id": 1, "info": "sample"}}]}
        self.assertTrue(cmr.is_item_feed(json_resp))

    def test_items_key_not_a_list(self):
        """Test with 'items' not being a list"""
        json_resp = {"items": {"meta": {"id": 1}}}
        self.assertFalse(cmr.is_item_feed(json_resp))

    def test_items_list_empty(self):
        """Test with 'items' being an empty list"""
        json_resp = {"items": []}
        self.assertFalse(cmr.is_item_feed(json_resp))

    def test_first_item_missing_meta(self):
        """Test with first item in 'items' missing 'meta' key"""
        json_resp = {"items": [{}]}
        self.assertFalse(cmr.is_item_feed(json_resp))

    def test_no_items_key(self):
        """Test with JSON not containing 'items' key"""
        json_resp = {"not_items": [{"meta": {"id": 1}}]}
        self.assertFalse(cmr.is_item_feed(json_resp))

    def test_empty_json(self):
        """Test with an empty JSON object"""
        json_resp = {}
        self.assertFalse(cmr.is_item_feed(json_resp))

    def test_none_json(self):
        """Test with None as input"""
        json_resp = None
        with self.assertRaises(TypeError):
            cmr.is_item_feed(json_resp)

    def test_items_list_has_multiple_entries(self):
        """Test with 'items' list containing multiple entries"""
        json_resp = {
            "items": [
                {"meta": {"id": 1}},
                {"meta": {"id": 2}},
            ]
        }
        self.assertTrue(cmr.is_item_feed(json_resp))

    def test_items_with_non_dict_first_element(self):
        """Test with 'items' list where the first element is not a dict"""
        json_resp = {"items": ["not_a_dict"]}
        self.assertFalse(cmr.is_item_feed(json_resp))

    # Tests for cmr.is_meta_item
    def test_valid_meta_item(self):
        """Test with valid 'meta' containing 'concept-id' and 'native-id'"""
        json_resp = {"meta": {"concept-id": "C12345", "native-id": "N54321"}}
        self.assertTrue(cmr.is_meta_item(json_resp))

    def test_meta_missing_concept_id(self):
        """Test with 'meta' missing 'concept-id'"""
        json_resp = {"meta": {"native-id": "N54321"}}
        self.assertFalse(cmr.is_meta_item(json_resp))

    def test_meta_missing_native_id(self):
        """Test with 'meta' missing 'native-id'"""
        json_resp = {"meta": {"concept-id": "C12345"}}
        self.assertFalse(cmr.is_meta_item(json_resp))

    def test_meta_not_a_dict(self):
        """Test with 'meta' not being a dictionary"""
        json_resp = {"meta": "not_a_dict"}
        self.assertFalse(cmr.is_meta_item(json_resp))

    def test_no_meta_key(self):
        """Test when 'meta' key is absent"""
        json_resp = {"not_meta": {"concept-id": "C12345", "native-id": "N54321"}}
        self.assertFalse(cmr.is_meta_item(json_resp))

    def test_empty_json_for_meta(self):
        """Test with an empty JSON object"""
        json_resp = {}
        self.assertFalse(cmr.is_meta_item(json_resp))

    def test_none_input_for_granule(self):
        """Test when None is passed to cmr.is_granule_item"""
        with self.assertRaises(TypeError):
            cmr.is_granule_item(None)

    # Tests for cmr.is_granule_item
    def test_valid_granule_item(self):
        """Test with valid 'umm' containing 'RelatedUrls'"""
        json_resp = {"umm": {"RelatedUrls": [{"URL": "http://example.com"}]}}
        self.assertTrue(cmr.is_granule_item(json_resp))

    def test_granule_missing_related_urls(self):
        """Test with 'umm' missing 'RelatedUrls'"""
        json_resp = {"umm": {"OtherKey": "Value"}}
        self.assertFalse(cmr.is_granule_item(json_resp))

    def test_granule_no_umm_key(self):
        """Test when 'umm' key is absent"""
        json_resp = {"not_umm": {"RelatedUrls": [{"URL": "http://example.com"}]}}
        self.assertFalse(cmr.is_granule_item(json_resp))

    def test_umm_not_a_dict(self):
        """Test with 'umm' not being a dictionary"""
        json_resp = {"umm": "not_a_dict"}
        self.assertFalse(cmr.is_granule_item(json_resp))

    def test_empty_json_for_granule(self):
        """Test with an empty JSON object"""
        json_resp = {}
        self.assertFalse(cmr.is_granule_item(json_resp))

    def test_none_input_for_meta(self):
        """Test when None is passed to cmr.is_meta_item"""
        with self.assertRaises(TypeError):
            cmr.is_meta_item(None)


# Run the tests
if __name__ == "__main__":
    unittest.main()
