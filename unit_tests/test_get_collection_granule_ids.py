import unittest
from unittest.mock import patch, MagicMock
import cmr


# Unit test class
class TestGetCollectionGranuleIds(unittest.TestCase):

    @patch('cmr.process_request_list')
    @patch('cmr.get_session')
    def test_valid_response(self, mock_get_session, mock_process_request_list):
        """Test with a valid response."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_process_request_list.return_value = ["G123", "G124"]

        ccid = "C1234567890-EDSC"
        result = cmr.get_collection_granule_ids(ccid)

        mock_process_request_list.assert_called_once_with(
            'https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id=C1234567890-EDSC',
            cmr.collection_granules_list,
            mock_session,
            -1,
            page_size=500
        )
        self.assertEqual(result, ["G123", "G124"])

    @patch('cmr.process_request_list')
    @patch('cmr.get_session')
    def test_limit_num_results(self, mock_get_session, mock_process_request_list):
        """Test with a limit on the number of results."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_process_request_list.return_value = ["G123"]

        ccid = "C1234567890-EDSC"
        num = 1
        result = cmr.get_collection_granule_ids(ccid, num=num)

        mock_process_request_list.assert_called_once_with(
            'https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id=C1234567890-EDSC',
            cmr.collection_granules_list,
            mock_session,
            1,
            page_size=500
        )
        self.assertEqual(result, ["G123"])

    @patch('cmr.process_request_list')
    @patch('cmr.get_session')
    def test_descending_order(self, mock_get_session, mock_process_request_list):
        """Test with descending order."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_process_request_list.return_value = ["G124", "G123"]

        ccid = "C1234567890-EDSC"
        result = cmr.get_collection_granule_ids(ccid, descending=True)

        mock_process_request_list.assert_called_once_with(
            'https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id=C1234567890-EDSC&sort_key=-start_date',
            cmr.collection_granules_list,
            mock_session,
            -1,
            page_size=500
        )
        self.assertEqual(result, ["G124", "G123"])

    @patch('cmr.process_request_list')
    @patch('cmr.get_session')
    def test_custom_service_url(self, mock_get_session, mock_process_request_list):
        """Test with a custom service URL."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_process_request_list.return_value = ["G123", "G124"]

        ccid = "C1234567890-EDSC"
        service = "custom.earthdata.nasa.gov"
        result = cmr.get_collection_granule_ids(ccid, service=service)

        mock_process_request_list.assert_called_once_with(
            'https://custom.earthdata.nasa.gov/search/granules.json?collection_concept_id=C1234567890-EDSC',
            cmr.collection_granules_list,
            mock_session,
            -1,
            page_size=500
        )
        self.assertEqual(result, ["G123", "G124"])

    @patch('cmr.process_request_list')
    @patch('cmr.get_session')
    def test_no_results(self, mock_get_session, mock_process_request_list):
        """Test when no granules are returned."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_process_request_list.return_value = []

        ccid = "C1234567890-EDSC"
        result = cmr.get_collection_granule_ids(ccid)

        self.assertEqual(result, [])

# Run the tests
if __name__ == "__main__":
    unittest.main()
