import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
import responses

from app.tools.tabc_open_data import TABCOpenDataClient, get_pending_restaurant_licenses
from app.tools.hc_permits import HarrisCountyPermitsClient, search_restaurant_permits
from app.tools.socrata_mcp import SocrataMCPClient, discover_and_query_datasets


class TestTABCIngestion:
    """Test TABC data ingestion with mocked responses."""
    
    def setup_method(self):
        """Setup test environment."""
        self.client = TABCOpenDataClient()
    
    @responses.activate
    def test_query_pending_licenses_success(self):
        """Test successful TABC license query."""
        # Mock response data
        mock_data = [
            {
                "license_number": "12345",
                "business_name": "Joe's Pizza LLC",
                "trade_name": "Joe's Pizza",
                "address": "123 Main St",
                "city": "Houston",
                "state": "TX",
                "zip_code": "77001",
                "county": "Harris",
                "status": "Original Pending",
                "status_date": "2024-01-15T00:00:00",
                "application_date": "2024-01-10T00:00:00",
                "license_type": "Mixed Beverage Permit"
            }
        ]
        
        # Mock the Socrata API endpoint
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            json=mock_data,
            status=200
        )
        
        result = self.client.query_pending_licenses("Harris", 90)
        
        assert len(result) == 1
        assert result[0]["source"] == "tabc"
        assert result[0]["business_name"] == "Joe's Pizza LLC"
        assert result[0]["status"] == "Original Pending"
    
    @responses.activate 
    def test_query_pending_licenses_pagination(self):
        """Test TABC query with pagination."""
        # Mock first page
        page1_data = [{"license_number": f"1234{i}", "business_name": f"Restaurant {i}"} for i in range(1000)]
        
        # Mock second page (smaller)
        page2_data = [{"license_number": f"5678{i}", "business_name": f"Cafe {i}"} for i in range(500)]
        
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            json=page1_data,
            status=200,
            match=[responses.matchers.query_param_matcher({"$offset": "0"})]
        )
        
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            json=page2_data,
            status=200,
            match=[responses.matchers.query_param_matcher({"$offset": "1000"})]
        )
        
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            json=[],
            status=200,
            match=[responses.matchers.query_param_matcher({"$offset": "1500"})]
        )
        
        result = self.client.query_pending_licenses("Harris", 90)
        
        assert len(result) == 1500  # Should get both pages
    
    @responses.activate
    def test_query_pending_licenses_error_handling(self):
        """Test TABC query error handling."""
        # Mock API error
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            status=500
        )
        
        result = self.client.query_pending_licenses("Harris", 90)
        
        assert result == []  # Should return empty list on error
    
    @responses.activate
    def test_search_by_business_name(self):
        """Test TABC search by business name."""
        mock_data = [
            {
                "license_number": "12345",
                "business_name": "Joe's Pizza Palace",
                "trade_name": "Joe's Pizza",
                "status": "Active"
            }
        ]
        
        responses.add(
            responses.GET,
            f"{self.client.base_url}/resource/{self.client.licenses_dataset}.json",
            json=mock_data,
            status=200
        )
        
        result = self.client.search_by_business_name("Joe's Pizza")
        
        assert len(result) == 1
        assert "Joe's Pizza" in result[0]["business_name"]
    
    @responses.activate
    def test_discover_datasets(self):
        """Test TABC dataset discovery."""
        mock_discovery_response = {
            "results": [
                {
                    "resource": {
                        "id": "tabc-licenses-123",
                        "name": "TABC License Applications",
                        "description": "Texas ABC license applications data",
                        "updatedAt": "2024-01-01T00:00:00Z",
                        "columns_field_name": ["license_number", "business_name", "status"]
                    }
                }
            ]
        }
        
        responses.add(
            responses.GET,
            f"{self.client.base_url}/api/catalog/v1",
            json=mock_discovery_response,
            status=200
        )
        
        datasets = self.client.discover_datasets()
        
        assert len(datasets) == 1
        assert datasets[0]["id"] == "tabc-licenses-123"
        assert datasets[0]["name"] == "TABC License Applications"
    
    def test_normalize_tabc_records(self):
        """Test TABC record normalization."""
        raw_records = [
            {
                "license_number": "12345",
                "business_name": "Joe's Pizza LLC",
                "trade_name": "Joe's Pizza",
                "address": "123 Main St",
                "city": "Houston",
                "state": "TX",
                "zip_code": "77001",
                "status": "Original Pending"
            }
        ]
        
        normalized = self.client._normalize_tabc_records(raw_records)
        
        assert len(normalized) == 1
        assert normalized[0]["source"] == "tabc"
        assert normalized[0]["source_id"] == "12345"
        assert normalized[0]["business_name"] == "Joe's Pizza LLC"
    
    def test_get_pending_restaurant_licenses_filtering(self):
        """Test restaurant license filtering."""
        with patch.object(self.client, 'query_pending_licenses') as mock_query:
            mock_query.return_value = [
                {
                    "license_type": "Mixed Beverage Permit",
                    "business_name": "Joe's Restaurant",
                    "status": "Original Pending"
                },
                {
                    "license_type": "Wholesale Beer License", 
                    "business_name": "ABC Distributing",
                    "status": "Original Pending"
                },
                {
                    "license_type": "Wine and Beer Retailer",
                    "business_name": "Mary's Cafe",
                    "status": "Original Pending"
                }
            ]
            
            result = get_pending_restaurant_licenses("Harris", 90)
            
            # Should filter out wholesale distributor, keep restaurants
            assert len(result) == 2
            restaurant_names = [r["business_name"] for r in result]
            assert "Joe's Restaurant" in restaurant_names
            assert "Mary's Cafe" in restaurant_names
            assert "ABC Distributing" not in restaurant_names


class TestHarrisCountyPermitsIngestion:
    """Test Harris County permits scraping with mocked browser."""
    
    @patch('app.tools.hc_permits.sync_playwright')
    def test_search_permits_success(self, mock_playwright):
        """Test successful permit search."""
        # Mock Playwright components
        mock_browser = Mock()
        mock_page = Mock()
        mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        # Mock search results
        mock_row1 = Mock()
        mock_row1.query_selector_all.return_value = [
            Mock(inner_text=Mock(return_value="P12345")),
            Mock(inner_text=Mock(return_value="Restaurant Build-out")),
            Mock(inner_text=Mock(return_value="Tenant finish for new restaurant")),
            Mock(inner_text=Mock(return_value="123 Main St, Houston, TX")),
            Mock(inner_text=Mock(return_value="Approved")),
            Mock(inner_text=Mock(return_value="01/15/2024")),
            Mock(inner_text=Mock(return_value="02/01/2024")),
            Mock(inner_text=Mock(return_value="Joe's Pizza LLC"))
        ]
        
        mock_page.query_selector_all.return_value = [mock_row1]
        mock_page.query_selector.return_value = None  # No next page
        
        client = HarrisCountyPermitsClient()
        result = client.search_permits(["restaurant"], datetime.now(), datetime.now())
        
        assert len(result) == 1
        assert result[0]["source"] == "hc_permits"
        assert result[0]["permit_id"] == "P12345"
        assert "restaurant" in result[0]["description"].lower()
    
    @patch('app.tools.hc_permits.sync_playwright')
    def test_search_permits_no_results(self, mock_playwright):
        """Test permit search with no results."""
        mock_browser = Mock()
        mock_page = Mock()
        mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        mock_page.query_selector_all.return_value = []  # No results
        
        client = HarrisCountyPermitsClient()
        result = client.search_permits(["restaurant"])
        
        assert result == []
    
    @patch('app.tools.hc_permits.sync_playwright')
    def test_search_permits_pagination(self, mock_playwright):
        """Test permit search with pagination."""
        mock_browser = Mock()
        mock_page = Mock()
        mock_playwright.return_value.__enter__.return_value.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        # Mock first page results
        mock_row1 = Mock()
        mock_row1.query_selector_all.return_value = [
            Mock(inner_text=Mock(return_value="P12345")),
            Mock(inner_text=Mock(return_value="Restaurant")),
            Mock(inner_text=Mock(return_value="Description")),
            Mock(inner_text=Mock(return_value="Address")),
            Mock(inner_text=Mock(return_value="Status"))
        ]
        
        # Mock second page results
        mock_row2 = Mock()
        mock_row2.query_selector_all.return_value = [
            Mock(inner_text=Mock(return_value="P67890")),
            Mock(inner_text=Mock(return_value="Food Service")),
            Mock(inner_text=Mock(return_value="Description 2")),
            Mock(inner_text=Mock(return_value="Address 2")),
            Mock(inner_text=Mock(return_value="Status 2"))
        ]
        
        # Mock pagination
        next_button = Mock()
        mock_page.query_selector.side_effect = [next_button, None]  # Next button exists, then None
        mock_page.query_selector_all.side_effect = [[mock_row1], [mock_row2]]
        
        client = HarrisCountyPermitsClient()
        result = client.search_permits(["restaurant"])
        
        assert len(result) == 2
        assert result[0]["permit_id"] == "P12345"
        assert result[1]["permit_id"] == "P67890"
    
    def test_is_restaurant_related_filtering(self):
        """Test restaurant-related permit filtering."""
        client = HarrisCountyPermitsClient()
        
        restaurant_permits = [
            {"permit_type": "Restaurant Build-out", "description": "New restaurant construction"},
            {"permit_type": "Food Service", "description": "Commercial kitchen setup"},
            {"permit_type": "Tenant Finish", "description": "Restaurant tenant improvements"}
        ]
        
        non_restaurant_permits = [
            {"permit_type": "Office Build-out", "description": "Office space renovation"},
            {"permit_type": "Retail", "description": "Clothing store setup"}
        ]
        
        for permit in restaurant_permits:
            assert client._is_restaurant_related(permit) is True
        
        for permit in non_restaurant_permits:
            assert client._is_restaurant_related(permit) is False
    
    def test_extract_business_type(self):
        """Test business type extraction from permit descriptions."""
        client = HarrisCountyPermitsClient()
        
        test_cases = [
            ("Restaurant build-out for new dining establishment", "restaurant"),
            ("Fast food drive-thru construction", "fast_food"),
            ("Coffee shop and cafe setup", "cafe"),
            ("Bar and grill renovation", "bar"),
            ("Food truck commissary kitchen", "food_truck"),
            ("Catering facility construction", "catering"),
            ("Generic commercial build-out", "unknown")
        ]
        
        for description, expected_type in test_cases:
            result = client._extract_business_type(description)
            assert result == expected_type
    
    def test_deduplicate_permits(self):
        """Test permit deduplication."""
        client = HarrisCountyPermitsClient()
        
        permits = [
            {"permit_id": "P12345", "description": "Restaurant A"},
            {"permit_id": "P67890", "description": "Restaurant B"}, 
            {"permit_id": "P12345", "description": "Restaurant A duplicate"},  # Duplicate
            {"permit_id": "P99999", "description": "Restaurant C"}
        ]
        
        unique_permits = client._deduplicate_permits(permits)
        
        assert len(unique_permits) == 3
        permit_ids = [p["permit_id"] for p in unique_permits]
        assert permit_ids.count("P12345") == 1  # No duplicates


class TestSocrataMCPIngestion:
    """Test Socrata MCP client functionality."""
    
    @pytest.mark.asyncio
    async def test_discover_datasets_fallback_to_rest(self):
        """Test dataset discovery with fallback to REST."""
        client = SocrataMCPClient()
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {
                "results": [
                    {
                        "resource": {
                            "id": "test-dataset-123",
                            "name": "Test Restaurant Data",
                            "description": "Sample dataset",
                            "updatedAt": "2024-01-01T00:00:00Z"
                        }
                    }
                ]
            }
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await client.discover_datasets("restaurant license")
            
            assert len(result) == 1
            assert result[0]["id"] == "test-dataset-123"
            assert result[0]["source"] == "socrata_rest_fallback"
    
    @pytest.mark.asyncio
    async def test_query_dataset_tabc_fallback(self):
        """Test dataset query with TABC fallback."""
        client = SocrataMCPClient()
        
        with patch('app.tools.tabc_open_data.tabc_client') as mock_tabc:
            mock_tabc.query_pending_licenses.return_value = [
                {"license_number": "12345", "business_name": "Test Restaurant"}
            ]
            
            result = await client.query_dataset("tabc-licenses", "SELECT * WHERE county = 'Harris'")
            
            assert len(result) == 1
            assert result[0]["source"] == "tabc_fallback"
    
    @pytest.mark.asyncio  
    async def test_discover_and_query_datasets_integration(self):
        """Test integrated discovery and querying."""
        with patch('app.tools.socrata_mcp.socrata_mcp_tool') as mock_tool:
            mock_tool.discover_restaurant_datasets.return_value = [
                {"id": "dataset1", "name": "Restaurant Licenses"},
                {"id": "dataset2", "name": "Food Permits"}
            ]
            
            mock_tool.query_for_candidates.return_value = [
                {"business_name": "Restaurant 1", "dataset_source": {"id": "dataset1"}},
                {"business_name": "Restaurant 2", "dataset_source": {"id": "dataset1"}}
            ]
            
            result = await discover_and_query_datasets("Harris")
            
            # Should have called query for each dataset
            assert mock_tool.query_for_candidates.call_count == 2


@pytest.fixture
def mock_tabc_response():
    """Mock TABC API response data."""
    return [
        {
            "license_number": "MB123456",
            "business_name": "Joe's Pizza Palace LLC", 
            "trade_name": "Joe's Pizza",
            "address": "123 Main Street",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77001",
            "county": "Harris",
            "status": "Original Pending",
            "status_date": "2024-01-15T00:00:00.000",
            "application_date": "2024-01-10T00:00:00.000",
            "license_type": "Mixed Beverage Permit",
            "phone": "713-555-0123"
        }
    ]


@pytest.fixture
def mock_hc_permit_response():
    """Mock Harris County permit response data."""
    return [
        {
            "permit_id": "P2024-001234",
            "permit_type": "Tenant Build-out",
            "description": "Restaurant tenant improvements and kitchen build-out",
            "address": "456 Oak Avenue, Houston, TX 77002",
            "status": "Plan Review Approved",
            "application_date": "2024-01-20",
            "issued_date": None,
            "applicant": "Mary's Cafe & Bistro",
            "business_type": "restaurant"
        }
    ]


class TestIngestionIntegration:
    """Integration tests for data ingestion pipeline."""
    
    def test_tabc_to_candidate_conversion(self, mock_tabc_response):
        """Test conversion of TABC data to candidate format."""
        from app.agents.agent_signal_scout import SignalScoutAgent
        
        scout = SignalScoutAgent()
        candidate = scout._convert_tabc_to_candidate(mock_tabc_response[0])
        
        assert candidate["source"] == "tabc"
        assert candidate["venue_name"] == "Joe's Pizza"
        assert candidate["legal_name"] == "Joe's Pizza Palace LLC"
        assert candidate["address"] == "123 Main Street"
        assert candidate["city"] == "Houston"
        assert candidate["zip_code"] == "77001"
        assert candidate["source_flags"]["tabc"] == "original_pending"
    
    def test_hc_permit_to_candidate_conversion(self, mock_hc_permit_response):
        """Test conversion of Harris County permit data to candidate format."""
        from app.agents.agent_signal_scout import SignalScoutAgent
        
        scout = SignalScoutAgent()
        candidate = scout._convert_permit_to_candidate(mock_hc_permit_response[0])
        
        assert candidate["source"] == "hc_permits"
        assert candidate["venue_name"] == "Mary's Cafe & Bistro"
        assert candidate["source_flags"]["hc_permit"] == "found"
        assert "Tenant Build-out" in candidate["signals"]["permit_types"]
    
    def test_mixed_source_deduplication(self, mock_tabc_response, mock_hc_permit_response):
        """Test deduplication across different data sources."""
        from app.agents.agent_signal_scout import SignalScoutAgent
        
        scout = SignalScoutAgent()
        
        # Create candidates from different sources for same restaurant
        tabc_candidate = scout._convert_tabc_to_candidate(mock_tabc_response[0])
        tabc_candidate["venue_name"] = "Joe's Pizza"
        tabc_candidate["address"] = "123 Main St, Houston, TX 77001"
        
        hc_candidate = scout._convert_permit_to_candidate(mock_hc_permit_response[0])
        hc_candidate["venue_name"] = "Joe's Pizza Restaurant" 
        hc_candidate["address"] = "123 Main Street, Houston, TX 77001"  # Slightly different format
        
        candidates = [tabc_candidate, hc_candidate]
        
        unique_candidates = scout._deduplicate_candidates(candidates)
        
        # Should deduplicate to 1 candidate since they represent same restaurant
        assert len(unique_candidates) == 1
