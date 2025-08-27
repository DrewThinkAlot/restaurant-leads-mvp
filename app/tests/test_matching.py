import pytest
from app.tools.geocode_local import geocoder, calculate_business_name_similarity, normalize_business_name
from app.agents.agent_resolver import ResolverAgent


class TestAddressMatching:
    """Test address parsing and matching functionality."""
    
    def test_parse_basic_address(self):
        """Test basic address parsing."""
        address = "123 Main Street, Houston, TX 77001"
        
        components = geocoder.parse_address(address)
        
        assert components.street_number == "123"
        assert components.street_name == "Main Street"
        assert components.city == "Houston"
        assert components.state == "TX"
        assert components.zip_code == "77001"
    
    def test_parse_address_with_suite(self):
        """Test address parsing with suite/unit."""
        address = "456 Oak Ave Suite 100, Sugar Land, TX 77478"
        
        components = geocoder.parse_address(address)
        
        assert components.street_number == "456"
        assert components.street_name == "Oak Ave"
        assert components.suite == "100"
        assert components.city == "Sugar Land"
    
    def test_calculate_address_similarity_exact_match(self):
        """Test exact address matching."""
        addr1 = "123 Main St, Houston, TX 77001"
        addr2 = "123 Main Street, Houston, TX 77001"
        
        similarity = geocoder.calculate_address_similarity(addr1, addr2)
        
        assert similarity >= 0.9  # Should be very high similarity
    
    def test_calculate_address_similarity_suite_difference(self):
        """Test address matching with suite differences."""
        addr1 = "123 Main St Suite A, Houston, TX 77001"
        addr2 = "123 Main St Suite B, Houston, TX 77001"
        
        similarity = geocoder.calculate_address_similarity(addr1, addr2)
        
        assert similarity >= 0.7  # Should be high but not exact due to suite difference
    
    def test_calculate_address_similarity_different_streets(self):
        """Test address matching with different streets."""
        addr1 = "123 Main St, Houston, TX 77001"
        addr2 = "456 Oak Ave, Houston, TX 77001"
        
        similarity = geocoder.calculate_address_similarity(addr1, addr2)
        
        assert similarity < 0.5  # Should be low similarity
    
    def test_harris_county_detection(self):
        """Test Harris County address detection."""
        harris_addresses = [
            "123 Main St, Houston, TX 77001",
            "456 Oak Ave, Sugar Land, TX 77478",
            "789 Pine St, Katy, TX 77494"
        ]
        
        non_harris_addresses = [
            "123 Main St, Dallas, TX 75201",
            "456 Oak Ave, Austin, TX 78701"
        ]
        
        for addr in harris_addresses:
            assert geocoder.is_harris_county_address(addr)
        
        for addr in non_harris_addresses:
            assert not geocoder.is_harris_county_address(addr)


class TestBusinessNameMatching:
    """Test business name normalization and matching."""
    
    def test_normalize_business_name_basic(self):
        """Test basic business name normalization."""
        name = "Joe's Pizza Restaurant LLC"
        normalized = normalize_business_name(name)
        
        assert normalized == "joe's pizza"
        assert "llc" not in normalized
        assert "restaurant" not in normalized
    
    def test_normalize_business_name_punctuation(self):
        """Test business name normalization with punctuation."""
        name = "Mary's Cafe & Grill, Inc."
        normalized = normalize_business_name(name)
        
        assert "mary" in normalized
        assert "cafe" in normalized
        assert "grill" in normalized
        assert "inc" not in normalized
        assert "&" not in normalized
    
    def test_calculate_business_name_similarity_exact(self):
        """Test exact business name matching."""
        name1 = "Joe's Pizza"
        name2 = "Joe's Pizza"
        
        similarity = calculate_business_name_similarity(name1, name2)
        
        assert similarity == 1.0
    
    def test_calculate_business_name_similarity_variations(self):
        """Test business name matching with variations."""
        name1 = "Joe's Pizza Restaurant LLC"
        name2 = "Joe's Pizza Inc"
        
        similarity = calculate_business_name_similarity(name1, name2)
        
        assert similarity >= 0.8  # Should be high despite suffix differences
    
    def test_calculate_business_name_similarity_abbreviations(self):
        """Test business name matching with abbreviations."""
        name1 = "McDonald's Restaurant"
        name2 = "McDonald's Rest"
        
        similarity = calculate_business_name_similarity(name1, name2)
        
        assert similarity >= 0.7  # Should handle abbreviations reasonably
    
    def test_calculate_business_name_similarity_different(self):
        """Test business name matching with completely different names."""
        name1 = "Joe's Pizza"
        name2 = "Mary's Burgers"
        
        similarity = calculate_business_name_similarity(name1, name2)
        
        assert similarity < 0.3  # Should be low similarity


class TestEntityResolver:
    """Test entity resolution logic."""
    
    def setup_method(self):
        """Setup test environment."""
        self.resolver = ResolverAgent()
    
    def test_deterministic_exact_address_match(self):
        """Test deterministic matching with exact addresses."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza",
            "address": "123 main st, houston, tx 77001",
            "phone": None,
            "email": None
        }
        
        record2 = {
            "candidate_id": "456", 
            "venue_name": "Joe's Pizzeria",
            "address": "123 main st, houston, tx 77001",
            "phone": None,
            "email": None
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is True
    
    def test_deterministic_phone_match(self):
        """Test deterministic matching with phone numbers."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St, Houston, TX",
            "phone": "(713) 555-0123",
            "email": None
        }
        
        record2 = {
            "candidate_id": "456",
            "venue_name": "Different Name",
            "address": "456 Oak Ave, Houston, TX", 
            "phone": "713-555-0123",  # Same number, different format
            "email": None
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is True
    
    def test_deterministic_email_match(self):
        """Test deterministic matching with email addresses."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St, Houston, TX",
            "phone": None,
            "email": "info@joespizza.com"
        }
        
        record2 = {
            "candidate_id": "456",
            "venue_name": "Different Name",
            "address": "456 Oak Ave, Houston, TX",
            "phone": None,
            "email": "INFO@joespizza.com"  # Same email, different case
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is True
    
    def test_deterministic_high_similarity_match(self):
        """Test deterministic matching with high address and name similarity."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza Restaurant",
            "address": "123 Main Street, Houston, TX 77001",
            "phone": None,
            "email": None
        }
        
        record2 = {
            "candidate_id": "456",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St, Houston, TX 77001",
            "phone": None,
            "email": None
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is True
    
    def test_deterministic_suite_difference_match(self):
        """Test matching records that differ only by suite number."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St Suite A, Houston, TX 77001",
            "phone": None,
            "email": None
        }
        
        record2 = {
            "candidate_id": "456",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St Suite B, Houston, TX 77001",
            "phone": None,
            "email": None
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is True
    
    def test_deterministic_no_match(self):
        """Test records that should not match deterministically."""
        record1 = {
            "candidate_id": "123",
            "venue_name": "Joe's Pizza",
            "address": "123 Main St, Houston, TX 77001",
            "phone": "(713) 555-0123",
            "email": "info@joespizza.com"
        }
        
        record2 = {
            "candidate_id": "456",
            "venue_name": "Mary's Burgers",
            "address": "456 Oak Ave, Sugar Land, TX 77478",
            "phone": "(281) 555-0456",
            "email": "info@marysburgers.com"
        }
        
        is_match = self.resolver._is_deterministic_match(record1, record2)
        
        assert is_match is False
    
    def test_extract_base_address(self):
        """Test base address extraction (removing suite/unit)."""
        addresses = [
            ("123 Main St Suite 100, Houston, TX", "123 Main St, Houston, TX"),
            ("456 Oak Ave #200, Sugar Land, TX", "456 Oak Ave, Sugar Land, TX"),
            ("789 Pine St Unit A, Katy, TX", "789 Pine St, Katy, TX"),
            ("321 Elm St Apartment 5, Houston, TX", "321 Elm St, Houston, TX")
        ]
        
        for full_address, expected_base in addresses:
            base = self.resolver._extract_base_address(full_address)
            assert expected_base.lower() in base.lower()
    
    def test_resolve_entities_simple_case(self):
        """Test entity resolution with simple duplicate case."""
        candidates = [
            {
                "candidate_id": "123",
                "venue_name": "Joe's Pizza",
                "address": "123 Main St, Houston, TX 77001",
                "source_flags": {"tabc": "pending"}
            },
            {
                "candidate_id": "456", 
                "venue_name": "Joe's Pizza Restaurant",
                "address": "123 Main Street, Houston, TX 77001",
                "source_flags": {"hc_permit": "found"}
            },
            {
                "candidate_id": "789",
                "venue_name": "Mary's Cafe",
                "address": "456 Oak Ave, Sugar Land, TX 77478",
                "source_flags": {"tabc": "active"}
            }
        ]
        
        resolved = self.resolver.resolve_entities(candidates)
        
        # Should resolve to 2 unique entities (Joe's Pizza merged, Mary's Cafe separate)
        assert len(resolved) == 2
        
        # Find the merged Joe's Pizza record
        joes_record = None
        for record in resolved:
            if "joe" in record["venue_name"].lower():
                joes_record = record
                break
        
        assert joes_record is not None
        assert joes_record["_merged_from"] == 2  # Should indicate 2 records merged
        assert "tabc" in joes_record["source_flags"]
        assert "hc_permit" in joes_record["source_flags"]


@pytest.fixture
def sample_address_pairs():
    """Sample address pairs for testing."""
    return [
        # High similarity pairs
        ("123 Main St, Houston, TX", "123 Main Street, Houston, TX", 0.9),
        ("456 Oak Ave Suite A", "456 Oak Ave Suite B", 0.8),
        
        # Medium similarity pairs  
        ("123 Main St, Houston", "125 Main St, Houston", 0.7),
        ("Joe's on Main St", "Joe's Restaurant Main St", 0.6),
        
        # Low similarity pairs
        ("123 Main St, Houston", "456 Oak Ave, Dallas", 0.2),
        ("Downtown Location", "Suburban Location", 0.1)
    ]


@pytest.fixture
def sample_business_name_pairs():
    """Sample business name pairs for testing."""
    return [
        # High similarity pairs
        ("Joe's Pizza", "Joe's Pizza", 1.0),
        ("McDonald's Restaurant LLC", "McDonald's Inc", 0.8),
        
        # Medium similarity pairs
        ("Joe's Pizza & Subs", "Joe's Italian Food", 0.6),
        ("Main Street Cafe", "Main St Coffee", 0.5),
        
        # Low similarity pairs
        ("Joe's Pizza", "Mary's Burgers", 0.1),
        ("Chinese Restaurant", "Mexican Grill", 0.0)
    ]


class TestMatchingBenchmarks:
    """Test matching algorithms against expected benchmarks."""
    
    def test_address_similarity_benchmarks(self, sample_address_pairs):
        """Test address similarity against expected scores."""
        for addr1, addr2, expected_min in sample_address_pairs:
            similarity = geocoder.calculate_address_similarity(addr1, addr2)
            
            # Allow some tolerance in similarity scores
            assert similarity >= expected_min - 0.2, f"Failed for {addr1} vs {addr2}"
    
    def test_business_name_similarity_benchmarks(self, sample_business_name_pairs):
        """Test business name similarity against expected scores.""" 
        for name1, name2, expected_min in sample_business_name_pairs:
            similarity = calculate_business_name_similarity(name1, name2)
            
            # Allow some tolerance in similarity scores
            assert similarity >= expected_min - 0.2, f"Failed for {name1} vs {name2}"
