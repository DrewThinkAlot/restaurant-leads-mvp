import pytest
from datetime import datetime, timedelta
from app.rules import ETARulesEngine, parse_milestone_text


class TestETARulesEngine:
    """Test deterministic ETA rules."""
    
    def setup_method(self):
        """Setup test environment."""
        self.engine = ETARulesEngine()
    
    def test_high_probability_ship_rule(self):
        """Test high probability shipping rule."""
        # Setup test data
        candidate_data = {
            "venue_name": "Test Restaurant",
            "address": "123 Main St, Houston, TX 77001"
        }
        
        signals = {
            "tabc_status": "Original Pending",
            "health_status": "Plan Review Approved",
            "milestone_dates": {
                "plan_review_approved": (datetime.now() - timedelta(days=30)).isoformat()
            }
        }
        
        result = self.engine.evaluate_candidate(candidate_data, signals)
        
        assert result is not None
        assert result.rule_name == "high_probability_ship"
        assert result.confidence_0_1 == 0.75
        assert result.eta_days == 45
        assert 30 <= result.eta_days <= 60
    
    def test_final_inspection_scheduled_rule(self):
        """Test final inspection scheduled rule."""
        candidate_data = {"venue_name": "Test Restaurant"}
        
        signals = {
            "permit_types": ["Final Inspection Scheduled"],
            "milestone_dates": {
                "final_inspection": datetime.now().isoformat()
            }
        }
        
        result = self.engine.evaluate_candidate(candidate_data, signals)
        
        assert result is not None
        assert result.rule_name == "final_inspection_scheduled"
        assert result.confidence_0_1 == 0.8
        assert 7 <= result.eta_days <= 30
    
    def test_medium_tabc_pending_rule(self):
        """Test medium confidence TABC pending rule."""
        candidate_data = {"venue_name": "Test Restaurant"}
        
        signals = {
            "tabc_status": "Original Pending",
            "tabc_dates": {
                "application_date": (datetime.now() - timedelta(days=45)).isoformat()
            }
        }
        
        result = self.engine.evaluate_candidate(candidate_data, signals)
        
        assert result is not None
        assert result.rule_name == "medium_tabc_pending"
        assert result.confidence_0_1 == 0.6
        assert 30 <= result.eta_days <= 75
    
    def test_medium_plan_review_building_rule(self):
        """Test medium confidence plan review + building permit rule."""
        candidate_data = {"venue_name": "Test Restaurant"}
        
        signals = {
            "health_status": "Plan Review Received",
            "permit_types": ["Building Permit", "Tenant Build-out"]
        }
        
        result = self.engine.evaluate_candidate(candidate_data, signals)
        
        assert result is not None
        assert result.rule_name == "medium_plan_review_building"
        assert result.confidence_0_1 == 0.55
        assert 45 <= result.eta_days <= 90
    
    def test_downweight_factors(self):
        """Test down-weighting factors reduce confidence."""
        candidate_data = {
            "venue_name": "Test Restaurant",
            "address": "Bad"  # Too short address
        }
        
        signals = {
            "tabc_status": "Original Pending Denied",  # Negative status
            "permit_types": ["Expired Permit"]  # Expired permit
        }
        
        # This should trigger multiple down-weighting factors
        multiplier = self.engine._apply_downweight_factors(candidate_data, signals)
        
        assert multiplier < 1.0
        assert multiplier >= 0.1  # Minimum confidence threshold
    
    def test_no_matching_rules(self):
        """Test case where no rules match."""
        candidate_data = {"venue_name": "Test Restaurant"}
        
        signals = {
            "tabc_status": "Active",  # Not pending
            "health_status": "Unknown"
        }
        
        result = self.engine.evaluate_candidate(candidate_data, signals)
        
        assert result is None
    
    def test_should_create_lead_gates(self):
        """Test lead creation gates."""
        # High confidence, near-term ETA should create lead
        high_conf_result = type('ETARuleResult', (), {
            'confidence_0_1': 0.75,
            'eta_start': datetime.now() + timedelta(days=30),
            'eta_end': datetime.now() + timedelta(days=45)
        })()
        
        assert self.engine.should_create_lead(high_conf_result) is True
        
        # Low confidence should not create lead
        low_conf_result = type('ETARuleResult', (), {
            'confidence_0_1': 0.5,
            'eta_start': datetime.now() + timedelta(days=30),
            'eta_end': datetime.now() + timedelta(days=45)
        })()
        
        assert self.engine.should_create_lead(low_conf_result) is False
        
        # Far future ETA should not create lead
        far_future_result = type('ETARuleResult', (), {
            'confidence_0_1': 0.75,
            'eta_start': datetime.now() + timedelta(days=90),
            'eta_end': datetime.now() + timedelta(days=120)
        })()
        
        assert self.engine.should_create_lead(far_future_result) is False


class TestMilestoneParser:
    """Test milestone text parsing."""
    
    def test_parse_simple_milestones(self):
        """Test parsing simple milestone text."""
        text = """
        Application filed: 01/15/2024
        Plan review approved: 02/20/2024
        Inspection scheduled: 03/10/2024
        """
        
        milestones = parse_milestone_text(text)
        
        assert "application_date" in milestones
        assert "approved_date" in milestones
        assert "scheduled_date" in milestones
    
    def test_parse_mixed_date_formats(self):
        """Test parsing different date formats."""
        text = """
        Filed: 2024-01-15
        Approved: January 20, 2024
        Issued: 01/25/2024
        """
        
        milestones = parse_milestone_text(text)
        
        assert len(milestones) >= 2  # Should parse at least some dates
    
    def test_parse_no_dates(self):
        """Test parsing text with no dates."""
        text = "Restaurant permit application in progress"
        
        milestones = parse_milestone_text(text)
        
        assert len(milestones) == 0


@pytest.fixture
def sample_candidate():
    """Sample candidate data for testing."""
    return {
        "candidate_id": "123e4567-e89b-12d3-a456-426614174000",
        "venue_name": "Joe's Pizza",
        "address": "123 Main St, Houston, TX 77001",
        "city": "Houston",
        "state": "TX",
        "zip": "77001"
    }


@pytest.fixture
def sample_signals():
    """Sample signals data for testing."""
    return {
        "tabc_status": "Original Pending",
        "tabc_dates": {
            "application_date": "2024-01-15",
            "status_date": "2024-01-20"
        },
        "health_status": "Plan Review Approved",
        "permit_types": ["Food Service Permit"],
        "milestone_dates": {
            "plan_review_approved": "2024-02-01",
            "inspection_scheduled": "2024-02-15"
        }
    }


class TestIntegratedRules:
    """Test integrated rule evaluation scenarios."""
    
    def test_multiple_rules_best_match(self, sample_candidate, sample_signals):
        """Test that engine returns best matching rule."""
        engine = ETARulesEngine()
        
        # Signals that could match multiple rules
        signals = {
            "tabc_status": "Original Pending",
            "health_status": "Plan Review Approved", 
            "permit_types": ["Final Inspection Scheduled"],  # Could match final inspection rule
            "tabc_dates": {
                "application_date": (datetime.now() - timedelta(days=30)).isoformat()
            },
            "milestone_dates": {
                "plan_review_approved": (datetime.now() - timedelta(days=20)).isoformat()
            }
        }
        
        result = engine.evaluate_candidate(sample_candidate, signals)
        
        assert result is not None
        # Should pick the highest confidence rule (final inspection = 0.8)
        assert result.rule_name == "final_inspection_scheduled"
        assert result.confidence_0_1 == 0.8
    
    def test_confidence_below_threshold(self, sample_candidate):
        """Test rules with confidence below minimum threshold."""
        engine = ETARulesEngine()
        
        # Create signals that would result in very low confidence
        signals = {
            "tabc_status": "Original Pending Denied Expired",  # Multiple negative terms
            "permit_types": ["Voided Permit", "Cancelled Application"]
        }
        
        # Even if a rule matches, down-weighting should push below 0.5
        result = engine.evaluate_candidate(sample_candidate, signals)
        
        # Should return None because no rules pass minimum confidence
        assert result is None
