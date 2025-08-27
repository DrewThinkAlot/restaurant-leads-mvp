import pytest
from unittest.mock import Mock, patch, AsyncMock
import json
from datetime import datetime, timedelta
from uuid import UUID

from app.agents.agent_extractor import ExtractorAgent
from app.agents.agent_resolver import ResolverAgent
from app.agents.agent_eta import ETAAgent
from app.agents.agent_verifier import VerifierAgent
from app.agents.agent_pitch import PitchAgent
from app.agents.crew import RestaurantLeadsCrew
from app.schemas import RestaurantCandidate, MatchEvaluation, ETAResult, LeadOutput


class TestExtractorAgent:
    """Test LLM-based data extraction and normalization."""
    
    def setup_method(self):
        """Setup test environment."""
        self.agent = ExtractorAgent()
    
    def test_extract_from_tabc_raw_data(self):
        """Test extraction from TABC raw record."""
        raw_data = {
            "license_number": "MB123456",
            "business_name": "Joe's Pizza Palace LLC",
            "trade_name": "Joe's Pizza",
            "address": "123 Main Street",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77001",
            "status": "Original Pending",
            "license_type": "Mixed Beverage Permit"
        }
        
        with patch.object(self.agent, '_call_llm') as mock_llm:
            mock_llm.return_value = {
                "venue_name": "Joe's Pizza",
                "legal_name": "Joe's Pizza Palace LLC",
                "address": "123 Main Street",
                "city": "Houston",
                "state": "TX",
                "zip_code": "77001",
                "business_type": "restaurant",
                "cuisine_type": "pizza",
                "source": "tabc",
                "source_id": "MB123456"
            }
            
            result = self.agent.extract_candidate(raw_data)
            
            assert result["venue_name"] == "Joe's Pizza"
            assert result["legal_name"] == "Joe's Pizza Palace LLC"
            assert result["business_type"] == "restaurant"
            assert result["source"] == "tabc"
    
    def test_extract_with_fallback_on_llm_error(self):
        """Test fallback extraction when LLM fails."""
        raw_data = {
            "business_name": "Mary's Cafe",
            "address": "456 Oak Ave",
            "city": "Houston"
        }
        
        with patch.object(self.agent, '_call_llm') as mock_llm:
            mock_llm.side_effect = Exception("LLM API error")
            
            result = self.agent.extract_candidate(raw_data)
            
            # Should fall back to basic extraction
            assert result["venue_name"] == "Mary's Cafe"
            assert result["address"] == "456 Oak Ave"
            assert result["city"] == "Houston"
            assert result["extraction_confidence"] < 0.8  # Lower confidence for fallback
    
    def test_normalize_business_name(self):
        """Test business name normalization."""
        test_cases = [
            ("Joe's Pizza LLC", "Joe's Pizza"),
            ("MARY'S RESTAURANT INC.", "Mary's Restaurant"),
            ("The Best Burger Joint Co", "The Best Burger Joint"),
            ("ABC FOOD SERVICE CORPORATION", "ABC Food Service"),
            ("Quick Eats, Inc", "Quick Eats")
        ]
        
        for input_name, expected in test_cases:
            result = self.agent._normalize_business_name(input_name)
            assert result == expected
    
    def test_parse_address_components(self):
        """Test address parsing into components."""
        addresses = [
            ("123 Main Street, Houston, TX 77001", {
                "street": "123 Main Street",
                "city": "Houston", 
                "state": "TX",
                "zip_code": "77001"
            }),
            ("456 Oak Ave Suite 100, Harris County, Texas", {
                "street": "456 Oak Ave Suite 100",
                "city": "Harris County",
                "state": "Texas",
                "zip_code": None
            })
        ]
        
        for address_text, expected in addresses:
            result = self.agent._parse_address_components(address_text)
            assert result["street"] == expected["street"]
            assert result["city"] == expected["city"]
            assert result["state"] == expected["state"]
    
    def test_infer_business_type(self):
        """Test business type inference from names and descriptions."""
        test_cases = [
            ("Joe's Pizza", "Mixed Beverage", "restaurant"),
            ("Starbucks Coffee", "Food Handler", "cafe"),
            ("McDonald's #1234", "Food Service", "fast_food"),
            ("The Wine Bar", "Wine and Beer", "bar"),
            ("Taco Truck Express", "Mobile Food", "food_truck"),
            ("ABC Catering Services", "Catering License", "catering")
        ]
        
        for name, permit_type, expected in test_cases:
            result = self.agent._infer_business_type(name, permit_type)
            assert result == expected


class TestResolverAgent:
    """Test entity resolution and duplicate detection."""
    
    def setup_method(self):
        """Setup test environment."""
        self.agent = ResolverAgent()
    
    def test_deterministic_matching_exact_match(self):
        """Test exact matching for identical candidates."""
        candidate1 = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St",
            city="Houston",
            zip_code="77001",
            source="tabc"
        )
        
        candidate2 = RestaurantCandidate(
            venue_name="Joe's Pizza", 
            address="123 Main Street",
            city="Houston",
            zip_code="77001",
            source="hc_permits"
        )
        
        result = self.agent._deterministic_match(candidate1, candidate2)
        
        assert result.is_match is True
        assert result.confidence > 0.9
        assert "exact_name" in result.match_reasons
    
    def test_deterministic_matching_different_entities(self):
        """Test non-matching for different entities."""
        candidate1 = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St",
            city="Houston",
            source="tabc"
        )
        
        candidate2 = RestaurantCandidate(
            venue_name="Mary's Cafe",
            address="456 Oak Ave", 
            city="Houston",
            source="hc_permits"
        )
        
        result = self.agent._deterministic_match(candidate1, candidate2)
        
        assert result.is_match is False
        assert result.confidence < 0.3
    
    def test_llm_resolution_for_ambiguous_pairs(self):
        """Test LLM resolution for ambiguous cases."""
        candidate1 = RestaurantCandidate(
            venue_name="Joe's Pizza Palace",
            address="123 Main Street",
            city="Houston",
            source="tabc"
        )
        
        candidate2 = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St", 
            city="Houston",
            source="permits"
        )
        
        with patch.object(self.agent, '_call_llm') as mock_llm:
            mock_llm.return_value = {
                "same_entity": True,
                "confidence": 0.85,
                "explanation": "Same restaurant with slight name/address variations"
            }
            
            result = self.agent._llm_evaluate_match(candidate1, candidate2)
            
            assert result.same_entity is True
            assert result.confidence == 0.85
    
    def test_merge_candidates_with_different_sources(self):
        """Test merging candidates from different sources."""
        candidate1 = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St",
            city="Houston", 
            source="tabc",
            source_flags={"tabc": "pending"},
            signals={"license_applications": ["Mixed Beverage"]}
        )
        
        candidate2 = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St",
            city="Houston",
            source="permits", 
            source_flags={"permit": "approved"},
            signals={"permits": ["Restaurant Build-out"]}
        )
        
        merged = self.agent._merge_candidates(candidate1, candidate2)
        
        assert merged.venue_name == "Joe's Pizza"
        assert merged.source_flags["tabc"] == "pending"
        assert merged.source_flags["permit"] == "approved"
        assert "Mixed Beverage" in merged.signals["license_applications"]
        assert "Restaurant Build-out" in merged.signals["permits"]
    
    def test_resolve_entity_groups(self):
        """Test resolving groups of potentially duplicate entities."""
        candidates = [
            RestaurantCandidate(venue_name="Joe's Pizza", address="123 Main St", source="tabc"),
            RestaurantCandidate(venue_name="Joe's Pizza Palace", address="123 Main Street", source="permits"),
            RestaurantCandidate(venue_name="Mary's Cafe", address="456 Oak Ave", source="health")
        ]
        
        with patch.object(self.agent, '_deterministic_match') as mock_deterministic:
            with patch.object(self.agent, '_llm_evaluate_match') as mock_llm:
                # Mock Joe's Pizza variants as matching
                mock_deterministic.side_effect = [
                    MatchEvaluation(is_match=False, confidence=0.6),  # Ambiguous
                    MatchEvaluation(is_match=False, confidence=0.2),  # Different
                    MatchEvaluation(is_match=False, confidence=0.2)   # Different
                ]
                
                mock_llm.return_value = MatchEvaluation(same_entity=True, confidence=0.8)
                
                resolved = self.agent.resolve_entities(candidates)
                
                # Should merge Joe's Pizza variants, keep Mary's separate
                assert len(resolved) == 2


class TestETAAgent:
    """Test ETA estimation with rules and LLM adjustment."""
    
    def setup_method(self):
        """Setup test environment."""
        self.agent = ETAAgent()
    
    def test_eta_estimation_with_tabc_pending(self):
        """Test ETA estimation for TABC pending license."""
        candidate = RestaurantCandidate(
            venue_name="Joe's Pizza",
            address="123 Main St",
            source="tabc",
            source_flags={"tabc": "original_pending"},
            signals={"milestones": ["License Application Submitted"]}
        )
        
        with patch('app.rules.estimate_opening_eta') as mock_rules:
            mock_rules.return_value = ETAResult(
                eta_days=45,
                confidence=0.75,
                reasoning=["TABC pending license rule applied"]
            )
            
            with patch.object(self.agent, '_call_llm') as mock_llm:
                mock_llm.return_value = {
                    "adjusted_eta_days": 42,
                    "confidence_adjustment": 0.05,
                    "rationale": "Similar businesses typically open 3 days sooner"
                }
                
                result = self.agent.estimate_eta(candidate)
                
                assert result.eta_days == 42
                assert result.confidence == 0.80  # 0.75 + 0.05
                assert "LLM adjusted" in result.reasoning[-1]
    
    def test_eta_with_insufficient_signals(self):
        """Test ETA when insufficient data available."""
        candidate = RestaurantCandidate(
            venue_name="New Restaurant",
            address="789 Elm St",
            source="unknown"
        )
        
        result = self.agent.estimate_eta(candidate)
        
        assert result.eta_days is None
        assert result.confidence < 0.3
        assert "insufficient_signals" in result.reasoning
    
    def test_eta_confidence_threshold_gating(self):
        """Test ETA gating based on confidence thresholds."""
        candidate = RestaurantCandidate(
            venue_name="Low Confidence Restaurant",
            source="permits"
        )
        
        with patch('app.rules.estimate_opening_eta') as mock_rules:
            mock_rules.return_value = ETAResult(
                eta_days=30,
                confidence=0.2,  # Below minimum threshold
                reasoning=["Low confidence estimate"]
            )
            
            result = self.agent.estimate_eta(candidate)
            
            # Should be gated due to low confidence
            assert result.eta_days is None or result.confidence < 0.4


class TestVerifierAgent:
    """Test quality checks and validation."""
    
    def setup_method(self):
        """Setup test environment.""" 
        self.agent = VerifierAgent()
    
    def test_verify_complete_candidate(self):
        """Test verification of complete, high-quality candidate."""
        candidate = RestaurantCandidate(
            venue_name="Joe's Pizza",
            legal_name="Joe's Pizza Palace LLC",
            address="123 Main Street",
            city="Houston",
            state="TX",
            zip_code="77001",
            business_type="restaurant",
            source="tabc",
            eta_days=45,
            eta_confidence=0.85,
            source_flags={"tabc": "pending", "permits": "approved"}
        )
        
        result = self.agent.verify_candidate(candidate)
        
        assert result.is_valid is True
        assert result.quality_score > 0.8
        assert len(result.issues) == 0
    
    def test_verify_incomplete_candidate(self):
        """Test verification of incomplete candidate."""
        candidate = RestaurantCandidate(
            venue_name="Incomplete Restaurant",
            source="unknown"
            # Missing required fields
        )
        
        result = self.agent.verify_candidate(candidate)
        
        assert result.is_valid is False
        assert result.quality_score < 0.5
        assert "missing_address" in result.issues
        assert "missing_city" in result.issues
    
    def test_verify_suspicious_eta(self):
        """Test detection of suspicious ETA estimates."""
        candidate = RestaurantCandidate(
            venue_name="Fast Restaurant",
            address="123 Main St",
            city="Houston",
            eta_days=5,  # Suspiciously fast
            eta_confidence=0.9,
            source="permits"
        )
        
        result = self.agent.verify_candidate(candidate)
        
        assert "suspicious_eta" in result.issues
        assert result.quality_score < 0.7  # Penalized for suspicious ETA
    
    def test_verify_conflicting_sources(self):
        """Test detection of conflicting source information."""
        candidate = RestaurantCandidate(
            venue_name="Conflicted Restaurant",
            address="123 Main St", 
            city="Houston",
            source_flags={
                "tabc": "denied",  # Conflicting statuses
                "permits": "approved"
            },
            source="multi"
        )
        
        result = self.agent.verify_candidate(candidate)
        
        assert "conflicting_sources" in result.issues
    
    def test_address_validation(self):
        """Test address format validation."""
        valid_addresses = [
            "123 Main Street, Houston, TX 77001",
            "456 Oak Ave Suite 100",
            "789 Elm Boulevard"
        ]
        
        invalid_addresses = [
            "",
            "invalid",
            "123",
            "Main St"  # Too short
        ]
        
        for address in valid_addresses:
            assert self.agent._is_valid_address(address) is True
        
        for address in invalid_addresses:
            assert self.agent._is_valid_address(address) is False


class TestPitchAgent:
    """Test sales pitch generation."""
    
    def setup_method(self):
        """Setup test environment."""
        self.agent = PitchAgent()
    
    def test_generate_pitch_for_qualified_lead(self):
        """Test pitch generation for high-quality lead."""
        lead = LeadOutput(
            venue_name="Joe's Pizza Palace",
            legal_name="Joe's Pizza Palace LLC",
            address="123 Main Street",
            city="Houston",
            business_type="restaurant",
            cuisine_type="pizza", 
            eta_days=45,
            eta_confidence=0.85,
            quality_score=0.9,
            opening_signals=["TABC pending", "Permits approved"]
        )
        
        with patch.object(self.agent, '_call_llm') as mock_llm:
            mock_llm.return_value = {
                "how_to_pitch": "Focus on pre-opening marketing and grand opening promotion",
                "pitch_text": "Congratulations on Joe's Pizza Palace! We specialize in helping new restaurants...",
                "sms_text": "Hi! Saw Joe's Pizza opening soon in Houston. We help new restaurants with marketing. Quick chat?"
            }
            
            result = self.agent.generate_pitch(lead)
            
            assert result["how_to_pitch"] is not None
            assert "Joe's Pizza Palace" in result["pitch_text"]
            assert len(result["sms_text"]) <= 160  # SMS length limit
    
    def test_generate_pitch_with_fallback(self):
        """Test pitch generation with LLM fallback."""
        lead = LeadOutput(
            venue_name="New Restaurant",
            address="456 Oak Ave",
            city="Houston",
            eta_days=60
        )
        
        with patch.object(self.agent, '_call_llm') as mock_llm:
            mock_llm.side_effect = Exception("LLM error")
            
            result = self.agent.generate_pitch(lead)
            
            # Should have fallback content
            assert result["pitch_text"] is not None
            assert "New Restaurant" in result["pitch_text"]
            assert result["sms_text"] is not None
    
    def test_format_eta_window(self):
        """Test ETA window formatting."""
        test_cases = [
            (30, 0.9, "opening in 4-5 weeks"),
            (45, 0.75, "opening in 6-7 weeks"), 
            (60, 0.6, "opening in 7-9 weeks"),
            (None, 0.3, "opening timeline under review")
        ]
        
        for eta_days, confidence, expected_phrase in test_cases:
            result = self.agent._format_eta_window(eta_days, confidence)
            assert expected_phrase in result.lower()
    
    def test_business_context_analysis(self):
        """Test analysis of business context for pitching."""
        contexts = [
            {
                "business_type": "restaurant",
                "cuisine_type": "pizza",
                "location": "downtown Houston",
                "expected": "high-traffic location"
            },
            {
                "business_type": "fast_food", 
                "location": "suburban strip mall",
                "expected": "family-friendly"
            },
            {
                "business_type": "bar",
                "location": "entertainment district", 
                "expected": "nightlife"
            }
        ]
        
        for context in contexts:
            result = self.agent._analyze_business_context(context)
            assert context["expected"] in result.lower()


class TestRestaurantLeadsCrew:
    """Test CrewAI orchestration and task flow."""
    
    def setup_method(self):
        """Setup test environment."""
        self.crew = RestaurantLeadsCrew()
    
    @pytest.mark.asyncio
    async def test_crew_task_sequence(self):
        """Test full crew task execution sequence."""
        with patch.object(self.crew.signal_scout, 'discover_candidates') as mock_scout:
            with patch.object(self.crew.extractor, 'extract_candidates') as mock_extract:
                with patch.object(self.crew.resolver, 'resolve_entities') as mock_resolve:
                    with patch.object(self.crew.eta_agent, 'estimate_etas') as mock_eta:
                        with patch.object(self.crew.verifier, 'verify_candidates') as mock_verify:
                            with patch.object(self.crew.pitch_agent, 'generate_pitches') as mock_pitch:
                                
                                # Mock the task chain
                                mock_scout.return_value = ["raw_candidate_1"]
                                mock_extract.return_value = ["extracted_candidate_1"]  
                                mock_resolve.return_value = ["resolved_candidate_1"]
                                mock_eta.return_value = ["candidate_with_eta_1"]
                                mock_verify.return_value = ["verified_lead_1"]
                                mock_pitch.return_value = ["lead_with_pitch_1"]
                                
                                result = await self.crew.run_pipeline("Harris")
                                
                                # Verify all agents were called in sequence
                                mock_scout.assert_called_once()
                                mock_extract.assert_called_once()
                                mock_resolve.assert_called_once()
                                mock_eta.assert_called_once()
                                mock_verify.assert_called_once()
                                mock_pitch.assert_called_once()
                                
                                assert result == ["lead_with_pitch_1"]
    
    def test_crew_error_handling(self):
        """Test crew error handling and recovery."""
        with patch.object(self.crew.signal_scout, 'discover_candidates') as mock_scout:
            mock_scout.side_effect = Exception("Data source error")
            
            with pytest.raises(Exception):
                # Should propagate critical errors
                self.crew.run_pipeline("Harris")
    
    def test_crew_context_passing(self):
        """Test context passing between crew tasks."""
        mock_context = {"county": "Harris", "date_range": "2024-01-01 to 2024-12-31"}
        
        with patch.object(self.crew, '_execute_task') as mock_execute:
            mock_execute.return_value = {"results": [], "context": mock_context}
            
            result = self.crew.run_pipeline("Harris", context=mock_context)
            
            # Verify context was passed to tasks
            for call in mock_execute.call_args_list:
                assert "context" in call[1] or len(call[0]) > 1
