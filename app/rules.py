from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import re


@dataclass
class ETARuleResult:
    """Result from deterministic ETA rules."""
    eta_start: datetime
    eta_end: datetime
    eta_days: int
    confidence_0_1: float
    rule_name: str
    signals_used: List[str]
    rationale_text: str = ""  # Optional rationale text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "eta_start": self.eta_start.isoformat(),
            "eta_end": self.eta_end.isoformat(),
            "eta_days": self.eta_days,
            "confidence_0_1": self.confidence_0_1,
            "rule_name": self.rule_name,
            "signals_used": self.signals_used,
            "rationale_text": self.rationale_text
        }


class ETARulesEngine:
    """Deterministic rules for ETA estimation."""
    
    def __init__(self):
        self.today = datetime.now()
    
    def evaluate_candidate(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """Evaluate a candidate against all rules and return best match."""
        
        rules = [
            self._rule_high_probability_ship,
            self._rule_final_inspection_scheduled,
            self._rule_strong_early_signal,
            self._rule_medium_tabc_pending,
            self._rule_medium_plan_review_building,
            self._rule_health_plan_review_only,
        ]
        
        results = []
        for rule in rules:
            result = rule(candidate_data, signals)
            if result:
                results.append(result)
        
        # Apply down-weighting heuristics
        for result in results:
            result.confidence_0_1 *= self._apply_downweight_factors(candidate_data, signals)
        
        # Return highest confidence result above minimum threshold
        valid_results = [r for r in results if r.confidence_0_1 >= 0.5]
        if valid_results:
            return max(valid_results, key=lambda x: x.confidence_0_1)
        
        return None
    
    def _rule_high_probability_ship(self, candidate_data: Dict[str, Any], signals: Dict[str, Any]) -> Optional[ETARuleResult]:
        """High probability ship rule - very recent applications with positive signals."""
        
        tabc_status = (signals.get('tabc_status') or '').lower()
        permit_status = (signals.get('permit_status') or '').lower()
        inspection_result = (signals.get('inspection_result') or '').lower()
        
        if 'original' not in tabc_status or 'pending' not in tabc_status:
            return None
        
        if 'approved' not in signals.get('health_status', '').lower():
            return None
        
        # Check if plan review approved within last 45 days
        milestone_dates = signals.get('milestone_dates', {})
        plan_approved_date = self._get_latest_milestone_date(milestone_dates, ['plan', 'approved'])
        
        if plan_approved_date:
            days_since_approval = (self.today.date() - plan_approved_date.date()).days
            if days_since_approval <= 45:
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=30),
                    eta_end=self.today + timedelta(days=60),
                    eta_days=45,
                    confidence_0_1=0.75,
                    rule_name="high_probability_ship",
                    signals_used=["tabc_original_pending", "hcph_plan_approved"]
                )
        
        return None
    
    def _rule_strong_early_signal(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """Rule for strong early signals like tenant build-out + TABC application."""
        
        permit_types = signals.get('permit_types', [])
        tabc_status = (signals.get('tabc_status') or '').lower()
        milestone_dates = signals.get('milestone_dates', {})
        
        # Check for early-stage permits
        has_early_permit = any('tenant build-out' in p.lower() or 'new construction' in p.lower() for p in permit_types)
        
        # Check for recent TABC application
        if has_early_permit and 'pending' in tabc_status:
            application_date = self._get_latest_milestone_date(milestone_dates, ['application', 'filed'])
            if application_date and (self.today.date() - application_date.date()).days <= 60:
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=60),
                    eta_end=self.today + timedelta(days=120),
                    eta_days=90,
                    confidence_0_1=0.70,
                    rule_name="strong_early_signal",
                    signals_used=["early_permit", "tabc_pending_recent"]
                )
        
        return None

    def _rule_final_inspection_scheduled(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """Final inspection scheduled OR CO pending/scheduled."""
        
        permit_types = signals.get('permit_types', [])
        milestone_dates = signals.get('milestone_dates', {})
        
        # Look for final inspection or CO indicators
        final_inspection_indicators = [
            'final inspection', 'co pending', 'co scheduled', 
            'certificate of occupancy', 'final review'
        ]
        
        found_indicator = False
        
        # Check permit types
        for permit_type in permit_types:
            for indicator in final_inspection_indicators:
                if indicator in permit_type.lower():
                    found_indicator = True
                    break
        
        # Check milestone dates
        if not found_indicator:
            for date_key in milestone_dates.keys():
                for indicator in final_inspection_indicators:
                    if indicator in date_key.lower():
                        found_indicator = True
                        break
        
        if found_indicator:
            return ETARuleResult(
                eta_start=self.today + timedelta(days=7),
                eta_end=self.today + timedelta(days=30),
                eta_days=18,
                confidence_0_1=0.8,
                rule_name="final_inspection_scheduled",
                signals_used=["final_inspection_or_co"]
            )
        
        return None
    
    def _rule_medium_tabc_pending(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """TABC Original Pending only, age <= 60 days."""
        
        tabc_status = (signals.get('tabc_status') or '').lower()
        tabc_dates = signals.get('tabc_dates', {})
        
        if 'original' not in tabc_status or 'pending' not in tabc_status:
            return None
        
        # Check age of TABC application
        application_date = self._get_latest_milestone_date(tabc_dates, ['application', 'filed'])
        
        if application_date:
            days_since_application = (self.today.date() - application_date.date()).days
            
            # Tiered confidence based on application age
            if days_since_application <= 30:
                # Newer applications have a wider, more distant ETA
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=45),
                    eta_end=self.today + timedelta(days=90),
                    eta_days=67,
                    confidence_0_1=0.65,
                    rule_name="medium_tabc_pending_new",
                    signals_used=["tabc_original_pending_new"]
                )
            elif days_since_application <= 75:
                # Older applications are closer to opening
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=30),
                    eta_end=self.today + timedelta(days=60),
                    eta_days=45,
                    confidence_0_1=0.60,
                    rule_name="medium_tabc_pending_aged",
                    signals_used=["tabc_original_pending_aged"]
                )
        
        return None
    
    def _rule_medium_plan_review_building(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """HCPH plan review received + recent building permit."""
        
        health_status = signals.get('health_status', '').lower()
        permit_types = signals.get('permit_types', [])
        milestone_dates = signals.get('milestone_dates', {})
        
        if 'plan' not in health_status or 'review' not in health_status:
            return None
        
        # Look for building permit and its age
        building_permit_date = self._get_latest_milestone_date(milestone_dates, ['building', 'tenant'])
        has_building_permit = any('building' in p.lower() or 'tenant' in p.lower() for p in permit_types)
        
        if has_building_permit and building_permit_date:
            days_since_permit = (self.today.date() - building_permit_date.date()).days
            
            if days_since_permit <= 60:
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=45),
                    eta_end=self.today + timedelta(days=90),
                    eta_days=67,
                    confidence_0_1=0.55,
                    rule_name="medium_plan_review_building",
                    signals_used=["hcph_plan_review", "building_permit"]
                )
        
        return None

    def _rule_health_plan_review_only(self, candidate_data: Dict, signals: Dict) -> Optional[ETARuleResult]:
        """Rule for recent health department plan review approval."""

        health_status = signals.get('health_status', '').lower()
        milestone_dates = signals.get('milestone_dates', {})

        if 'plan_review_approved' in health_status:
            approval_date = self._get_latest_milestone_date(milestone_dates, ['plan', 'approved'])
            if approval_date and (self.today.date() - approval_date.date()).days <= 45:
                return ETARuleResult(
                    eta_start=self.today + timedelta(days=45),
                    eta_end=self.today + timedelta(days=90),
                    eta_days=67,
                    confidence_0_1=0.60,
                    rule_name="health_plan_review_only",
                    signals_used=["hcph_plan_approved_recent"]
                )

        return None

    def _apply_downweight_factors(self, candidate_data: Dict, signals: Dict) -> float:
        """Apply down-weighting factors based on negative signals."""
        
        multiplier = 1.0
        
        # Conflicting addresses/names
        address = candidate_data.get('address', '')
        venue_name = candidate_data.get('venue_name', '')
        
        # Check for obvious conflicts (simplified heuristic)
        if not address or len(address.strip()) < 10:
            multiplier *= 0.9
        
        if not venue_name or len(venue_name.strip()) < 3:
            multiplier *= 0.9
        
        # Expired/voided permits
        permit_types = signals.get('permit_types', [])
        for permit_type in permit_types:
            if any(term in permit_type.lower() for term in ['expired', 'voided', 'cancelled', 'denied']):
                multiplier *= 0.7
                break
        
        # TABC inactive/denied
        tabc_status = (signals.get('tabc_status') or '').lower()
        if any(term in tabc_status for term in ['inactive', 'denied', 'rejected', 'cancelled', 'withdrawn']):
            multiplier *= 0.5
        
        return max(multiplier, 0.1)  # Minimum 10% confidence
    
    def should_create_lead(self, eta_result: ETARuleResult) -> bool:
        """Determine if a lead should be created based on gates."""
        
        if eta_result.confidence_0_1 < 0.65:
            return False
        
        # Check if ETA window overlaps next 60 days
        sixty_days_out = self.today + timedelta(days=60)
        
        return eta_result.eta_start <= sixty_days_out

    def _get_latest_milestone_date(self, dates: Dict, keywords: List[str]) -> Optional[datetime]:
        """Get the latest date from a dictionary of dates based on keywords."""
        latest_date = None
        for date_key, date_str in dates.items():
            if any(keyword in date_key.lower() for keyword in keywords):
                try:
                    # Handle timezone-aware and naive datetimes
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    if latest_date is None or dt > latest_date:
                        latest_date = dt
                except (ValueError, TypeError):
                    continue
        return latest_date


def parse_milestone_text(text: str) -> Dict[str, str]:
    """Parse milestone text and extract dates."""
    
    milestones = {}
    
    # Common date patterns
    date_patterns = [
        r'(\d{1,2}\/\d{1,2}\/\d{4})',  # MM/DD/YYYY
        r'(\d{4}-\d{2}-\d{2})',        # YYYY-MM-DD
        r'(\w+ \d{1,2}, \d{4})'        # Month DD, YYYY
    ]
    
    # Milestone keywords
    keywords = [
        'application', 'filed', 'submitted', 'approved', 'issued',
        'scheduled', 'inspection', 'review', 'permit', 'license'
    ]
    
    # Simple extraction (can be enhanced)
    lines = text.lower().split('\n')
    for line in lines:
        for keyword in keywords:
            if keyword in line:
                for pattern in date_patterns:
                    matches = re.findall(pattern, line)
                    if matches:
                        milestones[f"{keyword}_date"] = matches[0]
                        break
    
    return milestones
