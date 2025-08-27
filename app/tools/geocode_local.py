import re
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class AddressComponents:
    """Parsed address components."""
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    suite: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    normalized: Optional[str] = None


class LocalGeocoder:
    """Naive local geocoding and address normalization."""
    
    def __init__(self):
        # Texas cities mapping for normalization
        self.city_aliases = {
            'houston': ['houston', 'htown', 'space city'],
            'sugar land': ['sugar land', 'sugarland'],
            'the woodlands': ['the woodlands', 'woodlands'],
            'katy': ['katy'],
            'pearland': ['pearland'],
            'pasadena': ['pasadena'],
            'league city': ['league city'],
            'cypress': ['cypress'],
            'spring': ['spring'],
            'tomball': ['tomball'],
            'humble': ['humble'],
            'kingwood': ['kingwood']
        }
        
        # Common street type abbreviations
        self.street_types = {
            'street': ['st', 'str', 'street'],
            'avenue': ['ave', 'av', 'avenue'],
            'boulevard': ['blvd', 'boulevard', 'bv'],
            'road': ['rd', 'road'],
            'drive': ['dr', 'drive'],
            'lane': ['ln', 'lane'],
            'court': ['ct', 'court'],
            'circle': ['cir', 'circle'],
            'way': ['way'],
            'place': ['pl', 'place'],
            'parkway': ['pkwy', 'parkway']
        }
    
    def parse_address(self, address_text: str) -> AddressComponents:
        """Parse address text into components."""
        
        if not address_text:
            return AddressComponents()
        
        # Clean input
        address = address_text.strip().lower()
        
        # Extract ZIP code
        zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
        zip_code = zip_match.group(1) if zip_match else None
        if zip_code:
            address = address.replace(zip_code, '').strip()
        
        # Extract state (assume TX for this pipeline)
        state_match = re.search(r'\b(tx|texas)\b', address)
        state = 'TX' if state_match else None
        if state_match:
            address = address.replace(state_match.group(0), '').strip()
        
        # Extract suite/apartment/unit
        suite_patterns = [
            r'(suite|ste|unit|apt|apartment|#)\s*([a-z0-9\-]+)',
            r'#\s*([a-z0-9\-]+)'
        ]
        
        suite = None
        for pattern in suite_patterns:
            suite_match = re.search(pattern, address)
            if suite_match:
                suite = suite_match.group(2) if len(suite_match.groups()) > 1 else suite_match.group(1)
                address = address.replace(suite_match.group(0), '').strip()
                break
        
        # Split remaining parts
        parts = [p.strip() for p in address.split(',') if p.strip()]
        
        if len(parts) == 0:
            return AddressComponents(zip_code=zip_code, state=state, suite=suite)
        
        # First part should be street address
        street_part = parts[0]
        
        # Extract street number
        number_match = re.match(r'^(\d+[a-z]?)\s+(.+)', street_part)
        if number_match:
            street_number = number_match.group(1)
            street_name = number_match.group(2)
        else:
            street_number = None
            street_name = street_part
        
        # Normalize street name
        street_name = self._normalize_street_name(street_name)
        
        # Extract city from remaining parts
        city = None
        if len(parts) > 1:
            city_part = parts[1].strip()
            city = self._normalize_city_name(city_part)
        
        # Build normalized address
        normalized_parts = []
        if street_number:
            normalized_parts.append(street_number)
        if street_name:
            normalized_parts.append(street_name)
        
        normalized_street = ' '.join(normalized_parts) if normalized_parts else None
        
        full_normalized_parts = []
        if normalized_street:
            full_normalized_parts.append(normalized_street)
        if city:
            full_normalized_parts.append(city)
        if state:
            full_normalized_parts.append(state)
        if zip_code:
            full_normalized_parts.append(zip_code)
        
        normalized_address = ', '.join(full_normalized_parts) if full_normalized_parts else address_text
        
        return AddressComponents(
            street_number=street_number,
            street_name=street_name,
            suite=suite,
            city=city,
            state=state or 'TX',
            zip_code=zip_code,
            normalized=normalized_address
        )
    
    def _normalize_street_name(self, street_name: str) -> str:
        """Normalize street name."""
        
        if not street_name:
            return street_name
        
        # Normalize street type
        words = street_name.split()
        if words:
            last_word = words[-1].lower()
            
            for canonical_type, aliases in self.street_types.items():
                if last_word in aliases:
                    words[-1] = canonical_type.title()
                    break
        
        return ' '.join(words).title()
    
    def _normalize_city_name(self, city_name: str) -> str:
        """Normalize city name."""
        
        if not city_name:
            return city_name
        
        city_lower = city_name.lower().strip()
        
        for canonical_city, aliases in self.city_aliases.items():
            if city_lower in aliases:
                return canonical_city.title()
        
        return city_name.title()
    
    def calculate_address_similarity(self, address1: str, address2: str) -> float:
        """Calculate similarity score between two addresses."""
        
        if not address1 or not address2:
            return 0.0
        
        addr1 = self.parse_address(address1)
        addr2 = self.parse_address(address2)
        
        score = 0.0
        total_weight = 0.0
        
        # Street number (high weight)
        if addr1.street_number and addr2.street_number:
            if addr1.street_number.lower() == addr2.street_number.lower():
                score += 0.4
            total_weight += 0.4
        elif addr1.street_number or addr2.street_number:
            total_weight += 0.4
        
        # Street name (high weight)
        if addr1.street_name and addr2.street_name:
            street_sim = self._string_similarity(addr1.street_name.lower(), addr2.street_name.lower())
            score += 0.3 * street_sim
            total_weight += 0.3
        elif addr1.street_name or addr2.street_name:
            total_weight += 0.3
        
        # City (medium weight)
        if addr1.city and addr2.city:
            if addr1.city.lower() == addr2.city.lower():
                score += 0.2
            total_weight += 0.2
        elif addr1.city or addr2.city:
            total_weight += 0.2
        
        # ZIP code (medium weight)
        if addr1.zip_code and addr2.zip_code:
            if addr1.zip_code == addr2.zip_code:
                score += 0.1
            total_weight += 0.1
        elif addr1.zip_code or addr2.zip_code:
            total_weight += 0.1
        
        return score / total_weight if total_weight > 0 else 0.0
    
    def _string_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity using simple approach."""
        
        if not s1 or not s2:
            return 0.0
        
        if s1 == s2:
            return 1.0
        
        # Simple token-based similarity
        tokens1 = set(s1.split())
        tokens2 = set(s2.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        return intersection / union if union > 0 else 0.0
    
    def is_harris_county_address(self, address: str) -> bool:
        """Check if address is likely in Harris County."""
        
        addr = self.parse_address(address)
        
        # Known Harris County cities
        harris_cities = [
            'houston', 'sugar land', 'the woodlands', 'katy', 'pearland',
            'pasadena', 'league city', 'cypress', 'spring', 'tomball',
            'humble', 'kingwood', 'missouri city', 'stafford', 'bellaire',
            'west university place', 'southside place'
        ]
        
        if addr.city:
            return addr.city.lower() in harris_cities
        
        # Check ZIP codes (sample of Harris County ZIPs)
        harris_zip_prefixes = ['770', '771', '772', '773', '774', '775']
        
        if addr.zip_code:
            return any(addr.zip_code.startswith(prefix) for prefix in harris_zip_prefixes)
        
        return True  # Default to true if we can't determine


def normalize_business_name(business_name: str) -> str:
    """Normalize business name for matching."""
    
    if not business_name:
        return business_name
    
    # Convert to lowercase
    name = business_name.lower().strip()
    
    # Remove common business suffixes
    suffixes = [
        'llc', 'inc', 'corp', 'ltd', 'co', 'company', 'corporation',
        'incorporated', 'limited', 'restaurant', 'cafe', 'bar', 'grill'
    ]
    
    for suffix in suffixes:
        # Remove suffix at end of name
        pattern = rf'\b{suffix}\.?\s*$'
        name = re.sub(pattern, '', name).strip()
    
    # Remove common punctuation
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


def calculate_business_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between business names."""
    
    if not name1 or not name2:
        return 0.0
    
    norm_name1 = normalize_business_name(name1)
    norm_name2 = normalize_business_name(name2)
    
    if norm_name1 == norm_name2:
        return 1.0
    
    # Token-based similarity
    tokens1 = set(norm_name1.split())
    tokens2 = set(norm_name2.split())
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = len(tokens1 & tokens2)
    union = len(tokens1 | tokens2)
    
    jaccard_sim = intersection / union if union > 0 else 0.0
    
    # Boost score if one name contains the other
    if norm_name1 in norm_name2 or norm_name2 in norm_name1:
        jaccard_sim = min(1.0, jaccard_sim + 0.2)
    
    return jaccard_sim


# Global geocoder instance
geocoder = LocalGeocoder()
