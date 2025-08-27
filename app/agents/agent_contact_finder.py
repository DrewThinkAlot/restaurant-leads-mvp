"""ContactFinder Agent for discovering decision-maker contacts."""

import json
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

from crewai import Agent, Task, Crew
from ..tools.contact_tools import (
    TABCLookupTool, ComptrollerLookupTool, PermitLookupTool,
    EmailPatternTool, ContactabilityEvaluator
)
from ..tools.firecrawl_tools import FirecrawlContactTool
from ..db import db_manager
from ..models import Contact
from ..settings import settings

logger = logging.getLogger(__name__)


class ContactFinderAgent:
    """Agent for finding decision-maker contacts with compliance controls."""
    
    def __init__(self):
        self.tabc_tool = TABCLookupTool()
        self.comptroller_tool = ComptrollerLookupTool()
        self.permit_tool = PermitLookupTool()
        self.web_scrape_tool = FirecrawlContactTool()
        self.email_pattern_tool = EmailPatternTool()
        self.contactability = ContactabilityEvaluator()
        
        # Initialize CrewAI agent
        self.agent = Agent(
            role="Contact Discovery Specialist",
            goal="Find verified contact information for restaurant decision-makers with full compliance",
            backstory="""You are an expert at finding legitimate business contacts for restaurant 
            owners and decision-makers. You prioritize official sources, respect privacy laws, 
            and ensure all contact methods comply with CAN-SPAM and TCPA regulations.""",
            tools=[
                self.tabc_tool,
                self.comptroller_tool, 
                self.permit_tool,
                self.web_scrape_tool,
                self.email_pattern_tool
            ],
            verbose=True,
            allow_delegation=False
        )
    
    def find_contacts(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find contacts for multiple candidates."""
        
        logger.info(f"Finding contacts for {len(candidates)} candidates")
        results = []
        
        for candidate in candidates:
            try:
                contacts = self._find_candidate_contacts(candidate)
                if contacts:
                    # Store in database
                    self._store_contacts_in_db(candidate["candidate_id"], contacts)
                    
                    # Add to candidate data
                    candidate_with_contacts = candidate.copy()
                    candidate_with_contacts["contacts"] = contacts
                    results.append(candidate_with_contacts)
                else:
                    results.append(candidate)
                    
            except Exception as e:
                logger.error(f"Contact discovery failed for candidate {candidate.get('candidate_id')}: {e}")
                results.append(candidate)
        
        logger.info(f"Found contacts for {len([r for r in results if 'contacts' in r])} candidates")
        return results
    
    def _find_candidate_contacts(self, candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find contacts for a single candidate using prioritized sources."""
        
        venue_name = candidate.get("venue_name", "")
        legal_name = candidate.get("legal_name", "")
        address = candidate.get("address", "")
        
        logger.info(f"Finding contacts for: {venue_name}")
        
        all_contacts = []
        
        # Step 1: TABC lookup (highest priority official source)
        tabc_contacts = self._lookup_tabc_contacts(venue_name, address)
        all_contacts.extend(tabc_contacts)
        
        # Step 2: TX Comptroller lookup (second priority official source)
        if legal_name:
            comptroller_contacts = self._lookup_comptroller_contacts(legal_name)
            all_contacts.extend(comptroller_contacts)
        
        # Step 3: Permit lookup
        permit_contacts = self._lookup_permit_contacts(address)
        all_contacts.extend(permit_contacts)
        
        # Step 4: Website scraping (if we have a domain)
        website_contacts = self._scrape_website_contacts(candidate)
        all_contacts.extend(website_contacts)
        
        # Step 5: Pattern generation (lowest confidence)
        pattern_contacts = self._generate_pattern_contacts(candidate, all_contacts)
        all_contacts.extend(pattern_contacts)
        
        # Apply confidence scoring and deduplication
        final_contacts = self._score_and_deduplicate_contacts(all_contacts)
        
        # Limit to max 2 contacts per venue
        return final_contacts[:2]
    
    def _lookup_tabc_contacts(self, venue_name: str, address: str) -> List[Dict[str, Any]]:
        """Lookup contacts from TABC records."""
        
        try:
            result_json = self.tabc_tool._run(venue_name, address)
            result = json.loads(result_json)
            
            contacts = []
            if result.get("success") and result.get("licensee"):
                contact = {
                    "full_name": result["licensee"],
                    "role": "owner",
                    "email": None,
                    "phone": None,
                    "source": "tabc",
                    "source_url": result["source_url"],
                    "provenance_text": f"TABC licensee for {venue_name}",
                    "confidence_0_1": 0.6,  # Base confidence for official source
                    "notes": f"Mailing address: {result.get('mailing_address', 'N/A')}"
                }
                contacts.append(contact)
                
            return contacts
            
        except Exception as e:
            logger.error(f"TABC lookup failed: {e}")
            return []
    
    def _lookup_comptroller_contacts(self, legal_name: str) -> List[Dict[str, Any]]:
        """Lookup contacts from TX Comptroller records."""
        
        try:
            result_json = self.comptroller_tool._run(legal_name)
            result = json.loads(result_json)
            
            contacts = []
            if result.get("success"):
                # Add registered agent
                if result.get("registered_agent"):
                    contact = {
                        "full_name": result["registered_agent"],
                        "role": "unknown",
                        "email": None,
                        "phone": None,
                        "source": "comptroller",
                        "source_url": result["source_url"],
                        "provenance_text": f"Registered agent for {legal_name}",
                        "confidence_0_1": 0.5,
                        "notes": "Listed as registered agent"
                    }
                    contacts.append(contact)
                
                # Add officers/managers
                for officer in result.get("officers", []):
                    contact = {
                        "full_name": officer,
                        "role": "managing_member",
                        "email": None,
                        "phone": None,
                        "source": "comptroller", 
                        "source_url": result["source_url"],
                        "provenance_text": f"Officer/Manager for {legal_name}",
                        "confidence_0_1": 0.5,
                        "notes": "Listed as officer/manager"
                    }
                    contacts.append(contact)
            
            return contacts
            
        except Exception as e:
            logger.error(f"Comptroller lookup failed: {e}")
            return []
    
    def _lookup_permit_contacts(self, address: str) -> List[Dict[str, Any]]:
        """Lookup contacts from permit records."""
        
        try:
            result_json = self.permit_tool._run(address=address)
            result = json.loads(result_json)
            
            contacts = []
            if result.get("success"):
                # Add applicant/owner
                for name_field in ["applicant_name", "owner_name"]:
                    name = result.get(name_field)
                    if name:
                        contact = {
                            "full_name": name,
                            "role": "owner" if "owner" in name_field else "unknown",
                            "email": None,
                            "phone": result.get("contact_phone"),
                            "source": "permit",
                            "source_url": result["source_url"],
                            "provenance_text": f"Permit {name_field.replace('_', ' ')} for {address}",
                            "confidence_0_1": 0.4,
                            "notes": f"From permit records"
                        }
                        contacts.append(contact)
            
            return contacts
            
        except Exception as e:
            logger.error(f"Permit lookup failed: {e}")
            return []
    
    def _scrape_website_contacts(self, candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape website for contact information."""
        
        # Try to derive domain from venue name or existing data
        venue_name = candidate.get("venue_name", "")
        domain = self._guess_domain(venue_name)
        
        if not domain:
            return []
        
        try:
            result_json = self.web_scrape_tool._run(domain)
            result = json.loads(result_json)
            
            contacts = []
            if result.get("success"):
                # Process found emails
                for email in result.get("emails", []):
                    contact = {
                        "full_name": "Unknown",
                        "role": "unknown",
                        "email": email,
                        "phone": None,
                        "source": "site",
                        "source_url": result["source_urls"][0] if result["source_urls"] else domain,
                        "provenance_text": f"Email found on company website",
                        "confidence_0_1": 0.4,
                        "notes": f"Found on website contact pages"
                    }
                    contacts.append(contact)
                
                # Process found phones
                for phone in result.get("phones", []):
                    contact = {
                        "full_name": "Unknown",
                        "role": "unknown", 
                        "email": None,
                        "phone": phone,
                        "source": "site",
                        "source_url": result["source_urls"][0] if result["source_urls"] else domain,
                        "provenance_text": f"Phone found on company website",
                        "confidence_0_1": 0.4,
                        "notes": f"Found on website contact pages"
                    }
                    contacts.append(contact)
            
            return contacts
            
        except Exception as e:
            logger.error(f"Website scraping failed: {e}")
            return []
    
    def _generate_pattern_contacts(self, candidate: Dict[str, Any], existing_contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate pattern-based email contacts."""
        
        venue_name = candidate.get("venue_name", "")
        domain = self._guess_domain(venue_name)
        
        if not domain:
            return []
        
        contacts = []
        
        # Get names from existing contacts
        names = [c["full_name"] for c in existing_contacts if c["full_name"] != "Unknown"]
        
        for name in names:
            try:
                result_json = self.email_pattern_tool._run(domain, name)
                result = json.loads(result_json)
                
                if result.get("success") and result.get("mx_valid"):
                    for email in result.get("emails", []):
                        contact = {
                            "full_name": name,
                            "role": "unknown",
                            "email": email,
                            "phone": None,
                            "source": "pattern",
                            "source_url": f"https://{domain}",
                            "provenance_text": f"Pattern-generated email for {name}",
                            "confidence_0_1": 0.2,  # Low confidence for patterns
                            "notes": f"Generated email pattern - requires validation"
                        }
                        contacts.append(contact)
                        
            except Exception as e:
                logger.error(f"Pattern generation failed: {e}")
                continue
        
        return contacts
    
    def _guess_domain(self, venue_name: str) -> Optional[str]:
        """Guess domain from venue name."""
        if not venue_name:
            return None
        
        # Simple domain guessing - in production, use better heuristics
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', venue_name.lower())
        clean_name = clean_name.replace(' ', '')
        
        # Common domain patterns
        possible_domains = [
            f"{clean_name}.com",
            f"{clean_name}restaurant.com",
            f"{clean_name}tx.com"
        ]
        
        # Return first one for now - in production, validate these
        return possible_domains[0] if clean_name else None
    
    def _score_and_deduplicate_contacts(self, contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply confidence scoring and remove duplicates."""
        
        if not contacts:
            return []
        
        # Apply confidence boosts for cross-source validation
        name_sources = {}
        for contact in contacts:
            name = contact["full_name"]
            if name not in name_sources:
                name_sources[name] = []
            name_sources[name].append(contact["source"])
        
        # Boost confidence for names appearing in multiple sources
        for contact in contacts:
            name = contact["full_name"]
            sources = name_sources[name]
            unique_sources = set(sources)
            
            if len(unique_sources) >= 2:
                contact["confidence_0_1"] += 0.4  # Cross-source validation boost
            
            # Email domain match boost
            if contact.get("email") and contact["source"] in ["site"]:
                contact["confidence_0_1"] += 0.2
            
            # Pattern penalty
            if contact["source"] == "pattern":
                contact["confidence_0_1"] -= 0.3
            
            # Cap confidence
            contact["confidence_0_1"] = min(contact["confidence_0_1"], 0.9)
            
            # Apply contactability evaluation
            contactability = {"ok_to_email": True, "ok_to_call": False, "ok_to_sms": False}
            
            if contact.get("email"):
                email_eval = self.contactability.evaluate_email(contact["email"], contact["source"])
                contactability.update(email_eval)
            
            if contact.get("phone"):
                phone_eval = self.contactability.evaluate_phone(contact["phone"], contact["source"])
                contactability.update(phone_eval)
            
            contact["contactability"] = contactability
        
        # Remove duplicates and sort by confidence
        seen = set()
        unique_contacts = []
        
        # Sort by confidence descending
        contacts.sort(key=lambda x: x["confidence_0_1"], reverse=True)
        
        for contact in contacts:
            # Create key for deduplication
            key = (contact["full_name"], contact.get("email"), contact.get("phone"))
            
            if key not in seen:
                seen.add(key)
                unique_contacts.append(contact)
        
        return unique_contacts
    
    def _store_contacts_in_db(self, candidate_id: str, contacts: List[Dict[str, Any]]):
        """Store contacts in database."""
        
        try:
            with db_manager.get_session() as session:
                for contact_data in contacts:
                    contact = Contact(
                        id=uuid.uuid4(),
                        candidate_id=candidate_id,
                        full_name=contact_data["full_name"],
                        role=contact_data["role"],
                        email=contact_data.get("email"),
                        phone=contact_data.get("phone"),
                        source=contact_data["source"],
                        source_url=contact_data["source_url"],
                        provenance_text=contact_data["provenance_text"],
                        confidence_0_1=contact_data["confidence_0_1"],
                        contactability=contact_data["contactability"],
                        notes=contact_data.get("notes")
                    )
                    session.add(contact)
                
                session.commit()
                logger.info(f"Stored {len(contacts)} contacts for candidate {candidate_id}")
                
        except Exception as e:
            logger.error(f"Failed to store contacts: {e}")
            raise
    
    def get_contacts_for_candidate(self, candidate_id: str) -> List[Dict[str, Any]]:
        """Get stored contacts for a candidate."""
        
        try:
            with db_manager.get_session() as session:
                contacts = session.query(Contact).filter(
                    Contact.candidate_id == candidate_id
                ).all()
                
                return [{
                    "id": str(contact.id),
                    "candidate_id": str(contact.candidate_id),
                    "full_name": contact.full_name,
                    "role": contact.role,
                    "email": contact.email,
                    "phone": contact.phone,
                    "source": contact.source,
                    "source_url": contact.source_url,
                    "provenance_text": contact.provenance_text,
                    "confidence_0_1": contact.confidence_0_1,
                    "contactability": contact.contactability,
                    "notes": contact.notes,
                    "created_at": contact.created_at.isoformat()
                } for contact in contacts]
                
        except Exception as e:
            logger.error(f"Failed to retrieve contacts: {e}")
            return []
