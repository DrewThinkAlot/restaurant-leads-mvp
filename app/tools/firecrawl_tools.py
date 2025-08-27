"""Firecrawl-based contact discovery tools for website scraping."""

import json
import logging
import os
from typing import Dict, List, Any, Optional
import requests
from crewai.tools import BaseTool

logger = logging.getLogger(__name__)


class FirecrawlContactTool(BaseTool):
    """Tool for scraping website contact information using Firecrawl API."""

    name: str = "firecrawl_contact"
    description: str = "Scrape website for contact information using Firecrawl API"

    def __init__(self):
        super().__init__()
        self._api_key = os.getenv("FIRECRAWL_API_KEY")
        self._base_url = "https://api.firecrawl.dev/v2/scrape"

    def _run(self, domain: str) -> str:
        """Scrape website for contact information using Firecrawl."""
        
        if not self._api_key:
            return json.dumps({
                "success": False,
                "error": "Firecrawl API key not configured",
                "contacts": []
            })

        try:
            # Ensure domain has protocol
            if not domain.startswith(('http://', 'https://')):
                url = f"https://{domain}"
            else:
                url = domain

            # Prepare Firecrawl request
            payload = {
                "url": url,
                "formats": [{"type": "json", "prompt": "Extract all contact information including email addresses, phone numbers, contact form URLs, and any names of key personnel like owners, managers, or decision makers. Focus on business contact details."}],
                "onlyMainContent": True,
                "timeout": 30000
            }

            headers = {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                self._base_url,
                json=payload,
                headers=headers,
                timeout=60
            )

            if response.status_code != 200:
                error_message = response.text
                try:
                    error_message = response.json().get("error", {}).get("message", error_message)
                except ValueError:
                    pass
                return json.dumps({
                    "success": False,
                    "error": f"Firecrawl API error: {response.status_code} - {error_message}",
                    "contacts": []
                })

            result = response.json()
            
            if not result.get("success"):
                return json.dumps({
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "contacts": []
                })

            # Parse extracted data
            extracted_data = result.get("data", {}).get("extract", {})
            contacts = self._parse_firecrawl_contacts(extracted_data, url)

            return json.dumps({
                "success": len(contacts) > 0,
                "contacts": contacts,
                "source_url": url,
                "raw_data": extracted_data
            })

        except Exception as e:
            logger.error(f"Firecrawl scraping failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "contacts": []
            })

    def _parse_firecrawl_contacts(self, extracted_data: Dict[str, Any], source_url: str) -> List[Dict[str, Any]]:
        """Parse Firecrawl extracted data into contact format."""
        
        contacts = []
        
        # Handle different response formats
        if isinstance(extracted_data, dict):
            # Direct contact extraction
            emails = extracted_data.get("emails", [])
            phones = extracted_data.get("phone_numbers", [])
            names = extracted_data.get("names", [])
            
            # Handle various key names
            if not emails and "email_addresses" in extracted_data:
                emails = extracted_data["email_addresses"]
            if not phones and "phone_numbers" in extracted_data:
                phones = extracted_data["phone_numbers"]
            if not emails and "email" in extracted_data:
                emails = [extracted_data["email"]]
            if not phones and "phone" in extracted_data:
                phones = [extracted_data["phone"]]
            
            # Create contacts from emails
            for email in emails:
                if isinstance(email, dict):
                    email_address = email.get("email", "")
                    name = email.get("name", "Unknown")
                else:
                    email_address = str(email)
                    name = "Unknown"
                
                if email_address and "@" in email_address:
                    contacts.append({
                        "full_name": name,
                        "role": "unknown",
                        "email": email_address,
                        "phone": None,
                        "source": "firecrawl",
                        "source_url": source_url,
                        "provenance_text": f"Contact found via Firecrawl on {source_url}",
                        "confidence_0_1": 0.7,  # Higher confidence for AI extraction
                        "notes": "Extracted via Firecrawl AI scraping"
                    })
            
            # Create contacts from phone numbers
            for phone in phones:
                if isinstance(phone, dict):
                    phone_number = phone.get("phone", "")
                    name = phone.get("name", "Unknown")
                else:
                    phone_number = str(phone)
                    name = "Unknown"
                
                if phone_number and len(phone_number) >= 10:
                    contacts.append({
                        "full_name": name,
                        "role": "unknown",
                        "email": None,
                        "phone": phone_number,
                        "source": "firecrawl",
                        "source_url": source_url,
                        "provenance_text": f"Contact found via Firecrawl on {source_url}",
                        "confidence_0_1": 0.7,
                        "notes": "Extracted via Firecrawl AI scraping"
                    })
            
            # Handle names if provided separately
            for name_info in names:
                if isinstance(name_info, dict):
                    name = name_info.get("name", "Unknown")
                    role = name_info.get("role", "unknown")
                    
                    contacts.append({
                        "full_name": name,
                        "role": role,
                        "email": None,
                        "phone": None,
                        "source": "firecrawl",
                        "source_url": source_url,
                        "provenance_text": f"Key personnel found via Firecrawl on {source_url}",
                        "confidence_0_1": 0.6,
                        "notes": "Personnel extracted via Firecrawl AI"
                    })
        
        # Handle string responses
        elif isinstance(extracted_data, str):
            # Parse string for contact patterns
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            phone_pattern = r'\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b'
            
            emails = re.findall(email_pattern, extracted_data)
            phones = re.findall(phone_pattern, extracted_data)
            
            for email in emails:
                contacts.append({
                    "full_name": "Unknown",
                    "role": "unknown",
                    "email": email,
                    "phone": None,
                    "source": "firecrawl",
                    "source_url": source_url,
                    "provenance_text": f"Email found via Firecrawl on {source_url}",
                    "confidence_0_1": 0.6,
                    "notes": "Email extracted from page content"
                })
            
            for phone in phones:
                phone_clean = re.sub(r'\D', '', phone)
                if len(phone_clean) >= 10:
                    contacts.append({
                        "full_name": "Unknown",
                        "role": "unknown",
                        "email": None,
                        "phone": phone_clean,
                        "source": "firecrawl",
                        "source_url": source_url,
                        "provenance_text": f"Phone found via Firecrawl on {source_url}",
                        "confidence_0_1": 0.6,
                        "notes": "Phone extracted from page content"
                    })

        return contacts


class FirecrawlBatchTool(BaseTool):
    """Tool for batch scraping multiple URLs using Firecrawl."""

    name: str = "firecrawl_batch"
    description: str = "Batch scrape multiple websites for contact information"

    def __init__(self):
        super().__init__()
        self._api_key = os.getenv("FIRECRAWL_API_KEY")
        self._base_url = "https://api.firecrawl.dev/v0/batch/scrape"

    def _run(self, urls: List[str]) -> str:
        """Batch scrape multiple URLs."""
        
        if not self._api_key:
            return json.dumps({
                "success": False,
                "error": "Firecrawl API key not configured",
                "results": []
            })

        try:
            payload = {
                "urls": urls,
                "formats": ["json"],
                "onlyMainContent": True,
                "extract": {
                    "prompt": "Extract all contact information including email addresses, phone numbers, and key personnel names. Return as structured data."
                }
            }

            headers = {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                self._base_url,
                json=payload,
                headers=headers,
                timeout=120
            )

            if response.status_code != 200:
                return json.dumps({
                    "success": False,
                    "error": f"Firecrawl batch API error: {response.status_code}",
                    "results": []
                })

            result = response.json()
            return json.dumps({
                "success": True,
                "results": result.get("data", [])
            })

        except Exception as e:
            logger.error(f"Firecrawl batch scraping failed: {e}")
            return json.dumps({
                "success": False,
                "error": str(e),
                "results": []
            })
