"""Tools for contact discovery and validation."""

import re
import json
import logging
import requests
import time
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin, urlparse, quote
import dns.resolver
from crewai.tools import BaseTool
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import os

logger = logging.getLogger(__name__)


class TABCLookupTool(BaseTool):
    """Tool for looking up TABC license information."""

    name: str = "tabc_lookup"
    description: str = "Lookup TABC license holder and contact information"

    def _run(self, venue_name: str, address: str) -> str:
        """Look up TABC license information using web scraping."""
        try:
            # Use TABC Public Inquiry system - web scraping approach
            base_url = "https://www.tabc.texas.gov/public-information/tabc-public-inquiry/"

            # Create a headless browser instance with optimized performance options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # Performance optimizations (Phase 1)
            chrome_options.add_argument("--disable-images")  # Don't load images for speed
            chrome_options.add_argument("--disable-extensions")  # Disable extensions
            chrome_options.add_argument("--disable-plugins")  # Disable plugins
            chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
            chrome_options.add_argument("--disable-web-security")  # Disable web security (for testing)
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")  # Disable certain features
            chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources to load

            # Additional performance options
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--hide-scrollbars")
            chrome_options.add_argument("--metrics-recording-only")
            chrome_options.add_argument("--mute-audio")
            chrome_options.add_argument("--no-crash-upload")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")

            driver = None
            try:
                driver = webdriver.Chrome(options=chrome_options)

                # Navigate to TABC Public Inquiry
                driver.get(base_url)

                # Enhanced waiting strategy with multiple checks
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )

                # Additional wait for dynamic content with retry logic
                self._wait_for_page_ready(driver, base_url)

                # Analyze the page structure first
                page_analysis = self._analyze_tabc_page(driver)
                logger.info(f"TABC page analysis: {page_analysis}")

                search_results = []

                # Try multiple search strategies based on page analysis
                if page_analysis["has_search_form"]:
                    search_results = self._try_tabc_search_strategies_with_retry(driver, venue_name, address, page_analysis)

                # If no results from search, try to extract any visible license data
                if not search_results:
                    search_results = self._extract_visible_tabc_data(driver.page_source, venue_name)

                # Process results
                contacts = []
                for result in search_results:
                    if result.get("licensee"):
                        contact = {
                            "full_name": result["licensee"],
                            "role": "owner",
                            "email": None,
                            "phone": None,
                            "source": "tabc",
                            "source_url": result.get("license_url", base_url),
                            "provenance_text": f"TABC licensee for {venue_name}",
                            "confidence_0_1": 0.6,
                            "notes": f"License: {result.get('license_number', 'N/A')}, Status: {result.get('status', 'N/A')}"
                        }
                        contacts.append(contact)

                if contacts:
                    return json.dumps({
                        "success": True,
                        "contacts": contacts,
                        "search_method": "web_scraping",
                        "search_term": venue_name,
                        "page_analysis": page_analysis
                    })
                else:
                    return json.dumps({
                        "success": False,
                        "contacts": [],
                        "message": f"No TABC license found for {venue_name}",
                        "page_analysis": page_analysis
                    })

            except Exception as e:
                logger.error(f"TABC web scraping failed: {e}")
                return json.dumps({
                    "success": False,
                    "contacts": [],
                    "error": str(e),
                    "message": "Web scraping failed, TABC may require manual lookup"
                })
            finally:
                if driver:
                    driver.quit()

        except Exception as e:
            logger.error(f"TABC lookup failed: {e}")
            return json.dumps({
                "success": False,
                "contacts": [],
                "error": str(e)
            })

    def _analyze_tabc_page(self, driver) -> Dict[str, Any]:
        """Analyze TABC page structure to determine best search approach."""
        analysis = {
            "has_search_form": False,
            "search_input_selectors": [],
            "search_button_selectors": [],
            "form_method": None,
            "page_title": driver.title,
            "url": driver.current_url
        }

        try:
            # Check for various search form patterns
            search_selectors = [
                "input[type='search']",
                "input[name*='search']",
                "input[placeholder*='search']",
                "input[id*='search']",
                "#search",
                ".search-input",
                "input[type='text']",
                "input[name*='query']",
                "input[name*='term']"
            ]

            for selector in search_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        analysis["has_search_form"] = True
                        analysis["search_input_selectors"].append(selector)
                except:
                    continue

            # Check for search buttons
            button_selectors = [
                "button[type='submit']",
                "input[type='submit']",
                ".search-submit",
                "button[class*='search']",
                "#search-button",
                ".btn-search",
                "button:contains('Search')",
                "input[value*='Search']"
            ]

            for selector in button_selectors:
                try:
                    if ":contains" in selector:
                        # Handle CSS pseudo-selectors
                        elements = driver.find_elements(By.XPATH, f"//button[contains(text(), 'Search')]")
                    else:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        analysis["search_button_selectors"].append(selector)
                except:
                    continue

            # Check form method
            try:
                forms = driver.find_elements(By.TAG_NAME, "form")
                for form in forms:
                    method = form.get_attribute("method")
                    if method:
                        analysis["form_method"] = method.upper()
                        break
            except:
                pass

        except Exception as e:
            logger.warning(f"Page analysis failed: {e}")

        return analysis

    def _try_tabc_search_strategies(self, driver, venue_name: str, address: str, page_analysis: Dict) -> List[Dict]:
        """Try multiple search strategies based on page analysis."""
        all_results = []

        # Strategy 1: Use identified search inputs
        for input_selector in page_analysis["search_input_selectors"][:2]:  # Try first 2 selectors
            try:
                results = self._search_with_selector(driver, input_selector, venue_name, page_analysis)
                if results:
                    all_results.extend(results)
                    break  # Stop at first successful strategy
            except Exception as e:
                logger.warning(f"Search strategy with {input_selector} failed: {e}")
                continue

        # Strategy 2: If address available, try combined search
        if not all_results and address:
            combined_term = f"{venue_name} {address.split()[0]}"  # First part of address
            for input_selector in page_analysis["search_input_selectors"][:1]:  # Try first selector only
                try:
                    results = self._search_with_selector(driver, input_selector, combined_term, page_analysis)
                    if results:
                        all_results.extend(results)
                        break
                except Exception as e:
                    logger.warning(f"Combined search strategy failed: {e}")

        return all_results

    def _search_with_selector(self, driver, input_selector: str, search_term: str, page_analysis: Dict) -> List[Dict]:
        """Search using a specific input selector."""
        results = []

        try:
            # Find and interact with search input
            search_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, input_selector))
            )

            # Clear and enter search term
            search_input.clear()
            search_input.send_keys(search_term)

            # Find and click search button
            search_button = None
            for button_selector in page_analysis["search_button_selectors"]:
                try:
                    if ":contains" in button_selector:
                        search_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Search')]"))
                        )
                    else:
                        search_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
                        )
                    break
                except:
                    continue

            if not search_button:
                # Try pressing Enter in the search input
                search_input.send_keys("\n")
            else:
                search_button.click()

            # Wait for results to load
            WebDriverWait(driver, 15).until(
                lambda d: self._results_loaded(d, search_term)
            )

            # Extract results
            results_html = driver.page_source
            results = self._parse_tabc_results(results_html, search_term)

        except Exception as e:
            logger.warning(f"Search with selector {input_selector} failed: {e}")

        return results

    def _wait_for_page_ready(self, driver, url: str, max_retries: int = 3):
        """Enhanced waiting strategy with retry logic."""
        for attempt in range(max_retries):
            try:
                # Wait for document ready
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )

                # Wait for key page elements to be present
                WebDriverWait(driver, 5).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "body")) > 0
                )

                # Additional wait for dynamic content
                time.sleep(1)

                # Verify page loaded correctly
                current_url = driver.current_url
                if "error" not in current_url.lower() and "404" not in current_url.lower():
                    return True

            except Exception as e:
                logger.warning(f"Page ready check failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # Brief pause before retry
                continue

        return False

    def _try_tabc_search_strategies_with_retry(self, driver, venue_name: str, address: str, page_analysis: Dict) -> List[Dict]:
        """Try multiple search strategies with retry logic."""
        all_results = []

        # Strategy 1: Use identified search inputs
        for input_selector in page_analysis["search_input_selectors"][:2]:  # Try first 2 selectors
            try:
                results = self._search_with_retry(driver, input_selector, venue_name, page_analysis)
                if results:
                    all_results.extend(results)
                    break  # Stop at first successful strategy
            except Exception as e:
                logger.warning(f"Search strategy with {input_selector} failed: {e}")
                continue

        # Strategy 2: If address available, try combined search
        if not all_results and address:
            combined_term = f"{venue_name} {address.split()[0]}"  # First part of address
            for input_selector in page_analysis["search_input_selectors"][:1]:  # Try first selector only
                try:
                    results = self._search_with_retry(driver, input_selector, combined_term, page_analysis)
                    if results:
                        all_results.extend(results)
                        break
                except Exception as e:
                    logger.warning(f"Combined search strategy failed: {e}")

        return all_results

    def _search_with_retry(self, driver, input_selector: str, search_term: str, page_analysis: Dict, max_retries: int = 3) -> List[Dict]:
        """Search using a specific input selector with retry logic."""
        for attempt in range(max_retries):
            try:
                results = self._search_single_attempt(driver, input_selector, search_term, page_analysis)
                if results:
                    return results
            except Exception as e:
                logger.warning(f"Search attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                continue

        return []

    def _search_single_attempt(self, driver, input_selector: str, search_term: str, page_analysis: Dict) -> List[Dict]:
        """Single search attempt with enhanced element interaction."""
        results = []

        try:
            # Find and interact with search input using enhanced method
            search_input = self._find_and_wait_for_clickable_element(driver, input_selector)

            # Clear and enter search term with retry
            self._safe_clear_and_send_keys(search_input, search_term)

            # Find and click search button using enhanced method
            search_button = None
            for button_selector in page_analysis["search_button_selectors"]:
                try:
                    search_button = self._find_and_wait_for_clickable_element(driver, button_selector)
                    break
                except:
                    continue

            if not search_button:
                # Try pressing Enter in the search input
                search_input.send_keys("\n")
            else:
                self._safe_click_element(search_button)

            # Wait for results to load with enhanced detection
            WebDriverWait(driver, 20).until(
                lambda d: self._results_loaded_enhanced(d, search_term)
            )

            # Extract results
            results_html = driver.page_source
            results = self._parse_tabc_results(results_html, search_term)

        except Exception as e:
            logger.warning(f"Single search attempt failed: {e}")
            raise

        return results

    def _find_and_wait_for_clickable_element(self, driver, selector: str, timeout: int = 10):
        """Enhanced element finding with multiple strategies."""
        # Strategy 1: CSS Selector
        try:
            return WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
        except:
            pass

        # Strategy 2: XPath (convert simple CSS to XPath)
        try:
            xpath = self._css_to_xpath(selector)
            return WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
        except:
            pass

        # Strategy 3: By name attribute
        try:
            name = selector.replace('[name*=', '').replace(']', '').replace('"', '').replace("'", '')
            return WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.NAME, name))
            )
        except:
            pass

        # Strategy 4: By ID
        try:
            element_id = selector.replace('#', '')
            return WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, element_id))
            )
        except:
            pass

        raise NoSuchElementException(f"Could not find clickable element: {selector}")

    def _css_to_xpath(self, css_selector: str) -> str:
        """Convert simple CSS selectors to XPath."""
        if css_selector.startswith('#'):
            return f"//*[@id='{css_selector[1:]}']"
        elif css_selector.startswith('.'):
            return f"//*[contains(@class, '{css_selector[1:]}')]"
        elif '[' in css_selector and ']' in css_selector:
            return f"//{css_selector.split('[')[0]}[{css_selector.split('[')[1].replace(']', '')}]"
        else:
            return f"//{css_selector}"

    def _safe_clear_and_send_keys(self, element, text: str):
        """Safely clear and send keys to an element."""
        try:
            element.clear()
            time.sleep(0.1)  # Brief pause
            element.send_keys(text)
            time.sleep(0.1)  # Brief pause
        except Exception as e:
            logger.warning(f"Safe clear/send keys failed: {e}")
            raise

    def _safe_click_element(self, element):
        """Safely click an element with retry."""
        for attempt in range(3):
            try:
                # Scroll element into view
                element.location_once_scrolled_into_view
                time.sleep(0.2)

                # Try to click
                element.click()
                return
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"Click attempt {attempt + 1} failed: {e}")
                    time.sleep(0.5)
                else:
                    raise

    def _results_loaded_enhanced(self, driver, search_term: str) -> bool:
        """Enhanced result detection with multiple indicators."""
        try:
            page_text = driver.page_source.lower()

            # Check for common result indicators
            result_indicators = [
                ".results", ".search-results", "table", ".license-info",
                ".search-result", "#results", ".data-table",
                "tbody tr", ".license-data",
                "license", "permit", search_term.lower(),
                "no results found", "search completed"
            ]

            # Check for URL changes (indicating navigation)
            current_url = driver.current_url.lower()
            if "search" in current_url or "results" in current_url:
                return True

            # Check for content changes
            for indicator in result_indicators:
                if indicator in page_text:
                    return True

            # Check for loading indicators disappearing
            try:
                loading_elements = driver.find_elements(By.CSS_SELECTOR, ".loading, .spinner, .progress")
                if not loading_elements:
                    return True
            except:
                pass

            return False
        except Exception as e:
            logger.warning(f"Enhanced result detection failed: {e}")
            return False

    def _results_loaded(self, driver, search_term: str) -> bool:
        """Check if search results have loaded."""
        try:
            # Check for common result indicators
            result_indicators = [
                ".results", ".search-results", "table", ".license-info",
                ".search-result", "#results", ".data-table",
                "tbody tr", ".license-data"
            ]

            for indicator in result_indicators:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, indicator)
                    if elements:
                        return True
                except:
                    continue

            # Check if page content changed significantly
            page_text = driver.page_source.lower()
            if any(keyword in page_text for keyword in ["license", "permit", search_term.lower()]):
                return True

            return False
        except:
            return False

    def _parse_tabc_results(self, html_content: str, search_term: str) -> List[Dict]:
        """Parse TABC search results HTML with improved patterns."""
        results = []

        # Enhanced patterns for TABC data extraction
        patterns = {
            "license_number": [
                r'([A-Z]{2}\d{6})',  # Standard TABC format
                r'License\s*#?\s*([A-Z]{2}\d{6})',
                r'Permit\s*#?\s*([A-Z]{2}\d{6})'
            ],
            "licensee": [
                r'(?:licensee|holder|owner|dba)\s*[:\-]?\s*([^<>]{3,50}?)(?:<|\s*license|\s*premises|\s*status)',
                r'Business\s*Name\s*[:\-]?\s*([^<>]{3,50})',
                r'Company\s*[:\-]?\s*([^<>]{3,50})'
            ],
            "address": [
                r'Premises\s*[:\-]?\s*([^<>]{10,100})',
                r'Address\s*[:\-]?\s*([^<>]{10,100})'
            ],
            "status": [
                r'Status\s*[:\-]?\s*([^<>]{3,20})',
                r'Condition\s*[:\-]?\s*([^<>]{3,20})'
            ]
        }

        # Try to find license blocks in the HTML
        license_blocks = re.findall(r'<tr[^>]*>.*?</tr>', html_content, re.DOTALL | re.IGNORECASE)
        if not license_blocks:
            # Try alternative block patterns
            license_blocks = re.findall(r'<div[^>]*class="[^"]*license[^"]*"[^>]*>.*?</div>', html_content, re.DOTALL | re.IGNORECASE)

        for block in license_blocks[:10]:  # Limit to first 10 blocks
            contact_info = {}

            # Extract data using patterns
            for field, field_patterns in patterns.items():
                for pattern in field_patterns:
                    match = re.search(pattern, block, re.IGNORECASE | re.DOTALL)
                    if match:
                        contact_info[field] = match.group(1).strip()
                        break

            if contact_info.get("licensee"):
                contact_info["license_url"] = "https://www.tabc.texas.gov/public-information/tabc-public-inquiry/"

                # Only include if name is relevant to search
                licensee_lower = contact_info["licensee"].lower()
                search_lower = search_term.lower()

                if any(word in licensee_lower for word in search_lower.split()):
                    results.append(contact_info)

        # If no structured results, try to extract any license mentions
        if not results:
            all_licenses = []
            for pattern in patterns["license_number"]:
                all_licenses.extend(re.findall(pattern, html_content, re.IGNORECASE))

            all_names = []
            for pattern in patterns["licensee"]:
                all_names.extend(re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL))

            # Pair licenses with names
            for i, license_num in enumerate(set(all_licenses)):
                contact_info = {
                    "license_number": license_num.upper(),
                    "licensee": all_names[i] if i < len(all_names) else "Unknown",
                    "license_url": "https://www.tabc.texas.gov/public-information/tabc-public-inquiry/"
                }

                if contact_info["licensee"] != "Unknown":
                    name_lower = contact_info["licensee"].lower()
                    if any(word in name_lower for word in search_term.lower().split()):
                        results.append(contact_info)

        return results

    def _extract_visible_tabc_data(self, html_content: str, venue_name: str) -> List[Dict]:
        """Extract any visible TABC license data from the page."""
        results = []

        # Look for any license numbers on the page
        license_pattern = r'([A-Z]{2}\d{6})'
        licenses = re.findall(license_pattern, html_content, re.IGNORECASE)

        # Look for business names near licenses
        for license_num in licenses[:5]:  # Limit to first 5
            # Find text around the license number
            license_context = re.search(
                rf'.{{0,200}}{re.escape(license_num)}.{{0,200}}',
                html_content,
                re.IGNORECASE | re.DOTALL
            )

            if license_context:
                context = license_context.group(0)
                # Look for business name in context
                name_match = re.search(
                    r'(?:licensee|holder|owner|dba|business)\s*[:\-]?\s*([^<>]{3,50})',
                    context,
                    re.IGNORECASE
                )

                if name_match:
                    licensee = name_match.group(1).strip()
                    if any(word in licensee.lower() for word in venue_name.lower().split()):
                        results.append({
                            "license_number": license_num.upper(),
                            "licensee": licensee,
                            "license_url": "https://www.tabc.texas.gov/public-information/tabc-public-inquiry/"
                        })

        return results


class ComptrollerLookupTool(BaseTool):
    """Tool for looking up TX Comptroller business records."""

    name: str = "comptroller_lookup"
    description: str = "Lookup TX Comptroller registered agent and officers"

    def _run(self, legal_name: str) -> str:
        """Look up comptroller business records using web scraping."""
        try:
            # TX Comptroller Business Entity Search
            search_url = "https://www.cpa.state.tx.us/taxinfo/bus_entity_search/bus_entity_search.php"

            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # Performance optimizations (Phase 1)
            chrome_options.add_argument("--disable-images")  # Don't load images for speed
            chrome_options.add_argument("--disable-extensions")  # Disable extensions
            chrome_options.add_argument("--disable-plugins")  # Disable plugins
            chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources to load

            # Additional performance options
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--hide-scrollbars")
            chrome_options.add_argument("--metrics-recording-only")
            chrome_options.add_argument("--mute-audio")
            chrome_options.add_argument("--no-crash-upload")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")

            driver = None
            try:
                driver = webdriver.Chrome(options=chrome_options)

                # Navigate to business entity search
                driver.get(search_url)

                # Enhanced waiting strategy with multiple checks
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )

                # Additional wait for dynamic content with retry logic
                self._wait_for_page_ready(driver, search_url)

                # Analyze the page structure
                page_analysis = self._analyze_comptroller_page(driver)
                logger.info(f"Comptroller page analysis: {page_analysis}")

                search_results = []

                # Try multiple search strategies
                if page_analysis["has_search_form"]:
                    search_results = self._try_comptroller_search_strategies(driver, legal_name, page_analysis)

                # If no results from search, try to extract any visible entity data
                if not search_results:
                    search_results = self._extract_visible_comptroller_data(driver.page_source, legal_name)

                # Process results
                contacts = []
                for result in search_results:
                    if result.get("registered_agent"):
                        contact = {
                            "full_name": result["registered_agent"],
                            "role": "registered_agent",
                            "email": None,
                            "phone": None,
                            "source": "comptroller",
                            "source_url": result.get("entity_url", search_url),
                            "provenance_text": f"Registered agent for {legal_name}",
                            "confidence_0_1": 0.5,
                            "notes": f"Entity: {result.get('entity_name', legal_name)}"
                        }
                        contacts.append(contact)

                    # Add officers if found
                    for officer in result.get("officers", []):
                        contact = {
                            "full_name": officer,
                            "role": "officer",
                            "email": None,
                            "phone": None,
                            "source": "comptroller",
                            "source_url": result.get("entity_url", search_url),
                            "provenance_text": f"Officer for {legal_name}",
                            "confidence_0_1": 0.5,
                            "notes": f"Entity: {result.get('entity_name', legal_name)}"
                        }
                        contacts.append(contact)

                return json.dumps({
                    "success": len(contacts) > 0,
                    "contacts": contacts,
                    "entity_info": search_results[0] if search_results else {},
                    "search_term": legal_name,
                    "page_analysis": page_analysis
                })

            except Exception as e:
                logger.error(f"Comptroller web scraping failed: {e}")
                return json.dumps({
                    "success": False,
                    "contacts": [],
                    "error": str(e),
                    "message": "Web scraping failed, Comptroller search may require manual lookup"
                })
            finally:
                if driver:
                    driver.quit()

        except Exception as e:
            logger.error(f"Comptroller lookup failed: {e}")
            return json.dumps({
                "success": False,
                "contacts": [],
                "error": str(e)
            })

    def _analyze_comptroller_page(self, driver) -> Dict[str, Any]:
        """Analyze Comptroller page structure."""
        analysis = {
            "has_search_form": False,
            "search_input_selectors": [],
            "search_button_selectors": [],
            "form_method": None,
            "page_title": driver.title,
            "url": driver.current_url
        }

        try:
            # Check for business name input fields
            search_selectors = [
                "input[name*='name']",
                "input[name*='business']",
                "input[name*='entity']",
                "input[placeholder*='name']",
                "input[id*='name']",
                "#business_name",
                ".business-name-input",
                "input[type='text']"
            ]

            for selector in search_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        analysis["has_search_form"] = True
                        analysis["search_input_selectors"].append(selector)
                except:
                    continue

            # Check for search buttons
            button_selectors = [
                "input[type='submit']",
                "button[type='submit']",
                "input[value*='Search']",
                "button[class*='search']",
                "#search-button",
                ".search-btn"
            ]

            for selector in button_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        analysis["search_button_selectors"].append(selector)
                except:
                    continue

            # Check form method
            try:
                forms = driver.find_elements(By.TAG_NAME, "form")
                for form in forms:
                    method = form.get_attribute("method")
                    if method:
                        analysis["form_method"] = method.upper()
                        break
            except:
                pass

        except Exception as e:
            logger.warning(f"Comptroller page analysis failed: {e}")

        return analysis

    def _try_comptroller_search_strategies(self, driver, legal_name: str, page_analysis: Dict) -> List[Dict]:
        """Try multiple search strategies for Comptroller."""
        all_results = []

        # Strategy 1: Use identified search inputs
        for input_selector in page_analysis["search_input_selectors"][:2]:
            try:
                results = self._search_comptroller_with_selector(driver, input_selector, legal_name, page_analysis)
                if results:
                    all_results.extend(results)
                    break
            except Exception as e:
                logger.warning(f"Comptroller search strategy failed: {e}")
                continue

        return all_results

    def _search_comptroller_with_selector(self, driver, input_selector: str, search_term: str, page_analysis: Dict) -> List[Dict]:
        """Search Comptroller using specific selector."""
        results = []

        try:
            # Find and interact with search input
            search_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, input_selector))
            )

            # Clear and enter search term
            search_input.clear()
            search_input.send_keys(search_term)

            # Find and click search button
            search_button = None
            for button_selector in page_analysis["search_button_selectors"]:
                try:
                    search_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
                    )
                    break
                except:
                    continue

            if search_button:
                search_button.click()
            else:
                # Try pressing Enter
                search_input.send_keys("\n")

            # Wait for results
            WebDriverWait(driver, 20).until(
                lambda d: self._comptroller_results_loaded(d, search_term)
            )

            # Parse results
            results_html = driver.page_source
            results = self._parse_comptroller_results(results_html, search_term)

        except Exception as e:
            logger.warning(f"Comptroller search with selector failed: {e}")

        return results

    def _comptroller_results_loaded(self, driver, search_term: str) -> bool:
        """Check if Comptroller search results have loaded."""
        try:
            page_text = driver.page_source.lower()
            result_indicators = [
                "search results", "entity details", "registered agent",
                "officer", "manager", "director", search_term.lower()
            ]

            return any(indicator in page_text for indicator in result_indicators)
        except:
            return False

    def _parse_comptroller_results(self, html_content: str, search_term: str) -> Dict[str, Any]:
        """Parse TX Comptroller search results."""
        info = {
            "entity_name": None,
            "registered_agent": None,
            "officers": [],
            "entity_url": "https://www.cpa.state.tx.us/taxinfo/bus_entity_search/bus_entity_search.php"
        }

        # Enhanced patterns for Comptroller data
        patterns = {
            "entity_name": [
                r'(?:entity name|business name|company name)[\s:]*([^<>]{5,100})',
                r'Entity\s*Information\s*:\s*([^<>]{5,100})',
                r'Business\s*Entity\s*:\s*([^<>]{5,100})'
            ],
            "registered_agent": [
                r'(?:registered agent|agent name|agent)[\s:]*([^<>]{3,50})',
                r'Agent\s*:\s*([^<>]{3,50})',
                r'Registered\s*Agent\s*:\s*([^<>]{3,50})'
            ],
            "officers": [
                r'(?:officer|manager|director|president|secretary|treasurer)[\s:]*([^<>]{3,50})',
                r'Officers?\s*:\s*([^<>]{3,50})'
            ]
        }

        # Extract data using patterns
        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if matches:
                    if field == "officers":
                        info[field].extend([match.strip() for match in matches if len(match.strip()) > 3])
                    else:
                        info[field] = matches[0].strip()
                    break

        # Remove duplicates from officers
        info["officers"] = list(set(info["officers"]))

        return info

    def _extract_visible_comptroller_data(self, html_content: str, legal_name: str) -> List[Dict]:
        """Extract any visible Comptroller entity data."""
        results = []

        # Look for entity information on the page
        entity_info = self._parse_comptroller_results(html_content, legal_name)

        if entity_info.get("entity_name") or entity_info.get("registered_agent"):
            results.append(entity_info)

        return results


class PermitLookupTool(BaseTool):
    """Tool for looking up permit records."""
    
    name: str = "permit_lookup"
    description: str = "Lookup permit applicant/owner information"
    
    def _run(self, permit_id: str = None, address: str = None) -> str:
        """Look up permit information using local government portals."""
        try:
            # This is challenging as permits vary by jurisdiction
            # For Harris County (where most Houston restaurants are), use:
            # Harris County Permit Portal or City of Houston portal
            
            if not permit_id and not address:
                return json.dumps({
                    "success": False,
                    "message": "Either permit_id or address required"
                })
            
            # Try Houston permitting portal
            houston_permit_url = "https://www.houstonpermitting.org/"
            
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            
            # Performance optimizations (Phase 1)
            chrome_options.add_argument("--disable-images")  # Don't load images for speed
            chrome_options.add_argument("--disable-extensions")  # Disable extensions
            chrome_options.add_argument("--disable-plugins")  # Disable plugins
            chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources to load

            # Additional performance options
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--hide-scrollbars")
            chrome_options.add_argument("--metrics-recording-only")
            chrome_options.add_argument("--mute-audio")
            chrome_options.add_argument("--no-crash-upload")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            
            driver = None
            try:
                driver = webdriver.Chrome(options=chrome_options)
                driver.get(houston_permit_url)
                
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Try to search by address if provided
                if address:
                    try:
                        search_input = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search'], #search-input"))
                        )
                        search_input.clear()
                        search_input.send_keys(address)
                        
                        search_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], .search-btn")
                        search_button.click()
                        
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".results, .permit-info, table"))
                        )
                        
                        results_html = driver.page_source
                        permit_info = self._parse_permit_results(results_html, address)
                        
                        contacts = []
                        if permit_info.get("applicant"):
                            contact = {
                                "full_name": permit_info["applicant"],
                                "role": "applicant",
                                "email": None,
                                "phone": permit_info.get("phone"),
                                "source": "permit",
                                "source_url": houston_permit_url,
                                "provenance_text": f"Permit applicant for {address}",
                                "confidence_0_1": 0.4,
                                "notes": f"Permit: {permit_info.get('permit_number', 'N/A')}"
                            }
                            contacts.append(contact)
                        
                        return json.dumps({
                            "success": len(contacts) > 0,
                            "contacts": contacts,
                            "permit_info": permit_info
                        })
                        
                    except Exception as e:
                        logger.warning(f"Houston permit search failed: {e}")
                
                return json.dumps({
                    "success": False,
                    "contacts": [],
                    "message": "Permit lookup requires specific jurisdiction portal access"
                })
                
            except Exception as e:
                logger.error(f"Permit lookup failed: {e}")
                return json.dumps({
                    "success": False,
                    "contacts": [],
                    "error": str(e)
                })
            finally:
                if driver:
                    driver.quit()
            
        except Exception as e:
            logger.error(f"Permit lookup failed: {e}")
            return json.dumps({
                "success": False,
                "contacts": [],
                "error": str(e)
            })
    
    def _parse_permit_results(self, html_content: str, address: str) -> Dict[str, Any]:
        """Parse permit search results."""
        info = {
            "permit_number": None,
            "applicant": None,
            "phone": None
        }
        
        # Look for permit details
        permit_pattern = r'(?:permit number|permit #|permit id)[\s:]*([A-Z0-9\-]{5,20})'
        permit_match = re.search(permit_pattern, html_content, re.IGNORECASE)
        if permit_match:
            info["permit_number"] = permit_match.group(1)
        
        # Look for applicant/owner
        applicant_pattern = r'(?:applicant|owner|contractor)[\s:]*([^<>]{3,50})'
        applicant_match = re.search(applicant_pattern, html_content, re.IGNORECASE)
        if applicant_match:
            info["applicant"] = applicant_match.group(1).strip()
        
        # Look for phone
        phone_pattern = r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})'
        phone_match = re.search(phone_pattern, html_content)
        if phone_match:
            info["phone"] = phone_match.group(1)
        
        return info


class WebContactScrapeTool(BaseTool):
    """Tool for scraping website contact information."""
    
    name: str = "web_contact_scrape"
    description: str = "Scrape website for contact information"
    
    def _run(self, domain: str) -> str:
        """Scrape website for contact information."""
        try:
            if not domain.startswith(('http://', 'https://')):
                domain = f"https://{domain}"
            
            contacts = {
                "emails": [],
                "phones": [],
                "source_urls": [],
                "success": False
            }
            
            # Contact pages to check
            contact_pages = ['/contact', '/about', '/contact-us', '/about-us', '/', '/team']
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            for page in contact_pages:
                try:
                    url = urljoin(domain, page)
                    response = session.get(url, timeout=10)
                    response.raise_for_status()
                    
                    content = response.text.lower()
                    
                    # Extract emails (avoid social media)
                    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                    emails = re.findall(email_pattern, content)
                    
                    # Filter out social media and common non-business emails
                    business_emails = []
                    for email in emails:
                        email_domain = email.split('@')[1].lower()
                        if not any(social in email_domain for social in 
                                 ['facebook', 'twitter', 'instagram', 'linkedin', 'youtube']):
                            if email not in business_emails:
                                business_emails.append(email)
                    
                    # Extract phone numbers
                    phone_pattern = r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})'
                    phones = re.findall(phone_pattern, content)
                    
                    if business_emails or phones:
                        contacts["emails"].extend(business_emails)
                        contacts["phones"].extend(phones)
                        contacts["source_urls"].append(url)
                        contacts["success"] = True
                    
                    # Rate limiting
                    time.sleep(1)
                    
                except requests.RequestException:
                    continue
            
            # Remove duplicates
            contacts["emails"] = list(set(contacts["emails"]))
            contacts["phones"] = list(set(contacts["phones"]))
            
            return json.dumps(contacts)
            
        except Exception as e:
            logger.error(f"Web contact scrape failed: {e}")
            return json.dumps({
                "emails": [],
                "phones": [],
                "source_urls": [],
                "success": False,
                "error": str(e)
            })


class EmailPatternTool(BaseTool):
    """Tool for generating and validating email patterns."""
    
    name: str = "email_pattern_generator"
    description: str = "Generate likely email patterns and validate domains"
    
    def _run(self, domain: str, full_name: str) -> str:
        """Generate email patterns for a name and domain."""
        try:
            if not domain or not full_name:
                return json.dumps({"emails": [], "mx_valid": False, "success": False})
            
            # Clean domain
            domain = domain.replace('http://', '').replace('https://', '').replace('www.', '')
            domain = domain.split('/')[0]  # Remove path if present
            
            # Clean name
            name_parts = re.sub(r'[^a-zA-Z\s]', '', full_name.lower()).split()
            if len(name_parts) < 2:
                name_parts.append('lastname')  # fallback
            
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            # Generate email patterns
            patterns = [
                f"info@{domain}",
                f"contact@{domain}",
                f"{first_name}@{domain}",
                f"{first_name}.{last_name}@{domain}",
                f"{first_name}{last_name}@{domain}",
                f"{first_name[0]}{last_name}@{domain}",
                f"{first_name}{last_name[0]}@{domain}"
            ]
            
            # Remove duplicates while preserving order
            seen = set()
            unique_patterns = []
            for pattern in patterns:
                if pattern not in seen:
                    seen.add(pattern)
                    unique_patterns.append(pattern)
            
            # Validate MX record
            mx_valid = False
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                mx_valid = len(mx_records) > 0
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, Exception):
                mx_valid = False
            
            return json.dumps({
                "emails": unique_patterns,
                "mx_valid": mx_valid,
                "success": True,
                "domain": domain
            })
            
        except Exception as e:
            logger.error(f"Email pattern generation failed: {e}")
            return json.dumps({
                "emails": [],
                "mx_valid": False,
                "success": False,
                "error": str(e)
            })


class ContactabilityEvaluator:
    """Helper class for evaluating contact method appropriateness."""
    
    @staticmethod
    def evaluate_email(email: str, source: str) -> Dict[str, Any]:
        """Evaluate if email is appropriate for outreach."""
        ok_to_email = True
        
        # Prefer generic/role emails over personal
        generic_prefixes = ['info', 'contact', 'hello', 'admin', 'sales', 'business']
        is_generic = any(email.lower().startswith(prefix) for prefix in generic_prefixes)
        
        # Avoid personal-looking emails unless from official sources
        if not is_generic and source == 'pattern':
            ok_to_email = False
        
        return {
            "ok_to_email": ok_to_email,
            "is_generic": is_generic,
            "rationale": f"Generic email: {is_generic}, Source: {source}"
        }
    
    @staticmethod
    def evaluate_phone(phone: str, source: str) -> Dict[str, Any]:
        """Evaluate if phone is appropriate for calling."""
        # Clean phone number
        digits = re.sub(r'\D', '', phone)
        
        # Basic mobile detection (very simplistic)
        # Real implementation would use carrier lookup APIs
        is_likely_mobile = len(digits) == 10  # This is oversimplified
        
        # Prefer business lines from official sources
        ok_to_call = source in ['tabc', 'comptroller', 'permit'] or not is_likely_mobile
        
        return {
            "ok_to_call": ok_to_call,
            "ok_to_sms": False,  # Default to false per compliance requirements
            "is_likely_mobile": is_likely_mobile,
            "rationale": f"Mobile likely: {is_likely_mobile}, Source: {source}"
        }
