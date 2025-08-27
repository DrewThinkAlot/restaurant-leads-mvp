from crewai import Agent
from crewai.tools import BaseTool
from typing import List, Dict, Any, cast
import json
import logging
from datetime import datetime

from ..settings import settings
from ..llm import get_llm
from ..schemas import LeadOutput

logger = logging.getLogger(__name__)


class LLMPitchGenerationTool(BaseTool):
    """Tool for LLM-based pitch content generation."""
    
    name: str = "llm_pitch_generator"
    description: str = "Generate sales pitch content for restaurant leads"
    
    def _run(self, candidate_data: str, eta_window: str) -> str:
        """Execute LLM pitch generation."""
        try:
            llm = get_llm(temperature=0.3, max_tokens=600)
            
            import os
            prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "pitch.md")
            with open(prompt_path, "r") as f:
                prompt_template = f.read()
            
            prompt = prompt_template.format(
                candidate_data=candidate_data,
                eta_window=eta_window
            )
            
            response = llm._call(prompt)
            
            return response
            
        except Exception as e:
            logger.error(f"LLM pitch generation failed: {e}")
            return json.dumps({
                "how_to_pitch": "Contact about upcoming opening opportunity",
                "pitch_text": "Your restaurant is opening soon. We'd love to discuss POS solutions.",
                "sms_text": "POS solutions for your new restaurant opening soon."
            })


class PitchAgent:
    """Agent for creating sales-ready pitch content."""
    
    def __init__(self):
        self.tools = [LLMPitchGenerationTool()]
        
        self.agent = Agent(
            role="Pitch Writer",
            goal="Create compelling, value-focused sales pitch content for POS sales representatives",
            backstory="""
            You are an expert sales copywriter specializing in B2B technology sales for restaurants.
            You understand the pain points of new restaurant owners: cash flow concerns, operational
            complexity, staff training needs, and the critical importance of reliable payment processing.
            You craft concise, benefit-focused messaging that resonates with busy entrepreneurs who
            need concrete solutions, not hype. Your pitches directly drive revenue by helping sales
            reps connect with prospects at the perfect moment - right before they open.
            """,
            tools=cast(List[BaseTool], self.tools),
            verbose=True,
            allow_delegation=False,
            llm=get_llm()  # Use Ollama Turbo LLM wrapper
        )
    
    def create_pitch_content(self, verified_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create sales pitch content for verified candidates."""
        
        logger.info(f"Creating pitch content for {len(verified_candidates)} candidates")
        
        leads = []
        
        for candidate in verified_candidates:
            try:
                lead_output = self._create_single_pitch(candidate)
                
                if lead_output:
                    leads.append(lead_output)
                    
                    logger.info(f"Created pitch for: {candidate['venue_name']}")
                    
            except Exception as e:
                logger.warning(f"Pitch creation failed for {candidate.get('venue_name')}: {e}")
                continue
        
        logger.info(f"Pitch creation complete: {len(leads)} leads generated")
        
        return leads
    
    def _create_single_pitch(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Create pitch content for a single candidate."""
        
        # Extract key information
        venue_name = candidate.get("venue_name", "")
        address = candidate.get("address", "")
        phone = candidate.get("phone")
        eta_result = candidate.get("eta_result", {})
        
        # Format ETA window
        eta_window = self._format_eta_window(eta_result)
        
        # Determine business context
        business_context = self._analyze_business_context(candidate)
        
        # Generate pitch content
        pitch_content = self._generate_pitch_content(candidate, business_context, eta_window)
        
        # Create lead output
        import uuid
        
        lead_output = {
            "lead_id": str(uuid.uuid4()),
            "candidate_id": candidate.get("candidate_id"),
            "venue_name": venue_name,
            "entity_name": candidate.get("legal_name"),
            "address": address,
            "phone": phone,
            "eta_window": eta_window,
            "confidence_0_1": eta_result.get("confidence_0_1", 0.0),
            "how_to_pitch": pitch_content.get("how_to_pitch", ""),
            "pitch_text": pitch_content.get("pitch_text", ""),
            "sms_text": pitch_content.get("sms_text", "")
        }
        
        return lead_output
    
    def _format_eta_window(self, eta_result: Dict[str, Any]) -> str:
        """Format ETA window in human-readable format."""
        
        try:
            eta_start = eta_result.get("eta_start")
            eta_end = eta_result.get("eta_end")
            
            if not eta_start or not eta_end:
                return "Next 60 days"
            
            start_date = datetime.fromisoformat(eta_start.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(eta_end.replace('Z', '+00:00'))
            
            # Format as "Month DD – Month DD" or "Month DD – DD" if same month
            start_formatted = start_date.strftime("%b %d")
            
            if start_date.month == end_date.month:
                end_formatted = end_date.strftime("%d")
                return f"{start_formatted} – {end_formatted}"
            else:
                end_formatted = end_date.strftime("%b %d")
                return f"{start_formatted} – {end_formatted}"
                
        except Exception as e:
            logger.warning(f"ETA formatting failed: {e}")
            return "Next 60 days"
    
    def _analyze_business_context(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze business context for pitch personalization."""
        
        venue_name = candidate.get("venue_name", "").lower()
        address = candidate.get("address", "").lower()
        source_flags = candidate.get("source_flags", {})
        
        context = {
            "business_type": "restaurant",
            "size_category": "unknown",
            "location_type": "unknown",
            "permit_stage": "unknown",
            "urgency_level": "medium"
        }
        
        # Infer business type
        if any(term in venue_name for term in ["fast", "quick", "drive"]):
            context["business_type"] = "fast_casual"
        elif any(term in venue_name for term in ["bar", "grill", "pub", "tavern"]):
            context["business_type"] = "bar_grill"
        elif any(term in venue_name for term in ["cafe", "coffee", "bistro"]):
            context["business_type"] = "cafe"
        elif any(term in venue_name for term in ["pizza"]):
            context["business_type"] = "pizza"
        
        # Infer size
        if any(term in venue_name for term in ["chain", "franchise"]) or len(venue_name) > 50:
            context["size_category"] = "large"
        elif any(term in venue_name for term in ["family", "local", "neighborhood"]):
            context["size_category"] = "small"
        else:
            context["size_category"] = "medium"
        
        # Infer location type
        if any(term in address for term in ["mall", "center", "plaza", "strip"]):
            context["location_type"] = "retail_center"
        elif any(term in address for term in ["downtown", "main st", "main street"]):
            context["location_type"] = "downtown"
        else:
            context["location_type"] = "standalone"
        
        # Determine permit stage urgency
        tabc_status = source_flags.get("tabc", "")
        hc_health = source_flags.get("hc_health", "")
        
        if "pending" in tabc_status.lower() and "approved" in hc_health.lower():
            context["urgency_level"] = "high"
        elif "pending" in tabc_status.lower():
            context["urgency_level"] = "medium"
        
        return context
    
    def _generate_pitch_content(self, candidate: Dict[str, Any], context: Dict[str, Any], eta_window: str) -> Dict[str, Any]:
        """Generate pitch content using business context."""
        
        venue_name = candidate.get("venue_name", "")
        business_type = context.get("business_type", "restaurant")
        size_category = context.get("size_category", "medium")
        urgency_level = context.get("urgency_level", "medium")
        
        # Create tailored messaging based on context
        if urgency_level == "high":
            how_to_pitch = f"Immediate outreach - {venue_name} is in final permitting stages and likely selecting vendors now"
        elif urgency_level == "medium":
            how_to_pitch = f"Proactive outreach - {venue_name} is progressing through permits and will need POS solutions soon"
        else:
            how_to_pitch = f"Educational approach - {venue_name} is early in opening process but good time to build relationship"
        
        # Generate business-type specific pitch
        if business_type == "fast_casual":
            value_props = [
                "fast order processing for high-volume service",
                "integrated online ordering and delivery management",
                "labor cost optimization with efficient workflows"
            ]
        elif business_type == "bar_grill":
            value_props = [
                "seamless bar tab management and split billing",
                "inventory tracking for food and beverage",
                "integration with entertainment and event booking"
            ]
        elif business_type == "cafe":
            value_props = [
                "quick payment processing for coffee rush hours",
                "loyalty program integration for repeat customers",
                "simple menu management for daily specials"
            ]
        else:
            value_props = [
                "reliable payment processing from day one",
                "comprehensive reporting for business insights",
                "scalable solution that grows with your business"
            ]
        
        # Build main pitch text
        pitch_text = f"""Hi! I noticed {venue_name} is opening {eta_window} and wanted to reach out about POS solutions.

New restaurants face huge challenges in their first months - from managing cash flow to training staff efficiently. Our POS system helps with {value_props[0]}, {value_props[1]}, and {value_props[2]}.

We're offering special launch pricing for new restaurants in Harris County, including free setup and training. This could save you thousands versus setting up after opening when you're swamped.

Would you have 15 minutes this week to discuss how we can help ensure your opening goes smoothly?"""
        
        # Create SMS version (40 words max)
        sms_text = f"{venue_name} opening {eta_window}? Special POS launch pricing + free setup for new Harris County restaurants. Save $$ vs waiting. Quick call this week? [Your Name]"
        
        return {
            "how_to_pitch": how_to_pitch,
            "pitch_text": pitch_text,
            "sms_text": sms_text
        }
    
    def _generate_pitch_with_llm_fallback(self, candidate: Dict[str, Any], context: Dict[str, Any], eta_window: str) -> Dict[str, Any]:
        """Use LLM for pitch generation as fallback or enhancement."""
        
        try:
            llm = get_llm(temperature=0.3, max_tokens=500)
            
            candidate_summary = {
                "venue_name": candidate.get("venue_name"),
                "business_type": context.get("business_type"),
                "location": candidate.get("address"),
                "eta_window": eta_window,
                "confidence": candidate.get("eta_result", {}).get("confidence_0_1", 0)
            }
            
            prompt = f"""
            Create sales pitch content for a POS system to a new restaurant. Target audience: busy restaurant owner preparing to open.
            
            Restaurant details: {json.dumps(candidate_summary, indent=2)}
            
            Generate JSON with:
            - how_to_pitch: One sentence strategy for sales rep
            - pitch_text: Professional email pitch (≤120 words) focusing on concrete value, timing urgency, and specific benefits
            - sms_text: Brief SMS version (≤40 words)
            
            Key principles:
            - No hype or fluff
            - Focus on timing advantage (getting POS before opening vs after)
            - Mention Harris County specifically
            - Include specific business benefits, not generic features
            - Create urgency without being pushy
            
            Return only JSON:
            """
            
            response = llm._call(prompt)
            
            result = json.loads(response)
            
            # Validate word counts
            pitch_words = len(result.get("pitch_text", "").split())
            if pitch_words > 120:
                logger.warning(f"LLM pitch text too long: {pitch_words} words")
            
            sms_words = len(result.get("sms_text", "").split())
            if sms_words > 40:
                logger.warning(f"LLM SMS text too long: {sms_words} words")
            
            return result
            
        except Exception as e:
            logger.warning(f"LLM pitch generation failed: {e}")
            return self._generate_pitch_content(candidate, context, eta_window)
    
    def get_pitch_analytics(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get analytics on generated pitches."""
        
        if not leads:
            return {
                "total_leads": 0,
                "avg_confidence": 0,
                "eta_distribution": {},
                "business_types": {}
            }
        
        confidences = [lead.get("confidence_0_1", 0) for lead in leads]
        
        # ETA window analysis
        eta_windows = [lead.get("eta_window", "") for lead in leads]
        eta_categories = {
            "Next 30 days": len([w for w in eta_windows if any(term in w.lower() for term in ["days", "week", "soon"])]),
            "30-60 days": len([w for w in eta_windows if any(month in w for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])]),
            "Other": len([w for w in eta_windows if w and not any(term in w.lower() for term in ["days", "week", "soon"]) and not any(month in w for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])])
        }
        
        # Business type analysis (inferred from venue names)
        venue_names = [lead.get("venue_name", "").lower() for lead in leads]
        business_types = {
            "Fast Casual": len([n for n in venue_names if any(term in n for term in ["fast", "quick", "express"])]),
            "Bar/Grill": len([n for n in venue_names if any(term in n for term in ["bar", "grill", "pub", "tavern"])]),
            "Cafe/Coffee": len([n for n in venue_names if any(term in n for term in ["cafe", "coffee", "bistro"])]),
            "Other": len([n for n in venue_names if not any(term in n for term in ["fast", "quick", "express", "bar", "grill", "pub", "tavern", "cafe", "coffee", "bistro"])])
        }
        
        return {
            "total_leads": len(leads),
            "avg_confidence": sum(confidences) / len(confidences) if confidences else 0,
            "high_confidence_leads": len([c for c in confidences if c >= 0.8]),
            "eta_distribution": eta_categories,
            "business_types": business_types,
            "leads_with_phone": len([lead for lead in leads if lead.get("phone")])
        }
