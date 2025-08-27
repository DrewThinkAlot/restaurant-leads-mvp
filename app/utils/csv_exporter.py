import csv
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..settings import settings


class CSVExporter:
    """Export restaurant leads to CSV format for sales teams."""

    def __init__(self):
        self.export_path = Path(settings.csv_export_path)
        self.export_path.mkdir(parents=True, exist_ok=True)

    def export_leads(self, leads: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
        """
        Export leads to CSV file.

        Args:
            leads: List of lead dictionaries
            filename: Optional custom filename (auto-generated if None)

        Returns:
            Path to the exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"restaurant_leads_{timestamp}.csv"

        filepath = self.export_path / filename

        if not leads:
            print("⚠️ No leads to export")
            return str(filepath)

        # Define CSV columns for sales team
        fieldnames = [
            "venue_name",
            "legal_name",
            "address",
            "city",
            "state",
            "zip_code",
            "phone",
            "email",
            "confidence_score",
            "estimated_open_date",
            "source_flags",
            "pitch_text",
            "contact_info",
            "lead_quality",
            "export_timestamp"
        ]

        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for lead in leads:
                # Format lead data for CSV
                csv_row = self._format_lead_for_csv(lead)
                writer.writerow(csv_row)

        print(f"✅ Exported {len(leads)} leads to {filepath}")
        return str(filepath)

    def _format_lead_for_csv(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Format a single lead for CSV export."""
        return {
            "venue_name": lead.get("venue_name", ""),
            "legal_name": lead.get("legal_name", ""),
            "address": lead.get("address", ""),
            "city": lead.get("city", ""),
            "state": lead.get("state", "TX"),
            "zip_code": lead.get("zip_code", ""),
            "phone": lead.get("phone", ""),
            "email": lead.get("email", ""),
            "confidence_score": lead.get("confidence_0_1", ""),
            "estimated_open_date": lead.get("estimated_open_date", ""),
            "source_flags": json.dumps(lead.get("source_flags", {})),
            "pitch_text": lead.get("pitch_text", ""),
            "contact_info": json.dumps(lead.get("contact_info", {})),
            "lead_quality": self._calculate_lead_quality(lead),
            "export_timestamp": datetime.now().isoformat()
        }

    def _calculate_lead_quality(self, lead: Dict[str, Any]) -> str:
        """Calculate lead quality rating."""
        confidence = lead.get("confidence_0_1", 0)

        if confidence >= 0.8:
            return "A - High Confidence"
        elif confidence >= 0.65:
            return "B - Medium Confidence"
        elif confidence >= 0.5:
            return "C - Low Confidence"
        else:
            return "D - Very Low Confidence"

    def export_pipeline_results(
        self,
        pipeline_result: Dict[str, Any],
        include_summary: bool = True
    ) -> Dict[str, Any]:
        """
        Export complete pipeline results with optional summary.

        Args:
            pipeline_result: Result from pipeline run
            include_summary: Whether to create a summary file

        Returns:
            Dict with export paths and statistics
        """
        leads = pipeline_result.get("leads", [])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Export main leads file
        main_file = self.export_leads(leads, f"pipeline_results_{timestamp}.csv")

        results = {
            "main_export": main_file,
            "total_leads": len(leads),
            "qualified_leads": pipeline_result.get("qualified_leads", 0),
            "export_timestamp": timestamp
        }

        # Export qualified leads only
        qualified_leads = [lead for lead in leads
                          if lead.get("confidence_0_1", 0) >= 0.65]

        if qualified_leads:
            qualified_file = self.export_leads(
                qualified_leads,
                f"qualified_leads_{timestamp}.csv"
            )
            results["qualified_export"] = qualified_file

        # Create summary file if requested
        if include_summary:
            summary_file = self._create_summary_file(pipeline_result, timestamp)
            results["summary_file"] = summary_file

        return results

    def _create_summary_file(self, pipeline_result: Dict[str, Any], timestamp: str) -> str:
        """Create a summary file with pipeline statistics."""
        summary_path = self.export_path / f"pipeline_summary_{timestamp}.txt"

        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("RESTAURANT LEADS PIPELINE SUMMARY\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Export Timestamp: {datetime.now().isoformat()}\n\n")

            f.write("PIPELINE RESULTS:\n")
            f.write(f"- Total Leads: {pipeline_result.get('total_candidates', 0)}\n")
            f.write(f"- Qualified Leads: {pipeline_result.get('qualified_leads', 0)}\n")
            f.write(f"- AI Enhancement: {pipeline_result.get('ai_enhancement', False)}\n")
            f.write(".2f")

            # Pipeline stages info
            stages = pipeline_result.get("pipeline_stages", {})
            if stages:
                f.write("\nPIPELINE STAGES:\n")
                f.write(f"- Raw Candidates: {stages.get('raw_candidates', 0)}\n")
                f.write(f"- AI Enhanced: {stages.get('ai_enhanced', 0)}\n")
                f.write(f"- Final Leads: {stages.get('final_leads', 0)}\n")

            # Lead quality breakdown
            leads = pipeline_result.get("leads", [])
            if leads:
                f.write("\nLEAD QUALITY BREAKDOWN:\n")
                quality_counts = {}
                for lead in leads:
                    confidence = lead.get("confidence_0_1", 0)
                    if confidence >= 0.8:
                        quality = "A - High"
                    elif confidence >= 0.65:
                        quality = "B - Medium"
                    elif confidence >= 0.5:
                        quality = "C - Low"
                    else:
                        quality = "D - Very Low"

                    quality_counts[quality] = quality_counts.get(quality, 0) + 1

                for quality, count in sorted(quality_counts.items()):
                    f.write(f"- {quality}: {count} leads\n")

        return str(summary_path)

    def list_exports(self) -> List[Dict[str, Any]]:
        """List all exported files with metadata."""
        exports = []

        if not self.export_path.exists():
            return exports

        for file_path in self.export_path.glob("*.csv"):
            stat = file_path.stat()
            exports.append({
                "filename": file_path.name,
                "path": str(file_path),
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

        return sorted(exports, key=lambda x: x["created"], reverse=True)


# Convenience function for quick exports
def export_pipeline_results(pipeline_result: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function to export pipeline results."""
    exporter = CSVExporter()
    return exporter.export_pipeline_results(pipeline_result)


def export_leads(leads: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
    """Convenience function to export leads to CSV."""
    exporter = CSVExporter()
    return exporter.export_leads(leads, filename)
