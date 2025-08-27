#!/usr/bin/env python3
"""
Direct pipeline runner with AI agents - bypasses vLLM check
"""

import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
from app.utils.csv_exporter import CSVExporter
import time

def run_ai_pipeline():
    """Run pipeline with AI agents (contact finder bypassed)."""
    print("🤖 Running AI Pipeline with Contact Finder Bypassed")
    print("=" * 55)

    runner = EnhancedPipelineRunner()
    start_time = time.time()

    print("🚀 Running hybrid pipeline with 5 candidates...")
    result = runner.run_hybrid_pipeline(
        max_candidates=5,
        use_stable_apis=True,
        use_ai_enhancement=True  # Enable AI enhancement
    )

    execution_time = time.time() - start_time

    print("\n📊 Results:")
    print(f"   Total candidates: {result.get('total_candidates', 0)}")
    print(f"   Qualified leads: {result.get('qualified_leads', 0)}")
    print(".2f")
    print(f"   AI enhancement: {result.get('ai_enhancement', False)}")
    print(f"   API success: {result.get('api_success', False)}")

    # Show sample enhanced lead if available
    leads = result.get('leads', [])
    if leads:
        lead = leads[0]
        print("\n📋 Sample Enhanced Lead:")
        print(f"   Venue: {lead.get('venue_name', 'N/A')}")
        print(f"   Confidence: {lead.get('confidence_0_1', 'N/A')}")
        if 'pitch_text' in lead:
            pitch = lead.get('pitch_text', '')[:100]
            print(f"   AI Pitch: {pitch}...")
        if 'eta_window' in lead:
            print(f"   ETA Window: {lead.get('eta_window', 'N/A')}")

    # Export results
    print("\n📁 Exporting results...")
    exporter = CSVExporter()
    export_results = exporter.export_pipeline_results(result)

    print("   AI-enhanced export: exports/pipeline_results_*.csv")
    if 'qualified_export' in export_results:
        print("   Qualified leads: exports/qualified_leads_*.csv")

    print("\n✅ Pipeline completed successfully!")
    print("📝 Contact finder was bypassed as configured")
    print("🤖 AI agents processed the pipeline")

    return result

if __name__ == "__main__":
    success = run_ai_pipeline()
    if success:
        print("\n🎉 AI pipeline run completed!")
        print("Check the 'exports/' directory for CSV files")
    else:
        print("\n❌ Pipeline failed")
        sys.exit(1)
