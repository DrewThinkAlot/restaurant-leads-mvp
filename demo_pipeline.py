#!/usr/bin/env python3
"""
Restaurant Leads Pipeline Demo

This script demonstrates how to use the hybrid pipeline that combines:
1. Direct API calls for reliable data fetching
2. AI agents for enhanced analysis and lead generation
3. CSV export for sales-ready leads

Usage:
    python demo_pipeline.py [--ai] [--max-candidates N] [--export-only]

Options:
    --ai: Enable AI enhancement (requires vLLM server running)
    --max-candidates N: Maximum candidates to process (default: 10)
    --export-only: Only export existing results without running pipeline
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Any

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
from app.utils.csv_exporter import CSVExporter
from app.settings import settings


def run_api_only_demo(max_candidates: int = 10):
    """Run pipeline in API-only mode (no AI enhancement)."""
    print("🍕 Running API-Only Pipeline Demo")
    print("=" * 50)

    runner = EnhancedPipelineRunner()
    start_time = time.time()

    print(f"Fetching up to {max_candidates} candidates from APIs...")
    result = runner.run_hybrid_pipeline(
        max_candidates=max_candidates,
        use_stable_apis=True,
        use_ai_enhancement=False  # API-only mode
    )

    execution_time = time.time() - start_time

    print("\n📊 Results:")
    print(f"   Total candidates: {result.get('total_candidates', 0)}")
    print(f"   Qualified leads: {result.get('qualified_leads', 0)}")
    print(".2f")
    print(f"   API success: {result.get('api_success', False)}")

    # Export results
    print("\n📁 Exporting results...")
    exporter = CSVExporter()
    export_results = exporter.export_pipeline_results(result)

    print("   Main export: exports/pipeline_results_*.csv")
    if 'qualified_export' in export_results:
        print("   Qualified export: exports/qualified_leads_*.csv")
    if 'summary_file' in export_results:
        print("   Summary: exports/pipeline_summary_*.txt")
    return result


def run_ai_enhanced_demo(max_candidates: int = 5):
    """Run pipeline with AI enhancement (requires vLLM server)."""
    print("🤖 Running AI-Enhanced Pipeline Demo")
    print("=" * 50)

    # Check if vLLM is available
    try:
        import requests
        response = requests.get(f"{settings.vllm_base_url}/models", timeout=5)
        if response.status_code == 200:
            print("✅ vLLM server is running")
        else:
            print("❌ vLLM server not responding properly")
            return None
    except:
        print("❌ vLLM server not available")
        print("   Start vLLM with: vllm serve openai-community/openai-gpt-20b --host 0.0.0.0 --port 8000")
        return None

    runner = EnhancedPipelineRunner()
    start_time = time.time()

    print(f"Running hybrid pipeline with {max_candidates} candidates...")
    result = runner.run_hybrid_pipeline(
        max_candidates=max_candidates,
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
        if 'estimated_open_date' in lead:
            print(f"   Est. Open: {lead.get('estimated_open_date', 'N/A')}")

    # Export results
    print("\n📁 Exporting results...")
    exporter = CSVExporter()
    export_results = exporter.export_pipeline_results(result)

    print("   AI-enhanced export: exports/pipeline_results_*.csv")
    if 'qualified_export' in export_results:
        print("   Qualified leads: exports/qualified_leads_*.csv")
    return result


def compare_modes():
    """Compare API-only vs AI-enhanced results."""
    print("🔍 Comparing Pipeline Modes")
    print("=" * 50)

    print("Running API-only mode...")
    api_result = run_api_only_demo(max_candidates=5)

    print("\n" + "=" * 50)
    print("Running AI-enhanced mode...")
    ai_result = run_ai_enhanced_demo(max_candidates=5)

    if api_result and ai_result:
        print("\n📊 Comparison:")
        print(f"   API-only leads: {api_result.get('qualified_leads', 0)}")
        print(f"   AI-enhanced leads: {ai_result.get('qualified_leads', 0)}")
        print(".2f")
        print(".2f")

        # Compare lead quality
        api_leads = api_result.get('leads', [])
        ai_leads = ai_result.get('leads', [])

        if api_leads and ai_leads:
            api_avg_confidence = sum(lead.get('confidence_0_1', 0) for lead in api_leads) / len(api_leads)
            ai_avg_confidence = sum(lead.get('confidence_0_1', 0) for lead in ai_leads) / len(ai_leads)

            print(".3f")
            print(".3f")
            print(".3f")


def show_export_history():
    """Show previously exported files."""
    print("📁 Export History")
    print("=" * 50)

    exporter = CSVExporter()
    exports = exporter.list_exports()

    if not exports:
        print("No exported files found")
        return

    print(f"Found {len(exports)} exported files:")
    for export in exports[:5]:  # Show last 5
        print(f"   {export['filename']} ({export['size_mb']} MB) - {export['created']}")


def main():
    parser = argparse.ArgumentParser(description="Restaurant Leads Pipeline Demo")
    parser.add_argument("--ai", action="store_true", help="Enable AI enhancement mode")
    parser.add_argument("--max-candidates", type=int, default=10, help="Maximum candidates to process")
    parser.add_argument("--export-only", action="store_true", help="Only show export history")
    parser.add_argument("--compare", action="store_true", help="Compare API-only vs AI-enhanced modes")

    args = parser.parse_args()

    print("🍕 Restaurant Leads Pipeline Demo")
    print("==================================")
    print(f"vLLM URL: {settings.vllm_base_url}")
    print(f"Model: {settings.model_id}")
    print(f"Export path: {settings.csv_export_path}")
    print()

    if args.export_only:
        show_export_history()
        return

    if args.compare:
        compare_modes()
        return

    if args.ai:
        result = run_ai_enhanced_demo(args.max_candidates)
        if not result:
            print("\n💡 Tip: Make sure vLLM server is running for AI enhancement")
            print("   Command: vllm serve openai-community/openai-gpt-20b --host 0.0.0.0 --port 8000")
    else:
        result = run_api_only_demo(args.max_candidates)

    if result:
        print("\n🎉 Demo completed successfully!")
        print("Check the 'exports/' directory for CSV files")

        if not args.ai:
            print("\n💡 Try with AI enhancement: python demo_pipeline.py --ai")
        else:
            print("\n💡 Try API-only mode: python demo_pipeline.py --max-candidates 20")


if __name__ == "__main__":
    main()
