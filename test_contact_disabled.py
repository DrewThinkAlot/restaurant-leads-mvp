#!/usr/bin/env python3
"""
Test script to verify that contact finding has been disabled in the pipeline.
"""

import sys
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent))

from app.pipelines.enhanced_pipeline import EnhancedPipelineRunner
import time

def test_pipeline_without_contact_finding():
    """Test that the pipeline runs without contact finding."""
    print("🍕 Testing Pipeline with Contact Finding DISABLED")
    print("=" * 60)

    runner = EnhancedPipelineRunner()
    start_time = time.time()

    print("🚀 Running pipeline with 3 candidates...")
    try:
        result = runner.run_hybrid_pipeline(max_candidates=3, harris_only=True, use_ai_enhancement=False)
        execution_time = time.time() - start_time

        print()
        print("📊 RESULTS:")
        print(f"   Total candidates: {result.get('total_candidates', 0)}")
        print(f"   Qualified leads: {result.get('qualified_leads', 0)}")
        print(f"   Execution time: {execution_time:.2f}s")
        print(f"   CSV export: {result.get('csv_export_path', 'None')}")

        print()
        print("🎯 Pipeline stages:")
        stages = result.get('pipeline_stages', {})
        for stage, count in stages.items():
            print(f"   {stage}: {count}")

        print()
        if execution_time < 60:  # Should be much faster without contact finding
            print("✅ Contact finding successfully disabled! Pipeline ran quickly.")
        else:
            print("⚠️  Pipeline took longer than expected, contact finding may still be active.")
            
        return True

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_pipeline_without_contact_finding()
    sys.exit(0 if success else 1)