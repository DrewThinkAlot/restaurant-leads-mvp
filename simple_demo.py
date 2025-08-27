#!/usr/bin/env python3
"""
Simple demo of the stable API integration concept.
"""

def demo_data_flow():
    """Show how data flows through the new stable API system."""
    print("🚀 Restaurant Leads MVP - Stable API Integration")
    print("=" * 55)

    print("\n📊 BEFORE (Brittle Scraping):")
    print("   🌐 Web scraping → ❌ Broken links → 🔄 Manual fixes → 📉 Unreliable data")

    print("\n✅ AFTER (Stable APIs):")
    print("   🏛️ Government APIs → 🔒 Reliable endpoints → 🤖 Auto-processing → 📈 Quality data")

    print("\n🔄 Data Flow:")
    print("   1. 📡 TABC API (Pending licenses)")
    print("   2. 🏥 Houston Health API (Inspections)")
    print("   3. 🏗️ Harris County Permits API")
    print("   4. 🏢 TX Comptroller API (Entity enrichment)")
    print("   5. 🤖 DataSourceManager (Normalize, dedupe, score)")
    print("   6. 🧠 AI Agents (Your existing ContactFinder, ETA, Pitch agents)")
    print("   7. 📄 CSV Export (Sales-ready leads)")

    print("\n🎯 Key Improvements:")
    print("   ✅ No more broken scrapers")
    print("   ✅ Official government data")
    print("   ✅ Better compliance (CAN-SPAM, TCPA)")
    print("   ✅ Higher data quality")
    print("   ✅ Incremental updates with watermarks")
    print("   ✅ Cross-source validation")

def show_setup_steps():
    """Show what you need to do to get started."""
    print("\n🔧 SETUP STEPS:")
    print("=" * 30)

    print("1. 📝 Get API Keys:")
    print("   • OpenAI: https://platform.openai.com/api-keys")
    print("   • TABC: https://data.texas.gov/login (get App Token)")
    print("   • TX Comptroller: https://comptroller.texas.gov/api/ (optional)")

    print("\n2. ✏️ Update .env file:")
    print("   OPENAI_API_KEY=your_openai_key_here")
    print("   TABC_APP_TOKEN=your_socrata_token_here")
    print("   TX_COMPTROLLER_API_KEY=your_comptroller_key_here")

    print("\n3. 🧪 Test:")
    print("   python test_api_integration.py")

    print("\n4. 🚀 Run pipeline:")
    print("   from app.pipelines.run_pipeline import PipelineRunner")
    print("   runner = PipelineRunner()")
    print("   result = runner.run_complete_pipeline(use_stable_apis=True)")

def show_sample_output():
    """Show what the CSV output looks like."""
    print("\n📄 SAMPLE CSV OUTPUT:")
    print("=" * 40)

    csv_sample = """venue_name,address,city,state,zip_code,composite_lead_score,source,estimated_min_days,estimated_max_days
Bella Vista Bistro,123 Main St,Houston,TX,77002,0.85,tabc_pending,60,120
Downtown Grill House,456 Commerce St,Houston,TX,77002,0.72,houston_health,30,60
Urban Kitchen & Bar,789 Market Ave,Houston,TX,77002,0.78,harris_permits,45,90"""

    print(csv_sample)

    print("\n📊 This gives sales teams:")
    print("   • Venue name & address")
    print("   • Confidence score (0-1)")
    print("   • Data source provenance")
    print("   • Predicted opening window")
    print("   • Ready for outreach campaigns")

if __name__ == "__main__":
    demo_data_flow()
    show_setup_steps()
    show_sample_output()

    print("\n" + "=" * 55)
    print("🎉 Your stable API integration is ready!")
    print("Just add the API keys and you're good to go! 🚀")
