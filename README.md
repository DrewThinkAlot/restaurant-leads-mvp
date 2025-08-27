# Restaurant Opening Leads MVP Pipeline

An AI-powered pipeline to predict restaurant openings 30-60 days in advance in Harris County, TX using only free and open-source tools.

## 🎯 Overview

This system ingests data from official sources (TABC, Harris County permits, Health Department), applies deterministic rules and LLM processing to identify high-quality restaurant opening leads, and provides sales-ready pitch content.

**Key Features:**
- **Free & Open Source**: No paid APIs, uses local LLM via vLLM/Ollama
- **Multi-Source Data**: TABC licenses, building permits, health permits
- **AI-Powered**: LLM normalization, entity resolution, ETA adjustment
- **Production Ready**: Docker deployment, comprehensive tests, logging
- **API Interface**: FastAPI with health checks, pagination, filtering

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Data Sources   │ -> │   CrewAI Agents  │ -> │   FastAPI App   │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • TABC Socrata  │    │ • Signal Scout   │    │ • /run_pipeline │
│ • HC ePermits   │    │ • Extractor      │    │ • /leads        │
│ • HCPH          │    │ • Resolver       │    │ • /candidates   │
│ • Socrata MCP   │    │ • ETA Agent      │    │ • /health       │
└─────────────────┘    │ • Verifier       │    │ • /stats        │
                       │ • Pitch Agent    │    └─────────────────┘
                       └──────────────────┘
```

**Processing Flow:**
1. **Discovery**: Scout agents collect raw signals from multiple sources
2. **Extraction**: LLM normalizes and structures raw data
3. **Resolution**: Hybrid entity resolution merges duplicates
4. **ETA Estimation**: Rules + LLM predict opening timeline  
5. **Verification**: Quality checks and lead scoring
6. **Pitch Generation**: LLM creates sales-ready content

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- NVIDIA GPU (recommended) or CPU fallback
- 8GB+ RAM, 4GB+ free disk space

### GPU Setup (Recommended)

```bash
# Clone repository
git clone <repository-url>
cd restaurant-leads-mvp

# Copy environment template
cp .env.example .env

# Start GPU services (vLLM + API)
make docker-up-gpu

# Wait for model loading (~2-3 minutes)
make logs-vllm

# Check health
curl http://localhost:8080/health
```

### CPU Setup (Development)

> **⚠️ CPU LIMITATIONS**: CPU fallback is for development only. Extraction and pitch generation on small batches work, but ETA/Resolver LLM steps are disabled by default to avoid timeouts. For production use, GPU is required.

```bash
# Start CPU services (Ollama + API)  
make docker-up-cpu

# Pull and run model
docker exec -it restaurant-leads-ollama ollama pull llama2:7b

# Check health
curl http://localhost:8080/health
```

### Run Pipeline

```bash
# Manual pipeline run
curl -X POST http://localhost:8080/run_pipeline \
  -H "Content-Type: application/json" \
  -d '{"county": "Harris", "days_ahead": 90}'

# View leads
curl "http://localhost:8080/leads?limit=10&min_confidence=0.65"

# Get specific lead
curl http://localhost:8080/leads/{lead_id}
```

## 🔧 Development Setup

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install

# Setup environment
cp .env.example .env
export $(cat .env | grep -v '#' | xargs)

# Run database migrations
python -c "from app.db import init_db; init_db()"

# Start local model server (choose one)
# vLLM (GPU):
vllm serve openai/gpt-oss-20b --host 0.0.0.0 --port 8000

# Ollama (CPU):
ollama serve &
ollama pull llama2:7b

# Start API server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test categories
pytest app/tests/test_rules.py -v           # Rules engine
pytest app/tests/test_matching.py -v        # Entity resolution  
pytest app/tests/test_ingestion.py -v       # Data ingestion
pytest app/tests/test_agents.py -v          # Agent logic

# Test with coverage
make test-coverage

# Lint code
make lint
```

## 📊 API Documentation

### Core Endpoints

#### `POST /run_pipeline`
Execute full pipeline for lead discovery.

**Request:**
```json
{
  "county": "Harris",
  "days_ahead": 90,
  "max_candidates": 1000
}
```

**Response:**
```json
{
  "pipeline_id": "uuid",
  "status": "completed",
  "leads_found": 25,
  "execution_time": 120.5
}
```

#### `GET /leads`
Retrieve leads with filtering and pagination.

**Parameters:**
- `limit`: Results per page (default: 50, max: 500)
- `offset`: Pagination offset  
- `min_confidence`: Minimum ETA confidence (0.0-1.0)
- `business_type`: Filter by type (restaurant, cafe, fast_food, etc.)
- `status`: Lead status (new, contacted, converted, etc.)
- `city`: Filter by city
- `days_ahead_min/max`: ETA range filtering

**Response:**
```json
{
  "leads": [
    {
      "id": "uuid",
      "venue_name": "Joe's Pizza Palace",
      "legal_name": "Joe's Pizza Palace LLC", 
      "address": "123 Main Street",
      "city": "Houston",
      "business_type": "restaurant",
      "cuisine_type": "pizza",
      "eta_days": 45,
      "eta_confidence": 0.85,
      "quality_score": 0.92,
      "opening_signals": ["TABC pending", "Building permit approved"],
      "pitch": {
        "how_to_pitch": "Focus on grand opening marketing",
        "pitch_text": "Congratulations on Joe's Pizza Palace opening...",
        "sms_text": "Hi! Saw Joe's Pizza opening in Houston..."
      },
      "created_at": "2024-01-15T10:30:00Z",
      "status": "new"
    }
  ],
  "total": 25,
  "limit": 50,
  "offset": 0
}
```

#### `GET /leads/{lead_id}`
Get specific lead details.

#### `PUT /leads/{lead_id}/status` 
Update lead status (new, contacted, converted, rejected).

#### `GET /candidates`
View raw candidates before lead qualification.

#### `GET /stats`
Pipeline execution statistics and health metrics.

#### `GET /health`
System health check including database and model server connectivity.

### Test Endpoints (Development)

- `POST /test_pipeline`: Run pipeline with mock data
- `GET /test_data`: Generate synthetic test data

## 📁 Project Structure

```
restaurant-leads-mvp/
├── app/
│   ├── agents/                 # CrewAI agents
│   │   ├── crew.py            # Orchestration
│   │   ├── agent_signal_scout.py
│   │   ├── agent_extractor.py
│   │   ├── agent_resolver.py
│   │   ├── agent_eta.py
│   │   ├── agent_verifier.py
│   │   └── agent_pitch.py
│   ├── api/
│   │   └── routes.py          # FastAPI routes
│   ├── pipelines/
│   │   └── run_pipeline.py    # Pipeline orchestration
│   ├── tools/                 # Data ingestion tools
│   │   ├── tabc_open_data.py
│   │   ├── hc_permits.py
│   │   ├── hc_food_permits.py
│   │   ├── socrata_mcp.py
│   │   └── geocode_local.py
│   ├── prompts/               # LLM prompt templates
│   ├── tests/                 # Comprehensive test suite
│   ├── main.py               # FastAPI app
│   ├── settings.py           # Configuration
│   ├── db.py                 # Database connection
│   ├── models.py             # SQLAlchemy ORM
│   ├── schemas.py            # Pydantic schemas
│   └── rules.py              # Deterministic ETA rules
├── docker/
│   ├── Dockerfile.gpu        # NVIDIA CUDA image
│   ├── Dockerfile.cpu        # CPU fallback image
│   └── entrypoint.sh         # Container startup
├── infra/
│   └── docker-compose.yml    # Multi-service orchestration
├── requirements.txt
├── Makefile                  # Development tasks
└── README.md
```

## 🗃️ Data Sources

### TABC (Texas Alcoholic Beverage Commission)
- **API**: Socrata Open Data
- **Data**: Pending license applications
- **Update Frequency**: Daily
- **Coverage**: Statewide, filtered to Harris County
- **Key Fields**: Business name, address, license type, status, dates
- **Incremental Sync**:
  - Time-windowed pulls using `application_date` or `status_date`
  - Watermark column: `last_status_date_seen` in database
  - `$limit/$offset` paging with configurable batch sizes
  - Last seen timestamp persisted after each successful batch

### Harris County ePermits
- **Method**: Playwright web scraping  
- **Data**: Building permits, tenant improvements
- **Update Frequency**: Real-time
- **Coverage**: Unincorporated Harris County
- **Key Fields**: Permit ID, type, description, address, status, dates
- **Durability Features**:
  - HTML snapshotting to `/data/snapshots/hc_permits/<id>.html`
  - Exponential backoff with jitter for retries
  - Explicit robots.txt compliance checking
  - Rate limiting with configurable delays

### Harris County Public Health
- **API**: ArcGIS REST + scraping fallback
- **Data**: Food service permits and plan reviews
- **Update Frequency**: Weekly
- **Coverage**: Harris County health jurisdiction  
- **Key Fields**: Business name, address, permit type, status

### Socrata MCP (Optional)
- **Method**: Model Context Protocol client
- **Purpose**: Dataset discovery and debugging
- **Coverage**: Multi-domain Socrata instances
- **Usage**: Development and data exploration
- **Default**: Disabled (`ENABLE_SOCRATA_MCP=false`)
- **Recommendation**: Use REST API fallback for production reliability

## 🤖 AI Components

### Local LLM Setup

**GPU (Recommended): vLLM + OpenAI-compatible API**
- Model: OpenAI GPT-OSS 20B or similar
- Memory: ~12GB VRAM required
- Performance: ~10 tokens/sec on RTX 4090
- Startup: 2-3 minutes model loading

**CPU Fallback: Ollama**
- Model: Llama 2 7B or similar
- Memory: ~8GB RAM
- Performance: ~2 tokens/sec on modern CPU
- Startup: ~30 seconds

### LLM Tasks

1. **Data Extraction**: Raw records → structured JSON
2. **Entity Resolution**: Ambiguous duplicate evaluation  
3. **ETA Adjustment**: Rules refinement with business context
4. **Pitch Generation**: Sales content creation

### Prompt Engineering

All prompts in `app/prompts/` with strict JSON schema enforcement:
- **Zero-shot prompts** with examples
- **Schema validation** for all outputs
- **Fallback handling** for API failures
- **Token limit management** for long inputs

## 🔄 Pipeline Details

### Address/Name Normalization

**Fuzzy Matching & Indexing:**
- **RapidFuzz Similarity**: String similarity scoring for venue names and addresses (0.0-1.0)
- **SQLite FTS5 Index**: Full-text search index on `venue_name` and `address` columns
- **Candidate Matching**: Similarity threshold >0.8 for automatic deduplication
- **Optional usaddress Parsing**: Pure-Python address canonicalization (free, no dependencies)

**Normalization Process:**
1. Extract raw venue names and addresses from all sources
2. Canonicalize addresses using usaddress parser (optional)
3. Index normalized data in SQLite FTS5 for fast fuzzy search
4. Calculate similarity scores for entity resolution candidates
5. Merge duplicates above similarity threshold

### Signal Discovery
- **TABC**: Pending Mixed Beverage, Wine & Beer permits
- **Permits**: Restaurant, food service, tenant finish permits  
- **Health**: Food service plan reviews and permits
- **Filtering**: Restaurant-related keywords, Harris County geo-fence

### Deterministic Rules (app/rules.py)

**ETA Estimation Rules:**
- TABC Original Pending: 30-45 days
- TABC Renewal/Transfer: 14-21 days
- Building Permit Approved: 45-75 days
- Health Plan Review: 60-90 days
- Multiple signals: confidence boost

**Down-weighting:**
- Renewal/transfer applications: -50% confidence
- Chain restaurants: +10 days ETA
- Historical delays: -20% confidence

**Lead Gating:**
- Minimum 0.65 ETA confidence
- Minimum 0.5 overall quality score
- Required fields validation

### Quality Scoring

**Factors (0.0-1.0 scale):**
- **Completeness**: 0.3 weight (required fields)
- **Source Quality**: 0.2 weight (official vs scraped)
- **ETA Confidence**: 0.3 weight (rules + LLM)
- **Signal Strength**: 0.2 weight (multiple sources)

**Thresholds:**
- Qualified Lead: ≥0.6 quality score
- High-Value Lead: ≥0.8 quality score  
- ETA Confidence: ≥0.65 minimum

## 🐳 Docker Deployment

### Multi-Service Architecture

```yaml
# GPU Setup
services:
  vllm:          # Model server (GPU)
  api:           # FastAPI application  
  worker:        # Background pipeline jobs

# CPU Setup  
services:
  ollama:        # Model server (CPU)
  api:           # FastAPI application
  worker:        # Background pipeline jobs
```

### Environment Variables

```bash
# Core Configuration
ENV=production
DB_URL=sqlite:///./data/leads.db

# Model Server
VLLM_BASE_URL=http://vllm:8000/v1
MODEL_ID=openai/gpt-oss-20b

# Data Sources  
SOCRATA_APP_TOKEN=optional_token_here
SOCRATA_BASE=https://data.texas.gov
ENABLE_SOCRATA_MCP=false
SOCRATA_DOMAIN=https://data.texas.gov
SOCRATA_DATASET_TABC_PENDING=

# Web Scraping
REQUESTS_TIMEOUT=30
CRAWL_DELAY_SECONDS=1
USER_AGENT=RestaurantLeadsMVP/1.0

# Logging
LOG_LEVEL=INFO
```

### Production Checklist

- [ ] GPU drivers and CUDA installed
- [ ] Sufficient disk space (>10GB for models)
- [ ] Memory allocation (12GB+ GPU, 16GB+ system)
- [ ] Network firewall rules (port 8080)
- [ ] Data backup strategy for SQLite
- [ ] Log rotation and monitoring
- [ ] Health check endpoints configured
- [ ] SSL/TLS termination (reverse proxy)

## 🧪 Testing Strategy

### Unit Tests (`app/tests/`)

- **Rules Engine** (`test_rules.py`): ETA calculations, milestone parsing
- **Entity Resolution** (`test_matching.py`): Address/name similarity, deduplication
- **Data Ingestion** (`test_ingestion.py`): API mocking, scraper validation
- **Agent Logic** (`test_agents.py`): LLM integration, task orchestration

### Integration Testing

```bash
# End-to-end pipeline
make test-integration

# API endpoints
make test-api

# Database operations
make test-db
```

### Mock Data and Fixtures

- Synthetic TABC records
- Mock Harris County permit responses
- Sample LLM API responses
- Test database fixtures

**Playwright Testing:**
- **CI Skip**: Playwright tests skipped in CI unless `PLAYWRIGHT_TESTS=true`
- **Record Fixtures**: Use `playwright codegen` to record browser interactions
- **Live Data Avoidance**: `test_ingestion.py` never hits real websites

**Test Fixtures:**
- **SoQL Fixture**: Tiny JSON fixture for TABC data testing
  ```json
  {
    "business_name": "Test Restaurant LLC",
    "address": "123 Test St, Houston, TX 77001",
    "license_type": "Mixed Beverage",
    "status": "Pending",
    "application_date": "2024-01-15T00:00:00Z"
  }
  ```
- **HTML Sample**: Static HTML fixture for ePermits parser testing
  ```html
  <div class="permit-record">
    <span class="permit-id">BP-2024-001</span>
    <span class="business-name">Sample Restaurant</span>
    <span class="address">456 Sample Ave, Houston, TX</span>
    <span class="status">Approved</span>
  </div>
  ```

## 📈 Monitoring & Observability

### Logging

**Structured JSON logs** with correlation IDs:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO", 
  "pipeline_id": "uuid",
  "agent": "signal_scout",
  "message": "Discovered 15 TABC candidates",
  "data": {"county": "Harris", "source": "tabc"}
}
```

### Health Checks

- Database connectivity
- Model server availability  
- External API status
- Disk space and memory usage

### Metrics

- Pipeline execution time
- Lead discovery rate
- Source data freshness
- Model inference latency
- API response times

## 🔧 Configuration

### Environment Files

**`.env.example`** - Template with all configuration options
**`.env`** - Local overrides (gitignored)
**`.env.production`** - Production values

### Settings Management

Pydantic Settings with environment variable loading:
```python
# app/settings.py
class Settings(BaseSettings):
    env: str = "dev"
    db_url: str = "sqlite:///./leads.db"
    vllm_base_url: str = "http://localhost:8000/v1"
    
    class Config:
        env_file = ".env"
```

### Feature Flags

```python
# Runtime configuration
ENABLE_LLM_PROCESSING: bool = True
ENABLE_WEB_SCRAPING: bool = True  
MAX_CANDIDATES_PER_RUN: int = 1000
CRAWL_POLITENESS_DELAY: float = 1.0
```

## 🔒 Security & Operations

### Access Control
- **API Key Authentication**: Optional API key support for production deployments
- **Network Restrictions**: Default to local-network-only access (127.0.0.1/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
- **CORS Policy**: Default `CORS=off` - enable only for specific frontend domains in production

### Data Protection
- **PII Scrubbing**: Automatic removal of sensitive data from application logs
- **No Data Retention**: Raw scraped HTML snapshots deleted after processing (configurable)
- **Secure Defaults**: All external APIs use HTTPS, no plaintext credentials in logs

### Observability
- **Metrics Endpoint**: Simple `/metrics` with counters for pipeline runs, API calls, and error rates
- **Health Checks**: Comprehensive `/health` endpoint with dependency status
- **Structured Logging**: JSON logs with correlation IDs, no sensitive data exposure

### Production Checklist
- [ ] Configure API key authentication
- [ ] Set network access restrictions
- [ ] Enable PII scrubbing in logs
- [ ] Configure metrics collection
- [ ] Set up log aggregation and monitoring
- [ ] Enable CORS only for required domains

### Common Issues

**GPU Memory Errors**
```bash
# Check VRAM usage
nvidia-smi

# Reduce model size or use CPU fallback
docker-compose -f infra/docker-compose.yml up cpu
```

**Model Loading Timeout**
```bash
# Check model download progress
docker logs restaurant-leads-vllm

# Increase timeout in entrypoint.sh
WAIT_TIMEOUT=300
```

**Scraping Rate Limits**
```bash
# Increase delays in .env
CRAWL_DELAY_SECONDS=3

# Check crawler politeness
curl -I https://permits.harriscountytx.gov
```

**Database Locks**
```bash
# Check active connections
sqlite3 leads.db ".schema"

# Restart with fresh DB
rm leads.db && make migrate
```

### Debug Mode

```bash
# Enable verbose logging
export LOG_LEVEL=DEBUG

# Run single pipeline step
python -m app.agents.agent_signal_scout

# Dry-run mode
export DRY_RUN=true
```

## 🔮 Roadmap & Extensions

### Near-term (v1.1)
- [ ] CLI interface for daily pipeline runs
- [ ] Webhook notifications for new leads
- [ ] Lead scoring refinements
- [ ] Additional cuisins and business types

### Medium-term (v1.5)
- [ ] Multi-county expansion (Dallas, Austin, San Antonio)
- [ ] Historical trend analysis and seasonality
- [ ] Lead conversion tracking and feedback loop
- [ ] Advanced entity resolution with embeddings

### Long-term (v2.0)
- [ ] Real-time streaming pipeline
- [ ] Interactive web dashboard
- [ ] Predictive models for lead quality
- [ ] Integration with CRM systems

### Stretch Goals
- [ ] Mobile app for field sales teams  
- [ ] Computer vision for construction progress
- [ ] Social media sentiment analysis
- [ ] Competitive landscape mapping

## 📞 Support & Contributing

### Getting Help
- Check troubleshooting section
- Review Docker logs: `make logs`  
- Enable debug logging: `LOG_LEVEL=DEBUG`
- Open GitHub issue with reproduction steps

### Contributing
1. Fork repository
2. Create feature branch
3. Add tests for new functionality
4. Run full test suite: `make test`
5. Update documentation
6. Submit pull request

### Code Style
- Black formatting: `make format`
- Flake8 linting: `make lint`
- Type hints required
- Docstrings for public functions

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

- **CrewAI** for agent orchestration framework
- **vLLM** for high-performance model serving
- **FastAPI** for modern Python web API
- **Playwright** for reliable web scraping
- **Socrata** for open data accessibility
- **Texas ABC** and **Harris County** for data transparency
