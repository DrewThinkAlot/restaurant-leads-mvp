import json
import subprocess
import logging
from typing import List, Dict, Any, Optional
import asyncio
import aiohttp
from contextlib import asynccontextmanager

from ..settings import settings

logger = logging.getLogger(__name__)


class SocrataMCPClient:
    """Optional MCP client wrapper for Socrata discovery and ad-hoc queries."""
    
    def __init__(self):
        self.mcp_server_command = None  # Would be configured if MCP server exists
        self.fallback_enabled = True
        self.timeout = settings.requests_timeout
    
    async def discover_datasets(self, query: str = "license permit") -> List[Dict[str, Any]]:
        """Discover datasets using MCP with fallback to REST."""
        
        try:
            # Try MCP discovery first
            if self.mcp_server_command:
                result = await self._mcp_discover(query)
                if result:
                    return result
        except Exception as e:
            logger.warning(f"MCP discovery failed: {e}")
        
        # Fallback to REST discovery
        if self.fallback_enabled:
            return await self._fallback_discover(query)
        
        return []
    
    async def query_dataset(self, dataset_id: str, soql_query: str) -> List[Dict[str, Any]]:
        """Query dataset using MCP with fallback to REST."""
        
        try:
            # Try MCP query first
            if self.mcp_server_command:
                result = await self._mcp_query(dataset_id, soql_query)
                if result:
                    return result
        except Exception as e:
            logger.warning(f"MCP query failed: {e}")
        
        # Fallback to REST query
        if self.fallback_enabled:
            return await self._fallback_query(dataset_id, soql_query)
        
        return []
    
    async def _mcp_discover(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """Use MCP to discover datasets."""
        
        # This would implement actual MCP protocol communication
        # For now, return None to trigger fallback
        return None
    
    async def _mcp_query(self, dataset_id: str, soql_query: str) -> Optional[List[Dict[str, Any]]]:
        """Use MCP to query dataset."""
        
        # This would implement actual MCP protocol communication
        # For now, return None to trigger fallback
        return None
    
    async def _fallback_discover(self, query: str) -> List[Dict[str, Any]]:
        """Fallback to REST API for discovery."""
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                search_url = f"{settings.socrata_base}/api/catalog/v1"
                params = {
                    'q': query,
                    'only': 'datasets',
                    'limit': 100
                }
                
                async with session.get(search_url, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    datasets = []
                    for result in data.get('results', []):
                        resource = result.get('resource', {})
                        datasets.append({
                            'id': resource.get('id'),
                            'name': resource.get('name'),
                            'description': resource.get('description'),
                            'updated_at': resource.get('updatedAt'),
                            'row_count': resource.get('page_views', {}).get('page_views_total'),
                            'columns': len(resource.get('columns_field_name', [])),
                            'source': 'socrata_rest_fallback'
                        })
                    
                    return datasets
                    
        except Exception as e:
            logger.error(f"Fallback discovery failed: {e}")
            return []
    
    async def _fallback_query(self, dataset_id: str, soql_query: str) -> List[Dict[str, Any]]:
        """Fallback to REST API for queries."""
        
        try:
            # Import here to avoid circular dependency
            from .tabc_open_data import tabc_client
            
            # Use existing TABC client for fallback queries
            # This is a simplified approach - in practice, you'd want a more generic client
            
            if 'tabc' in dataset_id.lower() or 'license' in dataset_id.lower():
                # Route to TABC client
                records = tabc_client.query_pending_licenses(county="Harris", days_back=90)
                return [{'source': 'tabc_fallback', **record} for record in records]
            
            # For other datasets, implement generic Socrata query
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                query_url = f"{settings.socrata_base}/resource/{dataset_id}.json"
                params = {'$query': soql_query}
                
                if settings.socrata_app_token:
                    headers = {'X-App-Token': settings.socrata_app_token}
                else:
                    headers = {}
                
                async with session.get(query_url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    return [{'source': 'socrata_rest_fallback', **record} for record in data]
                    
        except Exception as e:
            logger.error(f"Fallback query failed: {e}")
            return []
    
    def set_mcp_server_command(self, command: List[str]):
        """Configure MCP server command if available."""
        self.mcp_server_command = command
    
    def disable_fallback(self):
        """Disable fallback to REST API."""
        self.fallback_enabled = False


class SocrataMCPTool:
    """Tool wrapper for agent use."""
    
    def __init__(self):
        self.client = SocrataMCPClient()
    
    async def discover_restaurant_datasets(self) -> List[Dict[str, Any]]:
        """Discover datasets relevant to restaurant licensing."""
        
        queries = [
            "restaurant license permit",
            "TABC alcohol license",
            "food service permit",
            "business license"
        ]
        
        all_datasets = []
        for query in queries:
            datasets = await self.client.discover_datasets(query)
            all_datasets.extend(datasets)
        
        # Deduplicate by ID
        seen_ids = set()
        unique_datasets = []
        for dataset in all_datasets:
            dataset_id = dataset.get('id')
            if dataset_id and dataset_id not in seen_ids:
                seen_ids.add(dataset_id)
                unique_datasets.append(dataset)
        
        return unique_datasets
    
    async def query_for_candidates(self, dataset_id: str, county: str = "Harris") -> List[Dict[str, Any]]:
        """Query dataset for restaurant candidates."""
        
        # Build SoQL query based on dataset type
        soql_queries = [
            f"SELECT * WHERE county = '{county}' AND status LIKE '%pending%'",
            f"SELECT * WHERE county = '{county}' AND application_date >= '2024-01-01'",
            f"SELECT * WHERE county = '{county}'"
        ]
        
        for soql in soql_queries:
            try:
                results = await self.client.query_dataset(dataset_id, soql)
                if results:
                    return results
            except Exception as e:
                logger.warning(f"Query failed: {e}")
                continue
        
        return []


# Global MCP tool instance
socrata_mcp_tool = SocrataMCPTool()


def configure_mcp_server(server_command: List[str]):
    """Configure MCP server if available."""
    socrata_mcp_tool.client.set_mcp_server_command(server_command)


async def discover_and_query_datasets(county: str = "Harris") -> List[Dict[str, Any]]:
    """Discover and query relevant datasets for restaurant candidates."""
    
    try:
        # Discover datasets
        datasets = await socrata_mcp_tool.discover_restaurant_datasets()
        
        logger.info(f"Discovered {len(datasets)} datasets")
        
        all_candidates = []
        
        # Query promising datasets
        for dataset in datasets[:5]:  # Limit to top 5 to avoid rate limits
            dataset_id = dataset.get('id')
            if not dataset_id:
                continue
            
            try:
                candidates = await socrata_mcp_tool.query_for_candidates(dataset_id, county)
                
                # Tag candidates with dataset info
                for candidate in candidates:
                    candidate['dataset_source'] = {
                        'id': dataset_id,
                        'name': dataset.get('name'),
                        'description': dataset.get('description')
                    }
                
                all_candidates.extend(candidates)
                
                # Respect rate limits
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.warning(f"Failed to query dataset {dataset_id}: {e}")
                continue
        
        return all_candidates
        
    except Exception as e:
        logger.error(f"Dataset discovery and query failed: {e}")
        return []
