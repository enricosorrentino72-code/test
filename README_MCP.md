# MCP Server Setup

## Overview

This repository includes a Model Context Protocol (MCP) server that exposes AI tool functions for use with LLM applications. The server registers Python functions as MCP tools, making them accessible to compatible LLM clients.

## Installation

1. Install MCP dependencies:
```bash
pip install -r requirements.txt
```

2. Verify installation:
```bash
python -c "import mcp; print('MCP installed successfully')"
```

## Running the MCP Server

### Local Development
```bash
python mcp_server.py
```

### Testing with MCP Inspector
```bash
mcp dev mcp_server.py
```

### Production Configuration
Use the provided `mcp_config.json` file with your MCP-compatible client:
```json
{
  "mcpServers": {
    "ai-tools": {
      "command": "python",
      "args": ["mcp_server.py"],
      "env": {}
    }
  }
}
```

## Available Tools

The MCP server exposes the following tools:

### `get_demographics(location: str) -> dict`
Get demographic information for a specified location.
- **Input**: Location name as string
- **Output**: Dictionary with demographics data and location
- **Example**: `get_demographics("Paris")` returns `{"demographics": "250000000 Persons", "location": "Paris"}`

### `get_weather_info(location: str) -> dict`
Get weather information for a specified location.
- **Input**: Location name as string  
- **Output**: Dictionary with temperature and location
- **Example**: `get_weather_info("Tokyo")` returns `{"temperature": "23°C", "location": "Tokyo"}`

### `add_numbers(a: int, b: int) -> int`
Add two integers together.
- **Input**: Two integers
- **Output**: Sum as integer
- **Example**: `add_numbers(5, 3)` returns `8`

### `multiply_numbers(a: int, b: int) -> int`
Multiply two integers together.
- **Input**: Two integers
- **Output**: Product as integer
- **Example**: `multiply_numbers(4, 3)` returns `12`

### `download_ml_model(url: str, output_path: str, min_size_mb: int = 1) -> bool`
Download a machine learning model from a URL.
- **Input**: URL, output path, minimum size in MB (optional, default 1)
- **Output**: Boolean indicating success/failure
- **Example**: `download_ml_model("http://example.com/model.pt", "model.pt")` returns `True` on success

## Testing

Run the comprehensive test suite:
```bash
# Run all tests
python -m pytest -v

# Run with coverage
python -m pytest --cov=. --cov-report=html

# Run specific test file
python -m pytest test_mcp_server.py -v
```

## Integration with LLM Clients

The MCP server can be integrated with any MCP-compatible LLM client. The tools will appear as callable functions that the LLM can use to:

- Retrieve demographic information for locations
- Get weather data for cities
- Perform mathematical calculations
- Download machine learning models

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed with `pip install -r requirements.txt`
2. **Port Conflicts**: The server uses the default MCP port; ensure no other MCP servers are running
3. **Tool Registration**: Verify all tools are properly decorated with `@mcp.tool()` in `mcp_server.py`

### Debug Mode
Run the server with debug output:
```bash
python mcp_server.py --debug
```

## Architecture

The MCP server is built using the FastMCP framework and imports functions from:
- `ai_tools_shared.py` - Core tool functions (demographics, weather, math operations)
- `Download.py` - Model download functionality

All functions are wrapped as MCP tools while preserving their original functionality and type hints.
