"""
MCP Server for AI Tools
Registers Python functions as MCP tools for use with LLM applications.
"""

from mcp.server.fastmcp import FastMCP
from ai_tools_shared import demographics, get_weather, add, multiply, create_llm
from Download import download_model

mcp = FastMCP("AI Tools Server")

@mcp.tool()
def get_demographics(location: str) -> dict:
    """Get demographic information for a location."""
    return demographics(location)

@mcp.tool()
def get_weather_info(location: str) -> dict:
    """Get weather information for a location."""
    return get_weather(location)

@mcp.tool()
def add_numbers(a: int, b: int) -> int:
    """Add two integers together."""
    return a + b

@mcp.tool()
def multiply_numbers(a: int, b: int) -> int:
    """Multiply two integers together."""
    return a * b

@mcp.tool()
def download_ml_model(url: str, output_path: str, min_size_mb: int = 1) -> bool:
    """Download a machine learning model from a URL."""
    return download_model(url, output_path, min_size_mb)

if __name__ == "__main__":
    mcp.run()
