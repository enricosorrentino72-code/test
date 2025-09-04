from langchain_ollama import ChatOllama
from langchain_core.tools import tool

system_prompt = """
You are a smart assistant that helps users by calling tools when needed.

You have access to the following tools:
- `weather`: use this tool to answer questions about weather conditions in a specific location.
- `demographics`: use this tool to answer questions about population, age, gender distribution, or other demographic statistics of a specific place.
When a user asks a question, decide whether a tool is needed. If yes, respond ONLY with a JSON object in the following format:

{{
  "tool": "<tool_name>",
  "args": {{
    <arguments as key-value pairs>
  }}
}}

Do not explain or include any additional text outside the JSON.

If no tool is needed, respond with a helpful natural language answer.

Examples:
User: What's the weather like in Tokyo?
Response:
{{
  "tool": "weather",
  "args": {{
    "location": "Tokyo"
  }}
}}

User: Tell me the average age in Berlin.
Response:
{{
  "tool": "demographics",
  "args": {{
    "location": "Berlin",
    "stat": "average_age"
  }}
}}
Your goal is to be accurate, concise, and only call tools when strictly necessary.
"""

@tool
def demographics(location: str) -> dict:
    """
    Provide the demographics information of a given location.
    Input Parameter Location:str
    Output Parameter:dict
    """
    return {"demographics": "250000000 Persons", "location": location}

@tool
def get_weather(location: str) -> dict:
    """
    Returns the current temperature for a given location.
    Input Parameter Location:str
    Output Parameter:dict
    """
    return {"temperature": "23°C", "location": location}

@tool
def add(a: int, b: int) -> int:
    """
    Add two integers.
    Args:
        a: First integer
        b: Second integer
    """
    return a + b

@tool
def multiply(a: int, b: int) -> int:
    """
    Multiply two integers.
    Args:
        a: First integer
        b: Second integer
    """
    return a * b

def create_llm(model="llama3.2", temperature=0, verbose=True, **kwargs):
    """
    Create a standardized ChatOllama instance with common configuration.
    """
    return ChatOllama(
        model=model,
        temperature=temperature,
        verbose=verbose,
        **kwargs
    )

tools = [multiply, add, get_weather, demographics]
