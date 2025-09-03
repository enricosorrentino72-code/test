from langchain_ollama import ChatOllama
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain.agents import initialize_agent, AgentType,AgentExecutor, create_tool_calling_agent
from langgraph.prebuilt import create_react_agent
 
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

llm = ChatOllama(
    model="llama3.2",
    temperature=0,
    verbose=True
    # other params...
    
)
print("##### LLM-Created ######")

@tool
def demographics(location:str)->dict:
    """
    Provide the demographics information of a given location.
    Input Parameter Location:str
    Output Parameter:dict
    """
    print("###############demographics!!!!!###########")
    return {"demographics": "250000000 Persons", "location": location}

# Funzione per ottenere informazioni meteo, correttamente definita con l'annotazione @tool
@tool
def get_weather(location: str)->dict:
    """
    Returns the current temperature for a given location.
    Input Parameter Location:str
    Output Parameter:dict
    """
    print("#######Entry Get Weather!!!!!#########")
    return {"temperature": "23°C", "location": location}


@tool
def add(a: int, b: int) -> int:
   """
   Add two integers.
    Args:
        a: First integer
        b: Second integer
    """
   print("######## Add Function #######")
   return a + b


@tool
def multiply(a: int, b: int) -> int:
   """
    Multiply two integers.

        Args:
            a: First integer
            b: Second integer
    """
   print("######## Multiply Function #######")
   return a * b

tools =  [multiply,add,get_weather,demographics]
print("##### LLM-Tool-Registered ######")


prompt = ChatPromptTemplate.from_messages(
    [
"system", system_prompt,
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)
print("##### ChatPromptTemplate ######")
agent = create_tool_calling_agent(llm, tools, prompt)
print("##### Create Tool Calling Agent ######")
agent_executor = AgentExecutor(agent=agent, tools=tools,verbose=True)
print("##### Create Agent Executor ######")

# Query per il tool Multiply
query_demographics = "What are the update demographics information in Paris Today ? You must call the service (Function) defined above tool (demographics) and Return a dictionary with the updated demographics information"
response_demographics=agent_executor.invoke({"input": query_demographics})
print("#############Agent Executor Response Demographics###########")
print(response_demographics) 

# Se vuoi anche testare il tool get_weather:
query_weather = "What are the weather information in Paris Today ? You must call the service (Function) defined above tool (query_weather) and Return a dictionary with the updated weather information"
response_weather=agent_executor.invoke({"input": query_weather})
print("#############Agent Executor Response Weather###########")
print(response_weather) 



