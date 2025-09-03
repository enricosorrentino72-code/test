from langchain_ollama import ChatOllama
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate

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
    Restituisce la temperatura attuale per una data posizione.
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
llm_with_tools = llm.bind_tools(tools)
query_weather="What are the weather information in Paris Today ?"
query_demographics = "What are the update demographics information in Paris Today"
print("##### LLM-TOOL Registered ######")
messages = [HumanMessage(query_demographics)]
ai_msg = llm_with_tools.invoke(messages)
messages.append(ai_msg)
print("######## Messages Tool Call #######")
print(messages)

#Tool Execution (Manual)
for tool_call in ai_msg.tool_calls:
    selected_tool = {"add": add, "multiply": multiply,"get_weather": get_weather,"demographics": demographics}[tool_call["name"].lower()]
    tool_msg = selected_tool.invoke(tool_call)
    messages.append(tool_msg)
messages
print("######## Messages Tool Message#######")
print(messages)

chat_prompt = ChatPromptTemplate.from_messages(
    [("system", system_prompt), ("placeholder", "{chat_history}")])
chain = chat_prompt | llm_with_tools
#Invoke the Chain
response = chain.invoke({"chat_history":messages},verbose=True)
messages.append(response)
print("######## Messages Response Chat Template#######")
print(messages)
