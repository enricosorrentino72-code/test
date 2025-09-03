from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain.agents import initialize_agent, AgentType,AgentExecutor, create_tool_calling_agent
from langgraph.prebuilt import create_react_agent
from ai_tools_shared import demographics, get_weather, add, multiply, system_prompt, create_llm, tools

llm = create_llm()
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



