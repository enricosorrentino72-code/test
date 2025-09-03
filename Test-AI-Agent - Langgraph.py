from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain.agents import initialize_agent, AgentType,AgentExecutor, create_tool_calling_agent
from langgraph.prebuilt import create_react_agent
from ai_tools_shared import demographics, get_weather, add, multiply, system_prompt, create_llm, tools

llm = create_llm()
print("##### LLM-Tool-Defined ######")

query_demographics = "What are the update demographics information in Paris Today ? You must call the service (Function) defined above tool (demographics) and Return a dictionary with the updated demographics information"
langgraph_agent_executor = create_react_agent(llm, tools,prompt=system_prompt)
print("##### Agent Executor (Langgraph) ######")

messages = langgraph_agent_executor.invoke({"messages": [("human", query_demographics)]})
print("##### Message Demographics ######")
print(messages)

query_weather = "What are the weather information in Paris Today ? You must call the service (Function) defined above tool (query_weather) and Return a dictionary with the updated weather information"
messages = langgraph_agent_executor.invoke({"messages": [("human", query_weather)]})
print("##### Message Weather ######")
print(messages)






