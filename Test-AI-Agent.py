from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain.agents import initialize_agent, AgentType,AgentExecutor, create_tool_calling_agent
from langgraph.prebuilt import create_react_agent
from ai_tools_shared import demographics, get_weather, add, multiply, system_prompt, create_llm, tools

llm = create_llm()


prompt = ChatPromptTemplate.from_messages(
    [
"system", system_prompt,
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools,verbose=True)

# Query per il tool Multiply
query_demographics = "What are the update demographics information in Paris Today ? You must call the service (Function) defined above tool (demographics) and Return a dictionary with the updated demographics information"
response_demographics=agent_executor.invoke({"input": query_demographics})

query_weather = "What are the weather information in Paris Today ? You must call the service (Function) defined above tool (query_weather) and Return a dictionary with the updated weather information"
response_weather=agent_executor.invoke({"input": query_weather})      



