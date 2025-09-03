from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticToolsParser
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from ai_tools_shared import demographics, get_weather, add, multiply, system_prompt, create_llm, tools

llm = create_llm()
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
