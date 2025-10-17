"""
Examples of using GitHub's hosted GPT-4.1 model with LangChain
"""
from dotenv import load_dotenv
import os
from github_llm import GitHubChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

load_dotenv()

def basic_chat_example():
    """Basic chat example with GitHub model"""
    llm = GitHubChatModel(temperature=0.7)
    
    messages = [
        SystemMessage(content="You are a helpful AI assistant."),
        HumanMessage(content="What is the capital of France?")
    ]
    
    response = llm.invoke(messages)
    print("Basic Chat Example:")
    print(f"Response: {response.content}")
    print("-" * 50)

def prompt_template_example():
    """Using prompt templates with GitHub model"""
    llm = GitHubChatModel(temperature=0.3)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an expert {domain} consultant."),
        ("human", "Explain {topic} in simple terms.")
    ])
    
    chain = prompt | llm | StrOutputParser()
    
    result = chain.invoke({
        "domain": "database",
        "topic": "SQL indexing"
    })
    
    print("Prompt Template Example:")
    print(f"Response: {result}")
    print("-" * 50)

def streaming_example():
    """Streaming example with GitHub model"""
    llm = GitHubChatModel(temperature=0.5)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a creative writer."),
        ("human", "Write a short story about {topic}.")
    ])
    
    chain = prompt | llm
    
    print("Streaming Example:")
    print("Generating story...")
    
    for chunk in chain.stream({"topic": "a robot learning to paint"}):
        if hasattr(chunk, 'content'):
            print(chunk.content, end="", flush=True)
    print("\n" + "-" * 50)

def function_calling_example():
    """Example showing how to use the model with structured output"""
    llm = GitHubChatModel(temperature=0)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a data analyst. Analyze the given data and provide insights."),
        ("human", "Analyze this data: {data}")
    ])
    
    chain = prompt | llm | StrOutputParser()
    
    sample_data = """
    Sales Data:
    - Q1: $100,000
    - Q2: $120,000
    - Q3: $95,000
    - Q4: $150,000
    """
    
    result = chain.invoke({"data": sample_data})
    
    print("Function Calling Example:")
    print(f"Analysis: {result}")
    print("-" * 50)

def agent_example():
    """Example of using GitHub model in a simple agent pattern"""
    from langchain_core.tools import tool
    
    @tool
    def get_weather(city: str) -> str:
        """Get weather information for a city"""
        # This is a mock implementation
        return f"Weather in {city}: Sunny, 25°C"
    
    llm = GitHubChatModel(temperature=0)
    
    # Create a simple agent
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You have access to weather information. Use the get_weather tool when asked about weather."),
        ("human", "{input}")
    ])
    
    chain = prompt | llm.bind_tools([get_weather])
    
    response = chain.invoke({"input": "What's the weather like in Paris?"})
    
    print("Agent Example:")
    print(f"Response: {response.content}")
    print("-" * 50)

if __name__ == "__main__":
    print("GitHub GPT-4.1 LangChain Integration Examples")
    print("=" * 60)
    
    try:
        basic_chat_example()
        prompt_template_example()
        streaming_example()
        function_calling_example()
        agent_example()
        
        print("\nAll examples completed successfully!")
        
    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure GITHUB_TOKEN is set in your .env file") 