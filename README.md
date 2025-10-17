# GitHub GPT-4.1 LangChain Integration

This project demonstrates how to integrate GitHub's hosted GPT-4.1 model with LangChain for building AI applications.

> Note: The database analyzer UI also supports Google Gemini via `langchain-google-genai`. Set `GOOGLE_API_KEY` to use Gemini models (default: `gemini-2.5-flash`).

## Features

- Custom LangChain integration for GitHub's hosted GPT-4.1 model
- Database analysis agent using the GitHub model
- Multiple usage examples (chat, streaming, agents, etc.)
- SQL database toolkit integration

## Setup

### 1. Environment Variables

Create a `.env` file in the root directory:

```bash
# GitHub API token for accessing the hosted model
GITHUB_TOKEN=your_github_token_here

# Database credentials
DB_PASSWORD=your_database_password

# Google Gemini (optional for DB analyzer)
GOOGLE_API_KEY=your_gemini_api_key
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Get GitHub Token

To access GitHub's hosted models, you need a GitHub token:

1. Go to [GitHub Settings > Developer settings > Personal access tokens](https://github.com/settings/tokens)
2. Generate a new token with appropriate permissions
3. Add the token to your `.env` file

## Usage Examples

### Basic Chat

```python
from src.github_llm import GitHubChatModel
from langchain_core.messages import HumanMessage, SystemMessage

llm = GitHubChatModel(temperature=0.7)

messages = [
    SystemMessage(content="You are a helpful AI assistant."),
    HumanMessage(content="What is the capital of France?")
]

response = llm.invoke(messages)
print(response.content)
```

### Database Analysis Agent

```python
from src.main_agent import agent

question = "List all tables and their definitions in the database."

for step in agent.stream(
    {"messages": [{"role": "user", "content": question}]},
    stream_mode="values",
):
    step["messages"][ -1].pretty_print()
```

### Prompt Templates

```python
from src.github_llm import GitHubChatModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

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
```

### Streaming

```python
from src.github_llm import GitHubChatModel
from langchain_core.prompts import ChatPromptTemplate

llm = GitHubChatModel(temperature=0.5)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a creative writer."),
    ("human", "Write a short story about {topic}.")
])

chain = prompt | llm

for chunk in chain.stream({"topic": "a robot learning to paint"}):
    if hasattr(chunk, 'content'):
        print(chunk.content, end="", flush=True)
```

## Running Examples

### Run all examples:

```bash
python src/github_examples.py
```

### Run the database agent:

```bash
python src/main_agent.py
```

## Customization

### Model Parameters

You can customize the GitHub model with various parameters:

```python
llm = GitHubChatModel(
    temperature=0.7,      # Controls randomness (0.0 to 1.0)
    max_tokens=4000,      # Maximum tokens in response
    model="openai/gpt-4.1"  # Model name (currently only gpt-4.1 is supported)
)
```

### Error Handling

The integration includes proper error handling:

```python
try:
    response = llm.invoke(messages)
except Exception as e:
    print(f"Error: {e}")
    # Check if GITHUB_TOKEN is set correctly
```

## Architecture

### GitHubChatModel Class

The `GitHubChatModel` class extends LangChain's `BaseChatModel` and provides:

- **Message Conversion**: Converts LangChain messages to Azure format
- **Parameter Handling**: Manages temperature, max_tokens, and other parameters
- **Error Handling**: Proper exception handling for API calls
- **Streaming Support**: Basic streaming implementation

### Integration Points

1. **LangChain Core**: Extends `BaseChatModel` for full LangChain compatibility
2. **Azure Inference**: Uses Azure's inference client to communicate with GitHub's hosted models
3. **Environment Management**: Uses dotenv for secure token management

## Troubleshooting

### Common Issues

1. **GITHUB_TOKEN not set**: Make sure your `.env` file contains the correct token
2. **Network issues**: Ensure you have internet access to reach GitHub's inference endpoint
3. **Rate limiting**: GitHub may have rate limits on model usage

### Debug Mode

To enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

llm = GitHubChatModel()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## References

- [GitHub Models Marketplace](https://github.com/marketplace/models/azure-openai/gpt-4-1/playground/code)
- [LangChain Documentation](https://python.langchain.com/)
- [Azure AI Inference Documentation](https://learn.microsoft.com/en-us/azure/ai-services/inference/) 