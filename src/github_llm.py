from typing import Any, List, Optional, Iterator
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage as AzureSystemMessage, UserMessage as AzureUserMessage
from azure.core.credentials import AzureKeyCredential
import os
from dotenv import load_dotenv

load_dotenv()

class GitHubChatModel(BaseChatModel):
    """LangChain integration for GitHub's hosted GPT-4.1 model."""
    
    endpoint: str = "https://models.github.ai/inference"
    model: str = "openai/gpt-4.1"
    temperature: float = 0.0
    max_tokens: Optional[int] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.token = os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError("GITHUB_TOKEN environment variable is required")
        
        self.client = ChatCompletionsClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.token),
        )
    
    @property
    def _llm_type(self) -> str:
        return "github_gpt4"
    
    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[Any] = None,
        **kwargs: Any,
    ) -> ChatResult:
        # Convert LangChain messages to Azure format
        azure_messages = []
        for message in messages:
            if isinstance(message, SystemMessage):
                azure_messages.append(AzureSystemMessage(content=message.content))
            elif isinstance(message, HumanMessage):
                azure_messages.append(AzureUserMessage(content=message.content))
            elif isinstance(message, AIMessage):
                # For assistant messages, we'll include them as user messages with role
                azure_messages.append(AzureUserMessage(content=f"Assistant: {message.content}"))
        
        # Prepare parameters
        params = {
            "model": self.model,
            "messages": azure_messages,
            "temperature": self.temperature,
        }
        
        if self.max_tokens:
            params["max_tokens"] = self.max_tokens
        
        if stop:
            params["stop"] = stop
        
        # Add any additional kwargs
        params.update(kwargs)
        
        try:
            response = self.client.chat.completions.create(**params)
            
            # Extract the response content
            if response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content
                generation = ChatGeneration(message=AIMessage(content=content))
                return ChatResult(generations=[generation])
            else:
                raise ValueError("No response content received from GitHub model")
                
        except Exception as e:
            raise Exception(f"Error calling GitHub model: {str(e)}")
    
    async def _agenerate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[Any] = None,
        **kwargs: Any,
    ) -> ChatResult:
        # For now, we'll use the synchronous version
        # In a production environment, you might want to implement proper async
        return self._generate(messages, stop, run_manager, **kwargs) 