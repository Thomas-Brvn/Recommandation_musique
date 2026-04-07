"""
Agent RAG pour les Festivals 2026
"""

import os
from dotenv import load_dotenv

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# Handle both direct execution and module import
try:
    from .tools import search_festival_store
    from .prompt import agent_prompt
except ImportError:
    from tools import search_festival_store
    from prompt import agent_prompt

# Charger les variables d'environnement
load_dotenv()

tools = [search_festival_store]

# ============================================================================
# CONFIGURATION DU PROMPT POUR LANGCHAIN
# ============================================================================

# Créer le prompt template compatible avec LangChain
prompt = ChatPromptTemplate.from_messages([
    ("system", agent_prompt()),
    MessagesPlaceholder(variable_name="chat_history", optional=True),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# ============================================================================
# CONFIGURATION DE L'AGENT
# ============================================================================

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    convert_system_message_to_human=True
)

# Créer l'agent avec tool calling
agent = create_tool_calling_agent(
    llm=llm,
    tools=tools,
    prompt=prompt,
)

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=False,  
    handle_parsing_errors=True,
    max_iterations=3,
)


def ask(question: str, chat_history: list = None) -> str:
    """
    Pose une question à l'agent.

    Args:
        question: La question de l'utilisateur
        chat_history: Historique de conversation (optionnel)

    Returns:
        La réponse de l'agent
    """
    response = agent_executor.invoke({
        "input": question,
        "chat_history": chat_history or [],
    })
    
    return response.get("output", "Désolé, je n'ai pas pu générer une réponse.")
   
if __name__ == "__main__":
    while True:
        question = input("Question (q pour quitter): ")
        if question.lower() == "q":
            break
        print(ask(question))