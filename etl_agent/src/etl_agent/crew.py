from crewai import Agent, Crew, Process, Task,LLM
from crewai.project import CrewBase, agent, crew, task
from crewai.agents.agent_builder.base_agent import BaseAgent
from typing import List
from tools.postgresfetch_tool import PostgresFetchTool
from tools.schematransformer_tool import SchemaTransformerTool
from tools.schemavalidator_tool import ValidatorTool
from tools.bigquery_tool import BigQueryLoadTool
from dotenv import load_dotenv
import os


load_dotenv()  
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# If you want to run a snippet of code before or after the crew starts,
# you can use the @before_kickoff and @after_kickoff decorators
# https://docs.crewai.com/concepts/crews#example-crew-class-with-decorators

@CrewBase
class ETLWorkflow():
    """ETL Workflow Crew"""

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"


    @agent
    def extractor_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['extractor_agent'],
            verbose=True,
            tools = [PostgresFetchTool()],
            llm="gemini/gemini-2.0-flash"

        )

    @agent
    def transformer_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['transformer_agent'],
            verbose=True,
            tools = [SchemaTransformerTool()],
            llm="gemini/gemini-2.0-flash"

        )

    @agent
    def validator_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['validator_agent'],
            verbose=True,
            tools = [ValidatorTool()],
            llm="gemini/gemini-2.0-flash"

        )

    @agent
    def loader_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['loader_agent'],
            verbose=True,
            tools = [BigQueryLoadTool()],
            llm="gemini/gemini-2.0-flash"

        )

    @task
    def fetch_task(self) -> Task:
        return Task(
            config=self.tasks_config['fetch_task'],
            
        )

    @task
    def transform_task(self) -> Task:
        return Task(
            config=self.tasks_config['transform_task'],
        )

    @task
    def validate_task(self) -> Task:
        return Task(
            config=self.tasks_config['validate_task'],
        )

    @task
    def load_task(self) -> Task:
        return Task(
            config=self.tasks_config['load_task'],
            # context=self.transform_task(),
        )

    @crew
    def crew(self) -> Crew:
        """Creates the EtlAgent crew"""
        # To learn how to add knowledge sources to your crew, check out the documentation:
        # https://docs.crewai.com/concepts/knowledge#what-is-knowledge

        return Crew(
            agents=self.agents, # Automatically created by the @agent decorator
            tasks=self.tasks, # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
            # process=Process.hierarchical, # In case you wanna use that instead https://docs.crewai.com/how-to/Hierarchical/
        )
