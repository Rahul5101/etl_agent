### AI-Driven ETL Pipeline: Autonomous Agents for Postgres to BigQuery Migration

This project implements an autonomous ETL (Extract-Transform-Load) pipeline using CrewAI agents, Google Gemini LLM, PostgreSQL, and Google BigQuery.
The workflow intelligently extracts data from Postgres, transforms it, and loads it into BigQuery — fully automated and orchestrated by AI agents.

## Project Overview

Traditional ETL systems require heavy orchestration and manual coding.
This project replaces that with AI-driven automation, where each agent has a role:

-> Extractor Agent → fetches data from Postgres

-> Transformer Agent → validates, cleans, processes, and prepares data

-> Loader Agent → loads the data into BigQuery

-> Supervisor → controls overall workflow

This creates a self-governing ETL pipeline that can adapt, debug itself, and scale easily.


+-------------------+      +----------------+      +------------------+
| Extractor Agent   | ---> | Transformer    | ---> | Loader Agent     |
| (Gemini + Tool)   |      | Agent          |      | BigQuery Loader  |
+-------------------+      +----------------+      +------------------+
          ^                          |
          |                          v
     +-----------+           +------------------+
     | Postgres  |           | BigQuery Dataset |
     +-----------+           +------------------+

1. Extractor uses PostgresFetchTool → runs SQL and returns JSON

2. Transformer Agent cleans & processes JSON

3. Loader Agent uses BigQueryLoadTool → loads into BigQuery table

4. Supervisor Agent monitors progress and ensures reliable execution

## Installation


1. clone the repo
    git clone https://github.com/yourusername/etl-agent.git
    cd etl-agent

2. create and activate the virtual environment
    python -m venv myenv
    myenv/Scripts/activate

3. install dependicies
    pip install -r requirements.txt

4. run the main.py file
    python run main.py (complete path of main.py file)


### Customizing

**Add your `gemini_api_key` into the `.env` file**

- Modify `src/etl_agent/main.py` to add custom inputs for your agents and tasks

# Modify - give your credential_path file to input into the main file (other two input are already give, you can change if you want)



## Understanding Your Crew

The etl-agent Crew is composed of multiple AI agents, each with unique roles, goals, and tools. These agents collaborate on a series of tasks, defined in `config/tasks.yaml`, leveraging their collective skills to achieve complex objectives. The `config/agents.yaml` file outlines the capabilities and configurations of each agent in your crew.

## Support

For support, questions, or feedback regarding the EtlAgent Crew or crewAI.
- Visit our [documentation](https://docs.crewai.com)
- Reach out to us through our [GitHub repository](https://github.com/joaomdmoura/crewai)
- [Join our Discord](https://discord.com/invite/X4JWnZnxPb)
- [Chat with our docs](https://chatg.pt/DWjSBZn)

Let's create wonders together with the power and simplicity of crewAI.
