import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI
from agents.optimization.datalayer.db_service import PostgresClient 

load_dotenv()

# VERIFIED CONFIGURATION
NEON_CONFIG = {
    "host": "ep-restless-union-ah7k5fyc-pooler.c-3.us-east-1.aws.neon.tech",
    "database": "neondb",
    "user": "neondb_owner",
    "password": "npg_Qh7UbgSrEzH9", 
    "port": "5432"
}

db_client = PostgresClient(NEON_CONFIG)
client = AzureOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview"
)

def handle_query_optimization(user_query: str) -> str:
    # 1. Resolve Columns & Schema
    real_columns = db_client.get_query_metadata(user_query)
    
    # 2. Extract Live Telemetry (Plan, Cost, I/O, Indexes)
    full_plan = db_client.investigate(user_query)
    current_effort = full_plan.get('total_cost', 0)

    prompt = f"""
    ROLE: Senior PostgreSQL SRE & Performance Architect
    USER SQL: {user_query}
    METADATA: {real_columns}
    LIVE TELEMETRY: {json.dumps(full_plan)}

    GOVERNANCE RULES:
    1. If '*' is used, ALWAYS expand it using the provided METADATA.
    2. Check 'existing_indexes'. If the required index is already present, health is 🟢.
    3. If 'is_scan' is True AND no relevant index exists, health is 🟡. Suggest a fix.
    4. If 'reads' > 0, mention that the query is hitting Physical Disk instead of RAM Cache.
    5. If 'is_scan' is True but indexes EXIST, explain it is a 'Small Table Seq Scan' but architecture is 🟢.

    RETURN JSON ONLY:
    {{
        "health_indicator": "🟢 | 🟡 | 🔴",
        "status": "Verified | Optimization Recommended | Critical Error",
        "performance_comparison": {{
            "workload_effort_original": "{current_effort}",
            "workload_effort_projected": "Calculate based on fix",
            "efficiency_gain": "Percentage"
        }},
        "optimized_sql": "Fully rewritten SQL",
        "suggested_fix": "CREATE INDEX... or 'Infrastructure Verified'",
        "audit_note": "Explain the I/O path (RAM vs Disk) and Scan Type."
    }}
    """

    try:
        response = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
            messages=[{"role": "system", "content": "You are a DB Auditor. Return only raw JSON."},
                      {"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return json.dumps({"error": str(e)})

if __name__ == "__main__":
    print("🚀 WATCHMAN AGENT ONLINE: Multi-Scenario PostgreSQL Intelligence")
    while True:
        user_input = input("\n🔍 Query: ")
        if user_input.lower() in ['exit', 'quit']: break
        if not user_input.strip(): continue
        
        print("\n--- AUDIT RESULTS ---")
        print(handle_query_optimization(user_input))
