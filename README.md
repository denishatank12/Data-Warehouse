# Pinecone + Airflow Class Project

## Repo
https://github.com/<YOUR_USERNAME>/pinecone-airflow-demo

## Steps
1. Modified `docker-compose.yaml` to install `sentence-transformers` + `pinecone`.
2. Restarted Docker containers (`docker compose down` + `up -d`).
3. Created Airflow Variable `PINECONE_CONFIG` with API key, region, etc.
4. Ran DAG `pinecone_ingest_demo`.
5. Verified tasks succeeded (`download_data`, `create_index`, `embed_and_upsert`, `semantic_search`).
6. Verified Pinecone index created and query results returned.

## Screenshots
- docker-compose.yaml edit
- docker compose up terminal output
- Airflow Variables page
- DAG list
- Task logs for each step
- Pinecone console (optional)
