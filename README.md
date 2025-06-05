# Waymark Care Interview  
**Position:** Healthcare and Business Intelligence Analyst

---

## Instructions
```bash
# Clone my waymark_interview repository
git clone https://github.com/Willis-Chan/waymark_interview.git
cd waymark_interview

# Install all required Python packages
pip install -r requirements.txt

# Run Data Pipeline
python3 pipeline.py
```

## Additional Comments
- Tried to keep completion of project within a reasonable amount of time.
- If I had additional time, I would add unit tests using pytest to test transformation logic using mock data.
- I'd automate pulling the GSheet down using the Google Sheets API.
- I might build an integration test to validate end-to-end logic.
- This same exercise could also be done using local Postgres instance and SQL (possibly via dbt core).
- There is also value in building data validation queries to ensure data quality.
- If scaling is an issue, we can index before merging, use chunking, or switch to Apache Spark.
- Add linter (Ruff Linter or Pylint) and run it over my code.
