### Table of Contents
- app
    - data: raw files and output.csv
    - app.py: application code
- docker-compose: compose config for local Spark testing

### Methodology
- Three rounds of matching
    - Match on UUIDs from both datasets for quick wins
    - Fuzzy Match on Names
    - Fuzzy Match on filtered Addresses

### Env Set up
Optional:
  - Get Spark set up locally with Docker
    - After cloning repo, pull in docker-spark submodule
      - `git submodule update --init --recursive`
    - `cd ./docker-spark`
    - `docker-compose up -d`
- Install requirements
  - `pip install -r ./requirements.txt`
- Run app with Python
  - `python ./app/app.py`