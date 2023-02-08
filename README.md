### Env Set up
- Get Spark set up locally with Docker
  - After cloning repo, pull in docker-spark submodule
    - `git submodule update --init --recursive`
  - `cd ./docker-spark`
  - `docker-compose up -d`
- Install requirements
  - `pip install -r ./requirements.txt`
- Run app with Python
  - `python ./app/app.py`