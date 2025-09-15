import sys

import uvicorn
from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel

import cypher_graphdb
from cypher_graphdb import CypherGraphDB

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB(backend="AGE", load_dotenv=True).connect()

app = FastAPI()


class ExecuteRequest(BaseModel):
    cypher_cmd: str


@app.get("/ping")
def ping():
    return "pong"


@app.post("/execute")
def execute_cypher(request: ExecuteRequest):
    try:
        return {"result": cdb.execute(request.cypher_cmd), "stats": cdb.exec_statistics()}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
