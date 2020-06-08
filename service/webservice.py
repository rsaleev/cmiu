
from enum import Enum
from typing import Optional
from fastapi import FastAPI
from starlette.responses import Response, RedirectResponse
import json
from pydantic import BaseModel, ValidationError, validator
import asyncio
from datetime import datetime, timedelta
from zeep.exceptions import TransportError, LookupError
from zeep.exceptions import Error as ClientError
from uuid import uuid4
from starlette.background import BackgroundTasks
import dateutil.parser as dp
import integration.service.settings as ws
import configuration.settings as cs
from modules.cmiu.service.routes import control, places
import uvicorn

app = FastAPI(debug=True if cs.CMIU_WEBSERVICE_LOG_LEVEL == 'debug' else False)

name = "WebService"

app.include_router(control.router)
app.include_router(places.router)


class ControlRequest(BaseModel):
    type: str


@app.on_event('startup')
async def startup():
    ws.LOGGER = await ws.LOGGER.getlogger(cs.IS_LOG)
    await ws.LOGGER.warning({'module': name, 'info': 'Starting...'})
    tasks = []
    await ws.DBCONNECTOR_IS.connect()
    await ws.DBCONNECTOR_WS.connect()
    await ws.SOAPCONNECTOR.connect()
    await ws.AMQPCONNECTOR.connect()
    await asyncio.gather(*tasks)
    await ws.LOGGER.warning({'module': name, 'info': 'Started'})


@app.on_event('shutdown')
async def shutdown():
    await ws.LOGGER.warning({'module': name, 'info': 'Shutting down...'})
    tasks = []
    tasks.append(ws.DBCONNECTOR_WS.disconnect())
    tasks.append(ws.DBCONNECTOR_IS.disconnect())
    tasks.append(ws.SOAPCONNECTOR.disconnect())
    tasks.append(ws.AMQPCONNECTOR.disconnect())
    await asyncio.gather(*tasks)
    await ws.LOGGER.warning({'module': name, 'info': 'Shutdown'})
    await ws.LOGGER.shutdown()


@app.get("/api/cmiu/v2/command")
async def redirect(request: ControlRequest):
    if request.type == "command":
        response = RedirectResponse(url='/api/cmiu/v2/control')
        return response
    elif request.type == "places":
        response = RedirectResponse(url='/api/cmiu/v2/places')
        return response
    elif request.typ == "season_card":
        response = RedirectResponse(url='/api/cmiu/v2/subscriptions')


def run():
    uvicorn.run(app=app, host=cs.IS_WEBSERVICE_HOST, port=cs.IS_WEBSERVICE_PORT, workers=cs._WEBSERVICE_WORKERS, log_level=cs.CMIU_WEBSERVICE_LOG_LEVEL)
