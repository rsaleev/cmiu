
from enum import Enum
from typing import Optional
from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
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

router = APIRouter()

name = "command.places"


class PlacesRequest(BaseModel):
    type: str
    error: 0
    date_event: str
    client_free: int
    client_busy: int
    vip_client_free: int
    vip_client_busy: int
    parking_number: int

    @validator('date_event')
    def date_validator(cls, v):
        dt = dp.parse(v)
        return dt


class PlacesResponse(BaseModel):
    type: str
    error: int = 0

    parking_number: Optional[int]

    @validator('date_event', pre=True)
    def date_validator(cls, v):
        return datetime.now().strftime('%d-%m-%Y %H:%M:%S')


@router.post('/api/cmiu/v2/places')
async def com_exec(*, request: PlacesRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = PlacesResponse(**request.dict())
    tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": 'PlacesModify', "request": request.dict(exclude_unset=True)})
    tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                             json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
    exec_tasks = []
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[0, 'Command', 'PLACES']))
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_places_upd', rows=0, values=[request.client_busy, request.vip_client_busy, None, cs.WS_MAIN_AREA]))
    exec_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_places_upd', rows=0, values=[request.client_busy, None, cs.WS_MAIN_AREA]))
    try:
        await asyncio.gather(*exec_tasks)
        request.error = 0
        request.date_event = datetime.now()
        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": 'PlacesModify', "response": response.dict(exclude_unset=True)})
        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
        return Response(json.dumps(request.dict(exclude_unset=True)), status_code=200, media_type='application/json', background=tasks)
    except Exception as e:
        request.error = 1
        request.date_event = datetime.now()
        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": 'PlacesModify', "response": repr(e)})
        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid), 'response': repr(e)}, ensure_ascii=False, default=str), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
