
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
from typing import List, Optional
import dateutil.parser as dp

router = APIRouter()

name = "subscriptions"


def convert(code):
    try:
        # 4 bytes Mifare
        if len(code) == 8:
            b1 = code[0:2]
            b2 = code[2:4]
            b3 = code[4:6]
            b4 = code[6:8]
            pre_str = b1+b2+b3+b4
            came_code = str(int(pre_str, 16))
            if len(came_code) < 10:
                came_code = '00000000080' + came_code
            else:
                came_code = '0000000008' + came_code
            return came_code
        # 7 bytes
        elif len(code) == 14:
            byte_1 = f"0{(int('8'+code[0:6], 16))}"
            if len(byte_1) < 10:
                byte_1_9 = byte_1[8:9]
                byte_1 = f'{byte_1[0:8]}0{byte_1_9}'
            byte_2 = f'{int(code[6:14], 16)}'
            if len(byte_2) == 8:
                byte_2 = f'00{byte_2}'
            elif len(byte_2) == 9:
                byte_2 = f'0{byte_2}'
            came_code = f'{byte_1}{byte_2}'
            return came_code
    except:
        return None


class SubscriptionRequest(BaseModel):
    type: "season_card"
    parking_number: int
    action_type: str
    card_name: Optional[str] = None
    card_number: Optional[int] = None
    card_type: Optional[int]
    valid_from: Optional[str]
    valid_to: Optional[str]
    location: Optional[str]
    lpr: Optional[str]
    addition_1: Optional[str]
    addition_2: Optional[str]
    date_event: str
    error: int

    @validator('action_type', pre=True)
    def action_type_check(cls, v):
        action_list = ['create', 'select', 'modify', 'create_modify', 'delete']
        if v in action_list:
            pass
        else:
            raise ValidationError

    @validator('valid_from')
    def data_from_parser(cls, v):
        date_from = dp.parse(v)
        return date_from

    @validator('valid_to')
    def date_event_parser(cls, v):
        date_to = dp.parse(v)
        return date_to

    @validator('location', pre=True)
    def location_parser(cls, v):
        if v == 'outside' or 'OUTSIDE':
            return 'O'
        elif v == 'inside' or 'INSIDE':
            return 'I'
        elif v == 'neutral' or 'NEUTRAL':
            return 'X'
# parser data from DB


class SubscriptionInfo(BaseModel):
    card_name: str
    card_number: str
    card_type: int
    valid_from: str
    valid_to: str
    location: str
    lpr: str
    addition_1: str
    addition_2: str

    @validator('location', pre=True)
    def location_convert(cls, v):
        if v == 'I':
            return 'inside'
        elif v == 'O':
            return 'outside'
        elif v == 'X':
            return 'neutral'

    @validator('valid_from', pre=True)
    def valid_from_convert(cls, v):
        return v.strftime('%d-%m-%Y %H:%M:%S')

    @validator('valid_to', pre=True)
    def valid_to_convert(cls, v):
        return v.strftime('%d-%m-%Y %H:%M:%S')


class SubscriptionResponse(BaseModel):
    type: "season_card"
    action_type: str
    parking_number: int
    cards: Optional[list] = None
    date_event: str
    error: int

    @validator('date_event', pre=True)
    def date_event_convert(cls, v):
        return v.strftime('%d-%m-%Y %H:%M:%S')


@router.post('/api/cmiu/v2/subscriptions')
async def subcription_data(*, request: SubscriptionRequest):
    try:
        if request.action_type == 'select':
            data = await ws.DBCONNECTOR_WS.callproc('wp_sub_get', rows=-1, values=[request.card_number, None])
            response = SubscriptionResponse(**request.dict(exclude_unset=True))
            response.cards = data
            response.date_event = datetime.now()
            response.error = 0
            return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
        elif request.action_type == 'delete':
            data_check = await ws.DBCONNECTOR_WS.callproc('wp_sub_get', rows=1, values=[request.card_number, None])
            if not data_check is None:
                try:
                    await ws.DBCONNECTOR_WS.callproc('wp_sub_del', rows=0, values=[request.card_number, None])
                    response.date_event = datetime.now()
                    response.error = 0
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
            else:
                response.date_event = datetime.now()
                response.error = 3
                return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
        elif request.action_type in ['create', 'create_modify']:
            data_check = await ws.DBCONNECTOR_WS.callproc('wp_sub_get', rows=1, values=[request.card_number, None])
            if data_check is None:
                came_code = convert(request.card_number)
                if not came_code is None:
                    try:
                        await ws.DBCONNECTOR_WS.callproc('wp_sub_ins', rows=0, values=[came_code, request.lpr, request.addition_1, request.valid_from, request.valid_to,
                                                                                       request.card_number, request.card_name, request.addition_2])
                        response.date_event = datetime.now()
                        response.error = 0
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
                    except:
                        response.date_event = datetime.now()
                        response.error = 4
                else:
                    response.date_event = datetime.now()
                    response.error = 4
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
            else:
                response.date_event = datetime.now()
                response.error = 2
                return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
        elif request.action_type == 'modify':
            came_code = convert(request.card_number)
            data_check = await ws.DBCONNECTOR_WS.callproc('wp_sub_get', rows=1, values=[request.card_number, None])
            if data_check is None:
                response.date_event = datetime.now()
                response.error = 5
                return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
            else:
                try:
                    await ws.DBCONNECTOR_WS.callproc('wp_sub_get', rows=1, values=[request.card_number, came_code, request.lpr, request.addition_1, request.valid_from, request.valid_to,
                                                                                   request.card_name, request.addition_2, request.location])
                except:
                    response.date_event = datetime.now()
                    response.error = 5
    except ValidationError:
        response.date_event = datetime.now()
        if request.action_type in ['create', 'create_modiy']:
            response.error = 4
        elif request.action_type == 'delete':
            response.error = 6
        elif request.action_type == 'modify':
            response.error = 5
        return Response(json.dumps(response.dict(exclude_unset=True), default=str), media_type='application/json', status_code=200)
