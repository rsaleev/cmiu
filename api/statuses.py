from enum import Enum
import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from socket import gaierror
import aiohttp
import asyncio
import os
import signal
import functools
import importlib
from datetime import datetime
import json
importlib.reload(cs)


class Network(Enum):
    OFFLINE = 1
    ONLINE = 2


class BarrierAdvanced(Enum):
    MANUALLY_OPENED = 4
    RECENTLY_OPENED = 5
    MANUALLY_CLOSED = 7
    RECENTLY_CLOSED = 8
    LOCKED = 10
    RECENTLY_LOCKED = 11
    UNLOCKED = 13
    RECENTLY_UNLOCKED = 14


class BarrierStatus(Enum):
    OPENED = 40
    CLOSED = 41


# IOBoards -> Barrier
class IOBoards(Enum):
    BOARD1_ONLINE_BOARD2_ONLINE_BOARD3_ONLINE = 23
    BOARD1_OFFLINE_BOARD2_ONLINE_BOARD3_ONLINE = 24
    BOARD1_ONLINE_BOARD2_OFFLINE_BOARD3_ONLINE = 24
    BOARD1_OFFLINE_BOARD2_OFFLINE_BOARD3_ONLINE = 24
    BOARD1_OFFLINE_BOARD2_OFFLINE_BOARD3_OFFLINE = 24
    BOARD1_ONLINE_BOARD2_ONLINE = 23
    BOARD1_OFFLINE_BOARD2_ONLINE = 24
    BOARD1_ONLINE_BOARD2_OFFLINE = 24
    BOARD1_OFFLINE_BOARD2_OFFLINE = 24


class General(Enum):
    IN_SERVICE = 19
    OUT_OF_SERVICE = 16
    RECENTLY_TURNED_OFF = 17
    RECENTLY_TURNED_ON = 20


class Device(Enum):
    IN_SERVICE = 42
    PLACES_OK = 181
    PLACES_KO = 182


class CCReader(Enum):
    NO_RESPONSE = 21
    OK = 22
    NOT_USED = 44


class NotesEscrow(Enum):
    NO_ERROR = 32
    ACTION_FAILED_COMMUNICATION_ERROR = 31


class Cashbox(Enum):
    OK = 28
    ALMOST_FULL = 27
    FULL = 28


class UpperDoor(Enum):
    OPENED = 33
    CLOSED = 34


class BarrierLoop1Status(Enum):
    OCCUPIED = 111
    FREE = 112


class BarrierLoop2Status(Enum):
    OCCUPIED = 113
    FREE = 114


class CommandResult(Enum):
    CHALLENGED_IN = 151
    CHALLENGED_OUT = 151
    PLACES_CHANGED = 181
    PLACES_NOT_CHANGED = 182


class Ticket(Enum):
    PAPER_PRESENTED = 55
    OUT_OF_PAPER = 53
    ALMOST_OUT_OF_PAPER = 54


class FiscalPrinter(Enum):
    NO_RESPONSE = 57
    OK = 51
    SHIFT_CLOSED = 48


class OneShotEvent(Enum):
    CHALLENGED_IN = 39
    OCCASIONAL_IN = 38


class PlacesAlert(Enum):
    OCCASIONAL_FULL = 36
    CHALLENGED_FULL = 37


class StatusNotifier:
    def __init__(self):
        self.__eventloop = None
        self.__eventsignal = False
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.name = 'StatusesNotifier'
        self.alias = 'statuses'

    @property
    def eventloop(self):
        return self.__loop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v):
        return self.__eventsignal

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class StatusRequest:
        def __init__(self, incoming_data):
            self.__type = 'status'
            self.__device_number = incoming_data['ampp_id']
            self.__device_type = incoming_data['ampp_type']
            self.__status_number = int
            self.__status_codename = incoming_data['codename']
            self.__status_value = incoming_data['value']
            self.__date_event = incoming_data['ts']
            self.__action_uid = incoming_data['act_uid']
            self.__transaction_uid = incoming_data['tra_uid']
            self.__error = 0

        @property
        def device_id(self):
            return self.__device_number

        @device_id.getter
        def device_id(self):
            return self.__device_number

        @property
        def status_codename(self):
            return self.__status_codename

        @status_codename.getter
        def status_codename(self):
            return self.__status_codename

        @property
        def status_number(self):
            return self.__status_number

        @status_number.getter
        def status_number(self):
            try:
                if self.__status_codename == 'Network':
                    return Network[self.__status_value].value
                elif self.__status_codename == 'General':
                    return General[self.__status_value].value
                elif self.__status_codename == 'BarrierAdvancedStatus':
                    return BarrierAdvanced[self.__status_value].value
                elif self.__status_codename == 'BarrierStatus':
                    return BarrierStatus[self.__status_value].value
                elif self.__status_codename == 'BarrierLoop1Status':
                    return BarrierLoop1Status[self.__status_value].value
                elif self.__status_codename == 'Barrierloop2Status':
                    return BarrierLoop2Status[self.__status_value].value
                elif self.__status_codename == 'IOBoards' and self.__device_type in [2, 3]:
                    return IOBoards[self.__status_value].value
                elif self.__status_codename == 'CCReader':
                    return CCReader[self.__status_value].value
                elif self.__status_codename == 'NotesEscrow':
                    return NotesEscrow[self.__status_value].value
                elif self.__status_codename == 'Cashbox':
                    return Cashbox[self.__status_value].value
                elif self.__status_codename == 'UpperDoor':
                    return UpperDoor[self.__status_value].value
                elif self.__status_codename == 'CommandResult':
                    return CommandResult[self.__status_value].value
                elif self.__status_codename in['PaperDevice1', 'PaperDevice2']:
                    return Ticket[self.__status_value].value
                elif self.__status_codename in ['FiscalPrinterStatus', 'FiscalPrinterBD', 'FiscalPrinterIssues']:
                    return FiscalPrinter[self.__status_value].value
                elif self.__status_codename == 'OneShotEvent':
                    return OneShotEvent[self.__status_value].value
                elif self.__status_codename == 'Places':
                    return PlacesAlert[self.__status_value].value
            except KeyError:
                return None

        @property
        def date_event(self):
            return datetime.now().strftime('%d-%m-%Y %H:%M:%S')

        @property
        def instance(self):
            return {'type': self.__type, 'device_number': self.__device_number, 'device_type': self.__device_type, 'status_number': self.status_number, 'date_event': self.date_event}

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cs.CMIU_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('cmiu_statuses', ['status.*.*, event.*.*, event.*, command.challenged.*, status.*'], durable=True)
        is_devices = await self.__dbconnector_is.callproc('is_device_get', rows=-1, values=[None, None, None, None, None])
        tasks = []
        for d in is_devices:
            if d['terType'] in [1, 2]:
                states = ['General', 'Network', 'BarrierStatus', 'BarrierAdvancedStatus', 'Tickets', 'IOBoards', 'BarrierLoop1Status', 'BarrierLoop2Status', 'UpperDoor', 'PaperDevice1']
                for st in states:
                    tasks.append(self.__dbconnector_is.callproc('cmiu_status_ins', rows=0, values=[d['amppId'], d['amppType'], st]))
            elif d['terType'] == 3:
                states = ['General', 'Network', 'CCReader', 'FiscalPrinterBD', 'FiscalPrinterStatus', 'Cashbox', 'NotesEscrow']
                for st in states:
                    tasks.append(self.__dbconnector_is.callproc('cmiu_status_ins', rows=0, values=[d['amppId'], d['amppType'], st]))
            elif d['terType'] == 0:
                states = ['General', 'Network', 'Places']
                for st in states:
                    tasks.append(self.__dbconnector_is.callproc('cmiu_status_ins', rows=0, values=[d['amppId'], d['amppType'], st]))
        await asyncio.gather(*tasks)

    async def _process(self, redelivered, key, data):
        request = self.StatusRequest(data)
        if not request.status_codename is None:
            statuses = await self.__dbconnector_is.callproc('cmiu_status_get', rows=1, values=[data['ampp_id']])
            st_value = next((st['stValue'] for st in statuses if st['stCodename'] == request.status_codename), None)
            pre_tasks = []
            post_tasks = []
            if st_value is None or request.status_number != st_value:
                try:
                    conn = aiohttp.TCPConnector(force_close=True, ssl=False, enable_cleanup_closed=True, ttl_dns_cache=3600)
                    async with aiohttp.ClientSession(connector=conn) as session:
                        async with session.post(url=f'{cs.CMIU_URL}/events', json=request.instance, timeout=cs.CMIU_TIMEOUT, raise_for_status=True) as r:
                            response = await r.json()
                            post_tasks.append(self.__logger.info({'module': self.name, 'uid': request.uid, 'response': response}))
                            post_tasks.append(self.__dbconnector_is.callproc('is_logs', rows=0, values=[
                                'cmiu', 'info', json.dumps({'uid': request.uid, 'response': response}, default=str), datetime.now()]))
                            post_tasks.append(self.__dbconnector_is.callproc('cmiu_status_upd', rows=0, values=[request.device_id, request.status_codename, request.status_number]))
                except (aiohttp.ClientError, aiohttp.InvalidURL, asyncio.TimeoutError, TimeoutError, OSError, gaierror) as e:
                    # log exception
                    post_tasks.append(self.__logger.error({"module": self.name, 'uid': request.uid, 'operation': request.operation, 'exception': repr(e)}))
                    post_tasks.append(self.__dbconnector_is.callproc('is_log_ins', rows=0, values=[self.source, 'error',
                                                                                                   json.dumps({'uid': request.uid, 'operation': self.alias, 'exception': repr(e)}, default=str), datetime.now()]))
                    post_tasks.append(self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 1, 1, datetime.now()]))
                finally:
                    try:
                        await session.close()
                    except:
                        pass
                    await asyncio.gather(*post_tasks)
