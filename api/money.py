from datetime import datetime
import asyncio
import signal
from socket import gaierror
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import json
import os
import functools
import aiohttp
from uuid import uuid4
import importlib
import configuration.settings as cs
importlib.reload(cs)


class MoneyListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__dbconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop = None
        self.__eventsignal = False
        self.name = 'MoneyNotifier'
        self.alias = 'cmiu_money'

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, v: bool):
        self.__eventloop = v

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v: bool):
        self.__eventsignal = v

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    @dataclass
    class MoneyRequest:
        def __init__(self, incoming_data, stored_data: dict):
            self.__type = 'money'
            self.__device_number = incoming_data['amppId']
            self.__device_type == incoming_data['amppType']
            self.__date_event = stored_data['ts']
            self.__money_value = stored_data['notesValue']
            self.__money_qty = stored_data['notesQty']
            self.__money_type = 2
            self.__money_method_type = stored_data['storageType']
            self.__error = 0
            self.__uid = uuid4()

        @property
        def device_id(self):
            return self.__device_number

        @property
        def date_event(self):
            return self.__date_event

        @date_event.getter
        def date_event(self):
            return self.__date_event.strftime('%d-%m-%Y %H:%M:%S')

        @property
        def money_value(self):
            return self.__money_value

        @money_value.getter
        def money_value(self):
            return self.__money_value * 100

        @property
        def storage_type(self):
            return self.__money_method_type

        @storage_type.getter
        def storage_type(self):
            if self.__money_method_type == 1:
                return 2
            elif self.__money_method_type == 2:
                return 1

        @property
        def uid(self):
            return self.__uid

        @uid.getter
        def uid(self):
            return str(self.__uid)

        @property
        def instance(self):
            return {'type': self.__type, 'device_number': self.__device_number,
                    'device_type': self.__device_type, 'date_event': self.date_event,
                    'money_value': self.money_value, 'quantity': self.__money_qty,
                    'money_type': self.__money_type, 'error': self.__error}

        # primary initialization of logging and connections

    async def _initialize(self) -> None:
        self.__logger = await AsyncLogger().getlogger(f"{cs.CMIU_LOG}money.log")
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('cmiu_money', ['payment.status.finished'], durable=True)
        cashiers = await self.__dbconnector_is.callproc('is_cashier_get', rows=-1, values=[None])
        tasks = []
        for c in cashiers:
            tasks.append(self._initialize_inventory(c['terId']))
        await asyncio.gather(*tasks)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('cmiu_processes_ins', rows=0, values=[self.name, 1, pid, datetime.now()])
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _initialize_inventory(self, device: dict) -> None:
        tasks = []
        money = await self.__dbconnector_w.callproc('is_money_get', rows=-1, values=[device['device_id']])
        for m in money:
            # storage type converts 1 -> 2 and 2 -> 1
            tasks.append(self.__dbconnector_is.callproc('is_money_ins', rows=0, values=[device['amppId'], m['storageType'], m['notesVal'], m['notesQty'], m['storageType'], m['ts']]))
        await asyncio.gather(*tasks)

    async def _process(self, redelivered, key, data) -> None:
        tasks = []
        post_tasks = []
        if not redelivered:
            if data['codename'] == 'PaymentStatus' and data['value'] in ['FINISHED_WITH_SUCCESS', 'FINISHED_WITH_ISSUES']:
                tasks = []
                tasks.append(self.__dbconnector_is.callproc('cmiu_money_get', rows=-1, values=[data['ampp_id']]))
                tasks.append(self.__dbconnector_is.callproc('is_money_get', rows=-1, values=[data['device_id']]))
                cmiu_inventory, is_inventory = await asyncio.gather(*tasks)
                for cmiu_inv, is_inv in zip(cmiu_inventory, is_inventory):
                    if (cmiu_inv['storageType'] == is_inv['storageType'] and
                        cmiu_inv['moneyValue'] == is_inv['notesValue']*100 and
                        cmiu_inv['moneyQuantity'] != is_inv['notesQty'] and
                            cmiu_inv['ts'] < is_inv['ts']):
                        request = self.MoneyRequest(data, is_inv)
                        tasks.append(self.__logger.info({'module': self.name, 'uid': request.uid, 'request': request.instance}))
                        tasks.append(self.__dbconnector_is.callproc('is_logs', rows=0, values=['cmiu', 'info', json.dumps(request.instance, default=str), datetime.now()]))
                        await asyncio.gather(*tasks, return_exceptions=True)
                        try:
                            conn = aiohttp.TCPConnector(forced_close=True, verify_ssl=False, enable_cleanup_closed=True, ttl_dns_cache=3600)
                            async with aiohttp.ClientSession(connector=conn) as session:
                                async with session.post(url=f'{cs.CMIU_URL}/money', json=request.instance, timeout=cs.CMIU_TIMEOUT, raise_for_status=True) as r:
                                    response = await r.json()
                                    post_tasks.append(self.__logger.info({'module': self.name, 'uid': request.uid, 'response': response}))
                                    post_tasks.append(self.__dbconnector_is.callproc('is_logs', rows=0, values=[
                                                      'cmiu', 'info', json.dumps({'uid': request.uid, 'response': response}), datetime.now()]))
                                    post_tasks.append(self.__dbconnector_is.callproc('cmiu_money_upd', rows=0, values=[
                                        cmiu_inv['amppId'], cmiu_inv['amppChannel'], cmiu_inv['moneyValue'], is_inv['moneyQty']]))
                        except (aiohttp.ClientError, aiohttp.InvalidURL, asyncio.TimeoutError, TimeoutError, OSError, gaierror) as e:
                            # log exception
                            post_tasks.append(self.__logger.error({"module": self.name, 'uid': request.uid, 'operation': request.operation, 'exception': repr(e)}))
                            post_tasks.append(self.__dbconnector_is.callproc('is_log_ins', rows=0, values=[self.source, 'error',
                                                                                                           json.dumps({'uid': request.uid, 'operation': self.alias, 'exception': repr(e)}, default=str), datetime.now()]))
                            # update process status to show that an error occured
                            post_tasks.append(self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 1, 1, datetime.now()]))
                        finally:
                            try:
                                await session.close()
                            except:
                                pass
                            await asyncio.gather(*post_tasks)
    # dispatcher

    async def _dispatch(self) -> None:
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass
        e
