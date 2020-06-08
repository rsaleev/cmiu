import contextlib
import functools
import os
import signal
from socket import gaierror
import json
from datetime import datetime
import aiohttp
import asyncio
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool, ProgrammingError, IntegrityError, OperationalError
import importlib
from uuid import uuid4
import configuration.settings as cs
importlib.reload(cs)


class PlacesNotifier:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.name = 'PlacesNotifier'

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
        self.__eventsignal = v

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class PlacesRequest:
        def __init__(self, stored_data):
            self.__type = 'places'
            self.__parking_id = cs.AMPP_PARKING_ID
            self.__commercial_free = next(sd['freePlaces'] for sd in stored_data if sd['clientType'] == 1)
            self.__commercial_occupied = next(sd['occupiedPlaces'] for sd in stored_data if sd['clientType'] == 1)
            self.__challenged_free = next(sd['occupiedPlaces'] for sd in stored_data if sd['clientType'] == 2)
            self.__challenged_occupied = next(sd['occupiedPlaces'] for sd in stored_data if sd['clientType'] == 2)
            self.__date_event = datetime.now()
            self.__error = 0
            self.__uid = uuid4()

        @property
        def uid(self):
            return self.__uid

        @property
        def date_event(self):
            return self.__date_event

        @date_event.getter
        def date_event(self):
            return self.__date_event.strftime('%d-%m-%Y %H:%M:%S')

        @property
        def instance(self):
            return {'type': self.__type, 'parking_number': self.__parking_id, 'client_free': self.__commercial_free, 'client_busy': self.__commercial_occupied, 'vip_client_free': self.__challenged_free,
                    'vip_client_busy': self.__challenged_occupied, 'date_event': self.date_event, 'error': self.__error}

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        # listen for loop2 status and lost ticket payment
        await self.__amqpconnector.bind('cmiu_places', ['status.loop2.*', 'event.challenged.*'], durable=False)
        await self.__dbconnector_is.callproc('cmiu_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _process(self, redelivered, key, data):
        pre_tasks = []
        post_tasks = []
        if key in ['status.loop2.entry', 'status.loop2.exit'] and data['value'] == 'OCCUPIED':
            places = await self.__dbconnector_is.callproc('is_places_get', rows=-1, values=[cs.WS_MAIN_AREA])
            request = self.PlacesRequest(places)
            pre_tasks.append(self.__logger.info({'module': self.name, 'uid': request.uid, 'request': request.instance}))
            pre_tasks.append(self.__dbconnector_is.callproc('is_logs', rows=0, values=['cmiu', 'info', json.dumps(request.instance, default=str), datetime.now()]))
            await asyncio.gather(*pre_tasks)
            try:
                conn = aiohttp.TCPConnector(force_close=True, ssl=False, enable_cleanup_closed=True, ttl_dns_cache=3600)
                async with aiohttp.ClientSession(connector=conn) as session:
                    async with session.post(url=f'{cs.CMIU_URL}/places', json=request.instance, timeout=cs.CMIU_TIMEOUT, raise_for_status=True) as r:
                        response = await r.json()
                        post_tasks.append(self.__logger.info({'module': self.name, 'uid': request.uid, 'response': response}))
                        post_tasks.append(self.__dbconnector_is.callproc('is_logs', rows=0, values=[
                            'cmiu', 'info', json.dumps({'uid': request.uid, 'response': response}, default=str), datetime.now()]))
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

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, 0, datetime.now()])
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_wp.disconnect())
        closing_tasks.append(self.__amqpconnector.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        asyncio.ensure_future(self._signal_cleanup())
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close process
        os._exit(0)

    def run(self):
        # use policy for own event loop
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future, (self._signal_handler(s))))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
