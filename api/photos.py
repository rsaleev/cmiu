from uuid import uuid4
from socket import gaierror
from datetime import datetime
import json
import aiohttp
import asyncio
import functools
import signal
import os
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import importlib
import io
import base64
import configuration.settings as cs
importlib.reload(cs)


class ImagesUploader:
    def __init__(self):
        self.__eventloop = None
        self.__eventsignal = False
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.name = 'ImagesUploader'
        self.alias = 'images'

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

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__soapconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('cmiu_images', ['event.*.loop1.free', 'event.*.loop2.free', 'event.*.barrier.closed'], durable=True)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid, datetime.now()])
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self

    async def _upload(self, data: dict, image):
        tasks = []
        post_tasks = []
        uid = str(uuid4())
        filename = f"{data['actUID']}.jpeg"
        data_out = base64.b64decode(data)
        date_event = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        headers = {'cmiu_parking_number': f'{cs.AMPP_PARKING_ID}', 'cmiu_camera_number': '', 'cmiu_action_uid': data['actUID'], 'cmiu_transaction_uid': data['traUID'], 'cmiu_date_event': date_event}
        conn = aiohttp.TCPConnector(force_close=True, ssl=False, enable_cleanup_closed=True, ttl_dns_cache=3600)
        try:
            async with aiohttp.ClientSession(connector=conn) as session:
                with aiohttp.MultipartWriter("form-data", boundary=uid, headers=headers) as mpwriter:
                    with aiohttp.MultipartWriter("form-data", boundary=uid) as subwriter:
                        part = subwriter.append(filename)
                        part.set_content_disposition("form-data", quote_fields=True, name="filename")
                    mpwriter.append(subwriter)
                    with aiohttp.MultipartWriter("form-data", boundary=uid) as subwriter:
                        part = subwriter.append('jpeg')
                        part.set_content_disposition("form-data", quote_fields=True, name="fileformat")
                    mpwriter.append(subwriter)
                    with aiohttp.MultipartWriter("form-data", boundary=uid) as filedata_writer:
                        part = subwriter.append(io.BytesIO(data_out))
                        part.set_content_disposition("form-data", quote_fields=True, name="cmiu_image", filename=filename)
                        part.headers[aiohttp.hdrs.CONTENT_TYPE] = 'image/jpeg'
                    mpwriter.append(subwriter)
                    async with session.post(url=cs.CMIU_URL/'upload_image', data=mpwriter, headers=headers, timeout=cs.CMIU_REQUEST_TIMEOUT, raise_for_status=True) as r:
                        response = await r.text()
                        await self.__logger.info({'module': self.alias, 'traUID': data['traUID'], 'actUID': data['actUID'], 'response': response})
        except Exception as e:
            post_tasks = []
            # log exception
            post_tasks.append(self.__logger.error({"module": self.name, 'uid': uid, 'operation': self.alias, 'exception': repr(e)}))
            post_tasks.append(self.__dbconnector_is.callproc('is_log_ins', rows=0, values=[self.source, 'error',
                                                                                           json.dumps({'uid': uid, 'operation': self.alias, 'exception': repr(e)}, default=str), datetime.now()]))
            # update process status to show that an error occured
            post_tasks.append(self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 1, 1, datetime.now()]))
            await asyncio.gather(*post_tasks, return_exceptions=True)

    async def _process(self, redelivered, key, data):
        if key in ['event.entry.loop1.free', 'event.exit.loop1.free', 'event.entry.loop1.reverse', 'event.entry.loop2.free', 'event.exit.loop2.free']:
            # sleep to ensure result
            result = await self.__dbconnector_is.callproc('is_photo_get', rows=1, values=[data['device_id'], data['tra_uid'], data['act_uid']])
            if not result is None and not result['photoImage'] is None and result['photoImage'] != '':
                await self._upload(result['photoImage'])
        elif key == 'event.entry.barrier.closed':
            # sleep to ensure result
            result = await self.__dbconnector_is.callproc('is_plate_get', rows=1, values=[data['device_id'], data['tra_uid'], data['act_uid']])
            if not result is None and not result['plateImage'] is None and result['plateImage'] != '':
                await self._upload(result['photoImage'])

    async def _dispatch(self, ):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('cmiu_processes_upd', rows=1, values=[self.name, 1, 0, datetime.now()])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 0, 0, datetime.now()])
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
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
        # use own event loop
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass
