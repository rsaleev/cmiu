import importlib
import configuration.settings as cs
import json
from uuid import uuid4
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from datetime import datetime
from socket import gaierror
import os
import functools
import signal
import asyncio
import aiohttp
importlib.reload(cs)


class PaymentNotifier:
    def __init__(self):
        self.__eventloop = None
        self.__eventsignal = False
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.name = 'PaymentNotifier'
        self.alias = 'payments'

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

    class PaymentRequest:
        def __init__(self, incoming_data: dict, stored_data: dict):
            self.__type = "payments"
            self.__device_number = incoming_data['ampp_id']
            self.__method = json.loads(stored_data['payData'])['payType']
            self.__method_info = stored_data['payCardType']
            self.__card = stored_data['payCardNum']
            self.__discount = json.loads(stored_data['payData'])['paymentDiscount']
            self.__ticket = json.loads(stored_data['payData'])['transitionTicket']
            self.__transaction_type = json.loads(stored_data['payData'])['transitionType']
            self.__pay_type = 1
            self.__pay_count = json.loads(stored_data['payData'])['paymentCount']
            self.__price = json.loads(stored_data['payData'])['paymentPaid']
            self.__action_uid = incoming_data['act_uid']
            self.__tra_uid = incoming_data['tra_uid']
            self.__error = 0
            self.__uid = uuid4()

        @property
        def uid(self):
            return self.__uid

        @uid.getter
        def uid(self):
            return str(self.__uid)

        @property
        def pay_method(self):
            return self.__method

        @pay_method.getter
        def pay_method(self):
            if self.__method == 'C':
                return 1
            elif self.__method == 'P':
                return 2
            elif self.__method == 'M':
                return 4

        @property
        def pay_type(self):
            return self.__pay_type

        @pay_type.getter
        def pay_type(self):
            if self.__discount > 0:
                return 3
            elif self.__transaction_type == 'SUBSCRIPTION':
                return 3
            else:
                return self.__pay_type

        @property
        def ticket_type(self):
            if self.__transaction_type == 'TRANSACTION':
                return 1
            elif self.__transaction_type == 'SUBSCRIPTION':
                return 2
            elif self.__transaction_type == 'LOST':
                return 3

        @property
        def payment_count(self):
            return self.__pay_count

        @payment_count.getter
        def payment_count(self):
            if self.__pay_count == 1:
                return 1
            elif self.__pay_count > 1:
                return 2

        @property
        def payment_price(self):
            return self.__price

        @payment_price.getter
        def payment_price(self):
            return self.__price * 100

        @property
        def date_event(self):
            return self.__date_event

        @date_event.getter
        def date_event(self):
            return self.__date_event.strftime('%d-%m-%Y %H:%M:%S')

        @property
        def instance(self):
            return {"type": self.__type, "device_number": self.__device_number, "device_type": self.__device_type, "payment_method": self.pay_method,
                    "payment_method_addition": self.__method_info, "card": self.__ticket,
                    "card_type": self.ticket_type, "payment_type": self.pay_type, "payment_count": self.payment_count, "price": self.payment_price,
                    "date_event": self.date_event, "error": self.__error}

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__soapconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('cmiu_entries', ['event.payment.*.finished'], durable=True)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('cmiu_processes_ins', rows=0, values=[self.name, 1, pid, datetime.now()])
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self

    async def _process(self, redelivered, key, data):
        await asyncio.sleep(0.5)
        consume_tasks = []
        consume_tasks.append(self.__dbconnector_is.callproc('is_payment_by_uid_get', rows=1, values=[data['tra_uid']]))
        consume_tasks.append(self.__dbconnector_is.calproc('is_payment_upd', rows=0, values=[data['tra_uid'], datetime.now(), None, None]))
        stored_data, _ = await asyncio.gather(*consume_tasks)
        if not stored_data is None:
            request = self.PaymentRequest(data, stored_data)
            pre_tasks = []
            uid = uuid4()
            pre_tasks.append(self.__dbconnector_is.callproc('is_logs_ins', rows=0, values=['cmiu', 'info',
                                                                                           json.dumps({'uid': request.uid, 'operation': self.alias, 'request': request.instance}, default=str), datetime.now()]))
            pre_tasks.append(self.__logger.info({'module': self.name, 'request': {'uid': request.uid, 'data': request.instance}}))
            await asyncio.gather(*pre_tasks)
            conn = aiohttp.TCPConnector(force_close=True, ssl=False, enable_cleanup_closed=True, ttl_dns_cache=3600)
            post_tasks = []
            async with aiohttp.ClientSession(connector=conn) as session:
                # convert dict to urlencoded on the fly
                try:
                    async with session.post(url=f'{cs.CMIU_URL}/payments', json=request.instance, timeout=cs.CMIU_REQUEST_TIMEOUT, raise_for_status=True) as r:
                        # xml is returned
                        response = await r.json()
                        post_tasks.append(self.__dbconnector_is.callproc('is_logs_ins', rows=0, values=['cmiu', 'info',
                                                                                                        json.dumps({'uid': request.uid, 'operation': self.alias, 'request': json.dumps(response)}, default=str), datetime.now()]))
                        post_tasks.append(self.__logger.info({'module': self.name, 'response': {'uid': request.uid, 'data': request.instance}}))
                # handle request exceptions
                except (aiohttp.ClientError, aiohttp.InvalidURL, asyncio.TimeoutError, TimeoutError, OSError, gaierror) as e:
                    # log exception
                    post_tasks.append(self.__logger.error({"module": self.name, 'uid': request.uid, 'operation': request.operation, 'exception': repr(e)}))
                    # add to queue
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
            await self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 1, 0, datetime.mow()])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.callproc('cmiu_processes_upd', rows=0, values=[self.name, 0, 0, datetime.mow()])
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
