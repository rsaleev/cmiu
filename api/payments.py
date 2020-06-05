import importlib
import configuration.settings as cs
import json
from uuid import uuid4
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncamqp import AsyncAMQP
from datetime import datetime
import os
import functools
import signal
import asyncio
importlib.reload(cs)


class PaymentNotifier:
    def __init__(self):
        self.__eventloop = None
        self.__eventsignal = False
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.name = 'PaymentNotifier'

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
        def date_event(self):
            return self.__date_event

        @date_event.getter
        def date_event(self):
            return self.__date_event.strftime('%d-%m-%Y %H:%M:%S')

        @property
        def instance(self):
            return {"type": self.__type, "device_number": self.__device_number, "device_type": self.__device_type, "transaction_type": self.tra_type,
                    "card": self.__card, "card_type": self.card_type, "lpr": self.__lpr, "date_event": self.date_event,  "transaction_uid": self.__transaction_uid,
                    "error": self.__error}

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__soapconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('cmiu_entries', ['event.payment.*.finished'], durable=True)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid, datetime.now()])
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self
