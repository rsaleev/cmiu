import configuration.settings as cs
from urllib.parse import urlencode
from multiprocessing import Process
import functools
import aiohttp
from contextlib import suppress
from datetime import datetime, timedelta
import signal
import json
import asyncio
from modules.cmiu.api.entries import EntryNotifier
from modules.cmiu.api.exits import ExitNotifier
from modules.cmiu.api.money import MoneyNotifier
from modules.cmiu.api.payments import PaymentNotifier
from modules.cmiu.api.places import PlacesNotifier
from modules.cmiu.api.photos import ImagesUploader
from modules.cmiu.api.statuses import StatusNotifier
from modules.cmiu.service import webservice
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import sys
import os
import sdnotify
import setproctitle


class Application:
    def __init__(self):
        self.__eventloop = None
        self.__eventsignal = False
        self.__logger = None
        self.__dbconnector_is = None
        self.__devices = []
        self.alias = 'cmiu'
        self.__processes = []

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, v):
        self.__eventloop = v

    @property
    def logger(self):
        return self.__logger

    @logger.setter
    def logger(self, v):
        self.__logger = v

    @logger.getter
    def logger(self):
        return self.__logger

    async def _initialize(self):
        n = sdnotify.SystemdNotifier()
        setproctitle('CMIU Integration Module')
        self.logger = await AsyncLogger().getlogger(cs.EPP_LOG)
        try:
            await self.logger.info('CMIU is starting...')
            self.__dbconnector_is = await AsyncDBPool(cs.IS_SQL_CNX).connect()
            # entries
            entry_notifier = EntryNotifier()
            entry_notifier_proc = Process(target=entry_notifier.run, name=entry_notifier.name)
            self.__processes.append(entry_notifier_proc)
            entry_notifier_proc.start()
            # exits
            exit_notifier = ExitNotifier()
            exit_notifier_proc = Process(target=exit_notifier.run, name=exit_notifier.name)
            self.__processes.append(exit_notifier_proc)
            exit_notifier_proc.start()
            # payments
            payment_notifier = PaymentNotifier()
            payment_notifier_proc = Process(target=payment_notifier.run, name=payment_notifier.name)
            self.__processes.append(payment_notifier_proc)
            payment_notifier_proc.start()
            # money
            money_notifier = MoneyNotifier()
            money_notifier_proc = Process(target=money_notifier.run, name=money_notifier.name)
            self.__processes.append(money_notifier_proc)
            money_notifier_proc.start()
            # events
            status_notifier = StatusNotifier()
            status_notifier_proc = Process(target=status_notifier.run, name=status_notifier.name)
            self.__processes.append(status_notifier_proc)
            status_notifier_proc.start()
            # images
            images_uploader = ImagesUploader()
            images_uploader_proc = Process(target=images_uploader.run, name=images_uploader.name)
            images_uploader_proc.start()
            # webservice
            service_proc = Process(target=webservice.run, name=webservice.name)
            # log parent process status
            await self.__dbconnector_is.callproc('is_services_ins', rows=0, values=[self.alias, os.getpid(), 1])
            await self.__dbconnector_is.disconnect()
            await self.logger.info('CMIU is started')
            n.notify("READY=1")
        except Exception as e:
            await self.logger.error(f'CMIU startup exception {e}')
            n.notify("READY=0")
            sys.exit(1)

    async def _signal_handler(self, signal):
        # perform cleaning tasks
        for p in self.__processes:
            p.terminate()
        self.eventloop.stop()
        self.eventloop.close()
        sys.exit(0)

    def run(self):
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            loop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                         self._signal_handler(s)))
        loop.run_until_complete(self._initialize())


if __name__ == "__main__":
    app = Application()
    app.run()
