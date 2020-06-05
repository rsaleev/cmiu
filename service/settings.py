import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool


DBCONNECTOR_WS = AsyncDBPool(cs.WS_SQL_CNX)
DBCONNECTOR_IS = AsyncDBPool(cs.IS_SQL_CNX)
SOAPCONNECTOR = AsyncSOAP(cs.WS_SOAP_USER, cs.WS_SOAP_PASSWORD, cs.WS_SERVER_ID, cs.WS_SOAP_TIMEOUT, cs.WS_SOAP_URL)
AMQPCONNECTOR = AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, 'integration', 'topic')
LOGGER = AsyncLogger()


# data producers
