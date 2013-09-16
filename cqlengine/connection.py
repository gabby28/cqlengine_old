from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from contextlib import contextmanager
from cqlengine.exceptions import CQLEngineException
from cassandra.query import SimpleStatement
import logging


LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

# global connection pool
connection_pool = None
default_consistency = None

def setup(hosts, username=None, password=None, default_keyspace=None, consistency=None):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """

    global connection_pool
    global default_consistency

    if connection_pool is not None:
        return

    if default_keyspace:
        from cqlengine import models
        models.DEFAULT_KEYSPACE = default_keyspace

    _hosts = []
    port = 9042
    for host in hosts:
        host = host.strip()
        host = host.split(':')
        if len(host) == 1:
            _hosts.append(host[0])
        elif len(host) == 2:
            _hosts.append(host[0])
            port = host[1]
        else:
            raise CQLConnectionError("Can't parse {}".format(''.join(host)))

    if not _hosts:
        raise CQLConnectionError("At least one host required")

    cluster = Cluster(_hosts, port=port)

    try:
        from cassandra.io.libevreactor import LibevConnection
        cluster.connection_class = LibevConnection
    except ImportError:
        LOG.info('Could not import cassandra.io.libevreactor.LibevConnection as connection_class')

    connection_pool = cluster

    if consistency is None:
        default_consistency = ConsistencyLevel.ONE
    else:
        default_consistency = consistency


def get_connection_pool():
    global connection_pool
    if hasattr(connection_pool, 'connect'):
        connection_pool = connection_pool.connect()
    return connection_pool

def get_consistency_level(consistency_level):
    global default_consistency
    if consistency_level is None:
        return default_consistency
    else:
        return consistency_level

def execute(query, params=None, consistency_level=None):
    params = params or {}
    consistency_level = get_consistency_level(consistency_level)
    session = get_connection_pool()
    query = SimpleStatement(query, consistency_level=consistency_level)
    return session.execute(query, parameters=params)

def execute_async(query, params=None, consistency_level=None):
    params = params or {}
    consistency_level = get_consistency_level(consistency_level)
    session = get_connection_pool()
    query = SimpleStatement(query, consistency_level=consistency_level)
    return session.execute_async(query, parameters=params)

@contextmanager
def connection_manager():
    yield get_connection_pool()
