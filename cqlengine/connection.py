from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from contextlib import contextmanager
from cqlengine.exceptions import CQLEngineException
from cassandra.query import SimpleStatement
import logging


LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

class Connection:
    configured = False
    connection_pool = None
    default_consistency = None
    cluster_args = None
    cluster_kwargs = None

def setup(hosts, username=None, password=None, default_keyspace=None, consistency=None, metrics_enabled=False):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """

    if Connection.configured:
        LOG.info('cqlengine connection is already configured')
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

    Connection.cluster_args = (_hosts, )
    Connection.cluster_kwargs = {
        'port': port,
        'metrics_enabled': metrics_enabled
    }
    if consistency is None:
        Connection.default_consistency = ConsistencyLevel.ONE
    else:
        Connection.default_consistency = consistency

def get_cluster():
    cluster = Cluster(*Connection.cluster_args, **Connection.cluster_kwargs)
    try:
        from cassandra.io.libevreactor import LibevConnection
        cluster.connection_class = LibevConnection
    except ImportError:
        pass
    return cluster

def get_connection_pool():
    if Connection.connection_pool is None or Connection.connection_pool.cluster._is_shutdown:
        cluster = get_cluster()
        Connection.connection_pool = cluster.connect()
    return Connection.connection_pool

def get_consistency_level(consistency_level):
    if consistency_level is None:
        return Connection.default_consistency
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
