from cassandra.cluster import Cluster
from contextlib import contextmanager
from cqlengine.exceptions import CQLEngineException
import logging


LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

_max_connections = 10

# global connection pool
connection_pool = None


def setup(hosts, username=None, password=None, max_connections=10, default_keyspace=None, consistency='ONE'):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """
    global _max_connections
    global connection_pool
    _max_connections = max_connections

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
    connection_pool = cluster.connect()

def execute(query, params=None):
    params = params or {}
    return connection_pool.execute(query, params)

@contextmanager
def connection_manager():
    """ :rtype: ConnectionPool """
    global connection_pool
    # tmp = connection_pool.get()
    yield connection_pool
    # connection_pool.put(tmp)
