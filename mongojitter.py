#!/usr/bin/env python
"""Given a slave, check its object count against its master"""
import pymongo


STATS_KEYS = ('collections', 'indexes', 'objects')
SKIP_DBS = set(('local', 'admin', 'test'))


def get_db_jitter(slave, master, db):
    master_stats = master[db].command('dbstats')
    slave_stats = slave[db].command('dbstats')

    jitter = {}
    for key in STATS_KEYS:
        yield key, master_stats[key] - slave_stats[key]

def compare(slave_host, slave_port=None):
    slave = pymongo.Connection(slave_host, slave_port, slave_okay=True)

    if slave.admin.command('isMaster').get('isMaster'):
        raise Exception('%s:%s is master' % (slave_host, slave_port))

    dbs = set(slave.database_names()) - SKIP_DBS
    print '%-40s %s' % ('<source>.<database>.<stat>', 'difference')
    print '-' * 40, '-' * 10
    for source in slave.local.sources.find():
        master = pymongo.Connection(source['host'])
        for db in dbs:
            for stat, value in get_db_jitter(slave, master, db):
                print '%-40s %d' % (
                        '.'.join((source['host'], db, stat)), value)

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        compare(sys.argv[1], int(sys.argv[2]))
    else:
        compare(sys.argv[1])
