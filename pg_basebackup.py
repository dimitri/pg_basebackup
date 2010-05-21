#!/usr/bin/env python

import sys, os, os.path, psycopg2
from zlib import decompress

CSIZE = 8192 * 1024 # we work with chunks of 8MB
LABEL = 'initslave.py'

file_list_sql = """
with recursive files(path, isdir, size, visited) as (
  select name, (pg_stat_file(name)).isdir, (pg_stat_file(name)).size, false
    from (select pg_ls_dir(setting) as name
            from pg_settings
	   where name = 'data_directory') as toplevel

  UNION ALL

  select name, (pg_stat_file(name)).isdir, (pg_stat_file(name)).size, false
    from (select path || '%s' || pg_ls_dir(path) as name
            from files
	   where isdir and not visited
         ) as curdir
) 
select * from files;
""" % (os.path.sep)

read_file_sql = """
CREATE OR REPLACE FUNCTION pg_bb_read_file 
(path text, 
 pos bigint, 
 size bigint default 8192*1024
)
 RETURNS BYTEA
 LANGUAGE plpythonu
AS $$
  from zlib import compress
  f = open(path, 'rb')
  f.seek(pos)
  return compress(f.read(size))
$$;

CREATE OR REPLACE FUNCTION pg_bb_count_chunks
 (path text, 
  length bigint default 8192*1024
 )
 RETURNS bigint
 LANGUAGE SQL
AS $$
  SELECT ceil((s::float + 1) / $2)::bigint
    FROM (SELECT (pg_stat_file($1)).size) as t(s);
$$;
"""

if __name__ == '__main__':
    dsn = sys.argv[1]
    dest = sys.argv[2]

    if os.path.isdir(dest):
        print "Error: destination directory already exists"
        sys.exit(1)

    try:
        conn = psycopg2.connect(dsn)
    except Exception, e:
        print "Error: couldn't connect to '%s':" % dsn
        print e
        sys.exit(2)

    try:
        os.makedirs(dest)
    except Exception, e:
        print "Error: coudn't create the destination PGDATA at '%s'" % dest
        sys.exit(3)

    curs = conn.cursor()
    curs.execute(read_file_sql)
    curs.execute("SELECT pg_start_backup(%s);", [LABEL])
    curs.execute(file_list_sql)

    for path, isdir, size, visited in curs.fetchall():
        print path

        if isdir:
            curdir = path
        else:
            curdir = os.path.dirname(path)

        cwd = os.path.join(dest, curdir)

        if not os.path.isdir(cwd):
            print "mkdir -p", cwd
            os.makedirs(cwd)

        if not isdir:
            f = open(os.path.join(dest, path), 'wb+')
            curs.execute("SELECT pg_bb_count_chunks(%s, %s);", [path, CSIZE])

            for c in range(0, curs.fetchone()[0]):
                curs.execute("SELECT pg_bb_read_file(%s,%s,%s);", 
                             [path, c*CSIZE, CSIZE])
                chunk = decompress(str(curs.fetchone()[0][1:]).decode('hex'))
                f.write(chunk)
            f.close()

    curs.execute("SELECT pg_stop_backup();")
    curs.close()
    
