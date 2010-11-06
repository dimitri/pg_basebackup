#!/usr/bin/env python

import sys, os, os.path, psycopg2, datetime
from zlib import decompress
from optparse import OptionParser

CSIZE = 8192 * 1024 # we work with chunks of 8MB
LABEL = 'pg_basebackup'
VERSION = 0.2
PGXLOG = 'pg_xlog'

list_files_sql = """
CREATE OR REPLACE FUNCTION pg_bb_list_files
( IN basepath text default '',
 OUT path text,
 OUT isdir bool,
 OUT size bigint
)
 returns setof record
 language SQL
as $$
with recursive files(path, isdir, size) as (
  select name, (pg_stat_file(name)).isdir, (pg_stat_file(name)).size
    from (select case when $1 = ''
                      then pg_ls_dir(setting)
                      else $1 || '%s' || pg_ls_dir(setting || '%s' || $1)
                  end as name
            from pg_settings
	   where name = 'data_directory') as toplevel

  UNION ALL

  select name, (pg_stat_file(name)).isdir, (pg_stat_file(name)).size
    from (select path || '%s' || pg_ls_dir(path) as name
            from files
	   where isdir
         ) as curdir
)
select * from files;
$$;
""" % (os.path.sep, os.path.sep, os.path.sep)

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

def log(msg):
    """Just print the message out, with timestamp and pid"""
    print "%s [%s] %s" \
        % (datetime.datetime.now().strftime("%Y%m%d %H:%M:%S.%f"),
           os.getpid(), msg)

def get_one_file(curs, dest, path, verbose, debug):
    """Create the file named path in dest, and fetch its content"""
    f = open(os.path.join(dest, path), 'wb+')

    if verbose:
        log(path)

    if debug:
        log("SELECT pg_bb_count_chunks(%s, %s);" % (path, CSIZE))

    curs.execute("SELECT pg_bb_count_chunks(%s, %s);", [path, CSIZE])
    for c in range(0, curs.fetchone()[0]):
        if debug:
            log("SELECT pg_bb_read_file(%s,%s,%s);" % (path, c*CSIZE, CSIZE))

        curs.execute("SELECT pg_bb_read_file(%s,%s,%s);",
                     [path, c*CSIZE, CSIZE])
        chunk = decompress(str(curs.fetchone()[0][1:]).decode('hex'))
        f.write(chunk)

    f.close()

def get_files(curs, dest, base, exclude, verbose, debug):
    """Get all files from master starting at path, and write them in dest"""
    sql = "SELECT * FROM pg_bb_list_files(%s)"
    if exclude:
        sql += " WHERE path !~ '%s' " % exclude

    if verbose:
        log(sql % base)

    curs.execute(sql, [base])

    for path, isdir, size in curs.fetchall():
        if isdir:
            curdir = path
        else:
            curdir = os.path.dirname(path)

        cwd = os.path.join(dest, curdir)

        if not os.path.isdir(cwd):
            if verbose:
                log("mkdir -p %s" % cwd)
            os.makedirs(cwd)

        if not isdir:
            get_one_file(curs, dest, path, verbose, debug)
    return

if __name__ == '__main__':
    usage  = '%prog [-x] "dsn" dest'
    parser = OptionParser(usage = usage)

    parser.add_option("--version", action = "store_true",
                      dest    = "version",
                      default = False,
                      help    = "show version and quit")

    parser.add_option("-x", "--pg_xlog", action = "store_true",
                      dest    = "xlog",
                      default = None,
                      help    = "backup the pg_xlog files")

    parser.add_option("-v", "--verbose", action = "store_true",
                      dest    = "verbose",
                      default = False,
                      help    = "be verbose and about processing progress")

    parser.add_option("-d", "--debug", action = "store_true",
                      dest    = "debug",
                      default = False,
                      help    = "show debug information, including SQL queries")

    parser.add_option("-n", "--dry-run", action = "store_true",
                      dest    = "dry_run",
                      default = False,
                      help    = "only tell what it would do")

    parser.add_option("-f", "--force", action = "store_true",
                      dest    = "force",
                      default = False,
                      help    = "remove destination directory if it exists")

    (opts, args) = parser.parse_args()

    if opts.version:
        print VERSION
        sys.exit(0)

    base = ''
    if opts.xlog:
        base = PGXLOG

    opts.verbose = opts.verbose or opts.dry_run or opts.debug
    if opts.verbose:
        if opts.debug:
            print "We'll be verbose, and show debug information"
        else:
            print "We'll be verbose"

    if len(args) != 2:
        print "Error: see usage "
        sys.exit(1)

    dsn = args[0]
    dest = args[1]

    # prepare destination directory
    if os.path.isdir(dest):
        if opts.force:
            import shutil
            print "rm -rf %s" % dest
            shutil.rmtree(dest)
        else:
            print "Error: destination directory already exists"
            sys.exit(1)
    else:
        if os.path.exists(dest):
            print "Error: '%s' is not a directory" % dest
            sys.exit(1)

    try:
        if opts.verbose:
            print "Connecting to '%s'" % dsn
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
    if opts.verbose:
        print "Creating support functions"

    curs.execute(list_files_sql)
    curs.execute(read_file_sql)

    label = '%s_%s' % (LABEL, datetime.datetime.today().isoformat())
    if opts.verbose:
        log("SELECT pg_start_backup('%s');" % label)
    curs.execute("SELECT pg_start_backup(%s);", [label])

    # do the copy
    exclude = None
    if not opts.xlog:
        exclude = PGXLOG

    get_files(curs, dest, base, exclude, opts.verbose, opts.debug)

    if opts.debug:
        log("SELECT pg_stop_backup();")
    curs.execute("SELECT pg_stop_backup();")
    curs.close()
