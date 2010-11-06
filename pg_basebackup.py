#!/usr/bin/env python

import sys, os, os.path, psycopg2, datetime, shlex, subprocess
from zlib import decompress
from optparse import OptionParser

CSIZE = 8192 * 1024 # we work with chunks of 8MB
LABEL = 'pg_basebackup'
VERSION = 0.2
PGXLOG = 'pg_xlog'
PYTHON = '/usr/bin/python'

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
    print "%s [%5d] %s" \
        % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
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
            yield path, size

    return

def spawn_xlogcopy(dsn, dest, delay, verbose, debug):
    """ return the subprocess object that's copying the WALs in a loop """
    opt = ""
    if opts.debug: opt += "-d "
    if opts.verbose: opt += "-v "
    cmd = shlex.split('%s %s %s -S -D %d -x "%s" %s' \
                          % (os.environ['_'],
                             os.path.join(os.environ['PWD'], sys.argv[0]),
                             opt, delay, dsn, dest))
    if opts.verbose:
        log("Spawning %s" % " ".join(cmd))

    return subprocess.Popen(cmd,
                            stdin  = subprocess.PIPE,
                            stdout = sys.stdout,
                            stderr = sys.stderr)

def xlogcopy_loop(curs, dest, base, delay, verbose, debug):
    """ loop over WAL files and copy them, until asked to terminate """
    import time, select
    base = PGXLOG
    wal_files = {} # remember what we did already
    finished = False

    p = select.poll()
    p.register(sys.stdin, select.POLLIN)

    while not finished:
        # get all PGLOXG files, then again, then again
        for path, size in get_files(curs, dest, base, None, verbose, debug):
            if path in wal_files and wal_files[path] == size:
                log("skipping '%s', size is still %d" % (path, size))
            else:
                get_one_file(curs, dest, path, verbose, debug)
                wal_files[path] = size

        log("polling stdin")
        if p.poll(1000 * int(delay)):
            command = sys.stdin.readline()
            finished = command == "terminate\n"

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

    parser.add_option("-f", "--force", action = "store_true",
                      dest    = "force",
                      default = False,
                      help    = "remove destination directory if it exists")

    parser.add_option("-D", "--delay", dest = "delay", default = 2,
                      help    = "subprocess loop delay, use with -x")

    parser.add_option("-S", "--slave", action = "store_true",
                      dest    = "slave",
                      default = False,
                      help    = "auxilliary process")

    parser.add_option("--stdin", action = "store_true",
                      dest    = "stdin",
                      default = False,
                      help    = "get list of files to backup from stdin")

    (opts, args) = parser.parse_args()

    if opts.version:
        print VERSION
        sys.exit(0)

    opts.verbose = opts.verbose or opts.debug
    if opts.verbose: log("verbose")
    if opts.debug: log("debug chatter activated")

    if len(args) != 2:
        print "Error: see usage "
        sys.exit(1)

    dsn = args[0]
    dest = args[1]

    # prepare destination directory
    if not opts.slave:
        if os.path.isdir(dest):
            if opts.force:
                import shutil
                log("rm -rf %s" % dest)
                shutil.rmtree(dest)
            else:
                print "Error: destination directory already exists"
                sys.exit(1)
        else:
            if os.path.exists(dest):
                print "Error: '%s' already exists" % dest
                sys.exit(1)

    try:
        if opts.verbose:
            log("Connecting to '%s'" % dsn)
        conn = psycopg2.connect(dsn)
    except Exception, e:
        print "Error: couldn't connect to '%s':" % dsn
        print e
        sys.exit(2)

    # mkdir standby's PGDATA
    if not opts.slave:
        try:
            os.makedirs(dest)
        except Exception, e:
            print "Error: coudn't create the destination PGDATA at '%s'" % dest
            sys.exit(3)

    # CREATE OR REPLACE FUNCTIONs in a separate transaction
    # so that functions are visible in the slave processes
    if not opts.slave:
        if opts.verbose:
            log("Creating support functions")
        curs = conn.cursor()
        curs.execute(list_files_sql)
        curs.execute(read_file_sql)
        curs.execute("COMMIT;")
        curs.close()

    # BEGIN
    curs = conn.cursor()

    if not opts.slave:
        label = '%s_%s' % (LABEL, datetime.datetime.today().isoformat())
        if opts.verbose:
            log("SELECT pg_start_backup('%s');" % label)
        curs.execute("SELECT pg_start_backup(%s);", [label])
    else:
        log("subprocess started: %s" % " ".join(sys.argv[1:]))

    # launch an helper process to care for the logs
    if not opts.slave and not opts.xlog:
        xlogcopy = spawn_xlogcopy(dsn, dest,
                                  opts.delay, opts.verbose, opts.debug)

    #
    # The following code of course is still run in slave processes too.
    #
    # do the copy, depending if we're there for the WALs or the base backup
    base = ''
    exclude = None

    if opts.xlog:
        # the only way this function returns is when we send 'terminate\n'
        # on its standard input
        xlogcopy_loop(curs, dest, base, opts.delay, opts.verbose, opts.debug)
        sys.exit(0)

    # main loop
    exclude = PGXLOG
    for path, size in get_files(curs, dest, base, exclude,
                                opts.verbose, opts.debug):
        get_one_file(curs, dest, path, opts.verbose, opts.debug)

    # terminate the xlogcopy process
    log("sending 'terminate' to %d" % xlogcopy.pid)
    print >> xlogcopy.stdin, "terminate"

    if opts.verbose:
        log("Waiting on pid %d" % xlogcopy.pid)
    r = xlogcopy.wait()

    if opts.verbose:
        log("subprocess %d: %s" % (xlogcopy.pid, r))

    # Stop the backup now, we have it all
    if not opts.slave:
        if opts.debug:
            log("SELECT pg_stop_backup();")
        curs.execute("SELECT pg_stop_backup();")

    curs.close()
