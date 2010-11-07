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
 OUT size bigint,
 OUT ctime timestamptz
)
 returns setof record
 language SQL
as $$
with recursive files(path, isdir, size) as (
  select name, (pg_stat_file(name)).isdir,
         (pg_stat_file(name)).size, (pg_stat_file(name)).change
    from (select case when $1 = ''
                      then pg_ls_dir(setting)
                      else $1 || '%s' || pg_ls_dir(setting || '%s' || $1)
                  end as name
            from pg_settings
	   where name = 'data_directory') as toplevel

  UNION ALL

  select name, (pg_stat_file(name)).isdir,
         (pg_stat_file(name)).size, (pg_stat_file(name)).change
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

    for path, isdir, size, ctime in curs.fetchall():
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
            yield path, size, ctime

    return

def spawn_helper(dsn, dest, stdin, pgxlog, delay, verbose, debug):
    """ return the subprocess object we need """
    opt = ""
    if debug:   opt += "-d "
    if verbose: opt += "-v "
    if stdin:   opt += "--stdin "
    if pgxlog:  opt += "-x "
    if delay:   opt += "-D %d " % delay

    cmd = shlex.split('%s %s -S %s "%s" %s' \
                          % (os.environ['_'],
                             os.path.join(os.environ['PWD'], sys.argv[0]),
                             opt, dsn, dest))
    if verbose:
        log("Spawning %s" % " ".join(cmd))

    return subprocess.Popen(cmd,
                            stdin  = subprocess.PIPE,
                            stdout = sys.stdout,
                            stderr = sys.stderr)

def spawn_xlogcopy(dsn, dest, delay, verbose, debug):
    """ return the subprocess object that's copying the WALs in a loop """
    return spawn_helper(dsn, dest, False, True, delay, verbose, debug)

def spawn_basecopy(dsn, dest, verbose, debug):
    """ return a slave subprocess object"""
    return spawn_helper(dsn, dest, True, False, None, verbose, debug)

def xlogcopy_loop(curs, dest, base, delay, verbose, debug):
    """ loop over WAL files and copy them, until asked to terminate """
    import time, select
    wal_files = {} # remember what we did already
    finished = False

    p = select.poll()
    p.register(sys.stdin, select.POLLIN)

    while not finished:
        # get all PGXLOG files, then again, then again
        for path, size, ctime \
                in get_files(curs, dest, base, None, verbose, debug):
            if path in wal_files and wal_files[path] == ctime:
                log("skipping '%s', same ctime" % path)
            else:
                get_one_file(curs, dest, path, verbose, debug)
                wal_files[path] = ctime

        if verbose:
            log("polling stdin")
        if p.poll(1000 * int(delay)):
            command = sys.stdin.readline()
            finished = command == "terminate\n"

    return

def basecopy_loop(curs, dest, base, verbose, debug):
    """ copy files given on stdin until we read 'terminate' """
    for path in sys.stdin:
        path = path[:-1]  # chomp \n
        get_one_file(curs, dest, path, verbose, debug)
    return

if __name__ == '__main__':
    usage  = '%prog [-v] [-f] [-j jobs] "dsn" dest'
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

    parser.add_option("-j", "--jobs", dest = "jobs",
                      type = "int", default = 1,
                      help    = "how many helper jobs to launch")

    parser.add_option("-D", "--delay", dest = "delay",
                      type = "int", default = 2,
                      help    = "pg_xlog subprocess loop delay, see -x")

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

    if not opts.slave and not opts.xlog:
        label = '%s_%s' % (LABEL, datetime.datetime.today().isoformat())
        log("SELECT pg_start_backup('%s');" % label)
        curs.execute("SELECT pg_start_backup(%s);", [label])
    else:
        if opts.verbose:
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
        if opts.verbose:
            log("Entering xlogcopy loop with delay %d" % opts.delay)
        try:
            xlogcopy_loop(curs, dest, PGXLOG, opts.delay, opts.verbose, opts.debug)
        except (Exception, KeyboardInterrupt), e:
            # pg_basebackup.py -x could be run on its own for a warm standby
            log(e)
        curs.close()
        sys.exit(0)

    if opts.slave:
        if opts.verbose:
            log("Entering basecopy loop")
        basecopy_loop(curs, dest, base, opts.verbose, opts.debug)
        curs.close()
        sys.exit(0)

    # main loop --- slaves have exited already, won't reach this code.
    exclude = PGXLOG

    # prepare the helpers
    if opts.jobs > 1:
        jobs = {}
        for j in range(opts.jobs):
            jobs[j] = spawn_basecopy(dsn, dest, opts.verbose, opts.debug)

    n = 0
    for path, size, ctime in get_files(curs, dest, base, exclude,
                                       opts.verbose, opts.debug):
        if opts.jobs == 1:
            # do the job ourself, how boring
            get_one_file(curs, dest, path, opts.verbose, opts.debug)
        else:
            # give next slave some work
            #log("%d <-- '%s'" % (jobs[n % opts.jobs].pid, path))
            print >> jobs[n % opts.jobs].stdin, path
            n += 1

    # teminate the helpers and wait on them
    if opts.jobs > 1:
        for j in range(opts.jobs):
            if opts.verbose:
                log("close %d" % jobs[j].pid)
            jobs[j].stdin.close()

    if opts.jobs > 1:
        for j in range(opts.jobs):
            jobs[j].wait()

    # terminate the xlogcopy process
    if opts.verbose:
        log("sending 'terminate' to %d" % xlogcopy.pid)
    print >> xlogcopy.stdin, "terminate"
    if opts.verbose:
        log("Waiting on pid %d" % xlogcopy.pid)
    xlogcopy.wait()

    # Stop the backup now, we have it all
    if not opts.slave:
        log("SELECT pg_stop_backup();")
        curs.execute("SELECT pg_stop_backup();")

    curs.close()

    log("Your cluster is ready at '%s'" % dest)
    print
