import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import NamedTupleCursor


NULL = None
SCALAR = 0
ROW = 1
ROWS = 2
NOTHING = None


def initialize(minconn: int, maxconn: int, dsn: str, cursor_factory=NamedTupleCursor):
    """
    initializes pool and creates a function to wrap your queries with
    :param minconn: minimum pool connections number
    :param maxconn: maximum pool connections number
    :param dsn: dsn string to connect to the database server
    :param cursor_factory: cursor factory to use with the connection (default: NamedTupleCursor)
    :return: tuple of pool and wrapper function
    """
    assert maxconn >= minconn > 0

    _pool = ThreadedConnectionPool(
        minconn,
        maxconn,
        dsn=dsn,
        cursor_factory=cursor_factory
    )

    def query_factory(sql, returning, default, commit, pipe=None):
        """
        turns sql code to a python function
        :param sql: sql code
        :param returning:
            what kind of value the sql returns.
            Can be nothing (always None), a row, multiple rows or a scalar.
        :param default:
            default value to return, in case None is returned from postgres backend. 
            For example, if you create a MAX query, postgres will return NULL, if there are no records found. But you
            can pass in a 0 here, so it is never a None value.
        :param commit:
            if True, commit is attempted. False, not committed.
        :param pipe:
            You can specify some function (or an object constructor).
            This function will be called with query results as input and the output of this function will be returned.
            If None is specified, raw query output is returned.
            >>> class User:
            >>> # some user methods, user fields and such
            >>>     ...
            >>> sql = "SELECT * FROM users WHERE id = %s LIMIT 1"
            >>> get_user_by_id = query_factory(sql, returning=ROW, default=NULL, pipe=User)
            >>> user = get_user_by_id(1)  # here you'll get an instance of User class, if such exists
        :return: pool connection and an sql-wrapping function
        """
        if returning == SCALAR:
            def fetch(cur):
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return default
                else:
                    return row[0]
        elif returning == ROW:
            def fetch(cur):
                row = cur.fetchone()
                if row is None:
                    return default
                else:
                    return row
        elif returning == ROWS:
            def fetch(cur):
                return cur.fetchall()
        elif returning is NOTHING:
            def fetch(cur):
                return NULL
        else:
            raise ValueError(f"Unknown query return type: {returning}.")

        def query(*args, **kwargs):
            arguments = None
            if args and kwargs:
                raise ValueError("Cannot pass both positional and keyword arguments to a query.")
            if args:
                arguments = args
            elif kwargs:
                arguments = kwargs

            conn = _pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, arguments)
                    res = fetch(cur)
                if commit:
                    conn.commit()
            except psycopg2.Error as ex:
                conn.rollback()  # transaction rollback on exception
                raise ex  # before this return the finally block gets executed
            finally:
                # before returning we must put connection back to the pool, as it is no longer being used
                _pool.putconn(conn)
            if pipe:
                return pipe(res)
            else:
                return res

        return query

    return _pool, query_factory
