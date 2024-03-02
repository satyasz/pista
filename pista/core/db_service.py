import time

import cx_Oracle

from core.config_service import ENV_CONFIG, ENV_CONST
from core.file_service import DataHandler
from core.log_service import Logging, printit


class DBService:
    """ To connect to databases """
    logger = Logging.get(__qualname__)

    # ORACLE_SCHEMAS = Config.ENV_CONFIG.getlist('db', 'oracle_schemas')
    # MYSQL_SCHEMAS = Config.ENV_CONFIG.getlist('db', 'mysql_schemas')
    DB_CONNS = {}  # Pair of schema name and db connection

    @classmethod
    def connect_db(cls, schema):
        ORA_CLIENT_DIRPATH = ENV_CONST.get('db', 'oracle_instaclient_dirpath')
        OR_HOST = ENV_CONFIG.get('orcl_db', 'wm_host')
        OR_PORT = ENV_CONFIG.get('orcl_db', 'wm_port')
        OR_SERVICE = ENV_CONFIG.get('orcl_db', 'wm_service')
        OR_USER = ENV_CONFIG.get('orcl_db', 'wm_user')
        OR_PWD = ENV_CONFIG.get('orcl_db', 'wm_pwd_encrypted')
        OR_PWD = DataHandler.decrypt_it(OR_PWD)

        # if schema not in cls.ORACLE_SCHEMAS and schema not in cls.MYSQL_SCHEMAS:
        #     assert False, 'Schema not defined: ' + schema
        # else:
        if schema not in cls.DB_CONNS.keys() or cls.DB_CONNS[schema] is None:
            printit('DB will connect')

            try:
                cx_Oracle.init_oracle_client(lib_dir=ORA_CLIENT_DIRPATH)
                # conn = None
                # if schema in cls.ORACLE_SCHEMAS:
                conn = cx_Oracle.connect(OR_USER + '/' + OR_PWD + '@' + OR_HOST + ':' + OR_PORT + '/' + OR_SERVICE)
                conn.callTimeout = 1000 * 200  # DPI-1067: call timeout of 1000 ms exceeded with ORA-3156
                # elif schema in cls.MYSQL_SCHEMAS:
                # conn = mysql.connector.connect(user + '/' + passwd + '@' + host_url)
                # pass
                cls.DB_CONNS[schema] = conn
            except cx_Oracle.DatabaseError as e:
                assert False, 'Exception during DB connection: ' + str(e)
            except Exception as e:
                assert False, 'Exception during DB connection: ' + str(e)

        return cls.DB_CONNS[schema]

    @classmethod
    def fetch_row(cls, query: str, schema: str = None) -> dict:
        """returns None if no data
        """
        printit('Sql', query)

        assert schema is not None, 'schema missing'

        row = {}
        cursor = None
        try:
            conn = cls.DB_CONNS[schema]
            cursor = conn.cursor()

            # if bind_var is None:
            cursor.execute(query)
            # else:
            #     cursor.execute(query, bind_var)

            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            row = cursor.fetchone()
        except cx_Oracle.DatabaseError as e:
            assert False, str(e) + ' exception during query run ' + query
        except Exception as e:
            assert False, str(e) + ' exception during query run ' + query
        finally:
            if cursor:
                cursor.close()

        noOfCol = 0 if row is None else len(row)
        printit('Cols found', str(noOfCol))

        return row  # row is None if no data

    @classmethod
    def fetch_rows(cls, query: str, schema: str = None) -> list[dict]:
        """len(returned list) = 0 if no data
        """
        printit('Sql', query)

        assert schema is not None, 'schema missing'

        rows = [{}]
        cursor = None
        try:
            conn = cls.DB_CONNS[schema]
            cursor = conn.cursor()

            # if bind_var is None:
            cursor.execute(query)
            # else:
            #     cursor.execute(query, bind_var)

            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            rows = cursor.fetchall()
        except cx_Oracle.DatabaseError as e:
            assert False, str(e) + ' exception during query run ' + query
        except Exception as e:
            assert False, str(e) + ' exception during query run ' + query
        finally:
            if cursor:
                cursor.close()

        noOfRows = 0 if rows is None else len(rows)
        printit('Rows found', str(noOfRows))

        return rows  # len(rows) = 0 if no data

    @classmethod
    def fetch_only_rows(cls, query: str, how_many_rows: int, schema: str = None) -> list[dict]:
        """len(returned list) = 0 if no data
        """
        printit('Sql', query)

        assert schema is not None, 'schema missing'

        rows = [{}]
        cursor = None
        try:
            conn = cls.DB_CONNS[schema]
            cursor = conn.cursor()

            # if bind_var is None:
            cursor.execute(query)
            # else:
            #     cursor.execute(query, bind_var)

            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            rows = cursor.fetchmany(how_many_rows)
        except cx_Oracle.DatabaseError as e:
            assert False, str(e) + ' exception during query run ' + query
        except Exception as e:
            assert False, str(e) + ' exception during query run ' + query
        finally:
            if cursor:
                cursor.close()

        noOfRows = 0 if rows is None else len(rows)
        printit('Rows found', str(noOfRows))

        return rows  # len(rows) = 0 if no data

    @classmethod
    def update_db(cls, query: str, schema: str = None) -> bool:
        printit('::: Sql', query)

        assert schema is not None, 'schema missing'

        isExecuted = False
        cursor = None
        try:
            conn = cls.DB_CONNS[schema]
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(query)
            # conn.commmit()
            isExecuted = True
            if cursor is not None:
                printit('Rows impacted', str(cursor.rowcount))
        except cx_Oracle.DatabaseError as e:
            assert False, str(e) + ' exception during query run ' + query
        except Exception as e:
            assert False, str(e) + ' exception during query run ' + query
        finally:
            if cursor:
                cursor.close()

        return isExecuted

    @classmethod
    def form_sql(cls, query: str, *args):
        for i in args:
            sqlstr = query.replace('%s', i)
        return query

    @classmethod
    def wait_for_records(cls, query: str, expected_cnt: int, schema: str = None, maxWaitInSec: int = None):
        printit('Sql', query)

        assert schema is not None, 'schema missing'

        record_found = False

        max_waittime_in_sec = maxWaitInSec if maxWaitInSec is not None else 30
        interval_waittime_in_sec = 3
        total_iteration = int(max_waittime_in_sec / interval_waittime_in_sec)

        for i in range(0, total_iteration):
            rows = DBService.fetch_rows(query, schema)
            if len(rows) >= expected_cnt:
                record_found = True
                break
            else:
                time.sleep(interval_waittime_in_sec)
        assert record_found, str(expected_cnt) + ' no. of record not found within waittime: ' + query

    @classmethod
    def wait_for_value(cls, query: str, column: str, expected_value: str, schema: str = None, maxWaitInSec: int = None):
        printit('Sql', query)

        assert schema is not None, 'schema missing'

        value_updated = False
        actual_value = ''

        max_waittime_in_sec = maxWaitInSec if maxWaitInSec is not None else 25
        interval_waittime_in_sec = 3
        total_iteration = int(max_waittime_in_sec / interval_waittime_in_sec)

        for i in range(0, total_iteration):
            row = DBService.fetch_row(query, schema)
            if row is not None and len(row) > 0:
                actual_value = str(row.get(column))
                if actual_value == expected_value:
                    value_updated = True
                    break
            if not value_updated:
                time.sleep(interval_waittime_in_sec)

        cls.compareEqual(actual_value, expected_value, column + ' value')
        assert value_updated, column + ' not updated to ' + expected_value + ' within waittime: ' + query

    @classmethod
    def _compareIfNone(cls, actualVal, expectedVal, whatIsThisDesc: str) -> bool:
        """Compare if both values are None
        Returns None/True/False
        """
        isMatched = None

        if actualVal is None and expectedVal is None:
            isMatched = True
            cls.logger.info(f"{whatIsThisDesc} matched with {actualVal}")
        elif (actualVal is None and expectedVal is not None) or (actualVal is not None and expectedVal is None):
            isMatched = False
            cls.logger.error(f"{whatIsThisDesc} didnt match, actual {actualVal}, expected {expectedVal}")
        return isMatched

    @staticmethod
    def _getStrValueOfSameType(actualVal, expectedVal) -> tuple[str, str]:
        """Get str values of same type from not None values
        """
        actualVal = float(actualVal) if type(actualVal) == float or type(actualVal) == int else actualVal
        expectedVal = float(expectedVal) if type(expectedVal) == float or type(expectedVal) == int else expectedVal

        actualVal = str(actualVal).replace('.0', '') if actualVal is not None and str(actualVal).endswith('.0') else actualVal
        expectedVal = str(expectedVal).replace('.0', '') if expectedVal is not None and str(expectedVal).endswith('.0') else expectedVal

        return str(actualVal), str(expectedVal)

    # @classmethod
    # def compareEqual_new(cls, actualVal, expectedVal, whatIsThisDesc: str, expValDesc: str = None):
    #     """Compare if 2 values are equal
    #     """
    #     expValDesc = '' if expValDesc is None else ' (' + str(expValDesc) + ')'
    #
    #     isMatched = cls._compareIfNone(actualVal, expectedVal, whatIsThisDesc)
    #     assertMsg = ''
    #
    #     if isMatched is None:
    #         actualVal, expectedVal = cls._getStrValueOfSameType(actualVal, expectedVal)
    #         isMatched = True if str(actualVal) == str(expectedVal) else False
    #         if isMatched:
    #             assertMsg = f"{whatIsThisDesc} matched with {expectedVal}{expValDesc}"
    #             cls.logger.info(assertMsg)
    #         else:
    #             assertMsg = f"{whatIsThisDesc} didnt match, actual {actualVal}, expected {expectedVal}{expValDesc}"
    #             cls.logger.error(assertMsg)
    #
    #     return isMatched, assertMsg

    @classmethod
    def compareEqual(cls, actualVal, expectedVal, whatIsThisDesc: str, expValDesc: str = None) -> bool:
        """Compare if 2 values are equal
        """
        expValDesc = '' if expValDesc is None else ' (' + str(expValDesc) + ')'

        isMatched = cls._compareIfNone(actualVal, expectedVal, whatIsThisDesc)

        if isMatched is None:
            actualVal, expectedVal = cls._getStrValueOfSameType(actualVal, expectedVal)
            isMatched = True if str(actualVal) == str(expectedVal) else False
            if isMatched:
                cls.logger.info(f"{whatIsThisDesc} matched with {expectedVal}{expValDesc}")
            else:
                cls.logger.error(f"{whatIsThisDesc} didnt match, actual {actualVal}, expected {expectedVal}{expValDesc}")
        return isMatched

    @classmethod
    def compareIn(cls, actualVal, expectedVals, whatIsThisDesc: str, expValDesc: str = None) -> bool:
        """Compare if actual val is in expected val list
        """
        expValDesc = '' if expValDesc is None else ' (' + str(expValDesc) + ')'

        isMatched = cls._compareIfNone(actualVal, expectedVals, whatIsThisDesc)

        if isMatched is None:
            isMatched = True if actualVal in expectedVals else False
            if isMatched:
                cls.logger.info(f"{whatIsThisDesc} matched with {actualVal}{expValDesc}")
            else:
                cls.logger.error(f"{whatIsThisDesc} didnt match in, actual {actualVal}, expected vals {expectedVals}{expValDesc}")
        return isMatched

# printit(DBService.form_sql('this is %s and %s', 'WM', 'ORDER'))
