import os
import random
import threading
import time
from enum import Enum

from core.common_service import Commons
from core.config_service import ENV_CONST
from core.file_service import ExcelUtil
from core.log_service import printit
from root import THREAD_DATA_RUNTIME_FILE


class RuntimeAttr(Enum):
    ITEMS = 'ITEMS'
    CONSOL_LOCNS = 'CONSOL_LOCNS'
    WAVE_TYPE = 'WAVE_TYPE'
    DOCKDOOR = 'DOCKDOOR'
    LOCNS = 'LOCNS'


class RuntimeXL:
    """Provides utilities to handle runtime threadData
    """
    RUNTIME_FILE_PATH = THREAD_DATA_RUNTIME_FILE
    RUNTIME_LOCKFILE_PATH = RUNTIME_FILE_PATH + ".lock"
    RUNTIME_SHEET = 'Sheet1'

    ITER_WAITTIME_SEC = 3
    RUNTIME_FILE_LOCK_STAT = {}  # {threadId:isLockCreated} e.g: {thread1:True, thread2:False}
    # _IS_USE_THREAD_DATAFILE = ENV_CONST.get('framework', 'is_use_thread_data_file')  # for runtime threadData use
    _IS_USE_THREAD_DATAFILE = ''

    # @classmethod
    # def _waitFor_lockFile_remove(cls):
    #     """Wait if lock file exists
    #     """
    #     MAX_WAITTIME_SEC = 9
    #     MAX_ITER = int(MAX_WAITTIME_SEC / cls.ITER_WAITTIME_SEC)
    #
    #     for i in range(MAX_ITER):
    #         if os.path.exists(cls.RUNTIME_LOCKFILE_PATH):
    #             time.sleep(cls.ITER_WAITTIME_SEC)
    #         else:
    #             break

    # TODO Might be useful
    # @classmethod
    # def _waitTillValExistsInThisAttrInAnyThread(cls, attr_name: RuntimeAttr):
    #     """Wait if any other thread has any attr val
    #     """
    #     if 'true' in cls._IS_USE_RUNTIME_FILE:
    #         MAX_WAITTIME_SEC = 60
    #         MAX_ITER = int(MAX_WAITTIME_SEC / cls.ITER_WAITTIME_SEC)
    #
    #         for i in range(MAX_ITER):
    #             all_data_for_col = cls._fetchThisAttrFromAllThreads(attr_name)
    #             if all_data_for_col is not None and len(all_data_for_col) > 0:
    #                 time.sleep(cls.ITER_WAITTIME_SEC)
    #             else:
    #                 break

    # TODO Might be useful
    # @classmethod
    # def _waitTillThisValExistsInThisAttrInAnyThread(cls, attr_name: RuntimeAttr, attr_val):
    #     """Wait if any other thread has specific attr val
    #     """
    #     if 'true' in cls._IS_USE_RUNTIME_FILE:
    #         MAX_WAITTIME_SEC = 60
    #         MAX_ITER = int(MAX_WAITTIME_SEC / cls.ITER_WAITTIME_SEC)
    #
    #         for i in range(MAX_ITER):
    #             all_data_for_col = cls._fetchThisAttrFromAllThreads(attr_name)
    #             if all_data_for_col is not None and len(all_data_for_col) > 0 and str(attr_val) in all_data_for_col:
    #                 time.sleep(cls.ITER_WAITTIME_SEC)
    #             else:
    #                 break

    @classmethod
    def createThreadLockFile(cls, maxWaitSec: int = None):
        """Wait if lock file is available
        Else create
        """
        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id
            if not RuntimeXL.RUNTIME_FILE_LOCK_STAT[thread_id]:
                printit('... Thread data lock file status', RuntimeXL.RUNTIME_FILE_LOCK_STAT)
                printit(f"... Thread {thread_id} creating thread data lock file")

                time.sleep(random.choice([1, 2, 3, 4, 5]))

                MAX_WAITTIME_SEC = 300 if maxWaitSec is None else maxWaitSec
                MAX_ITER = int(MAX_WAITTIME_SEC / cls.ITER_WAITTIME_SEC)

                for i in range(MAX_ITER):
                    if not os.path.exists(cls.RUNTIME_LOCKFILE_PATH):
                        try:
                            os.mkdir(cls.RUNTIME_LOCKFILE_PATH)
                            RuntimeXL.RUNTIME_FILE_LOCK_STAT[thread_id] = True
                            time.sleep(1.0)
                            break
                        except FileExistsError as e:
                            time.sleep(cls.ITER_WAITTIME_SEC)
                    else:
                        time.sleep(cls.ITER_WAITTIME_SEC)

    @classmethod
    def removeThreadLockFile(cls):
        """Remove lock file if available
        """
        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id
            printit('... Thread data lock file status', RuntimeXL.RUNTIME_FILE_LOCK_STAT)
            printit(f"... Thread {thread_id} removing thread data lock file")

            if RuntimeXL.RUNTIME_FILE_LOCK_STAT[thread_id]:
                if os.path.exists(cls.RUNTIME_LOCKFILE_PATH):
                    try:
                        os.rmdir(cls.RUNTIME_LOCKFILE_PATH)
                        RuntimeXL.RUNTIME_FILE_LOCK_STAT[thread_id] = False
                        time.sleep(1.0)
                    except FileNotFoundError as e:
                        printit('... TODO: Check thread data lock file not found to remove')

    @classmethod
    def _fetchThisAttrFromAllThreads(cls, attr_name: RuntimeAttr, row_header_to_avoid:None) -> str:
        """Returns a str with the values from all the rows for a column from the threadData file
        Returns empty i.e., '' if no val found
        """
        thisAttrVals = None

        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            try:
                thisAttrVals = ExcelUtil.read_all_xlrows_for_column(filepath=cls.RUNTIME_FILE_PATH, col_header=attr_name.value,
                                                                    row_header_to_avoid=row_header_to_avoid)
            finally:
                pass

        return thisAttrVals

    @classmethod
    def getThisAttrFromAllThreads_old(cls, attr_name: RuntimeAttr, replaceFrom: str = None, replaceWith: str = None) -> str:
        """Returns a tuple as str with the values from all the rows for a column from the threadData file
           after replacing each replaceFrom by provided replaceWith
        Returns None if no value found
        """
        thisAttrVals = None

        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thisAttrVals = cls._fetchThisAttrFromAllThreads(attr_name=attr_name)

            if thisAttrVals == '':
                thisAttrVals = None
            else:
                thisAttrVals = \
                    Commons.get_tuplestr_byreplace(thisAttrVals, replace_from=replaceFrom, replace_with=replaceWith)

        if thisAttrVals is not None:
            printit(f"... Thread excluding thread {attr_name.name} {thisAttrVals}")

        return thisAttrVals

    @classmethod
    def getThisAttrFromAllThreads(cls, attr_name: RuntimeAttr, replaceFrom: str = None, replaceWith: str = None) -> str:
        """Returns a tuple as str with the values from all the rows for a column from the threadData file
           after replacing each replaceFrom by provided replaceWith.
           Remember, this excludes the value for curr thread.
        Returns None if no value found
        """
        thisAttrVals = None

        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id
            
            thisAttrVals = cls._fetchThisAttrFromAllThreads(attr_name=attr_name, row_header_to_avoid=thread_id)

            if thisAttrVals == '':
                thisAttrVals = None
            else:
                thisAttrVals = \
                    Commons.get_tuplestr_byreplace(thisAttrVals, replace_from=replaceFrom, replace_with=replaceWith)

        if thisAttrVals is not None:
            printit(f"... Thread excluding other thread {attr_name.name} {thisAttrVals}")

        return thisAttrVals
    
    @classmethod
    def updateThisAttrForThread(cls, attr_name: RuntimeAttr, cell_val_as_csv):
        """Update/append provided attr val for provided thread
        """
        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id

            discard_vals = ['None', '', ' ']

            if cell_val_as_csv is not None and len(cell_val_as_csv) > 0:
                cell_val_as_list = str(cell_val_as_csv).split(',')
                cell_val_as_csv = ','.join(set(cell_val_as_list).difference(discard_vals))

            try:
                if len(cell_val_as_csv) > 0:
                    ExcelUtil.append_to_xlcell(cls.RUNTIME_FILE_PATH, thread_id, attr_name.value, cell_val_as_csv)
            finally:
                pass

    # @classmethod
    # def update_waitFor_allThread_1AttrVal_clear(cls, thread_id, attr_name: RuntimeAttr, cell_value):
    #     """Wait until 1 attr val for all threads get clear
    #     Then update
    #     """
    #     try:
    #         # cls._waitFor_lockFile_remove()
    #         # cls._waitFor_allThread_1AttrVal_clear(attr_name)
    #         # cls._create_lockFile()
    #         ExcelUtil.append_to_xlcell(cls.RUNTIME_FILE_PATH, thread_id, attr_name.value, cell_value)
    #     finally:
    #         cls._removeLockFile()

    @classmethod
    def clearThisAttrForThread(cls, attr_name: RuntimeAttr):
        """Clear provided attr for provided thread
        """
        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id

            try:
                ExcelUtil.clear_xlcells(filepath=cls.RUNTIME_FILE_PATH, row_header=thread_id,
                                        col_header=attr_name.value)
            finally:
                pass

    @classmethod
    def clearAllAttrForThread(cls):
        """Clear all attrs for provided thread
        """
        if 'true' in cls._IS_USE_THREAD_DATAFILE:
            thread_id = threading.current_thread().native_id

            try:
                ExcelUtil.clear_xlcells(filepath=cls.RUNTIME_FILE_PATH, row_header=thread_id)
            finally:
                pass

# RuntimeUtil.updateThreadDataForAttr('Thread2', RuntimeAttr.WAVE_TYPE, 'Nice Wave')
# RuntimeUtil.clearThreadDataForAttr('Thread2', RuntimeAttr.WAVE_TYPE)
# RuntimeUtil.clearThreadDataForAllAttr('Thread2')
# a = RuntimeUtil.readAllThreadDataForAttr(RuntimeAttr.WAVE_TYPE)
# printit(a)
