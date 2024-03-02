import inspect
import logging
import os
import smtplib
import ssl
import threading
from datetime import datetime
from email.message import EmailMessage
from logging import Logger
from types import FrameType

from root import LOG_DIR

LOG_DTTM_FORMAT = '%Y%m%dT%H%M%S'
LOG_FILE_PATH = os.path.join(LOG_DIR, 'log_{}.log')


def printit(*args, isToPrint:bool=True, isForLogFile:bool=True):
    global LOG_DTTM_FORMAT, LOG_FILE_PATH

    currDate = datetime.now().strftime(LOG_DTTM_FORMAT)
    thread_id = threading.current_thread().native_id

    # caller = inspect.stack()[1].function
    # caller2 = inspect.stack()[2].function
    # lineno = inspect.currentframe().f_back.f_lineno
    # final_caller_info = f"{caller2}.{caller}:{lineno}"
    
    caller1 = inspect.getframeinfo(inspect.stack()[1][0])
    caller2 = inspect.getframeinfo(inspect.stack()[2][0])
    caller1_info = f"{caller1.function}:{caller1.lineno}"
    caller2_info = f"{caller2.function}:{caller2.lineno}"
    # final_caller_info = f"{caller2_info}({caller1_info})"
    final_caller_info = f"({caller2_info})"

    all_msg = ' '.join(map(str, args))

    if isToPrint:
        msg_to_prnt = f"{currDate} {final_caller_info} {all_msg}"
        print(msg_to_prnt)

    if isForLogFile:
        msg_to_log = f"{currDate} {thread_id} PRNT {final_caller_info} {all_msg}"
        # FileUtil.append_file(LOG_FILE_PATH.format(thread_id), msg_to_log + '\n')
        content = msg_to_log + '\n'
        with open(LOG_FILE_PATH.format(thread_id), mode='a', encoding='utf-8') as f:
            f.write(content)


class Logging:
    global LOG_DTTM_FORMAT, LOG_FILE_PATH

    # LOG_FILE_PATH = os.path.join(LOG_DIR, 'log_{}.log')

    @classmethod
    def get(cls, name) -> Logger:
        # self.logger = None
        thread_id = threading.current_thread().native_id
        # printit('inside log serv', threading.current_thread().name, threading.current_thread().native_id)
        # thread_name = 'mainthread'
        # class_name = inspect.stack()[1][3]
        # module_path = inspect.currentframe().f_back.f_globals['__name__']

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # format='%(asctime)s %(threadName)-10s %(levelname)-5s [%(filename)s:%(lineno)d %(funcName)s]  %(message)s',
        # format='%(asctime)s %(threadName)-10s %(levelname)-5s [' + module_path + ':%(lineno)d %(funcName)s]  %(message)s',
        # format = '%(asctime)s %(thread)d %(levelname)s [%(name)s %(filename)s:%(lineno)d %(funcName)s] %(message)s',
        logging.basicConfig(filename=LOG_FILE_PATH.format(thread_id), filemode='a', level=logging.INFO,
                            format='%(asctime)s %(thread)d %(levelname)s [%(funcName)s:%(lineno)d] %(message)s',
                            datefmt=LOG_DTTM_FORMAT)
        logger = logging.getLogger(name)
        return logger

    @staticmethod
    def print_args(localsDict: dict):
        printit('Arguments', {k: v for k, v in localsDict.items() if not str(k).startswith("_") and k not in 'self'})

    @staticmethod
    def _capture_func(startSign: str, frameObj: FrameType, loggerObj: Logger = None, msg: str = ''):
        func_name = frameObj.f_code.co_name

        final_msg = '' if msg == '' else ' (' + msg + ')'
        final_ln = f"+++++ {func_name}{final_msg} +++++"

        printit(final_ln)
        if loggerObj is not None:
            loggerObj.info(final_ln)

        func_args = frameObj.f_locals.items()
        func_args_dict = {k: v for k, v in func_args if not str(k).startswith("_") and k not in 'self'}
        printit('Params', func_args_dict)

    @staticmethod
    def capture_action_func_start(frameObj: FrameType, loggerObj: Logger = None, msg: str = ''):
        Logging._capture_func('+++++', frameObj, loggerObj, msg)

    @staticmethod
    def capture_assert_func_start(frameObj: FrameType, loggerObj: Logger = None):
        Logging._capture_func('-----', frameObj, loggerObj)

    @staticmethod
    def send_mail(frompwd, env, startTime, endtime, thread_id, total_workers, testscollected, testsfailed):
        """Send mail"""
        if frompwd is not None and str(frompwd) != '':
            try:
                fromemail = '' # 'testautomationservice1@gmail.com' # TODO setup
                toemail = '' # 'satyanarayan_sahu@undocked.net' # TODO setup
                subject = 'Automation run (' + str(thread_id) + '/' + str(
                    1 if total_workers is None else total_workers) + ')'
                body_list = list()
                body_list.append('Total tests' + '\t' + str(testscollected))
                body_list.append('Tests failed' + '\t' + str(testsfailed))
                body_list.append('')
                body_list.append('Env' + '\t' + str(env))
                body_list.append('')
                body_list.append('Run start time' + '\t' + str(startTime))
                body_list.append('Run end time' + '\t' + str(endtime))
                final_body = '\n'.join(i for i in body_list)

                emsg = EmailMessage()
                emsg['From'] = fromemail
                emsg['To'] = toemail
                emsg['Subject'] = subject
                emsg.set_content(final_body)
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                    smtp.login(fromemail, frompwd)
                    smtp.sendmail(fromemail, toemail, emsg.as_string())
            except Exception as e:
                printit('Exception while sending mail ' + str(e))