import os
import random
import string
from collections import OrderedDict
from datetime import datetime, timedelta
import pytz
from typing import Union

import wmi


class Commons:

    @staticmethod
    def get_unique_num(length: int = 14) -> int:
        """ Get a unique number of a specific length based on date & time"""
        today = str(datetime.today())
        today = today[0:today.find('.')]
        today = today.replace('-', '').replace(':', '').replace(' ', '').replace('.', '')
        if length == 14:
            pass
        print('Unique num', today)
        return int(today)

    @staticmethod
    def get_random_alphanum(prefix: str, len_with_prefix=None, len_without_prefix=None) -> str:
        ran_len = int(len_with_prefix) - len(prefix) if len_with_prefix is not None else int(len_without_prefix)
        ran = ''.join(random.choices(string.ascii_uppercase + string.digits, k=ran_len))
        if prefix is not None:
            ran = prefix + str(ran)
        return ran

    @staticmethod
    def get_random_num(prefix: str, len_with_prefix=None, len_without_prefix=None) -> str:
        """ Provides a random num of a specific length """
        ran_len = int(len_with_prefix) - len(prefix) if len_with_prefix is not None else int(len_without_prefix)
        minrange = pow(10, ran_len - 1)
        maxrange = pow(10, ran_len) - 1
        ran = random.randint(minrange, maxrange)
        if prefix is not None:
            ran = prefix + str(ran)
        # print('Random num', randnum)
        return str(ran)

    @staticmethod
    def get_min_num(numLen: int) -> str:
        """ Get min number of a specific length"""
        num = '1'
        for i in range(1, numLen):
            num = num + '0'
        return num

    @staticmethod
    def get_next_alphanum(alphanum: str):
        """ Provides next alphanum string of same length """
        finalval = str()
        alphaval = str()
        numval = str()
        upcase = range(65, 91)  # capital letters
        lowcase = range(97, 123)  # small letters

        for i in range(0, len(alphanum)):
            if alphanum[i].isalpha():
                alphaval = alphaval + alphanum[i]
            elif alphanum[i].isdigit():
                numval = numval + alphanum[i]

        new_numval = int(numval) + 1
        if len(str(int(numval) + 1)) == len(numval):
            finalval = alphaval + str(new_numval)
        else:
            new_alphaval = str()
            new_rightval = str()
            for i in range(len(alphaval) - 1, -1, -1):
                new_alphaval = alphaval[0:i]
                alphachar = alphaval[i]
                if ord(alphachar) in upcase and (ord(alphachar) + 1) in upcase:
                    new_rightval = new_rightval + chr(ord(alphachar) + 1)
                    break
                else:
                    new_rightval = new_rightval + chr(ord(alphachar))
                    continue
            new_alphaval = new_alphaval + new_rightval[::-1]
            finalval = new_alphaval + str(new_numval)[:-1]

        if finalval == '':
            assert False, 'Next alphanum not found'
        print('Old alphanum', alphanum, 'New alphanum', finalval)
        return finalval

    @staticmethod
    def get_tuplestr(from_data: Union[str, int, float, list, set]):
        """Convert to tuple str"""
        if type(from_data) in (str, int, float):
            from_data = [from_data] # Convert to list

        tuple_data = tuple(from_data)
        # tuple_str = str(tuple_data).replace(',', '', 1) if len(tuple_data) == 1 else str(tuple_data)
        tuple_str = ''.join(str(tuple_data).rsplit(',', 1)) if len(tuple_data) == 1 else str(tuple_data)

        return tuple_str

    @staticmethod
    def get_tuplestr_byreplace(from_data: Union[str, int, float, list],
                               replace_from: str = None, replace_with: str = None):
        """Convert to tuple str
        Replace specific char by provided char
        """
        tuple_str = Commons.get_tuplestr(from_data)

        if replace_from is not None and replace_with is not None:
            tuple_str = tuple_str.replace(replace_from, replace_with)

        return tuple_str

    @staticmethod
    def remove_vals_from_list(base_list: list, vals_to_remove: list):
        # vals_to_remove = ['*', '#']
        for i in vals_to_remove:
            while i in base_list:
                base_list.remove(i)
        return base_list

    @staticmethod
    def if_contains_all_substrings(mainstring: str, substrings: list[str]):
        """Returns True if string contains all substrings
        Else False
        """
        result = all(i in mainstring for i in substrings)
        return result

    @staticmethod
    def format_date(date: datetime, dtformat: str) -> str:
        return date.strftime(dtformat)

    @staticmethod
    def format_current_date(dtformat: str) -> str:
        return datetime.today().strftime(dtformat)

    @staticmethod
    def format_utc_current_date(dtformat: str) -> str:
        local_time = datetime.now()
        utc_time = local_time.astimezone(pytz.UTC)
        return utc_time.strftime(dtformat)

    @staticmethod
    def build_date_forfilename() -> str:
        return datetime.today().strftime("%Y%m%dT%H%M%S")
        # return str(now_dt)[:str(now_dt).index('.')].replace(':', '.')

    @staticmethod
    def get_filename_frompath(filepath: str) -> str:
        filename = filepath.split(os.path.sep)[-1].split('.')[0]
        return filename

    @staticmethod
    def get_table_data_aligned(table_data):
        headers = list(table_data[0].keys())
        widths = [max(len(str(row[key])) for row in table_data) for key in headers]
        aligned_data = "  ".join((header.ljust(width) for header, width in zip(headers, widths)))
        for row in table_data:
            aligned_data = aligned_data + '\n' + "  ".join((str(row[key]).ljust(width) for key, width in zip(headers, widths)))
        return aligned_data

    @staticmethod
    def _close_node_process():
        """To close node process used for xterm
        """
        try:
            f = wmi.WMI()
            # today_dt = datetime.today()
            # yesterday_dt = today_dt + timedelta(days=-1)
            # todaydt_frmtd = Commons.format_date(today_dt, '%Y%m%d')
            # yesterdaydt_frmtd = Commons.format_date(yesterday_dt, '%Y%m%d')
            for process in f.Win32_Process():
                if f"{process.Name}".count('node.exe') > 0:
                    # print(f"{process.ProcessId} {process.Name}")
                    # if str(process.CreationDate)[0:8] in (todaydt_frmtd, yesterdaydt_frmtd):
                    if 'yarn' in f"{process.CommandLine}" and 'start' in f"{process.CommandLine}":
                        print('Terminating process', process.ProcessId, process.Name, process.CreationDate)
                        process.Terminate()
                    break
        except Exception as e:
            print('Exception during node terminate ' + str(e))

    @staticmethod
    def _is_node_running() -> bool:
        """Returns True is node for xterm is already running
        """
        is_node_running = False
        try:
            f = wmi.WMI()
            # today_dt = datetime.today()
            # yesterday_dt = today_dt + timedelta(days=-1)
            # todaydt_frmtd = Commons.format_date(today_dt, '%Y%m%d')
            # yesterdaydt_frmtd = Commons.format_date(yesterday_dt, '%Y%m%d')
            for process in f.Win32_Process():
                if f"{process.Name}".count('node.exe') > 0:
                    # print(f"{process.ProcessId} {process.Name}")
                    # if str(process.CreationDate)[0:8] in (todaydt_frmtd, yesterdaydt_frmtd):
                    if 'yarn' in f"{process.CommandLine}" and 'start' in f"{process.CommandLine}":
                        is_node_running = True
                    break
        except Exception as e:
            print('Exception during node terminate ' + str(e))

        return is_node_running

    @staticmethod
    def update_dict(curr_dict: dict, new_dict: dict, skip_existing_key: bool = True):
        """Update curr_dict with the items from new_dict.
        Skips for existing key.
        """
        # curr_dic.update(new_dict)
        curr_dict = {**curr_dict, **{k: v for k, v in new_dict.items() if k not in curr_dict}}
        return curr_dict

    @staticmethod
    def sort_ordered_dict_by_val(ordered_dict):
        sorted_dict = OrderedDict(
            sorted(ordered_dict.items(), key=lambda x: x[1]))  # Sort the OrderedDict by its values
        return sorted_dict

    @staticmethod
    def remove_duplicate_val_from_ordered_dict(ordered_dict):
        ordered_dict_new = OrderedDict()
        for k, v in ordered_dict.items():
            if v not in ordered_dict_new.values():
                ordered_dict_new[k] = v
        return ordered_dict_new

    @staticmethod
    def check_number_type(s):
        """Return int, float or str"""
        final_type = "str"
        if s.isdigit():
            final_type = "int"
        else:
            try:
                float(s)
                final_type = "float"
            except ValueError:
                final_type = "str"
        return final_type

    @staticmethod
    def get_test_filename():
        """Get test filename from os env variable"""
        file_absPath = os.environ['PYTEST_CURRENT_TEST'].split('::')[0]
        file_absPath_splt = file_absPath.split('/')
        filename = file_absPath_splt[len(file_absPath_splt) - 1].split('.')[0]
        return filename
