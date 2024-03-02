import inspect
import os

import pytest

from core.common_service import Commons
from core.config_service import ENV_CONST
from root import TEST_DIR

is_group_decider_found = False
is_mark_decider_found = False

if os.path.exists(os.path.join(TEST_DIR, 'group_decider.py')):
    is_group_decider_found = True
    from tests import group_decider

if os.path.exists(os.path.join(TEST_DIR, 'mark_decider.py')):
    is_mark_decider_found = True
    from tests import mark_decider


def allmarks(*decos):
    """Compose all markers
    """
    def composition(func):
        for deco in decos:
            func = deco(func)
        return func
    return composition


# def rerunflaky_if_order1(func):
#     if 'order' in func.pytestmark:
#         for mark in func.pytestmark:
#             if mark.name == 'order' and mark.args[0] == 1:
#                 func = pytest.mark.flaky(reruns=1)(func)
#     return func


def class_marker(cls):
    """Handle class/funcs annotations:
    - Assignes pytest.mark (as defined in tests/mark_decider.py) to all the tests in the class
    - Decides xdist_group name (as defined in tests/group_decider.py) for the class
    """
    currtest_filename = dict(cls.__dict__.items())['__module__']

    for attr_name, attr_value in cls.__dict__.items():
        if callable(attr_value) and attr_name.startswith('test_'):
            # '''Assign Jira test case ID to test methods'''
            # setattr(cls, attr_name, pytest.mark.jira(JiraMapper[attr_name].value)(attr_value))

            currtest_marks = dict(attr_value.__dict__.items())
            if 'pytestmark' not in currtest_marks.keys():
                currtest_marks['pytestmark'] = []

            '''Assign markers to test methods as defined in file'''
            if is_mark_decider_found:
                all_mark_decider = {k: v for k, v in mark_decider.__dict__.items() if not k.startswith('__')}
                if currtest_filename in all_mark_decider.keys():
                    for i in range(len(all_mark_decider[currtest_filename])):
                        currtest_marks['pytestmark'].append(all_mark_decider[currtest_filename][i].mark)
                        # setattr(cls, attr_name, attr_value)

            '''Assign group name to test methods based on diff classes'''
            xdistgrp = pytest.mark.xdist_group.mark
            xdistgrp.kwargs['name'] = groupid(currtest_filename)
            currtest_marks['pytestmark'].append(xdistgrp)

            '''Applies flaky reruns=1 to the test methods having marker order=1'''
            if 'is_use_flaky' in dict(dict(ENV_CONST)['framework']) and 'true' in ENV_CONST.get('framework', 'is_use_flaky'):
                if 'order' in str(attr_value.pytestmark):
                    for mark in attr_value.pytestmark:
                        if mark.name == 'order' and mark.args[0] == 1:
                            setattr(cls, attr_name, pytest.mark.flaky(reruns=1)(attr_value))
    return cls


def func_marker(func):
    # func = pytest.mark.flaky(reruns=5, reruns_delay=2)(func)
    # func = pytest.mark.order(3)(func)
    # pytest.mark.jira(reruns=5, reruns_delay=2)(func)
    return func


def groupid(filepath: str):
    """Returns group name for tests"""
    filename = Commons.get_filename_frompath(filepath)
    grpid = filename
    if is_group_decider_found:
        class_groups = [m[1] for m in inspect.getmembers(group_decider, inspect.isclass)]
        avalable_groups = class_groups
        for i in avalable_groups:
            if hasattr(i, filename):
                grpid = i.__name__
                break
    return grpid


def skipAfter_dataFound(skip:bool=None, reason:str= 'Data found, skipping test'):
    _IS_SKIP_AFTER_DATA_FOUND = ENV_CONST.get('framework', 'is_skip_after_data_found')
    if skip is True or 'true' in _IS_SKIP_AFTER_DATA_FOUND:
        pytest.skip(f"is_skip_after_data_found={_IS_SKIP_AFTER_DATA_FOUND}, {reason}")


def skipAfter_dataPrep(skip:bool=None, reason:str= 'Data prepared, skipping test'):
    _IS_SKIP_AFTER_DATA_PREP = ENV_CONST.get('framework', 'is_skip_after_data_prep')
    if skip is True or 'true' in _IS_SKIP_AFTER_DATA_PREP:
        pytest.skip(f"is_skip_after_data_prep={_IS_SKIP_AFTER_DATA_PREP}, {reason}")
