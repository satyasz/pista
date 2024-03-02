import os
from configparser import ConfigParser

from core.api_service import APIService
from core.common_service import Commons
from core.file_service import JsonUtil
from core.log_service import printit
from root import ROOT_DIR, RESOURCE_DIR

JIRA_CONFIG = ConfigParser()
JIRA_CONFIG.read(os.path.join(RESOURCE_DIR, 'jira.cfg'))

zephyr_baseUrl = JIRA_CONFIG.get('JIRA_CONFIG', 'zephyr_url')
zephyr_execution_endpoint = JIRA_CONFIG.get('JIRA_CONFIG', 'zephyr_test_execution_endpoint')
zephyr_token = JIRA_CONFIG.get('JIRA_CONFIG', 'zephyr_token')
projectId = JIRA_CONFIG.get('JIRA_CONFIG', 'zephyr_project_key')
zephyr_cycle_key = JIRA_CONFIG.get('JIRA_CONFIG', 'zephyr_cycle_key')


def update_results_to_jira(issueIds, statusName=""):
    """Update result to jira"""
    issueIds = [issueIds] if type(issueIds) == str else issueIds
    for issueId in issueIds:
        _update_result_to_jira(issueId, statusName)


def _update_result_to_jira(testCaseId, statusName):
    """Update result to jira"""
    try:
        if testCaseId == "":
            assert False, "Jira testcase id not found"
        else:
            testCycleId = os.environ.get('JIRA_CYCLE_KEY') or zephyr_cycle_key
            if testCycleId:
                jira_json_filepath = os.path.join(ROOT_DIR, r'core/jira/jira_json.json')
                payload = JsonUtil.get_json_str_from_file(jira_json_filepath)

                currentdate = Commons.format_utc_current_date("%Y-%m-%dT%H:%M:%SZ")
                commentvalue = "Test is " + statusName

                replacements = {
                    "#projectKey#": projectId,
                    "#testCycleKey#": testCycleId,
                    "#testCaseKey#": testCaseId,
                    "#statusName#": statusName,
                    "#comment#": commentvalue,
                    "#date#": currentdate
                }
                for key, value in replacements.items():
                    payload = payload.replace(key, value)
                apiUrl = zephyr_baseUrl + zephyr_execution_endpoint
                headers = {"Content-Type": "application/json", "Authorization": zephyr_token}

                response = APIService.call_post_api(api_url=apiUrl, data=payload, headers=headers)

                if response is not None and response.status_code == 201:
                    printit(f"Jira updated (project:{projectId} testcycle:{testCycleId} testcase:{testCaseId}) status:{statusName}")
                else:
                    printit(f"Jira didnt update (project:{projectId} testcycle:{testCycleId} testcase:{testCaseId}) status:{statusName}")
    except Exception as e:
        printit('Jira update exception found', str(e))
        raise e
