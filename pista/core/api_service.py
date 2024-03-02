import requests
from requests import Response

from core.file_service import JsonUtil
from core.log_service import printit, Logging


class APIService:
    """Use requests lib from requirements for below WS code
    """
    logger = Logging.get(__qualname__)
    
    @staticmethod
    def call_get_api(api_url, headers: dict = None) -> Response:
        """Send API request using GET method
        """
        printit(f"GET url {api_url}")

        resp = None
        try:
            resp = requests.get(api_url, headers=headers)

            jsonObj = resp.json()
            jsonStr = JsonUtil.get_json_str_from_dict(jsonObj)
            printit(f"Response {jsonStr}")
        except Exception as e:
            printit(f"Exception found during API call {e}")

        assert resp is not None, f"API call not success {api_url}"
        return resp

    @staticmethod
    def call_post_api(api_url, json=None, data=None, headers: dict = None) -> Response:
        """Send API request using POST method
        """
        printit(f"POST url {api_url}")

        resp = None
        try:
            resp = requests.post(url=api_url, json=json, data=data, headers=headers)

            jsonObj = resp.json()
            jsonStr = JsonUtil.get_json_str_from_dict(jsonObj)
            printit(f"Response {jsonStr}")
        except Exception as e:
            printit(f"Exception found during API call {e}")

        assert resp is not None, f"API call not success {api_url}"
        return resp

    @staticmethod
    def call_put_api(api_url, data, headers: dict = None) -> Response:
        """Send API request using PUT method
        """
        resp = requests.put(url=api_url, data=data, headers=headers)
        return resp

    @classmethod
    def assert_statuscode(cls, api_resp: Response, status_code):
        """To verify API response status code against expected
        """
        statcode = api_resp.status_code

        isMatched = statcode == status_code
        if isMatched:
            cls.logger.info(f"<API> Response statcode matched with {status_code}")
        else:
            cls.logger.error(f"<API> Response statcode didnt match, actual {statcode}, expected {status_code}")
        assert isMatched, f"<API> API resp status code didnt match, actual {statcode}, exp {status_code}"

    @classmethod
    def assert_json_val(cls, jsondict, jsonpath, exp_jsonval):
        """To verify a single json val
        """
        isMatched = JsonUtil.compare_json_val(jsondict, jsonpath, exp_jsonval)
        if isMatched:
            cls.logger.info(f"{jsonpath} matched with {exp_jsonval}")
        else:
            cls.logger.error(f"{jsonpath} didnt match with {exp_jsonval}")
        assert isMatched, f"Json {jsonpath} val didnt match with {exp_jsonval}"

    @classmethod
    def assert_json_type(cls, class_name, required_keys, json_dict=None, json_str=None):
        """To verify json with class type
        """
        isMatched = JsonUtil.compare_json_type(class_name, required_keys, json_dict, json_str)
        if isMatched:
            cls.logger.info(f"Response type matched with {class_name}")
        else:
            cls.logger.error(f"Response type didnt match with {class_name}")
        assert isMatched, 'Response type didnt match'
        
    @classmethod
    def assert_json_schema(cls, json_dict, json_schema):
        """To verify json with schema
        """
        isMatched = JsonUtil.compare_json_schema(json_dict, json_schema)
        if isMatched:
            cls.logger.info(f"<Json> Json schema matched")
        else:
            cls.logger.error(f"<Json> Json schema didnt match")
        assert isMatched, '<Json> Json schema validation failed'

    @classmethod
    def compareEqual(cls, actualVal, expectedVal, whatIsThisDesc: str, expValDesc: str = None) -> bool:
        """Compare if 2 values are equal
        """
        expValDesc = '' if expValDesc is None else ' (' + str(expValDesc) + ')'

        isMatched = True if f"{actualVal}" == f"{expectedVal}" else False
        if isMatched:
            cls.logger.info(f"{whatIsThisDesc} matched with {expectedVal}{expValDesc}")
        else:
            cls.logger.error(f"{whatIsThisDesc} didnt match, actual {actualVal}, expected {expectedVal}{expValDesc}")

        return isMatched

    @classmethod
    def compareContains(cls, actualVal, expectedVal, whatIsThisDesc: str, expValDesc: str = None) -> bool:
        """Compare if actual value contains expected value
        """
        expValDesc = '' if expValDesc is None else ' (' + str(expValDesc) + ')'

        isMatched = True if f"{expectedVal}" in f"{actualVal}" else False
        if isMatched:
            cls.logger.info(f"{whatIsThisDesc} matched with {expectedVal}{expValDesc}")
        else:
            cls.logger.error(f"{whatIsThisDesc} didnt match, actual {actualVal}, expected {expectedVal}{expValDesc}")

        return isMatched
