import inspect
import os

from apps.api.cubing_schema import CUBING_REQ_SCHEMA, CUBING_RESP_SCHEMA, CUBING_REQ_SCHEMA_NEW, CUBING_RESP_SCHEMA_NEW
from core.api_service import APIService
from core.config_service import ENV_CONFIG
from core.file_service import JsonUtil
from core.json_service import JsonService
from core.log_service import Logging, printit
from root import DATA_DIR


class AuthenticateAPI:
    logger = Logging.get(__qualname__)

    def __init__(self):
        self.baseUrl = ENV_CONFIG.get('api', 'cubing_base_url')

    def getAuth(self):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'api')

        apiUrl = ENV_CONFIG.get('api', 'cubing_authenticate')

        response = APIService.call_get_api(self.baseUrl + apiUrl)

        '''Validation'''
        APIService.assert_statuscode(response, 200)


class CubingAPI:
    logger = Logging.get(__qualname__)

    def __init__(self, sheet=None):
        self.baseUrl = ENV_CONFIG.get('api', 'cubing_base_url')
        self.xlfilepath = os.path.join(DATA_DIR, 'json_data_Cubing.xlsx')
        self.sheet = 'Cubing' if sheet is None else sheet

    def callGetOrderCubingAPI(self, column: str, o_statCode: int = 200):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'api')
        self.logger.info(f"Calling order cubing API with {column}")

        headers = {'company-api-key': '8a5d312c-4b15-11ee-a60d-00785c63da1f',
                   'user-api-key': '60c50d9a-dcab-473a-9982-5abdc43bd989'}

        '''Get request json'''
        jsonDict, jsonStr = JsonService.get_json_from_xl(self.xlfilepath, self.sheet, column)
        finalJsonDict = jsonDict['Cubing']
        finalJsonStr = JsonUtil.get_json_str_from_dict(finalJsonDict)
        printit("Cubing request json", finalJsonStr)
        APIService.assert_json_schema(json_dict=finalJsonDict, json_schema=CUBING_REQ_SCHEMA_NEW)

        '''Calling API'''
        apiUrl = ENV_CONFIG.get('api', 'cubing_getOrderCubing')
        final_url = self.baseUrl + apiUrl
        self.logger.info(f"POST url {final_url}")
        response = APIService.call_post_api(api_url=final_url, json=finalJsonDict, headers=headers)
        printit(response)

        '''Validation'''
        APIService.assert_statuscode(api_resp=response, status_code=o_statCode)
        self.assertByValidationSheet(xlfilepath=self.xlfilepath, testId=column, respJson=response.json())
        if o_statCode == 200:
            APIService.assert_json_schema(json_dict=response.json(), json_schema=CUBING_RESP_SCHEMA_NEW)
            self.assertCubingBatchIdExist(respJson=response.json())
            self.assertOrderCubingResp(reqJson=finalJsonDict, respJson=response.json())

        return response, finalJsonDict

    def assertByValidationSheet(self, xlfilepath, testId, respJson: dict):
        valSheetDict = JsonService.get_xl_validation_data_by_rowName(xlfilepath, testId)

        assertList = []
        for k, v in valSheetDict.items():
            if v and k == 'TOTAL_CNTRS':
                respTotalCntrs = int(respJson['data']['response']['totalContainers'])
                isMatched = APIService.compareEqual(respTotalCntrs, int(v), f"<Sheet> {k} in response")
                assertList.append(isMatched)
            elif v and k == 'CNTR_TYPES':
                respOutputDtlsAsList = respJson['data']['response']['outputDetails']
                respCntrTypesAsList = sorted([i['containerData']['containerType'] for i in respOutputDtlsAsList])
                reqCntrTypesAsList = sorted(v.split(','))
                isMatched = APIService.compareEqual(respCntrTypesAsList, reqCntrTypesAsList, f"<Sheet> {k} in response")
                assertList.append(isMatched)
            elif v and k == 'TOTAL_ITEMS':
                respTotalItems = int(respJson['data']['response']['totalItems'])
                isMatched = APIService.compareEqual(respTotalItems, int(v), f"<Sheet> {k} in response")
                assertList.append(isMatched)
            elif v and k == 'ERROR_MSG':
                respErrorMsg = respJson['data']['response']['message']
                isMatched = APIService.compareContains(respErrorMsg, v, f"<Sheet> {k} in response")
                assertList.append(isMatched)

        assert False not in assertList, '<Sheet> Validation sheet assertion failed'

    def assertCubingBatchIdExist(self, respJson):
        """Validate cubing batch ID exists in repsonse
        """
        resp_cubingBatchId = respJson['data']['response']['cubingBatchId']
        isMatched = resp_cubingBatchId is not None and resp_cubingBatchId != ''
        if isMatched:
            self.logger.info(f"<API> CubingBatchId exsits with {resp_cubingBatchId}")
        else:
            self.logger.error(f"<API> CubingBatchId exist")

        assert isMatched, '<API> Cubing batch ID not found'

    def assertOrderCubingResp(self, reqJson, respJson):
        """Validate cubing response result
        """
        assertlist = []

        '''Order ID in resp'''
        req_orderId = reqJson['orderData'][0]['orderId']
        resp_orderId = respJson['data']['response']['orderId']
        isMatched = APIService.compareEqual(resp_orderId, req_orderId, '<API> OrderId in response')
        assertlist.append(isMatched)

        '''Other validation'''
        
        assert not assertlist.count(False), '<API> Order cubing API validation failed'
