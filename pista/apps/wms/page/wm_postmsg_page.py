import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.app_utils import XMLBuilder
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging
from apps.wms.app_status import POStat, ASNStat, DOStat, LPNFacStat


class WMPostMsgPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Post Message'
    MODULE = 'Integration'
    _TITLE_XPATH = "//div[contains(@id,'title') and contains(text(),'Post Message')]"

    _POSTMSG_UPLOADFILE_BTN = "//input[@id='dataForm:uploadedFileID']"
    _POSTMSG_TEXTAREA_INBOX = "//textarea[contains(@id,'dataForm:xmlString')]"
    _POSTMSG_TEXTAREA_OUTBOX = "//textarea[contains(@id,'dataForm:resultString')]"
    _ALL_SUBMIT_BTNS = "//input[@type='submit']"
    _SEND_BTN = "//input[@type='submit' and @id='dataForm:postMessageCmdId']"
    _RESET_BTN = "//input[@type='submit' and @id='dataForm:resetCmdId']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens post msg UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(3)
            self.maximizeMenuPage()

    def postMsg(self, filepath=None, content=None) -> str:
        """"""
        self.switch_frame(0)
        '''If reset button available, click'''
        allSubmitBtns = self.get_webelements(self._ALL_SUBMIT_BTNS)
        for ele in allSubmitBtns:
            if ele.get_attribute('value') == 'Reset':
                self.click_by_xpath(self._RESET_BTN)

        if filepath is not None:
            self.fill_file_by_xpath(self._POSTMSG_UPLOADFILE_BTN, filepath)
        elif content is not None:
            self.fill_by_xpath(self._POSTMSG_TEXTAREA_INBOX, content)
        self.click_by_xpath(self._SEND_BTN)
        response = self._assertPostResponseOK()
        self.switch_default_content()
        return response

    def _assertPostResponseOK(self) -> str:
        response = self.get_text_by_xpath(self._POSTMSG_TEXTAREA_OUTBOX)
        assert response.count('<Error_Type>0</Error_Type>') > 0, 'Xml posting response has error'
        return response

    def postPOXml(self, items: list[str], qtys: list[int], poNum=None, varColumn=None) -> str:
        """Pass same no. of items and qtys
            Does validation"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Posting PO xml: items ' + str(items) + ', qtys ' + str(qtys))

        poNum, poXml = XMLBuilder.buildPOXml(items=items, qtys=qtys, poNum=poNum, varColumn=varColumn)
        self.postMsg(content=poXml)

        '''Validation'''
        itemList = items if type(items) == list else [items]
        qtyList = qtys if type(qtys) == list else [qtys]
        DBLib().assertPOHdr(i_po=poNum, o_status=POStat.CREATED)
        for i in range(len(itemList)):
            DBLib().assertPODtls(i_po=poNum, i_itemBrcd=itemList[i], o_dtlStatus=20, o_origQty=qtyList[i],
                                 o_qty=qtyList[i])

        return poNum

    def postSkuLvlASNXml(self, poNums: list[str], items: list[str], qtys: list[int], poLineNums: list[int],
                         asnNum=None, varColumn=None, o_fullPoShipped: bool = True) -> str:
        """Pass same no. of poNums, items, qtys and poLineNums
            Does validation"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Posting ASN xml (sku level): items ' + str(items) + ', qtys ' + str(qtys))

        asnNum, asnXml = XMLBuilder.buildSkuLevelASNXml(poNums=poNums, items=items, qtys=qtys,
                                                        poLineNums=poLineNums, asnNum=asnNum, varColumn=varColumn)
        self.postMsg(content=asnXml)

        '''Validation'''
        itemList = items if type(items) == list else [items]
        qtyList = qtys if type(qtys) == list else [qtys]
        # poList = poNums if type(poNums) == list else [poNums]
        poList = []
        poList = poList * len(itemList) if len(poNums) < len(itemList) else poNums
        isOnly1PO = all(element == poList[0] for element in poList)
        # if type(poNums) == str:
        #     poList.extend([poNums for i in range(len(itemList))])
        # else:
        #     poList = poNums
        DBLib().assertASNHdr(i_asn=asnNum, o_status=ASNStat.IN_TRANSIT.value)
        if o_fullPoShipped and isOnly1PO:
            DBLib().assertPOHdr(i_po=poList[0], o_status=POStat.SHIPPED)
        for i in range(len(itemList)):
            DBLib().assertASNDtls(i_asn=asnNum, i_po=poList[i], i_itemBrcd=itemList[i], o_dtlStatus=4,
                                  o_shippedQty=qtyList[i])
            DBLib().assertPODtls(i_po=poList[i], i_itemBrcd=itemList[i], o_dtlStatus=850, o_origQty=qtyList[i],
                                 o_qty=qtyList[i])
            if o_fullPoShipped and not isOnly1PO:
                DBLib().assertPOHdr(i_po=poList[i], o_status=POStat.SHIPPED)

        return asnNum

    def postLPNLvlASNXml(self, poNums: list[str], items: list[list[str]], qtys: list[list[int]], poLineNums: list[list[int]],
                         lpns: list[str] = None, asnNum=None, varColumn=None, noOfLPNs: int = None) -> str:
        """Pass same no. of items, qtys and poLineNums
        Pass same no. of poNums, lpns
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Posting ASN xml (lpn level): lpns ' + str(lpns) + ', items ' + str(items) + ', qtys ' + str(qtys))
        lpnList = []

        for i in range(noOfLPNs):
            lpnList.append(DBLib().getNewILPNNum())

        lpnList = lpns if lpns is not None else lpnList

        asnNum, asnXml = XMLBuilder.buildLpnLevelASNXml(poNum=poNums, lpns=lpnList, items=items, qtys=qtys,
                                                        poLineNums=poLineNums, asnNum=asnNum, varColumn=varColumn)
        self.postMsg(content=asnXml)

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asnNum, o_status=ASNStat.IN_TRANSIT.value)
        for i in range(len(lpnList)):
            DBLib().assertPOHdr(i_po=poNums[i], o_status=POStat.SHIPPED)
            DBLib().assertLPNHdr(i_lpn=lpnList[i], o_facStatus=LPNFacStat.ILPN_IN_TRANSIT, o_asn=asnNum)
            for j in range(len(items[i])):
                DBLib().assertASNDtls(i_asn=asnNum, i_po=poNums[i], i_itemBrcd=items[i][j], o_dtlStatus=4, o_shippedQty=qtys[i][j])
                DBLib().assertPODtls(i_po=poNums[i], i_itemBrcd=items[i][j], o_dtlStatus=850, o_origQty=qtys[i][j], o_qty=qtys[i][j])

        return asnNum

    def postDOXml(self, doType, items: list[str], qtys: list[int], shipVia=None, doNum=None, varColumn=None,
                  majorOrdGrpAttr=None) -> str:
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Posting DO xml: items ' + str(items) + ', qtys ' + str(qtys))

        doNum, doXml = XMLBuilder.buildDOXml(doType=doType, items=items, qtys=qtys, shipVia=shipVia, doNum=doNum,
                                             varColumn=varColumn, majorOrdGrpAttr=majorOrdGrpAttr)
        self.postMsg(content=doXml)

        '''Validation'''
        itemList = items if type(items) == list else [items]
        qtyList = qtys if type(qtys) == list else [qtys]
        DBLib().assertDOHdr(i_order=doNum, o_status=DOStat.RELEASED)
        for i in range(len(itemList)):
            DBLib().assertDODtls(i_order=doNum, i_itemBrcd=itemList[i], o_dtlStatus=110, o_origQty=qtyList[i],
                                 o_qty=qtyList[i])

        return doNum

    def postLPNXml(self, items: list[str], qtys: list[int], lpnId=None, isLpnLock: bool = None, varColumn=None,
                   o_lockCode: str = None, o_lpnCurrLocn: str = None) -> str:
        """Pass same no. of items and qtys
        Does validation"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Posting LPN xml: items ' + str(items) + ', qtys ' + str(qtys))

        lpnId, lpnXml = \
            XMLBuilder.buildLPNXml(items=items, qtys=qtys, lpnId=lpnId, isLpnLock=isLpnLock, varColumn=varColumn)
        self.postMsg(content=lpnXml)

        '''Validation'''
        itemList = items if type(items) == list else [items]
        qtyList = qtys if type(qtys) == list else [qtys]
        DBLib().assertLPNHdr(i_lpn=lpnId, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=o_lpnCurrLocn)
        for i in range(len(itemList)):
            DBLib().assertLPNDtls(i_lpn=lpnId, i_itemBrcd=itemList[i], o_qty=qtyList[i])
        if o_lockCode is not None:
            DBLib().assertLpnLockPresent(i_lpn=lpnId, i_lockCode='OR')

        return lpnId
