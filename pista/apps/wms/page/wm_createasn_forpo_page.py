import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.app_status import POStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMCreateASNForPOPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Create ASN From PO'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Create ASN From PO')]"

    _ASN_ADD_BTN = "//input[@id='dataForm:cbaddasn']"
    _ASSIGN_SAVE_BTN = "//input[@id='dataForm:cbdelasns']"

    '''PO section'''

    _PO_FILTER_TB = "//input[@id='dataForm:filterId:field10value1']"
    _PO_FILTER_APPLY_BTN = "//input[@id='dataForm:filterId:filterIdapply']"
    # _PO_CHECK_BOX = "//table[@id='dataForm:treeTable']//input[@type='checkbox']"
    _CHECKBOX_FOR_PO = "//table[@id='dataForm:treeTable']//tr//td//span[contains(@class,'outputText') and contains(text(),'#PO_NUM#')]//parent::td//input[@type='checkbox']"
    _DISPLAYED_ALL_PO_NAME = "//table[@id='dataForm:treeTable']//tr//td//span[contains(@class,'outputText') and contains(text(),' ')]"

    '''ASN num creation popup'''

    _ASN_INPUT_TB = "//input[@id='dataForm:asnidh1']"
    _ASN_UI_CALENDAR = "//*[@id='trigger_dataForm:sdqtyhcc']"
    _ASN_UI_SELECT_CURRENT_DATE = "//td[contains(@class,'today')]"
    _ASN_OK_BTN = "//input[@id='dataForm:sv']"
    _ASN_SEARCH_INPUT = "//input[@alt='Find ASN']"
    
    '''ASN section'''

    _CHECKBOX_FOR_ASN = "//input[@value='#ASN_NO#']//preceding-sibling::input[@type='checkbox']"
    _MOVE_RIGHT_ARROW_ASSIGN_BTN = "//input[@id='dataForm:cb5']"
    _EXPAND_BTN_FOR_ASN = "//input[@value='#ASN_NO#']//parent::td//parent::tr//td[contains(@class,'tree-node-handleicon')]"
    _PO_LIST = "//span[contains(text(),'#PO#')]"
    _PO_LIST_IN_ASN = "//div[@id='dataForm:atreeTable:0:atreeB:aadaptor:0::j_id139:childs']//span[contains(text(),'#PO#')]"
    _ASN_GENERATED = "//span[@class='h-outputTextH' and contains(text(),'#ASN#')]"
    _ASN_APPLY_BTN = "//input[@name='dataForm:filterId2:filterId2apply']"
    _ASN_ROW_VALIDATE = "//*[contains(text(),'#ASN#')]"
    
    def __init__(self, driver, isPageOpen: bool = False):
        """Opens create ASN from PO UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def _filterByPO(self, po: list[str]):
        posStr = ','.join(po)
        self.fill_by_xpath(self._PO_FILTER_TB, posStr)
        self.click_by_xpath(self._PO_FILTER_APPLY_BTN)
        self._assertDisplayedPOCount(po)

    def _assertDisplayedPOCount(self, po: list):
        all_displayed_po_col = self.get_webelements(self._DISPLAYED_ALL_PO_NAME)
        assert len(all_displayed_po_col) == len(po), "Displayed PO count didnt match"

    def _generateASNNum(self, asn):
        # Generate ASN num
        self.click_by_xpath(self._ASN_ADD_BTN)
        self.fill_by_xpath(self._ASN_INPUT_TB, asn)
        self.click_by_xpath(self._ASN_UI_CALENDAR)
        self.click_by_xpath(self._ASN_UI_SELECT_CURRENT_DATE)
        self.click_by_xpath(self._ASN_OK_BTN)

    def _assertPOInASN(self, asn: str, po: list):
        po_list = list
        # self.click_by_xpath(self._EXPAND_BTN_FOR_ASN.replace('#ASN_NO#', asn))
        self._ASN_GENERATED = self._ASN_GENERATED.replace('#ASN#', str(asn))
        if len(self._ASN_GENERATED) == 0:
            assert False, "ASN is not generated."
        for i in range(0, len(po)):
            po_list = self.get_text_by_xpath(self._PO_LIST_IN_ASN.replace('#PO#', str(po[i])))
        check = all(item in po for item in po_list)
        assert check, "POs not found in ASN"

    def createASNForPO(self, po: list[str], o_item: list[str], o_qty: list[int], asn: str = None,
                       o_isFullPoShip: bool = True):
        """Create 1ANS for 1 PO
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        
        self.logger.info('Creating ASN for PO ' + str(po))
        
        self.switch_frame(0)
        self._filterByPO(po)
        for p in po:
            self.click_by_xpath(self._CHECKBOX_FOR_PO.replace('#PO_NUM#', p), True)
        if asn is None:
            asn = DBLib().getNewASNNum()
        self._generateASNNum(asn)
        self.click_by_xpath(self._CHECKBOX_FOR_ASN.replace('#ASN_NO#', str(asn)))
        self.click_by_xpath(self._MOVE_RIGHT_ARROW_ASSIGN_BTN)
        # self._assertPOInASN(asn, po)
        self.click_by_xpath(self._ASSIGN_SAVE_BTN)
        self.switch_default_content()

        '''Validation'''
        isOnly1PO = all(element == po[0] for element in po)
        DBLib().assertWaitASNRecord(i_asn=asn)
        DBLib().assertASNHdr(i_asn=asn, o_status=20)
        if o_isFullPoShip and isOnly1PO:
            DBLib().assertPOHdr(i_po=po[0], o_status=POStat.SHIPPED)
        for i in range(len(o_item)):
            DBLib().assertASNDtls(i_asn=asn, i_po=po[0], i_itemBrcd=o_item[i], o_dtlStatus=4,
                                  o_shippedQty=o_qty[i])
            DBLib().assertPODtls(i_po=po[0], i_itemBrcd=o_item[i], o_dtlStatus=850, o_origQty=o_qty[i],
                                 o_qty=o_qty[i])
            if o_isFullPoShip and not isOnly1PO:
                DBLib().assertPOHdr(i_po=po[0], o_status=POStat.SHIPPED)

        return asn

    def addPOToASN(self, po: list[str], asn: str, o_item: list[str], o_qty: list[int], o_asnStatus: int = None, isASNVerified: bool = None):
        """Add 1/more PO to 1 ASN
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Adding POs ' + str(po) + ' to ASN ' + asn)

        self.switch_frame(0)
        self._filterByASN(ASN=asn, isASNVerified=isASNVerified)

        if not isASNVerified:
            self._filterByPO(po)
            for p in po:
                self.click_by_xpath(self._CHECKBOX_FOR_PO.replace('#PO_NUM#', p), True)
            self.click_by_xpath(self._CHECKBOX_FOR_ASN.replace('#ASN_NO#', str(asn)))
            self.click_by_xpath(self._MOVE_RIGHT_ARROW_ASSIGN_BTN)
            # self._assertPOInASN(asn, po)
            self.click_by_xpath(self._ASSIGN_SAVE_BTN)
            self.switch_default_content()

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
        if isASNVerified:
            for i in range(len(po)):
                DBLib().assertPONotOnASN(i_asn=asn, i_po=po[i])
        else:
            for i in range(len(o_item)):
                DBLib().assertASNDtls(i_asn=asn, i_po=po[i], i_itemBrcd=o_item[i], o_dtlStatus=4, o_shippedQty=o_qty[i])
                DBLib().assertPODtls(i_po=po[0], i_itemBrcd=o_item[i], o_dtlStatus=850, o_origQty=o_qty[i], o_qty=o_qty[i])
            DBLib().assertPOHdr(i_po=po[0], o_status=POStat.SHIPPED)

    def _filterByASN(self, ASN: str, isASNVerified: bool = None):
        self.fill_by_xpath(self._ASN_SEARCH_INPUT, ASN)
        self.click_by_xpath(self._ASN_APPLY_BTN)

        if isASNVerified:
            '''Here ASN is verified, so no record will be shown'''
            isNoASNRowFound = self.is_displayed_by_xpath(self._ASN_ROW_VALIDATE.replace('#ASN#', ASN))
            assert not isNoASNRowFound, "ASN Record Found for verified ASN"
        else:
            fetchedASN = self.is_displayed_by_xpath(self._ASN_ROW_VALIDATE.replace('#ASN#', ASN))
            assert fetchedASN, "ASN didnt filter"
        