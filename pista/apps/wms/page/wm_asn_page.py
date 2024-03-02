import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from apps.wms.app_status import ASNStat, POStat
from core.log_service import Logging


class WMASNPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'ASNs'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@id,'title') and contains(text(),'ASNs')]"

    _ASN_FILTER_DROPDOWN_BTN = "(//label[text()='Primary Fields']/following-sibling::div//input)[1]"
    _ASN_INPUT_FIELD_BTN = "//label[text()='Primary Fields']/following-sibling::div//input[@name='asnId']"
    _ASN_EQUAL_BTN = "(//label[text()='Primary Fields']/following-sibling::div//input)[5]"
    _SEND_BTN = "//button[@id='dataForm:postMessageCmdId']"
    _APPLY_BTN = "//span[text()='Apply']"
    _SELECT_CB_ASN = "(//div[contains(@id,'headercontainer')]//span)[1]"
    _CLICK_CANCEL_BTN = "//span[text()='Cancel']"
    _CONFIRM_CANCEL_ASN = "//span[text()='OK']"
    _MORE_BTN = "//span[text()='More']"
    _VERIFY_ASN_BTN = "//span[text()='Verify ASN']"
    _OVERRIDE_VERIFY_ASN_BTN = "//li[text()='Nothing has been received. Continue to verify?']//a[text()='Override']"

    _VIEW_BTN = "//span[@data-ref='btnInnerEl' and text()='View']"
    _NAVIGATE_TO_LINE = "//a[@name='ASNDetailASNLinesTab' and @id='ASNDetailASNLinesTab_lnk']"
    _CB_FOR_LINE = "(//input[@type='checkbox' and contains(@id,'ASNDetailPOListTable')])[1]"
    _EDIT_LINE_BTN = "//input[@class='btn' and @id='dataForm:ASNDtl_ASNLines_POList_commandbutton_Edit']"
    _SHIPPED_QTY_INPUT = "//input[@id='dataForm:ASNDetailsAddDetail_OT_shippedQtyString_P1']"
    _SAVE_BTN = "//input[@id='dataForm:ASNDtl_ASNLines_commandbutton_Update' and @class='btn']"
    
    '''Verify ASN window'''
    
    _CONFIRM_VERIFY_ASN = "//input[@type='button' and @value='Verify ASN']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens ASN UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()
            self.wait_for(1)

    def filterByASN(self, asnNum):
        self.fill_by_xpath(self._ASN_FILTER_DROPDOWN_BTN, 'ASN')
        self.press_enter_by_xpath(self._ASN_FILTER_DROPDOWN_BTN)
        self.fill_by_xpath(self._ASN_INPUT_FIELD_BTN, asnNum)
        self.click_by_xpath(self._APPLY_BTN)

    def cancelASN(self, asnNum, o_po, o_item: list[str]):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        self.filterByASN(asnNum)
        self.click_by_xpath(self._SELECT_CB_ASN)
        self.click_by_xpath(self._CLICK_CANCEL_BTN)
        self.click_by_xpath(self._CONFIRM_CANCEL_ASN)

        # Validate ASN Updates : ASN status must be 60(Canceled)
        DBLib().assertWaitASNStatus(i_asn=asnNum, o_status=ASNStat.CANCELED.value)

        # Validate PO Updates : PO status must be 20(Created)
        DBLib().assertPOHdr(i_po=o_po, o_status=POStat.CREATED)

        # Validate PO Line Updates : Each PO Line Item must be updated to 20(Created) with Shipped Qty as 0
        for i in range(len(o_item)):
            DBLib().assertPODtls(i_po=o_po, i_itemBrcd=o_item[i], o_dtlStatus=20, o_shippedQty=0)

    def verifyASN(self, asnNum, isShowOverrideWarn: bool = None):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        self.filterByASN(asnNum)
        self.click_by_xpath(self._SELECT_CB_ASN)
        self.click_by_xpath(self._MORE_BTN)
        self.click_by_xpath(self._VERIFY_ASN_BTN)
        self.wait_for(3)
        self.switch_frame(0)
        self.click_by_xpath(self._CONFIRM_VERIFY_ASN)

        if isShowOverrideWarn:
            self.click_by_xpath(self._OVERRIDE_VERIFY_ASN_BTN)
        
        self.switch_default_content()

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asnNum, o_status=ASNStat.RECEIVING_VERIFIED.value)

    def editLinesInASN(self, asnNum: str, newQty: int, o_po: str, o_item: str):
        """"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        self.filterByASN(asnNum)

        self.click_by_xpath(self._SELECT_CB_ASN)
        self.click_by_xpath(self._VIEW_BTN)

        self.switch_frame(0)
        self.click_by_xpath(self._NAVIGATE_TO_LINE)
        self.click_by_xpath(self._CB_FOR_LINE)
        self.scroll_to(self._EDIT_LINE_BTN)
        self.click_by_xpath(self._EDIT_LINE_BTN)
        self.scroll_to(self._SHIPPED_QTY_INPUT)
        self._fill_by(self._SHIPPED_QTY_INPUT, newQty, clearVal=True)
        self.scroll_to(self._SAVE_BTN)
        self.click_by_xpath(self._SAVE_BTN)
        self.switch_default_content()

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asnNum, o_status=ASNStat.IN_TRANSIT.value)
        DBLib().assertASNDtls(i_asn=asnNum, i_po=o_po, i_itemBrcd=o_item, o_dtlStatus=4, o_shippedQty=newQty)
