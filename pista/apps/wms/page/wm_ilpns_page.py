from apps.wms.app_status import LPNFacStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging
from apps.wms.app_db_lib import DBLib


class WMILpnsPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'iLPNs'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[@class = 'x-title-text x-title-text-default x-title-item' and text() = 'iLPNs']"

    _ILPN_TB = "// input[@type='text' and @alt='Find iLPN']"
    _APPLY_BTN = "// input[@id='dataForm:LPNListInOutboundMain_lv:LPNList_Inbound_filterId1:LPNList_Inbound_filterId1apply']"
    _DEALLOCATE_BTN = "//input[@type='button' and @value='Deallocate']"
    _LPN_FAC_STAT = "//span[@id='dataForm:LPNListInOutboundMain_lv:dataTable:0:LPNList_Outbound_lpnFacilityStatus_param_out']"
    _CHECK_BOX = "//input[@type='checkbox' and @name='checkAll_c0_dataForm:LPNListInOutboundMain_lv:dataTable']"
    _WARNING_MSG = "//div[@id='overlaymsgPop']//li[text()='LPN In Allocated or Pulled Status']"
    _ADJUST_BTN = "//input[@value='Adjust' and @type='button']"
    _CONFIRM_DEALLOCATE_BTN = "//input[@value='Deallocate']"
    _ILPN_ROW_VALIDATE = "//span[@id='dataForm:LPNListInOutboundMain_lv:dataTable:0:LPNList_Outbound_Link_NameText_param_out']"

    _NEW_QTY_INPUT = "//input[@id='dataForm:NewQty' and @name = 'dataForm:NewQty']"
    _SAVE_BTN = "//input[@type='button' and @id = 'rmButton_1Save1_154183000']"
    _REASON_CODE = "//select[@id='dataForm:adjustReasonSelect']"
    _LOCK_LPN_BTN = "//input[@type='button' and @id='rmButton_1Lock1_167270008']"
    _LOCK_CODE_DD = "//select[@id='dataForm:listView:dataTable:newRow_1:LockCodeSelect']"
    _SELECT_LOCK_CODE = "//select[@id='dataForm:listView:dataTable:newRow_1:LockCodeSelect']"

    _LOCK_UNLOCK_LPN_BTN = "//input[@type='button' and @id='LPNListInboundMain_commandbutton_LockUnlockLPN']"
    _CB_FOR_UNLOCK_LPN = "//input[@type='checkbox' and @id='checkAll_c0_dataForm:listView:dataTable']"
    _UNLOCK_BTN = "//input[@type='button' and @id = 'rmButton_1Unlock1_167270009']"
    _LOCK_UNLOCK_SAVE_BTN = "//input[@type='button' and @id='rmButton_1Save1_167270010']"
    
    def __init__(self, driver, isPageOpen: bool = False):
        """Opens iLPNs UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()

    def _filterByILPN(self, ilpn: str):
        self.switch_frame(0)
        self.fill_by_xpath(self._ILPN_TB, ilpn)
        self.click_by_xpath(self._APPLY_BTN)

        fetchedIlpn = self.get_text_by_xpath(self._ILPN_ROW_VALIDATE)
        assert fetchedIlpn == ilpn, "Ilpn didnt filter"

        self.switch_default_content()

    def deallocateILPN(self, ilpn: str):
        self._filterByILPN(ilpn=ilpn)

        self.switch_frame(0)
        self.click_by_xpath(self._CHECK_BOX)
        self.click_by_xpath(self._DEALLOCATE_BTN)
        self.click_by_xpath(self._CONFIRM_DEALLOCATE_BTN)
        self.switch_default_content()

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY)

    def adjustILPN(self, ilpn:str, newQty:int=None, itemBrcd:str=None, isWarnMsgShow:bool=None,
                   o_lpnFacStat:LPNFacStat=None, o_reasonCode:str=None):
        """"""
        self._filterByILPN(ilpn=ilpn)

        self.switch_frame(0)
        self.click_by_xpath(self._CHECK_BOX)
        self.click_by_xpath(self._ADJUST_BTN)

        if isWarnMsgShow:
            elementDisplay = self.is_displayed_by_xpath(self._WARNING_MSG)
            assert elementDisplay, "Adjust lpn warning msg didnt show"
        else:
            self.scroll_to(self._REASON_CODE)
            self.clear_textbox_by_xpath(self._NEW_QTY_INPUT)
            self.fill_by_xpath(self._NEW_QTY_INPUT,newQty)
            self.select_in_dropdown_by_xpath(self._REASON_CODE,o_reasonCode)
            self.click_by_xpath(self._SAVE_BTN)

        self.switch_default_content()

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=o_lpnFacStat)
        if not isWarnMsgShow:
            DBLib().assertLPNDtls(i_lpn=ilpn, i_itemBrcd=itemBrcd, o_qty=newQty)
            DBLib().assertPix(i_caseNbr=ilpn, i_tranType='300', i_itemBrcd=itemBrcd)

    # def allocateILPN(self,ilpn:str):
    #     self.filterByILPN(ilpn=ilpn)
    #     ilpnFacStat = self.get_text_by_xpath(self._LPN_FAC_STAT)
    #
    #     """validation"""
    #     isMatched = DBService().compareEqual(ilpnFacStat,"Allocated","ILPN Facility Status")
    #     assert isMatched,"ILPN Facility Status UI validation failed"

    def lockiLPN(self, ilpn: str, lockCode: str, o_item: str, o_facStat: LPNFacStat):
        """"""
        self._filterByILPN(ilpn=ilpn)

        self.switch_frame(0)
        self.click_by_xpath(self._CHECK_BOX)
        self.click_by_xpath(self._LOCK_UNLOCK_LPN_BTN)
        self.click_by_xpath(self._LOCK_LPN_BTN)
        self.select_in_dropdown_by_value(self._SELECT_LOCK_CODE, lockCode)
        self.click_by_xpath(self._LOCK_UNLOCK_SAVE_BTN)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=o_facStat)
        DBLib().assertLpnLockPresent(i_lpn=ilpn, i_lockCode=lockCode)
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=ilpn, i_tranType='300', i_tranCode='01', i_invnAdjType='S')
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=ilpn, i_tranType='606', i_tranCode='02', i_invnAdjType='A')

    def unLockiLPN(self, ilpn: str, lockCode: str, o_item: str, o_facStat: LPNFacStat):
        """"""
        self._filterByILPN(ilpn=ilpn)

        self.switch_frame(0)
        self.click_by_xpath(self._CHECK_BOX)
        self.click_by_xpath(self._LOCK_UNLOCK_LPN_BTN)
        self.click_by_xpath(self._CB_FOR_UNLOCK_LPN)
        self.click_by_xpath(self._UNLOCK_BTN)
        self.click_by_xpath(self._LOCK_UNLOCK_SAVE_BTN)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=o_facStat)
        DBLib().assertNoInvLockForLpn(i_lpn=ilpn, i_lockCode=lockCode)
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=ilpn, i_tranType='300', i_tranCode='01', i_invnAdjType='A')
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=ilpn, i_tranType='606', i_tranCode='02', i_invnAdjType='S')
        