from apps.wms.app_db_lib import DBLib
from apps.wms.app_status import TaskDtlStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMLeanTimeReplenishmentPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Lean Time Replenishment'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Lean Time Replenishment')]"

    _COLLAPSE_BTN = "//input[@class = 'fltrHidden']"
    _DESC_TB = "//input[@id = 'dataForm:listView1:filterId1:field30value1']"
    _APPLY_BTN = "//input[@class = 'btn  groupBtn' and @value = 'Apply']"
    _LTR_RECORD_CB = "//span[text()='AUTOMATION']//parent::td//preceding-sibling::td//input[@type = 'checkbox']"
    _SUBMIT_BTN = "//input[@type = 'button' and @value = 'Submit']"
    _REPLEN_STATUS = "//li[@class = 'overlayinfo -icons_info' and text() = 'Transaction successful.']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens Lean Time Replenishment UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def createReplenByLocn(self, fromLocn: str, toLocn: str, o_iLpns: list[str] = None, o_items: list[str] = None,
                           o_replenLocns: list[str] = None):
        """"""
        DBLib()._assertLeanTimeReplenRuleExist(ruleDesc='AUTOMATION')
        DBLib()._presetLTRRuleWithLocnRange(fromLocn=fromLocn, toLocn=toLocn)

        self.switch_frame(0)
        self.click_by_xpath(self._COLLAPSE_BTN)
        self.fill_by_xpath(self._DESC_TB, 'AUTOMATION')
        self.click_by_xpath(self._APPLY_BTN)
        self.click_by_xpath(self._LTR_RECORD_CB)
        self.click_by_xpath(self._SUBMIT_BTN)
        replenStatus = self.get_text_by_xpath(self._REPLEN_STATUS)

        isReplenSuccess = True if replenStatus == 'Transaction successful.' else False
        assert isReplenSuccess, f"LTR replen not success for locn from {fromLocn} to {toLocn}"
        # self.closeMenuPage()

        '''Validation'''
        # self.wait_for(15)
        DBLib().assertWaitForTask(i_cntrNbr=o_iLpns[0], i_itemBrcd=o_items[0], i_intType=1, i_destLocn=o_replenLocns[0], i_taskPrty=70)
        for i in range(len(o_replenLocns)):
            DBLib().assertTaskDtls(i_itemBrcd=o_items[i], i_cntrNbr=o_iLpns[i], i_intType=1,
                                   i_destLocn=o_replenLocns[i], o_statCode=TaskDtlStat.UNASSIGNED, o_taskPriority=70)

    def createReplenByItem(self, itemBrcd:str, isActvPerAboveConfigPer:bool=None, o_iLpns:list[str]=None, o_replenLocns:list[str]=None,
                           isAssertNoTask:bool=None):
        """"""
        actvUnitsPer = int(DBLib().getActvQtyPercent(item=itemBrcd))
        cutOffPer = (actvUnitsPer - 1) if isActvPerAboveConfigPer else (actvUnitsPer + 1)

        DBLib()._assertLeanTimeReplenRuleExist(ruleDesc='AUTOMATION')
        DBLib()._updateLTRRuleWithItem(itemBrcd=itemBrcd, cutOffPercent=cutOffPer)

        self.switch_frame(0)
        self.click_by_xpath(self._COLLAPSE_BTN)
        self.fill_by_xpath(self._DESC_TB, 'AUTOMATION')
        self.click_by_xpath(self._APPLY_BTN)
        self.click_by_xpath(self._LTR_RECORD_CB)
        self.click_by_xpath(self._SUBMIT_BTN)
        replenStatus = self.get_text_by_xpath(self._REPLEN_STATUS)

        isReplenSuccess = True if replenStatus == 'Transaction successful.' else False
        assert isReplenSuccess, f"LTR replen not success for item {itemBrcd}"
        # self.closeMenuPage()

        '''Validation'''
        # self.wait_for(15)
        if isAssertNoTask:
            DBLib().assertNoTaskExist(i_cntrNbr=o_iLpns[0], i_itemBrcd=itemBrcd, i_intType=1, i_destLocn=o_replenLocns[0], i_taskPrty=70)
        else:
            DBLib().assertWaitForTask(i_cntrNbr=o_iLpns[0], i_itemBrcd=itemBrcd, i_intType=1, i_destLocn=o_replenLocns[0], i_taskPrty=70)
            for i in range(len(o_replenLocns)):
                DBLib().assertTaskDtls(i_itemBrcd=itemBrcd, i_cntrNbr=o_iLpns[i], i_intType=1, i_destLocn=o_replenLocns[i],
                                       o_statCode=TaskDtlStat.UNASSIGNED, o_taskPriority=70)
