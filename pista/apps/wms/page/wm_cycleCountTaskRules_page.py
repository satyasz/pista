from apps.wms.app_db_lib import DBLib
from apps.wms.app_status import TaskHdrStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMCycleCountTaskRules(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Cycle Count Task Rules'
    MODULE = 'Configuration'
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Cycle Count Task Rules')]"

    _CC_TASKCRITERIA_CB = "//span[text()='CycleCount']//parent::td//preceding-sibling::td//input[@type = 'checkbox']"
    _RUN_BTN = "//input[@type = 'button' and @value = 'Run']"
    _REFRESH_BTN = "//div[@data-qtip = 'Refresh']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens cycle count task rules UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def createCycleCntTask(self, locnBrcd: str):
        """Creates cc task for resv locn
        """
        rules = None
        try:
            rules = DBLib()._presetCCTaskRuleForLocn(taskCriteria='CycleCount', ruleName='AUTOMATION', locnBrcd=locnBrcd)

            self.click_by_xpath(self._REFRESH_BTN)

            self.switch_frame(0)
            self.click_by_xpath(self._CC_TASKCRITERIA_CB)
            # self.wait_for(5)
            self.click_by_xpath(self._RUN_BTN)
            self.accept_alert_if_present()
            self.switch_default_content()
        finally:
            '''Revert the rules'''
            if rules is not None:
                for i in rules:
                    ruleId = i.get('RULE_ID')
                    statCode = i.get('STAT_CODE')
                    DBLib()._presetWMRuleStatus(ruleId=ruleId, statCode=statCode)

        dbRow = DBLib().getCCTask(i_locnBrcd=locnBrcd, i_intType=100, i_currTaskPrty=50)
        taskId = dbRow.get('TASK_ID')

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, i_currTaskPrty=50, o_intType=100, o_status=TaskHdrStat.RELEASED)

        return taskId
