from apps.wms.app_status import TaskHdrStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_home_page import WMHomePage


class WMTasksPage(WMBasePage):
    PAGE = 'Tasks'
    MODULE = 'Configuration'
    _TITLE_XPATH = "//div[contains(@id,'title') and contains(text(),'Tasks')]"

    _TASK_SEARCHBOX = "//input[@id='dataForm:lview:filterId:field10value1']"
    _APPLY_BTN = "//input[@id='dataForm:lview:filterId:filterIdapply']"
    _CHECKBOX_IN_TABLE = "//*[@id='checkAll_c0_dataForm:lview:dataTable']"
    _RELEASE_TASK_BTN = "//input[@value='Release Task']"
    _CANCEL_TASK = "//input[@type='button' and @value='Cancel Task']"
    _ERROR_MSG_FOR_PUTAWAY_TASK = "//*[contains(text(),'Cannot cancel Task')]"
    _ACCEPT_CANCEL_TASK = "//input[@id='softCheckAcceptButton']"

    _ASSIGN_USER_BTN = "//input[@type='button' and @id='rmButton_1AssignUser1_167271348']"
    _FILL_USER_ID_TEXT = "//input[@type='text' and @id='dataForm:editCtrl']"
    _SUBMIT_BTN = "//input[@type='button' and @id='saveButton01']"
    _CHANGE_PRIORITY_BTN = "//input[@type='button' and @id='rmButton_1ChangePriority1_167271696']"
    _CHANGE_PRIORITY_INPUT_TXT = "//input[@id='dataForm:inputChngPrtyId1' and @name='dataForm:inputChngPrtyId1']"
    _CHANGE_PRIORITY_OK_BTN = "//input[@type='button' and @id='dataForm:chngPrtyOkButton']"
    
    def __init__(self, driver, isPageOpen: bool = False):
        """Opens Tasks UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()
            self.wait_for(1)

    def _filterByTask(self, taskId):
        self.fill_by_xpath(self._TASK_SEARCHBOX, taskId)
        self.click_by_xpath(self._APPLY_BTN)

    def releaseTask(self, taskId):
        self.click_by_xpath(self._CHECKBOX_IN_TABLE)
        self.click_by_xpath(self._RELEASE_TASK_BTN)
        # TODO

    def cancelTask(self, taskId: str, isTaskCancelError: bool = False, o_taskStat: TaskHdrStat = None):
        self.switch_frame(0)

        self._filterByTask(taskId=taskId)

        self.click_by_xpath(self._CHECKBOX_IN_TABLE)
        self.click_by_xpath(self._CANCEL_TASK)
        self.accept_alert_if_present()
        self.click_by_xpath(self._ACCEPT_CANCEL_TASK)
        if isTaskCancelError:
            isElementFound = self.is_displayed_by_xpath(self._ERROR_MSG_FOR_PUTAWAY_TASK)
            assert isElementFound, "Cancel task error not found"

        self.switch_default_content()

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=o_taskStat)

    def assignTaskToUser(self,taskId:str, userId:str, taskPrty:int):
        """"""
        self.switch_frame(0)

        self._filterByTask(taskId=taskId)
        self.click_by_xpath(self._CHECKBOX_IN_TABLE)
        self.click_by_xpath(self._ASSIGN_USER_BTN)
        self.accept_alert_if_present()
        self.clear_textbox_by_xpath(self._FILL_USER_ID_TEXT)
        self.fill_by_xpath(self._FILL_USER_ID_TEXT, userId)
        self.click_by_xpath(self._SUBMIT_BTN)
        self.click_by_xpath(self._CHECKBOX_IN_TABLE)
        self.click_by_xpath(self._CHANGE_PRIORITY_BTN)
        self.fill_by_xpath(self._CHANGE_PRIORITY_INPUT_TXT, taskPrty)
        self.click_by_xpath(self._CHANGE_PRIORITY_OK_BTN)

        self.switch_default_content()

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_currTaskPrty=taskPrty, o_ownerUser=userId, o_status=TaskHdrStat.RELEASED, isIgnoreDateCheck=True)
