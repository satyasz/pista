import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMPickLocation(WMBasePage):
    logger = Logging.get(__qualname__)
    _TITLE_XPATH = "(//div[contains(@id,'title') and contains(text(),'Pick Locations')])[1]"
    PAGE = 'Pick Locations'
    MODULE = 'Configuration'

    _INPUT_FOR_ITEM = "//input[@id='dataForm:listView:filterId:itemLookUpId']"
    _INPUT_FOR_LOCN = "//input[@id='dataForm:listView:filterId:field20value1']"
    _APPLY_BTN = "//input[@id='dataForm:listView:filterId:filterIdapply']"
    _CHECKBOX_FOR_ITEM = "//input[@type='checkbox' and @name='dataForm:listView:dataTable_checkAll']"
    _PICK_LOCN_INVENT_BTN = "//input[@type='button' and @value='Pick Location Inventory']"
    _ADJUST_INVENT_BTN = "//input[@type='button' and @id='rmButton_1AdjustInventory1_100204000']"
    _CURRENT_QTY_INPUT = "//input[@id='dataForm:CurrentQty']"
    _NEW_QTY_INPUT = "//input[@id='dataForm:NewQty']"
    _SELECT_REASON_CODE = "//select[@id='dataForm:adjustReasonSelect']"
    _SAVE_BTN = "//input[@id='rmButton_1Save1_154183000' and @value='Save']"
    _CREATE_CYCLE_COUNT_BTN = "//input[@type='button' and @id='rmButton_1CreateCycleCountTasks1_167271492']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens Pick location UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()

    def _filterByItemAndLocn(self, item: str, locn: str):
        # self.switch_frame(0)
        self.fill_by_xpath(self._INPUT_FOR_ITEM, item)
        self.fill_by_xpath(self._INPUT_FOR_LOCN, locn)
        self.click_by_xpath(self._APPLY_BTN)
        self.click_by_xpath(self._CHECKBOX_FOR_ITEM)
        # self.switch_default_content()

    def adjustInventory(self, newQty: int, reasonCode, item, locn):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.switch_frame(0)
        self._filterByItemAndLocn(item=item, locn=locn)
        self.click_by_xpath(self._PICK_LOCN_INVENT_BTN)
        self.click_by_xpath(self._CHECKBOX_FOR_ITEM)
        self.click_by_xpath(self._ADJUST_INVENT_BTN)
        self.scroll_to(self._SELECT_REASON_CODE)
        self.select_in_dropdown_by_value(self._SELECT_REASON_CODE, reasonCode)
        self.clear_textbox_by_xpath(self._NEW_QTY_INPUT)
        self.fill_by_xpath(self._NEW_QTY_INPUT, str(newQty))
        self.click_by_xpath(self._SAVE_BTN)
        self.switch_default_content()

        '''Validation'''
        DBLib().assertWMInvnDtls(i_locn=locn, i_itemBrcd=item, o_onHandQty=newQty)
        DBLib().assertPix(i_itemBrcd=item, i_tranType='300', i_rsnCode=reasonCode)

    def createCycleCountActv(self, item: str, locn: str):
        self.switch_frame(0)
        self._filterByItemAndLocn(item=item, locn=locn)
        self.click_by_xpath(self._CREATE_CYCLE_COUNT_BTN)
        self.switch_default_content()

        '''Validation'''
        # DBLib().assertCycleCountTask(i_locnBrcd=locn,i_intType=101)
        # DBLib().assertCycleCountStatus(i_locnBrcd=locn,i_intType=101,o_statCode=10)
        taskId = DBLib().getCCTask(i_locnBrcd=locn, i_intType=101)
        taskId = taskId.get('TASK_ID')
        return taskId
