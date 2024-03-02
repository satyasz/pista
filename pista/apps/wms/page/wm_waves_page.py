import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.app_status import DOStat, AllocStat
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMWavesPage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Waves'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Waves')]"

    _ENTER_WAVE_NUMBER = "//*[@alt='Find Ship Wave number']"
    _APPLY_WAVE_NUMBER = "//*[@id='dataForm:listView:filterId:filterIdapply']"
    _CHECKBOX_FOR_WAVE_NUMBER = "//*[@id='checkAll_c0_dataForm:listView:dataTable']"
    _UNDO_WAVE_BUTTON = "//input[@value='Undo Wave']"
    _WAVE_STATUS = "//*[@id='dataForm:listView:dataTable:0:c0012']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens waves UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def filterByWaveNum(self, waveNum):
        self.fill_by_xpath(self._ENTER_WAVE_NUMBER, waveNum)
        self.click_by_xpath(self._APPLY_WAVE_NUMBER)

    def undoWave(self, waveNum: str, o_orders: list[str] = None,
                 o_lnItems: list[list[str]] = None, o_lnQtys: list[list[int]] = None,
                 o_ordStatus: list[DOStat] = None,
                 o_intTypes: list[list[int]] = None, o_allocStatus: list[list[AllocStat]] = None):
        """"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info(f"Running undo wave {waveNum}")

        self.switch_frame(0)
        self.filterByWaveNum(waveNum)
        self.click_by_xpath(self._CHECKBOX_FOR_WAVE_NUMBER)
        self.click_by_xpath(self._UNDO_WAVE_BUTTON)
        self.acceptAlertPopup()
        # self.switch_default_content()
        # self.clickRefreshBtn()

        '''Validation'''
        for i in range(len(o_orders)):
            DBLib().assertDOHdr(i_order=o_orders[i], o_status=o_ordStatus[i])
            for j in range(len(o_lnItems[i])):
                DBLib().assertAllocDtls(i_taskGenRefNbr=waveNum, i_intType=o_intTypes[i][j],
                                        i_itemBrcd=o_lnItems[i][j],
                                        o_qtyAlloc=o_lnQtys[i][j], o_statCode=o_allocStatus[i][j])

    # def clickRefreshBtn(self):
    #     while True:
    #         self.click_by_xpath(self.clickRefreshBtn())
    #         self.wait_for_display(self._WAVE_STATUS)
    #         wavestat = self.get_text_by_xpath(self._WAVE_STATUS)
    #         if "cancelled" in wavestat:
    #             break

    def acceptAlertPopup(self):
        self.accept_alert_if_present()
        self.accept_alert_if_present()
