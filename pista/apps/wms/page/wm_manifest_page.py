import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from apps.wms.app_status import LPNFacStat
from core.log_service import Logging


class WMManifestPage(WMBasePage):
    logger = Logging.get(__qualname__)
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Manifests')]"
    PAGE = 'Manifests'
    MODULE = 'Distribution'

    '''Summary screen'''

    _MANIFEST_ID_SEARCHBOX = "//*[@id='dataForm:listView:filterId:field10value1']"
    _FILTER_APPLY_BTN = "//*[@id='dataForm:listView:filterId:filterIdapply']"
    _CHECKBOX_FOR_MANIFEST = "//table//tr//td//*[text()='#MANIFEST_ID#']//parent::td//parent::tr/td[1]//input[@type='checkbox']"
    _CLOSE_MANIFEST_BTN = "//input[@type='button' and contains(@id,'rmButton_1CloseManifest1')]"

    '''Affter close screen'''

    _MANIFEST_ID_CLOSE_BTN = "//*[@id='dataForm:Case_LPN_Sel_saveButton']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens manifest UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(3)
            self.maximizeMenuPage()

    def _filterByManifest(self, manifestId):
        self.click_by_xpath(self._MANIFEST_ID_SEARCHBOX)
        self.fill_by_xpath(self._MANIFEST_ID_SEARCHBOX, manifestId)
        self.click_by_xpath(self._FILTER_APPLY_BTN)

    def closeManifest(self, manifestId: str, waveNum: str, oLpn: str, order: str, manifestStatus: int):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.switch_frame(0)
        self._filterByManifest(manifestId)
        self.click_by_xpath(self._CHECKBOX_FOR_MANIFEST.replace('#MANIFEST_ID#', manifestId))
        self.click_by_xpath(self._CLOSE_MANIFEST_BTN)
        self.click_by_xpath(self._MANIFEST_ID_CLOSE_BTN)
        self.switch_default_content()

        '''Validation'''
        DBLib().assertWaitManifestStatus(i_manifestId=manifestId, o_status=manifestStatus)
        # DBLib().assertManifestStatus(i_wave=waveNum, i_order=order, o_manifestStatus=manifestStatus)
        DBLib().assertWaitDOStatus(i_order=order, o_status=190)
        # DBLib().assertDOHdr(i_order=order, o_status=190)
        DBLib().assertLPNHdr(i_lpn=oLpn, o_facStatus=LPNFacStat.OLPN_SHIPPED)
        DBLib().assertManifestEDIFile(i_manifestId=manifestId)
