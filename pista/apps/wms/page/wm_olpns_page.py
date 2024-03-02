import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging
from apps.wms.app_status import DOStat, LPNFacStat

class WMOLpnsPage(WMBasePage):
    logger = Logging.get(__qualname__)
    PAGE = 'oLPNs'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[@class = 'x-title-text x-title-text-default x-title-item' and text() = 'oLPNs']"

    _LPN_TB = "// input[ @ id = 'dataForm:listView:filterId:field10value1']"
    _APPLY_BTN = "// input[ @ id = 'dataForm:listView:filterId:filterIdapply']"
    _LPN_CB = "//table [@id ='dataForm:listView:dataTable_body']//tr//td//span[text() = '#OLPNNUM#']/parent::td/preceding-sibling::td[@class = 'tbl_checkBox advtbl_col advtbl_body_col']//input[@name = 'checkAll_c0_dataForm:listView:dataTable']"
    _MORE_BTN = "// input[@ name = 'soheaderbuttonsmoreButton']"
    _DEMANIFEST_BTN = "// div[@ id= 'soheaderbuttonsfoActions'] // li // a[text()='De-Manifest oLPN']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens oLPNs UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()

    def deManifestOLpn(self,oLpn,order):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.switch_frame(0)
        self.click_by_xpath(self._LPN_TB)
        self.fill_by_xpath(self._LPN_TB,oLpn)
        self.click_by_xpath(self._APPLY_BTN)
        self.click_by_xpath(self._LPN_CB.replace('#OLPNNUM#',str(oLpn)))
        self.click_by_xpath(self._MORE_BTN)
        self.click_by_xpath(self._DEMANIFEST_BTN)
        self.accept_alert_if_present()
        self.switch_default_content()

        '''Validation'''
        self.wait_for(5)
        DBLib().assertDOHdr(i_order=order, o_status=DOStat.WEIGHED)
        DBLib().assertLPNHdr(i_lpn=oLpn, o_facStatus=LPNFacStat.OLPN_WEIGHED)





