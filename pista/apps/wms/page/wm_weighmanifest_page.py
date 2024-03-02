import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from apps.wms.app_status import DOStat, LPNFacStat
from core.log_service import Logging


class WMWeighManifestPage(WMBasePage):
    logger = Logging.get(__qualname__)
    _TITLE_XPATH = "//div[@class = 'x-title-text x-title-text-default x-title-item' and text() = 'Weigh and Manifest oLPN - Weigh and Manifest oLPNs']"
    PAGE = 'Weigh and Manifest oLPN'
    MODULE = 'Distribution'

    '''Step 1 screen'''

    _OLPN_TB_IN_SCAN_PAGE = "//span//input[@id = 'dataForm:EnterLPNNumber']"
    _NEXT_BTN_IN_SCAN_PAGE = "//input[@id = 'dataForm:nextButtonAddLPN' and @value = 'Next >']"

    '''Step 2 screen'''

    _NEXT_BTN_IN_ORDERNOTE_PAGE = "//input[@id = 'dataForm:nextButton' and @value = 'Next >']"

    '''Step 3 screen'''

    _ACT_WEIGHT = "//input[@id = 'dataForm:actWeight']"
    _SHIPVIA_DROPDOWN_BTN = "//select[@id = 'dataForm:shipviaList']"
    _CNTR_TYPE_DROPDOWN_BTN = "//select[@id = 'dataForm:containerTypeList']"
    _MANIFEST_BTN = "//input[@value = 'Manifest >' and @id = 'dataForm:nextButton']"
    _EXIT_BTN = "//input[@value ='Exit' and @id = 'dataForm:exitButton']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens Weigh and Manifest UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.maximizeMenuPage()

    def manifestOLPN(self, olpn: str, order: str, waveNum: str, hasOrderNote: bool, shipVia=None, olpnType=None,
                     o_doStatus: int = None):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs([order])[0]

        DBLib()._updateOLPNCntrTypeForDO(order=pc_order, olpn=olpn)
        actWeight = DBLib().getEstWeightFromOLPN(oLpn=olpn)

        self.switch_frame(0)
        self.fill_by_xpath(self._OLPN_TB_IN_SCAN_PAGE, olpn)
        self.click_by_xpath(self._NEXT_BTN_IN_SCAN_PAGE)
        if hasOrderNote:
            self.click_by_xpath(self._NEXT_BTN_IN_ORDERNOTE_PAGE)
        self.fill_by_xpath(self._ACT_WEIGHT, str(actWeight), clearVal=True)
        if shipVia is not None:
            self.select_in_dropdown_by_xpath(self._SHIPVIA_DROPDOWN_BTN, str(shipVia))
        if olpnType is not None:
            self.select_in_dropdown_by_xpath(self._CNTR_TYPE_DROPDOWN_BTN, str(olpnType))
        self.click_by_xpath(self._MANIFEST_BTN)
        self.click_by_xpath(self._EXIT_BTN)
        self.switch_default_content()

        manifestId = DBLib().getManifestIdFromWaveNum(waveNum=waveNum, order=pc_order, oLpn=olpn)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=olpn, o_facStatus=LPNFacStat.OLPN_MANIFESTED)
        DBLib().assertDOHdr(i_order=order, o_status=DOStat.MANIFESTED)

        return manifestId
