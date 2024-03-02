import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging
from apps.wms.app_status import DOStat, LPNFacStat, AllocStat

class WMOrdersPage(WMBasePage):
    logger = Logging.get(__qualname__)
    PAGE = 'Distribution Orders'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@id,'title-')]//div[text()='Distribution Orders']"

    '''Distribution Orders Page'''

    _FILTERBY_TB = "(//input[contains(@componentid,'combobox-')])[3]"
    _ORDERS_TB = "//input[contains(@id,'mpslookupfield-')]"
    _APPLY_BTN = "//span[text()='Apply']"
    _SELECT_ALL_CB = "(//div[@class = 'x-column-header-inner x-column-header-inner-empty']//span[contains(@id,'gridcolumn-') and @class ='x-column-header-text'])[1]"
    _SELECT_ORDERS = "//*[@class='x-column-header-text']"
    _MORE = "//span[text()='More']"
    _WAVE = "//span[text()='Wave']"
    _EDIT_HEADER_BTN = "//span[text()='Edit Header']"
    _SELECT_SHIP_VIA = "//select[@id='dataForm:DOEdit_HeaderTab_Drop_Shipvia']"
    _CARRIER_INPUT = "//input[@id='dataForm:DOEdit_HeaderTab_InText_Carrier']"
    _SAVE_BTN = "//input[@type='submit' and @value='Save']"
    
    '''Ship confirm'''

    _SHIP_CNFRM_BTN = "//span[text() = 'Ship Confirm']"
    _SHP_CNFRM_YES_BTN = "//div[contains(@id,'messagebox')]//span[text()='Yes']"
    _CREATE_CHASE_TASK_BTN = "//span[text() = 'Create Chase Task']"

    '''Create Chase Task'''

    _TITLE_FOR_RUNWAVE_PAGE = "//div[contains(text(),'Run Waves')]"
    _TITLE_FOR_SHIPWAVE_PAGE = "//div[contains(text(),'ShipWave Template - Run Waves')]"
    _CB_FOR_TEMPLATE = "(//table[@id='dataForm:listView:dataTable_body']//tr//td/span[contains(@id,'dataForm:listView:dataTable') and contains(@id,'wvdesc') and text()='#TEMPLATE_NAME#'])[1]/parent::td/parent::tr//input[@type='checkbox' and contains(@id,'checkAll')]"
    _RUN_WAVE_BTN = "//input[@value = 'Run Wave']"
    _SUBMIT_BTN = "//input[@value = 'Submit']"
    _GENERATED_WAVE_NBR = "//a[contains(@id,'dataForm:AwvNbrRun')]"

    _SUBMIT_WAVE = "//input[@value='Save Configuration']/following-sibling::input"
    _CHECKBOX_FOR_WAVE_NUMBER = "//*[id='checkAll_c0_dataForm:listView:dataTable']"
    # _SEARCHBOX_IN_MENU = "//input[@id='mps_menusearch-1083-inputEl']"
    # _DROPDOWN_OPTION_IN_MENU = "//input[@id='mps_menusearch-1083-inputEl' and text()='#MENU_NAME#']"
    _DISTRIBUTION_ORDER_FILTERDD = "//label[text()='Primary Fields']/following-sibling::div//input"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens  Distribution Orders UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def filterByOrder(self, orderNum):
        self.fill_by_xpath(self._FILTERBY_TB, 'Distribution Order')
        self.press_enter_by_xpath(self._FILTERBY_TB)
        self.fill_by_xpath(self._ORDERS_TB, orderNum)
        self.click_by_xpath(self._APPLY_BTN)
        self.click_by_xpath(self._SELECT_ALL_CB)

    def runWaveByOrder(self):
        self.click_by_xpath(self._SELECT_ORDERS)
        self.click_by_xpath(self._MORE)
        self.click_by_xpath(self._WAVE)

    def shipConfirmOrder(self, order: str, oLpns: list):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs([order])[0]

        self.filterByOrder(order)
        self.click_by_xpath(self._MORE)
        self.click_by_xpath(self._SHIP_CNFRM_BTN)
        self.click_by_xpath(self._SHP_CNFRM_YES_BTN)

        '''Validation'''
        DBLib().assertWaitDOStatus(i_order=order, o_status=190)
        DBLib().assertShipConfirmXmlMsgExist(order=order)
        for i in range(len(oLpns)):
            DBLib().assertLPNHdr(i_lpn=oLpns[i], o_facStatus=LPNFacStat.OLPN_SHIPPED)

    def createChaseTask(self, chaseWaveTemplate: str, order: str, item: str, qty: int, o_ordStatus: DOStat = None,
                        isAssertPickingShortItem: bool = None, shortedOLpn: str = None,
                        intType: int = None, o_allocStatus: AllocStat = None, o_ordLineStatus: int = None,
                        isAssertNoTask: bool = None):
        """creates chase wave for the shorted qty"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs(orders=[order])[0]

        self.filterByOrder(pc_order)
        self.click_by_xpath(self._MORE)
        self.click_by_xpath(self._CREATE_CHASE_TASK_BTN)
        self.wait_for_elementvisible(self._TITLE_FOR_SHIPWAVE_PAGE)
        self.switch_frame(0)
        self.click_by_xpath(self._CB_FOR_TEMPLATE.replace('#TEMPLATE_NAME#', chaseWaveTemplate))
        self.click_by_xpath(self._RUN_WAVE_BTN)
        self.click_by_xpath(self._SUBMIT_BTN)
        chaseWaveNum = self.get_text_by_xpath(self._GENERATED_WAVE_NBR)
        self.switch_default_content()
        self.logger.info('chaseWavenumber: ' + chaseWaveNum)

        '''Validation'''
        DBLib().assertWaitWaveStatus(i_wave=chaseWaveNum, i_status=90)
        chaseWaveOLpns = DBLib().getAllOLPNsFromWave(wave=chaseWaveNum)
        for i in range(len(chaseWaveOLpns)):
            DBLib().assertLPNHdr(i_lpn=chaseWaveOLpns[i], o_facStatus=LPNFacStat.OLPN_PRINTED, isConsLocnUpdated=True)

        DBLib().assertDOHdr(i_order=pc_order, o_status=o_ordStatus)
        DBLib().assertAllocDtls(i_itemBrcd=item, i_taskGenRefNbr=chaseWaveNum, i_intType=intType,
                                o_qtyAlloc=qty, o_statCode=o_allocStatus)

        if o_ordLineStatus is not None:
            DBLib().assertDODtls(i_order=pc_order, i_itemBrcd=item, i_waveNum=chaseWaveNum, o_qtyAllocated=qty,
                                 o_dtlStatus=o_ordLineStatus)

        if isAssertPickingShortItem:
            DBLib().assertPickShortItemDtls(i_order=order, i_lpn=shortedOLpn, i_item=item, o_statCode=90)

        if isAssertNoTask:
            DBLib().assertNoTaskPresent(taskRefNum=chaseWaveNum)

        return chaseWaveNum
