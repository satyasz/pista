import inspect

from apps.wms.app_db_lib import DBLib
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.log_service import Logging


class WMShipmentPlanPage(WMBasePage):
    logger = Logging.get(__qualname__)

    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Shipment Planning Workspace')]"
    PAGE = "Shipment Planning Workspace"
    MODULE = "Distribution"

    '''Order section'''

    _FILTERBY_TB_IN_ORDER_SEC = "(//input[contains(@componentid,'combobox-')])[3]"
    _ORDERS_TB_IN_ORDERS_SEC = "//input[contains(@id,'mpslookupfield-')]"
    _APPLY_BTN_IN_FILTR_SEC = "(//span[@data-ref = 'btnInnerEl' and contains(text(),'Apply')])[1]"
    _ORDER_ROWS = "//div[@class = 'x-grid-item-container']/table"
    _SELECT_ALL_CB = "(//div[@class='x-column-header-inner x-column-header-inner-empty']//span[contains(@class, 'x-column-header-text')])[2]"
    _COMBINE_OPTN = "(//div[contains(@id,'innerCt')]//span[ text()='Combine'])[last()]"
    _SINGLE_OPTN = "(//div[contains(@id,'innerCt')]//span[ text()='Single'])[last()]"
    _ASSIGN_BTN = "(//div[contains(@id,'innerCt')]//span[ text()='Assign'])[last()]"
    _FIRST_ROW_CB = "(//tr[@class = '  x-grid-row']/td/div/div[@class = 'x-grid-row-checker'])[1]"

    _APPLY_BTN = "(//div[contains(@class,'x-window-closable')]//div[contains(@class,'x-container') and contains(@id,'mpscard')]//div[contains(@id,'toolbar-')]//span[contains(@id,'button')]//span//span[text()='Apply'])[last()]"
    _SELECT_SHIPMENT = "(//td//div[text()='#SHIPMENT#']//ancestor::tr//td//div[@role='presentation'])[1]"
    _EDIT_SHIP_VIA = "//span[text()='Edit Ship Via']"
    _SEARCH_SHIP_VIA = "(//div[contains(@class,'x-window-closable')]//div[contains(@class,'x-container') and contains(@id,'mpscard')]//label[text()='Primary Fields']//parent::div//child::div//input)[last()-1]"
    _SEARCH_SHIP_VIA_NAME = "//div[contains(@class,'x-window-closable')]//div[contains(@class,'x-container') and contains(@id,'mpscard')]//label[text()='Primary Fields']//parent::div//child::div//input[@name='shipVia']"
    _SELECT_SHIP_VIA = "//div[contains(@class,'x-window-closable')]//td//div[text()='#SHIP_VIA#']//ancestor::tr//td//div[@role='presentation']"
    _CLICK_OK = "//div[contains(@class,'x-window-closable')]//div[contains(@class,'x-container') and contains(@id,'mpscard')]//span[contains(@id,'button') and contains(@data-ref,'btnInnerEl') and contains(text(),'Ok')]"
    
    '''Shipment section'''

    # _SHIPMENT_ROW = "//div[@class='x-grid-item-container']//td[@data-columnid='tCShipmentID']"
    _SHIPMENT_ROW = "//div[@class='x-grid-item-container']//td[@data-columnid='tCShipmentID']//div[contains(text(),'CS')]"
    _SHIPMENT_CB = "//div[@class='x-grid-item-container']//td[@data-columnid='tCShipmentID']//div[contains(text(),'#SHIPMENTNBR#')]/ancestor::tr//td//div[@role='presentation']"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens shipment planning UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            # self.maximizeMenuPage()

    # def createShipment(self, orders: list):
    #     listOfOrders = ','.join(orders)
    #     self.fill_by_xpath(self._FILTER_DRP_DWN, 'Distribution Order')
    #     self.press_enter_by_xpath(self._FILTER_DRP_DWN)
    #     self.fill_by_xpath(self._LOOKUP_FIELD_TB, listOfOrders)
    #     self.click_by_xpath(self._APPLY_BTN_IN_FILTR_SEC)
    #     ordersFiltered = self.get_webelements(self._ORDER_RECORDS)
    #     if len(listOfOrders) != len(ordersFiltered):
    #         assert False, 'orders are not filtered as expected'
    #     self.click_by_xpath(self._SELECT_ALL_CB)
    #     self.right_click_by_xpath(self._FIRST_ROW_CB)
    #     self.click_by_xpath(self._COMBINE_OPTN)
    #     # assert for shipment row

    def createShipment(self, orders: list[str], isDOWaved: bool, o_shipmentStat: int = None, u_shipVia: str = None,
                       isProNumberCreated: bool = None):
        """Pass list of orders to be in 1 shipemnt
        Does validation
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Creating shipment')

        pc_orders = DBLib()._getParentDOsIfExistElseChildDOs(orders=orders)
        orders = pc_orders

        listOfOrders = ','.join(orders)
        self.fill_by_xpath(self._FILTERBY_TB_IN_ORDER_SEC, 'Distribution Order')
        self.press_enter_by_xpath(self._FILTERBY_TB_IN_ORDER_SEC)
        self.fill_by_xpath(self._ORDERS_TB_IN_ORDERS_SEC, listOfOrders)
        self.click_by_xpath(self._APPLY_BTN_IN_FILTR_SEC)
        ordersFiltered = self.get_webelements(self._ORDER_ROWS)
        assert len(orders) == len(ordersFiltered), 'Orders are not filtered as expected'

        self.click_by_xpath(self._SELECT_ALL_CB)
        self.right_click_by_xpath(self._FIRST_ROW_CB)
        if len(ordersFiltered) > 1:
            self.click_by_xpath(self._COMBINE_OPTN)
        else:
            self.click_by_xpath(self._SINGLE_OPTN)
        isShipmentRowFound = self.is_displayed_by_xpath(self._SHIPMENT_ROW)
        assert isShipmentRowFound, 'No shipment row found'

        shipmentNbr = DBLib().get1ShipmentNumFromDOs(orders=orders)

        if u_shipVia is not None:
            self.click_by_xpath(self._SELECT_SHIPMENT.replace('#SHIPMENT#', shipmentNbr))
            self.right_click_by_xpath(self._SHIPMENT_ROW)
            self.click_by_xpath(self._EDIT_SHIP_VIA)

            self.fill_by_xpath(self._SEARCH_SHIP_VIA, "Ship Via")
            self.press_enter_by_xpath(self._SEARCH_SHIP_VIA)
            self.fill_by_xpath(self._SEARCH_SHIP_VIA_NAME, u_shipVia)
            self.click_by_xpath(self._APPLY_BTN)
            self.click_by_xpath(self._SELECT_SHIP_VIA.replace('#SHIP_VIA#', u_shipVia))
            self.click_by_xpath(self._CLICK_OK)

            self.switch_default_content()
            
        '''Validation'''
        DBLib().assertWaitShipmentStatus(i_shipment=shipmentNbr, o_status=o_shipmentStat)
        DBLib().assertDOInShipment(i_shipment=shipmentNbr, o_orders=orders)
        if isDOWaved:
            DBLib().assertShipmentForOLPNFromDO(i_shipment=shipmentNbr, i_orders=orders)
        if isProNumberCreated is not None:
            proNbrLevel = int(DBLib().getProNumberLevelFromShipVia(shipVia=u_shipVia))
            DBLib().assertProNumberForShipment(proNumberLevel=proNbrLevel, shipmentId=shipmentNbr)

        return shipmentNbr

    def addOrderToExistingShipment(self, shipmentNbr: str, existingOrders: list[str], newOrder: str):
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Add order to existing shipment')

        pc_existingOrders = DBLib()._getParentDOsIfExistElseChildDOs(orders=existingOrders)
        existingOrders = pc_existingOrders
        pc_newOrder = DBLib()._getParentDOsIfExistElseChildDOs(orders=[newOrder])[0]
        newOrder = pc_newOrder

        self.click_by_xpath(self._SHIPMENT_CB.replace('#SHIPMENTNBR#', str(shipmentNbr)))
        self.fill_by_xpath(self._FILTERBY_TB_IN_ORDER_SEC, 'Distribution Order', clearVal=True)
        self.press_enter_by_xpath(self._FILTERBY_TB_IN_ORDER_SEC)
        self.fill_by_xpath(self._ORDERS_TB_IN_ORDERS_SEC, newOrder, clearVal=True)
        self.click_by_xpath(self._APPLY_BTN_IN_FILTR_SEC)
        # orderFiltered = self.get_webelements(self._ORDER_ROWS)
        # assert len(addtnlOrder) == len(orderFiltered), 'Order is not filtered as expected'

        self.click_by_xpath(self._SELECT_ALL_CB)
        self.right_click_by_xpath(self._FIRST_ROW_CB)
        self.click_by_xpath(self._ASSIGN_BTN)
        existingOrders.append(newOrder)

        '''Validation'''
        DBLib().assertWaitForDOInShipment(shipmentNum=shipmentNbr, order=newOrder)
        DBLib().assertDOInShipment(i_shipment=shipmentNbr, o_orders=existingOrders)
