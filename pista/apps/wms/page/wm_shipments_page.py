from apps.wms.page.wm_base_page import WMBasePage


class WMShipmentsPage(WMBasePage):
    _TITLE_XPATH = "//title[text()='Google']"  # TODO change xpath

    # filter_by_shipment
    _SHIPMENT_FILTERDD = "//label[text()='Primary Fields']/following-sibling::div//input"
    _SHIPMENT = "//.[@id ='dataForm:TCShipmentIdString1']"  # TODO add taganame
    _SHIPMENT_FILTER = "CSS,.mps-filter-combo-option "  # TODO add xpath
    _SHIPMENTID_INPUT = "//input[@name='TCShipmentID']"
    _APPLY_BUTTON = "//span[text()='Apply']"

    # common buttons in the UI
    _SELECT_ORDERS_BTN = "//*[@class='x-column-header-text']"
    _MORE_BTN = "//span[text()='More']"
    _MAX_BTN = "CSS,.x-tool-img.x-tool-maximize"  # TODO add xpath

    # verify_asn_in_shipment
    _VERIFYSHIPMENTASN_BTN = "//span[text()='Verify Shipment's ASNs']"
    _VERIFYSHIPMENT_BTN = "//.[@id = 'rmButton_1VerifyShipment1_167271844']"  # TODO add taganame

    # unassign_dockdoor_from_shipment
    _UNASSIGN_DOCKDOOR = "//span[text()='Assign-Release Dock Door']"
    _UNASSIGN_BTN = "//input[@value='UnAssign']"
    _SHIPMENT_SAVE_BTN = "//.[@id = 'dataForm:save']"  # TODO add taganame

    # close_shipment
    _CLOSE_SHIPMENT = "//span[text()='Close']"
    _INVOICE_TRAILOR_CONTENTSCB = "//.[@id = 'dataForm:closeShipmentTable:0:invoiceTrailerContentval']"  # TODO add taganame
    _CLOSE_SHIPMENT_SAVE_BTN = "//.[@id = 'dataForm:save_button']"  # TODO add taganame

    def __init__(self, driver):
        super().__init__(driver, self._TITLE_XPATH)
        self.click_by_xpath(self._MAXIMIZE_PAGE_BTN)

    def filterByShipment(self, shipmentID):
        self.fill_by_xpath(self._SHIPMENT_FILTERDD, self._SHIPMENT)
        self.click_by_xpath(self._SHIPMENT_FILTER)  # TODO check dropdown step
        self.click_by_xpath(self._SHIPMENT)
        self.fill_by_xpath(self._SHIPMENTID_INPUT, shipmentID)
        self.click_by_xpath(self._APPLY_BUTTON)

    def verifyASNInShipment(self, shipmentID):
        self.click_by_xpath(self._SELECT_ORDERS_BTN)
        self.click_by_xpath(self._MORE_BTN)
        self.click_by_xpath(self._VERIFYSHIPMENTASN_BTN)
        self.click_by_xpath(self._MAX_BTN)
        self.click_by_xpath(self._VERIFYSHIPMENT_BTN)

    def unassignDockdoorFromShipment(self, shipmentID):
        self.click_by_xpath(self._SELECT_ORDERS_BTN)
        self.click_by_xpath(self._MORE_BTN)
        self.click_by_xpath(self._UNASSIGN_DOCKDOOR)
        self.click_by_xpath(self._MAX_BTN)
        self.click_by_xpath(self._UNASSIGN_BTN)
        self.click_by_xpath(self._SHIPMENT_SAVE_BTN)

    def closeShipment(self):
        self.click_by_xpath(self._SELECT_ORDERS_BTN)
        self.click_by_xpath(self._MORE_BTN)
        self.click_by_xpath(self._CLOSE_SHIPMENT)
        self.click_by_xpath(self._MAX_BTN)
        self.click_by_xpath(self._INVOICE_TRAILOR_CONTENTSCB)
        self.click_by_xpath(self._CLOSE_SHIPMENT_SAVE_BTN)
