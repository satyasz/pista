from apps.wms.page.wm_base_page import WMBasePage


class WMTrailersPage(WMBasePage):
    _TITLE_XPATH = "//title[text()='Google']"  # TODO change xpath

    _TRAILER_ID = "//.[@id='dataForm:listView:filterId:trailerLookUpFilterId']"  # TODO Xpath
    _TRAILER_APPLY_BTN = "//button[@id='dataForm:listView:filterId:filterIdapply']"
    _CHECKBOX_IN_TABLE = "//.[@id='checkAll_c0_dataForm:listView:dataTable']"
    _TRAILER_STATUS = "//.[@id='dataForm:listView:dataTable:0:custId52']"

    def __init__(self, driver):
        super().__init__(driver, self._TITLE_XPATH)
        self.click_by_xpath(self._MAXIMIZE_PAGE_BTN)

    def filterByTrailer(self, trailerNbr):
        self.fill_by_xpath(self._TRAILER_ID, trailerNbr)
        self.click_by_xpath(self._TRAILER_APPLY_BTN)
        self.click_by_xpath(self._CHECKBOX_IN_TABLE)

    def assertTrailerStatus(self, status):
        actualStatus = self.get_text_by_xpath(self._TRAILER_STATUS)
        assert status == actualStatus, 'Trailer status didnt match with ' + status + ', actual: ' + actualStatus
