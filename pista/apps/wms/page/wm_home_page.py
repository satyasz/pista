from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_login_page import WMLoginPage


class WMHomePage(WMBasePage):
    _TITLE_XPATH = "//title[text()='Manhattan Associates']"

    _MENU_BTN = "//span[contains(@id,'button-1013-btnIcon')]"
    _SEARCHBOX_IN_MENU = "//input[contains(@id,'mps_menusearch') and @role='combobox']"
    _PAGE_AND_MODULE_IN_DROPDOWN = "//ul[contains(@id,'boundlist-')]//li//div[contains(text(),'(#MODULE_NAME#)')]" \
                                   "//b[(normalize-space(text())='#PAGE_NAME#')]"

    def __init__(self, driver, isPageOpen: bool = False):
        """This will open WM url. Returns back if homepage opens up, else login"""
        if not isPageOpen:
            WMLoginPage(driver=driver, isLoggedIn=isPageOpen)
        super().__init__(driver, self._TITLE_XPATH)

    def openMenuPage(self, pageName, moduleName=None):
        if moduleName is None:
            moduleName = ''
        self.click_by_xpath(self._MENU_BTN)
        self.fill_by_xpath(self._SEARCHBOX_IN_MENU, pageName)
        self.click_by_xpath(self._PAGE_AND_MODULE_IN_DROPDOWN.replace('#PAGE_NAME#', pageName)
                            .replace('#MODULE_NAME#', moduleName))

    def logoutWM(self):
        self.click_by_xpath(self._USER_ICON)
        self.click_by_xpath(self._SIGNOUT_BTN)
        super().__init__(self.driver, self._TITLE_FOR_SIGNED_OUT_PAGE)
