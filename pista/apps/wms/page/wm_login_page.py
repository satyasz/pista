# from apps.page.wm_home_page import WMHomePage
from core.config_service import ENV_CONFIG
from core.file_service import DataHandler
from core.ui_service import UIService


class WMLoginPage(UIService):
    _TITLE_XPATH = "//title[contains(text(),'Sign In | Manhattan Associates')]"
    _HOMEPAGE_TITLE_XPATH = "//title[text()='Manhattan Associates']"
    _TITLE_TXT_FOR_HOMEAGE = "Manhattan Associates"

    _PRIVATE_CONN_WARNING = "//*[contains(text(),'Your connection is not private')]"
    _EXPAND_BTN_TO_ACCEPT_WARNING = "//button[text()='Advanced']"
    _PROCEED_LINK = "//*[@id='proceed-link']"

    _USER_XPATH = "//input[@id='username']"
    _PWD_XPATH = "//input[@id='password']"

    _AFTER_LOGIN_WARNIG = "//*[contains(text(),'about to submit is not secure')]"
    _AFTER_LOGIN_PROCEED_BTN = "//button[@id='proceed-button']"

    def __init__(self, driver, url=None, user=None, pwd=None, isLoggedIn: bool = False):
        """If isLoggedIn = False, opens url, else does not open url
            Then checks if it is homepage, else login"""
        if url is None:
            url = ENV_CONFIG.get('ui', 'wm_url')
        if user is None:
            user = ENV_CONFIG.get('ui', 'wm_user')
        if pwd is None:
            # pwd = ENV_CONFIG.get('ui', 'wm_pwd')
            pwd = ENV_CONFIG.get('ui', 'wm_pwd_encrypted')
            pwd = DataHandler.decrypt_it(pwd)

        super().__init__(driver)
        if not isLoggedIn:
            self.open_url(url)
            wasAlertPresent = self.accept_alert_if_present()
        if self._isPageTitlePresent(self._TITLE_TXT_FOR_HOMEAGE):
            pass
        else:
            super().__init__(driver, self._TITLE_XPATH)
            self._loginWM(user, pwd)
        # WMHomePage(self.driver)

    def _loginWM(self, user, pwd):
        self.fill_by_xpath(self._USER_XPATH, user)
        self.press_enter_by_xpath(self._USER_XPATH)
        self.fill_by_xpath(self._PWD_XPATH, pwd)
        self.press_enter_by_xpath(self._PWD_XPATH)
        if self.is_displayed_by_xpath(self._AFTER_LOGIN_WARNIG):
            self.click_by_xpath(self._AFTER_LOGIN_PROCEED_BTN)

    def _isPageTitlePresent(self, titleText) -> bool:
        isPageTitlePresent = False
        for i in range(0, 4):
            currTitle = self.driver.title
            if currTitle != '' and currTitle == titleText:
                isPageTitlePresent = True
                break
            else:
                self.wait_for(0.5)
        return isPageTitlePresent
