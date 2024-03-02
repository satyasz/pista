from core.ui_service import UIService


class WMBasePage(UIService):
    """Provides basic xpaths/actions for WM pages"""

    '''Signed out page'''

    _TITLE_FOR_SIGNED_OUT_PAGE = "//div[contains(text(),'You have been signed out')]"

    '''Global page header'''

    _USER_ICON = "//*[@id='button-1029-btnIconEl']"
    _SIGNOUT_BTN = "//*[@id='button-1045-btnInnerEl' and text()='Sign out']"

    '''Local page header'''

    _MAXIMIZE_PAGE_BTN = "//*[contains(@class, 'x-tool-img x-tool-maximize')]"
    _CLOSE_PAGE_BTN = "(//*[@class='x-tool-img x-tool-close'])[2]"

    def __init__(self, driver, page_title_xpath):
        self.wait_for(1)
        super().__init__(driver, page_title_xpath)

    def maximizeMenuPage(self):
        print('Page will maximize')
        # self.switch_default_content()
        self.click_by_xpath(self._MAXIMIZE_PAGE_BTN)
        print('Page maximized')

    def closeMenuPage(self):
        self.switch_default_content()
        self.click_by_xpath(self._CLOSE_PAGE_BTN)
