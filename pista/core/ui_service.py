import os
import threading
import time
#import pyautogui

from PIL import ImageGrab
from selenium.common import NoSuchElementException, StaleElementReferenceException, \
    NoAlertPresentException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

from core.driver_service import Browser
from core.common_service import Commons
from core.config_service import ENV_CONST
from core.log_service import Logging, printit
from root import ROOT_DIR, SCREENSHOT_DIR


class UIService:
    logger = Logging.get(__qualname__)

    _ALL_DRIVERS = Browser.ALL_DRIVERS
    MAX_WAITTIME_FOR_PAGELOAD = int(ENV_CONST.get('ui', 'pageload_waittime_in_sec'))
    MAX_WAITTIME_FOR_ELEMENT = int(ENV_CONST.get('ui', 'element_waittime_in_sec'))

    INTERVAL_WAITTIME_FOR_PAGE = 0.5
    ITERATIONS_FOR_PAGE = int(MAX_WAITTIME_FOR_PAGELOAD / INTERVAL_WAITTIME_FOR_PAGE)
    INTERVAL_WAITTIME_FOR_ELEMENT = 0.5
    ITERATIONS_FOR_ELEMENT = int(MAX_WAITTIME_FOR_ELEMENT / INTERVAL_WAITTIME_FOR_ELEMENT)

    # AJAXTRACER_PATH = os.path.join(RESOURCE_DIR, 'ajaxtracer.js')

    def __init__(self, driver: _ALL_DRIVERS = None, page_title_xpath: str = None, is_for_screenshot: bool = False):
        if is_for_screenshot:
            self.driver = driver
        else:
            if driver is None:
                _browser = Browser.TEST_BROWSER
                self.driver = Browser.open_browser(_browser)
            else:
                self.driver = driver

            self.wait_for_pageload()
            self.elementwait = WebDriverWait(self.driver, self.MAX_WAITTIME_FOR_ELEMENT, poll_frequency=1.0)
            self.driver.switch_to.default_content()

            if page_title_xpath is not None:
                page_ele = self.get_webelement(page_title_xpath)
                # self.wait_for_elementload(page_indicator_xpath)
                assert page_ele is not None, 'Page not loaded, ' + page_title_xpath
            # self.driver.switch_to.default_content()

    def open_url(self, url: str):
        self.driver.get(url)

    @staticmethod
    def wait_for(time_in_sec):
        # printit('waiting for ' + str(time_in_sec))
        time.sleep(float(time_in_sec))

    def wait_for_pageload(self):
        for i in range(0, self.ITERATIONS_FOR_PAGE):
            if self.driver.execute_script("return document.readyState") == 'complete':
                break
            else:
                self.wait_for(self.INTERVAL_WAITTIME_FOR_PAGE)

    def wait_for_elementload(self, xpath):
        ele_found = False
        for i in range(0, self.ITERATIONS_FOR_ELEMENT):
            try:
                webelements = self.driver.find_elements(By.XPATH, xpath)
                if len(webelements) > 0:
                    ele_found = True
                    break
                else:
                    self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
            except StaleElementReferenceException as e:
                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
            except Exception as e:
                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
        assert ele_found, 'Element not loaded within max waittime: ' + xpath

    def wait_for_elementvisible(self, xpath):
        self.elementwait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

    # def wait_for_display(self, xpath):
    # ele_found = False
    # webelement = self.get_webelement(xpath)
    # fwait = WebDriverWait(self, 10, poll_frequency=1,
    #                       ignored_exceptions=[NoSuchElementException, ElementNotVisibleException])
    # element = fwait.until(expected_conditions.visibility_of_element_located(webelement))
    # printit('Element in UI service ' + element + ' for xpath: ' + xpath)
    # return element
    # pass

    def refresh(self):
        self.driver.refresh()

    def _highlight_by_element(self, webelement):
        self.driver.execute_script("arguments[0].setAttribute('style', 'border: 3px solid blue');", webelement)
        time.sleep(1)
        self.driver.execute_script("arguments[0].setAttribute('style', 'border: 0px solid #FFFFFF');", webelement)

    def get_webelements(self, xpath) -> list[WebElement]:
        self.wait_for_pageload()
        webelements = self.driver.find_elements(By.XPATH, xpath)
        return webelements

    def get_webelement(self, xpath) -> WebElement:
        webelement = None
        self.wait_for(1)
        self.wait_for_pageload()
        for i in range(0, self.ITERATIONS_FOR_ELEMENT):
            try:
                webelement = self.driver.find_element(By.XPATH, xpath)
                if webelement is not None and webelement.is_displayed():
                    break
                else:
                    self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                    self.wait_for_pageload()
            except StaleElementReferenceException as e:
                printit('StaleElementReferenceException found while finding ' + xpath)
                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
            except NoSuchElementException as e:
                printit('NoSuchElementException found while finding ' + xpath)
                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
            except Exception as e:
                assert False, 'Exception found while finding ' + xpath
        return webelement

    def is_displayed_by_xpath(self, xpath) -> bool:
        webelements = self.driver.find_elements(By.XPATH, xpath)
        return len(webelements) > 0

    def is_enabled_by_xpath(self, xpath) -> bool:
        webelement = self.driver.find_element(By.XPATH, xpath)
        return webelement.is_enabled()

    def is_checked_by_xpath(self, xpath) -> bool:
        webelement = self.driver.find_element(By.XPATH, xpath)
        isChecked = self.driver.execute_script("return arguments[0].checked", webelement)
        return isChecked

    def _is_alert_present(self) -> bool:
        try:
            alert = self.driver.switch_to.alert
            isAlertPresent = True
        except NoAlertPresentException as e:
            isAlertPresent = False
        return isAlertPresent

    def accept_alert_if_present(self) -> bool:
        isAlertPresent = self._is_alert_present()
        if isAlertPresent:
            self.driver.switch_to.alert.accept()
        return isAlertPresent

    def switch_frame(self, frame_num):
        self.driver.switch_to.frame(frame_num)

    def switch_frame_by_xpath(self, xpath):
        webelement = self.get_webelement(xpath)
        self.driver.switch_to.frame(webelement)

    def scroll_to(self, xpath):
        webelement = self.get_webelement(xpath)
        self.driver.execute_script("arguments[0].scrollIntoView(false);", webelement)

    def switch_default_content(self):
        self.driver.switch_to.default_content()

    def clear_textbox_by_xpath(self, xpath):
        webelement = self.get_webelement(xpath)
        webelement.clear()

    def clear_textbox_by_value(self, xpath):
        webelement = self.get_webelement(xpath)
        value = webelement.get_attribute('value')
        for i in range(0, len(value)):
            webelement.send_keys(Keys.BACKSPACE)

    def fill_by_xpath(self, xpath, text, clearVal=False):
        self._fill_by(xpath, text, clearVal=clearVal)

    def _fill_by(self, xpath, text, isJSFill: bool = False, clearVal: bool = False):
        ele_filled = False
        try:
            for i in range(0, self.ITERATIONS_FOR_ELEMENT):
                try:
                    ele_filled = False
                    webelement = self.get_webelement(xpath)
                    if webelement is not None:
                        for j in range(0, self.ITERATIONS_FOR_ELEMENT):
                            if webelement.is_displayed():
                                self.elementwait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
                                break
                            else:
                                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                    if webelement is not None:
                        if clearVal:
                            self.clear_textbox_by_value(xpath)
                        self.driver.execute_script("arguments[0].scrollIntoView(false);", webelement)
                        if isJSFill:
                            self.driver.execute_script("arguments[0].value='" + text + "';", webelement)
                        else:
                            webelement.send_keys(text)
                        ele_filled = True
                        break
                    if webelement is None:
                        break
                except StaleElementReferenceException as e:
                    printit('StaleElementReferenceException during element fill ' + str(e))
                    self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                    self.wait_for_pageload()
                except Exception as e:
                    self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
        except Exception as e:
            assert False, 'Exception during element fill ' + str(e)
        assert ele_filled, 'Element not filled, check the script'

    def fill_file_by_xpath(self, xpath, file_path):
        self.fill_by_xpath(xpath, file_path)

    def press_enter_by_xpath(self, xpath):
        # self.wait_for_pageload()
        webelement = self.get_webelement(xpath)
        try:
            webelement.send_keys(Keys.ENTER)
        except Exception as e:
            assert False, xpath + ' exception found during enter. ' + str(e)

    def click_by_xpath(self, xpath, isJSClick: bool = None):
        self._click_by(xpath, isJSClick)

    def _click_by(self, xpath, isJSClick: bool = False):
        ele_clicked = False
        try:
            for i in range(0, self.ITERATIONS_FOR_ELEMENT):
                try:
                    ele_clicked = False
                    webelement = self.get_webelement(xpath)
                    # for webelement in webelements:
                    if webelement is not None:
                        for j in range(0, self.ITERATIONS_FOR_ELEMENT):
                            # and webelement.get_attribute('onclick') is not None
                            if webelement.is_displayed():
                                self.elementwait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                                break
                            else:
                                self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                    if webelement is not None:
                        self.driver.execute_script("arguments[0].scrollIntoView(false);", webelement)
                        if isJSClick:
                            self.driver.execute_script("arguments[0].click();", webelement)
                        else:
                            webelement.click()
                        ele_clicked = True
                        break
                    if webelement is None or not ele_clicked:
                        # self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                        break
                except StaleElementReferenceException as e:
                    printit('StaleElementReferenceException during element click ' + str(e))
                    self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                    self.wait_for_pageload()
                except Exception as e:
                    if 'element click intercepted' in str(e):
                        self.wait_for(self.INTERVAL_WAITTIME_FOR_ELEMENT)
                        self.wait_for_pageload()
                    else:
                        printit('Still exception for click', str(e))
        except Exception as e:
            assert False, 'Exception during element click ' + str(e)
        assert ele_clicked, 'Element not clicked, check the script'

    def double_click_by_xpath(self, xpath):
        # self.wait_for_pageload()
        webelement = self.get_webelement(xpath)
        try:
            action = ActionChains(self.driver)
            action.double_click(webelement).perform()
        except Exception as e:
            assert False, xpath + ' exception found during double click. ' + str(e)

    def right_click_by_xpath(self, xpath):
        # self.wait_for_pageload()
        webelement = self.get_webelement(xpath)
        try:
            action = ActionChains(self.driver)
            action.context_click(webelement).perform()
        except Exception as e:
            assert False, xpath + ' exception found during right click. ' + str(e)

    def select_in_dropdown_by_xpath(self, xpath, visibletext):
        self.wait_for(1)
        webelement = self.get_webelement(xpath)
        select = Select(webelement)
        select.select_by_visible_text(visibletext)

    def select_in_dropdown_by_value(self, xpath, value):
        self.wait_for(1)
        webelement = self.get_webelement(xpath)
        select = Select(webelement)
        select.select_by_value(value)

    def select_from_dropdown_by_xpath(self, dropdownXpath, optionXpath):
        self.click_by_xpath(dropdownXpath)
        self.click_by_xpath(optionXpath)

    def get_text_by_xpath(self, xpath) -> str:
        webelement = self.get_webelement(xpath)
        act_text = webelement.text
        printit('Element text: ' + act_text + ' for xpath: ' + xpath)
        return act_text

    def get_attr_by_xpath(self, xpath, attr) -> str:
        webelement = self.get_webelement(xpath)
        act_attr = webelement.get_attribute(attr)
        printit('Element attr: ' + act_attr + ' for xpath: ' + xpath)
        return act_attr

    def capture_screen(self, refFileName: str = ''):
        SCREEN_FILEPATH = os.path.join(SCREENSHOT_DIR, 'screen_{}_{}_{}.png')
        SYS_SCREEN_FILEPATH = os.path.join(SCREENSHOT_DIR, 'sys_screen_{}_{}_{}.png')
        thread_id = threading.current_thread().native_id
        file_suffix = Commons.build_date_forfilename()

        system_screenshot_file = SYS_SCREEN_FILEPATH.format(refFileName, thread_id, file_suffix)
        self.logger.info('System screenshot: ' + system_screenshot_file.replace(ROOT_DIR, ''))
        # try:
        #     pyautogui.screenshot(system_screenshot_file)
        # except Exception as e:
        #     self.logger.warning('Exception during system screenshot capture(pyautogui): ' + str(e))
        if True:
            try:
                screenshot = ImageGrab.grab()
                screenshot.save(system_screenshot_file, 'PNG')
            except Exception as e:
                self.logger.warning('Exception during system screenshot capture(pillow): ' + str(e))

        screenshot_file = SCREEN_FILEPATH.format(refFileName, thread_id, file_suffix)
        self.logger.info('Screenshot: ' + screenshot_file.replace(ROOT_DIR, ''))
        try:
            self.driver.save_screenshot(screenshot_file)
        except Exception as e:
            self.logger.warning('Exception found during screenshot capture: ' + str(e))
