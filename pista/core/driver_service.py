from selenium import webdriver
from selenium.common import WebDriverException
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
from selenium.webdriver.firefox.webdriver import WebDriver as FirefoxDriver
from selenium.webdriver.edge.webdriver import WebDriver as EdgeDriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.edge.service import Service as EdgeService

from core.api_service import APIService


class Browser:
    ALL_DRIVERS = (ChromeDriver, FirefoxDriver, EdgeDriver)
    TEST_BROWSER = str()

    @classmethod
    def open_browser(cls, browser_name: str = None) -> ALL_DRIVERS:
        driver = None
        if browser_name is None:
            browser_name = cls.TEST_BROWSER
        try:
            if browser_name == 'chrome':
                '''Approach: Original one'''
                # verResp = APIService.call_get_api(api_url="https://chromedriver.storage.googleapis.com/LATEST_RELEASE")
                #
                # caps = DesiredCapabilities.CHROME
                # caps["pageLoadStrategy"] = "none"
                # options = webdriver.ChromeOptions()
                # options.add_argument('ignore-certificate-errors')
                # options.add_argument('pageLoadStrategy')
                # # driver = webdriver.Chrome(chrome_options=options)
                # # driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options, desired_capabilities=caps)
                #
                # service = ChromeService(executable_path=ChromeDriverManager(version=verResp.text).install())
                # # service = ChromeService(executable_path=ChromeDriverManager(path=r".\\Drivers").install())
                # driver = webdriver.Chrome(service=service, options=options)

                '''Approach 2: Working in server, not in VDI(Browser initiate error)'''
                # service = ChromeService()
                # options = webdriver.ChromeOptions()
                # options.add_argument('ignore-certificate-errors')
                # options.add_argument('pageLoadStrategy')
                # # options.set_capability('browserVersion', '114.0.5735')
                # options.set_capability('browserVersion', '116.0.5845')
                # driver = webdriver.Chrome(service=service, options=options)

                '''Approach 3: Manual exe files, working in server, not in VDI(Unknown publisher error)'''
                # chrome_version = '116_0_5845_96'
                # # chrome_version = '117_0_5938_11'
                # chrome_base_dir = r"C:\Users\LATITUDE\Downloads"
                # service = ChromeService(executable_path=fr"{chrome_base_dir}\chromedriver-win64-{chrome_version}\chromedriver.exe")
                # options = webdriver.ChromeOptions()
                # options.add_argument('ignore-certificate-errors')
                # options.binary_location = fr"{chrome_base_dir}\chrome-win64-{chrome_version}\chrome.exe"
                # driver = webdriver.Chrome(options=options, service=service)

                '''Approach 4: With WDM for chrome version>=116'''
                service = ChromeService(executable_path=ChromeDriverManager().install())
                options = webdriver.ChromeOptions()
                options.add_argument('ignore-certificate-errors')
                options.add_argument('pageLoadStrategy')
                driver = webdriver.Chrome(service=service, options=options)
            elif browser_name == 'firefox':
                # profile = webdriver.FirefoxProfile()
                # profile.accept_untrusted_certs = True
                # # driver = webdriver.Firefox(GeckoDriverManager().install(), firefox_profile=profile)
                # driver = webdriver.Firefox(GeckoDriverManager().install())
                
                service = FirefoxService(executable_path=GeckoDriverManager().install())
                options = webdriver.FirefoxOptions()
                options.accept_insecure_certs = True
                driver = webdriver.Firefox(service=service, options=options)
            elif browser_name == 'edge':
                service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
                options = webdriver.EdgeOptions()
                options.accept_insecure_certs = True
                driver = webdriver.Edge(service=service, options=options)
            else:
                assert False, 'No browser name mentioned'
            driver.implicitly_wait(15)
            driver.maximize_window()
        except WebDriverException as e:
            assert False, 'WebDriverException during webdriver call ' + str(e)
        except Exception as e:
            assert False, 'Exception during webdriver call ' + str(e)

        if not isinstance(driver, cls.ALL_DRIVERS):
            assert False, 'Driver instance check failed, cannot capture screenshot. Fix the script'
        return driver
