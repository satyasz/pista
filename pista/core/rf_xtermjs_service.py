import os
import subprocess
import time
from typing import Union

from selenium.webdriver import ActionChains, Keys
from selenium.webdriver.common.by import By

from core.common_service import Commons
from core.config_service import ENV_CONST
from core.log_service import printit
from core.ui_service import UIService
from root import ROOT_DIR, OUTPUT_DIR

'''global'''
IS_NODE_STARTED = False

class RFXtermService(UIService):
    """ Project: https://github.com/xtermjs/xterm.js/
        Tools: Python 3.9.+, NodeJS v16.3.0, yarn v1.22.19,
        Visual Studio 'Desktop development with c++, Powershell
        Localhost: http://127.0.0.1:3000 """

    KEY_ENTER = Keys.ENTER
    KEY_DOWN = Keys.DOWN
    KEY_UP = Keys.UP
    KEY_CTRL_A_AcceptWarning = Keys.CONTROL + 'a'
    KEY_CTRL_A_EndTruck = Keys.CONTROL + 'a'
    KEY_CTRL_B_CloseTrailer = Keys.CONTROL + 'b'
    KEY_CTRL_C_EndScanning = Keys.CONTROL + 'c'
    KEY_CTRL_D_GoPageDown = Keys.CONTROL + 'd'
    KEY_CTRL_D_ShortPick = Keys.CONTROL + 'd'
    KEY_CTRL_E_EndIlpn = Keys.CONTROL + 'e'
    KEY_CTRL_E_EndOlpn = Keys.CONTROL + 'e'
    KEY_CTRL_E_EndPallet = Keys.CONTROL + 'e'
    KEY_CTRL_E_EndShipment = Keys.CONTROL + 'e'
    KEY_CTRL_E_EnterTask = Keys.CONTROL + 'e'
    KEY_CTRL_E_Halt = Keys.CONTROL + 'e'
    KEY_CTRL_F_SearchTran = Keys.CONTROL + 'f'
    KEY_CTRL_G_CancelTask = Keys.CONTROL + 'g'
    KEY_CTRL_K_SkipDtl = Keys.CONTROL + 'k'
    KEY_CTRL_N_EndLocnDuringCC = Keys.CONTROL + 'n'
    KEY_CTRL_S_ShortPull = Keys.CONTROL + 's'
    KEY_CTRL_S_Skip = Keys.CONTROL + 's'
    KEY_CTRL_S_TaskSel = Keys.CONTROL + 's'
    KEY_CTRL_T_ChangeTaskGrp = Keys.CONTROL + 't'
    KEY_CTRL_T_SubstituteLpn = Keys.CONTROL + 't'
    KEY_CTRL_W_GoBack = Keys.CONTROL + 'x'
    KEY_CTRL_X_ExitTran = Keys.CONTROL + 'x'
    KEY_CTRL_Y_AddDetail = Keys.CONTROL + 'y'

    _TITLE_XPATH = "//h1[contains(.,'xterm.js: A terminal')]"
    _TITLE_TAG = "//h1"

    _READ_MODE_CHECKBOX = "//input[@id='opt-screenReaderMode']"
    _ALL_LINES_IN_CONSOLE = "//div[@class='xterm-accessibility-tree']/div"
    _CONSOLE_TEXTS_IN_LINE = "//div[@class='xterm-accessibility-tree']/div[#LINE_NO#]"  # for line by line texts
    # _CANVAS_CURSOR_AREA = "//canvas[@class='xterm-cursor-layer']"
    _CANVAS_CURSOR_AREA = "//div[@class='xterm-screen']"

    XTERMJS_CODE_PATH = ENV_CONST.get('rf', 'xtermjs_code_path')
    XTERMJS_CODE_PATH = XTERMJS_CODE_PATH if XTERMJS_CODE_PATH.strip()[1] == ':' else os.path.join(ROOT_DIR, XTERMJS_CODE_PATH)
    XTERMJS_LOG_FILE = os.path.join(OUTPUT_DIR, ENV_CONST.get('rf', 'xtermjs_logfile'))
    XTERM_URL = ENV_CONST.get('rf', 'xtermjs_terminal_url')

    def __init__(self, driver, sshHost, sshUser, sshPwd, isPageOpen: bool = False):
        super().__init__(driver, None)
        if isPageOpen:
            # page_ele = self.get_webelements(self._TITLE_TAG)
            page_ele = self.driver.find_elements(By.XPATH, self._TITLE_XPATH)
            if len(page_ele) == 0:
                isPageOpen = False
        if not isPageOpen:
            # super().__init__(driver, None)
            self._invoke_terminal()
            self._connect_ssh(sshHost, sshUser, sshPwd)
        super().__init__(driver, self._TITLE_XPATH)

    @classmethod
    def _invoke_yarn(cls) -> bool:
        is_yarn_invoked = True
        proc = None
        try:
            cmd = 'yarn --trace-deprecation'
            # proc = subprocess.check_call('yarn --trace-deprecation', shell=True, cwd=cls.XTERMJS_CODE_PATH
            #                              , stdout=True, stderr=True, timeout=200.0)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd=cls.XTERMJS_CODE_PATH, stderr=True)
            log = ''
            while True:
                out = process.stdout.readline()
                log += str(out)
                if "Done in" in log:
                    printit("yarn - done")
                    break
                if "yarn install" in log and out == b'' and process.poll() is not None:
                    printit("yarn - ended")
                    break
        except subprocess.TimeoutExpired as e:
            printit(e, e.output)
        except subprocess.CalledProcessError as e:
            printit(e, e.output)
        except subprocess.SubprocessError as e:
            printit(e)
        except Exception as e:
            printit(e)

        printit('yarn - completed')
        return is_yarn_invoked

    # @classmethod
    # def _start_node(cls, xterm_code_path=XTERMJS_CODE_PATH) -> bool:
    #     global LOG
    #     is_node_started = True
    #     proc = None
    #     try:
    #         cmd = 'yarn start --trace-deprecation'
    #         # proc = subprocess.check_call('yarn start --trace-deprecation', shell=True, cwd=xterm_code_path
    #         #                              , stdout=True, stderr=True, timeout=70.0)
    #         process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd=xterm_code_path, stderr=True)
    #         log = ''
    #         while True:
    #             out = process.stdout.readline()
    #             # err = process.stderr.readline()
    #             # printit(out, err)
    #             log += str(out)
    #             LOG += log
    #             if "App listening to" in log and (("compiled" in str(out) and "successfully" in str(out))
    #                                               or ("address already in use" in str(log))):
    #                 printit("Yarn is started")
    #                 break
    #             if "App listening to" in log and out == b'' and process.poll() is not None:
    #                 printit("Cmd 'yarn start' ended")
    #                 break
    #     except subprocess.TimeoutExpired as e:
    #         printit('rfx TimeoutExpired', e, e.output)
    #     except subprocess.CalledProcessError as e:
    #         printit('rfx CalledProcessError', e, e.output)
    #     except subprocess.SubprocessError as e:
    #         printit('rfx SubprocessError', e)
    #     except Exception as e:
    #         printit('rfx Exception', e)
    #
    #     printit('Yarn start is success')
    #     return is_node_started

    @classmethod
    def _start_node(cls, xterm_code_path=XTERMJS_CODE_PATH) -> bool:
        printit(f"Xterm code path {xterm_code_path}")
        
        is_node_started = True
        proc = None
        try:
            cmd = 'npm start --trace-deprecation'
            with open(cls.XTERMJS_LOG_FILE, 'w') as f:
                process = subprocess.Popen(cmd, stdout=f, shell=True, cwd=xterm_code_path, stderr=subprocess.STDOUT)

            log = ''
            while True:
                out = process.stdout.readline()
                # err = process.stderr.readline()
                # printit(out, err)
                log += str(out)
                if ("App listening to" in log and (("compiled" in str(out) and "successfully" in str(out))
                                                   or ("address already in use" in str(log)))):
                    printit("yarn start - started")
                    break
                if "App listening to" in log and out == b'' and process.poll() is not None:
                    printit("yarn start - ended")
                    break
        except subprocess.TimeoutExpired as e:
            printit('rfx TimeoutExpired', e, e.output)
        except subprocess.CalledProcessError as e:
            printit('rfx CalledProcessError', e, e.output)
        except subprocess.SubprocessError as e:
            printit('rfx SubprocessError', e)
        except Exception as e:
            printit('rfx Exception', e)

        printit('yarn start - completed')
        return is_node_started

    def _invoke_terminal(self):
        """ Start node -> Open web terminal in localhost -> Assert terminal """
        global IS_NODE_STARTED
        if not IS_NODE_STARTED and not Commons._is_node_running():
            # self._start_node(self.XTERMJS_CODE_PATH)
            IS_NODE_STARTED = True

        is_host_connected = False
        self.open_url(self.XTERM_URL)
        # self.wait_for(5)
        self.accept_alert_if_present()

        for i in range(7):
            time.sleep(2.0)
            texts = self.readScreen()
            if texts.strip().endswith('>'):
                is_host_connected = True
                break

        # self.assertScreenTextExist(['PS', '>'])
        # texts = self.readScreen()
        # if texts.strip().endswith('>'):
        #     is_host_connected = True
        assert is_host_connected, 'Terminal is not connected'

    def _connect_ssh(self, sshHost, sshUser, sshPwd) -> str:
        # self.sendData('cls')
        # self.sendData(Keys.ENTER)
        self.sendData('ssh ' + sshUser + '@' + sshHost, isEnter=True)
        output = self.readScreen()
        if output.count('Are you sure you want to continue connecting') > 0 \
                and output.count('yes/no') > 0:
            self.sendData('yes', isEnter=True)
            output = self.readScreen()
        if output.endswith('password:'):
            self.sendData(sshPwd, isConfidential=True, isEnter=True)
        else:
            assert False, 'ssh connection unsuccessful'
        output = self.readScreen()
        return output

    def _assertWaitUntilNoBlankScreen(self):
        """Wait till blank screen exists
            Assert if blank screen is gone within waittime
            Return text displayed"""
        isBlankScreenGone = False
        all_texts = str()

        max_waittime_in_sec = 60
        interval_waittime_in_sec = 2
        total_iteration = int(max_waittime_in_sec / interval_waittime_in_sec)
        time.sleep(1.0)

        '''Enable reader mode'''
        self.driver.find_element(By.XPATH, self._READ_MODE_CHECKBOX).click()

        for it in range(total_iteration):
            '''Get lines count in terminal'''
            displayed_lines = self.driver.find_elements(By.XPATH, self._ALL_LINES_IN_CONSOLE)
            no_of_displayed_lines = len(displayed_lines)

            '''Capture texts in each line'''
            try:
                for i in range(1, no_of_displayed_lines):
                    line_text = self.driver.find_element(By.XPATH, self._CONSOLE_TEXTS_IN_LINE
                                                         .replace('#LINE_NO#', str(i))).text
                    all_texts += '\n' + line_text
                    all_texts = all_texts.replace('\n ', '')
            except Exception as e:
                printit('Exception found while reading terminal: ' + str(e))
            if all_texts.replace('\n', '').replace(' ', '').strip() != '':
                isBlankScreenGone = True
                break
            else:
                time.sleep(interval_waittime_in_sec)

        '''Disable reader mode'''
        self.driver.find_element(By.XPATH, self._READ_MODE_CHECKBOX).click()

        assert isBlankScreenGone, '<RF> RF blank screen didnt go within max waittime'
        return all_texts

    def readScreen(self) -> str:
        """Assert for no blank screen
            Return texts displayed in the terminal"""
        all_texts = self._assertWaitUntilNoBlankScreen()
        printit(all_texts)
        # printit('mmmmmmmmmmmmmmmmmmmm')
        return all_texts

    # def readScreen(self) -> str:
    #     """ To get the texts displayed in the terminal """
    #     all_texts = str()
    #     try:
    #         # self.click_by_xpath(self._READ_MODE_CHECKBOX)  # enable reader mode
    #         self.driver.find_element(By.XPATH, self._READ_MODE_CHECKBOX).click()
    #         # get lines count in terminal
    #         displayed_lines = self.driver.find_elements(By.XPATH, self._ALL_LINES_IN_CONSOLE)
    #         no_of_displayed_lines = len(displayed_lines)
    #         # capture displayed texts in each line
    #         for i in range(1, no_of_displayed_lines):
    #             line_text = self.driver.find_element(By.XPATH,
    #                                                  self._CONSOLE_TEXTS_IN_LINE.replace('#LINE_NO#', str(i))).text
    #             all_texts += '\n' + line_text
    #             all_texts = all_texts.replace('\n ', '')
    #         # self.click_by_xpath(self._READ_MODE_CHECKBOX)  # disable reader mode
    #         self.driver.find_element(By.XPATH, self._READ_MODE_CHECKBOX).click()
    #     except Exception as e:
    #         assert False, 'Exception found while reading terminal: ' + str(e)
    #     printit(all_texts)
    #     printit('~~~~~~~~~~above is rf screen~~~~~~~~~~')
    #     return all_texts

    def sendData(self, data, isConfidential: bool = False, isEnter: bool = False, isEnterIfLT20: bool = False):
        """ To input texts or keyboard keys """
        if data is not None:
            if not isConfidential:
                valToPrint = ascii(str(data)) if ascii(str(data)).startswith("'\\") else str(data)
            else:
                valToPrint = '*****'
            printit('Sending', str(valToPrint))

        try:
            # precondition: reader mode disabled
            self.driver.find_element(By.XPATH, self._CANVAS_CURSOR_AREA)
            action = ActionChains(self.driver)

            console_cursor_area = self.driver.find_element(By.XPATH, self._CANVAS_CURSOR_AREA)
            action.click(console_cursor_area).perform()

            active_ele = self.driver.switch_to.active_element
            active_ele.send_keys(data)
            self.wait_for(1)
            
            if isEnter:
                printit('Sending', 'enter')
                active_ele.send_keys(self.KEY_ENTER)
            elif isEnterIfLT20 and len(data) < 20:
                printit('Sending', 'enter')
                active_ele.send_keys(self.KEY_ENTER)
            self.wait_for(2)
        except Exception as e:
            assert False, 'Exception found while interacting terminal: ' + str(e)

    # def assertScreenTextExist(self, expectedtxt):
    #     screentxt = self.readScreen()
    #     if screentxt.count(expectedtxt) > 0:
    #         assert False, 'Terminal text didnt match, expected: ' + expectedtxt + ', actual: ' + screentxt

    def assertScreenTextExist(self, expectedTxts: Union[str, list]):
        # max_waittime_in_sec = 25
        # interval_waittime_in_sec = 2
        # total_iteration = int(max_waittime_in_sec / interval_waittime_in_sec)

        allTxtsFound = True
        missing_texts = []
        expTextsList = expectedTxts if type(expectedTxts) == list else [expectedTxts]

        screentxt = self.readScreen()

        # for i in range(0, total_iteration):
        #     allTxtsFound = True
        #     missing_text = ''
        #     screentxt = self.readScreen()
        for t in expTextsList:
            if screentxt.count(t) == 0:
                allTxtsFound = False
                missing_texts.append(t)
                # self.wait_for(interval_waittime_in_sec)
                # break
            # if allTxtsFound:
            #     break

        # if not allTxtsFound:
        #     printit('Curr screen texts:', screentxt)
        assert allTxtsFound, f"<RF> Terminal texts not found, expected: {missing_texts}, actual: {screentxt}"

    def acceptListOfMsgsIfExist(self, listOfTextsInMsgs: list[list[str]]):
        """Calls acceptMsgIfExist()
        eg: listOfTextsInMsgs = [['Exceed Max UOM', 'Location?'], ['Locn Temp', 'dedicated to a', 'diff Item']]
        """
        for i in listOfTextsInMsgs:
            self.acceptMsgIfExist(i)

    def acceptMsgIfExist(self, textsIn1Msg: list[str]):
        """Accept if all the texts of a msg exist
        eg: textsIn1Msg = ['Exceed Max UOM', 'Location?']
        """
        screentxt = self.readScreen()

        msg_found = False
        for i in textsIn1Msg:
            msg_found = True if i in screentxt else False
            if not msg_found:
                break
        if msg_found:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            
    def _loginRF(self, rfUser, rfPwd):
        if rfUser is None or rfPwd is None:
            assert False, 'RF creds not provided'
        self.assertScreenTextExist('User ID: _')
        self.sendData(rfUser)
        self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('User ID: ' + rfUser)
        self.assertScreenTextExist('Password: _')
        self.sendData(rfPwd, isConfidential=True)
        self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Choice:_')

    def readDataForTextInLine(self, textAsKeyInLine) -> str:
        final_data_from_ln = str()
        final_ln = str()
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')
        for ln in screenlns:
            if ln.count(textAsKeyInLine) > 0:
                final_ln = ln
                break
        if final_ln == '':
            assert False, 'Didnt find screen line having ' + textAsKeyInLine
        else:
            final_data_from_ln = final_ln.replace(textAsKeyInLine, '')
        if final_data_from_ln == '':
            assert False, 'Screen not showing system data'
        return final_data_from_ln

    def readDataBetweenTextInLine(self, startTxtInLine: str, endTxtInLine: str) -> str:
        final_data_from_ln = str()
        final_ln = str()
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')
        for ln in screenlns:
            if ln.count(startTxtInLine) > 0 \
                    and ln.count(endTxtInLine, ln.index(startTxtInLine) + len(startTxtInLine)) > 0:
                final_ln = ln
                break
        if final_ln == '':
            assert False, 'Didnt find line having texts: ' + startTxtInLine + ' & ' + endTxtInLine
        else:
            startLineTextEndIndex = final_ln.index(startTxtInLine) + len(startTxtInLine)
            endLineTextStartIndex = final_ln.index(endTxtInLine, startLineTextEndIndex)
            final_data_from_ln = final_ln[startLineTextEndIndex:endLineTextStartIndex]
        if final_data_from_ln == '':
            assert False, 'Screen not showing system data'
        return final_data_from_ln

    def readDataBetween2LineTexts(self, startLineTxt: str, endLineTxt: str) -> str:
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')

        final_ln_start = final_ln_end = None
        final_data_from_ln = str()

        for i in range(len(screenlns)):
            if screenlns[i].count(startLineTxt) > 0 and screenlns[i].startswith(startLineTxt):
                final_ln_start = i
                break
        if final_ln_start is not None:
            for i in range(final_ln_start, len(screenlns)):
                if screenlns[i].count(endLineTxt) > 0 and screenlns[i].startswith(endLineTxt):
                    final_ln_end = i
                    break
        if final_ln_start and final_ln_end:
            final_data_arr = screenlns[final_ln_start + 1:final_ln_end]
            final_data_from_ln = '\n'.join(final_data_arr)
            
        assert final_data_from_ln != '', 'Screen not showing system data'
        return final_data_from_ln

    def readDataBetweenLines(self, startLine: int, endLine: int) -> str:
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')
        req_lns = screenlns[startLine:endLine - 1]
        if req_lns is None or req_lns == '':
            assert False, 'Didnt find data betwee lines ' + str(startLine) + ' & ' + str(endLine)
        else:
            req_lns_str = '\n'.join(req_lns)
        return req_lns_str

    def readDataFromLine(self, lineNum: int) -> str:
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')
        final_ln_text = screenlns[lineNum - 1]
        if final_ln_text is None or final_ln_text == '':
            assert False, 'Didnt find data in line ' + str(lineNum)
        return final_ln_text
