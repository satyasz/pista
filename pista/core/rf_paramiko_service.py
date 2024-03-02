import re
import time
from typing import Union

import paramiko
from paramiko.channel import Channel
# from sshkeyboard import listen_keyboard

from core.log_service import printit


class RFParamikoService:
    """ Library to open ssh channel for RF trans """

    KEY_ENTER = '\n'
    KEY_DOWN = '\x0B'  # '\x9e[[B'  # '\x1B[B'  # '\\033[B'  # chr(0x1b) + "[B"  # r'\e[B'  # '\x1b[B'
    KEY_UP = '\x0A'  # '\x9e[[A'  # '\x1B[A'  # '\\033[A'  # chr(0x1b) + "[A"  # r'\e[A'  # '\x1b[A'
    KEY_CTRL_A_AcceptWarning = '\x01'
    KEY_CTRL_A_EndTruck = '\x01'
    KEY_CTRL_B_CloseTrailer = '\x02'
    KEY_CTRL_C_EndScanning = '\x03'
    KEY_CTRL_D_GoPageDown = '\x04'
    KEY_CTRL_D_ShortPick = '\x04'
    KEY_CTRL_E_EndIlpn = '\x05'
    KEY_CTRL_E_EndOlpn = '\x05'
    KEY_CTRL_E_EndPallet = '\x05'
    KEY_CTRL_E_EndShipment = '\x05'
    KEY_CTRL_E_EnterTask = '\x05'
    KEY_CTRL_E_Halt = '\x05'
    KEY_CTRL_F_SearchTran = '\x06'
    KEY_CTRL_G_CancelTask = '\x07'
    KEY_CTRL_K_SkipDtl = '\x0B'
    KEY_CTRL_N_EndLocnDuringCC = '\x0E'
    KEY_CTRL_S_ShortPull = '\x13'
    KEY_CTRL_S_Skip = '\x13'
    KEY_CTRL_S_TaskSel = '\x13'
    KEY_CTRL_T_ChangeTaskGrp = '\x14'
    KEY_CTRL_T_SubstituteLpn = '\x14'
    KEY_CTRL_W_GoBack = '\x17'
    KEY_CTRL_X_ExitTran = '\x18'
    KEY_CTRL_Y_AddDetail = '\x19'

    def __init__(self, sshHost, sshUser, sshPwd):
        self.output = None
        self.channel = self._connect_host(sshHost, sshUser, sshPwd)
        # ssh.close() TODO Close it when done

    def wait_for(self, time_in_sec):
        # printit('waiting for ' + str(time_in_sec))
        time.sleep(float(time_in_sec))

    def _connect_host(self, sshHost, sshUser, sshPwd):
        try:
            client = paramiko.SSHClient()  # Create an SSH client
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(sshHost, username=sshUser, password=sshPwd)  # Connect to the host
            channel = client.get_transport().open_session()  # Open a new channel
            channel.get_pty(term='VT100')  # Request a pseudo-terminal  # VT100+  # VT400
            channel.invoke_shell()  # VT100+
            # listen_keyboard(on_press=self.press(), on_release=release)
            time.sleep(5)
            if channel and not channel.closed:
                all_texts, orig_texts = self._assertWaitForNextScreen(channel)
                self.output = all_texts
                printit(all_texts)
                printit('mmmmmmmmmmmmmmmmmmmm')
            else:
                assert False, 'Ssh channel is closed while reading screen'
        except Exception as e:
            assert False, 'ssh connection unsuccessful'

        return channel

    def _connect_ssh(self, sshHost, sshUser, sshPwd):
        channel = self._connect_host(sshHost, sshUser, sshPwd)
        return channel

    def _assertWaitForNextScreen(self, channel: Channel, dataSent=''):
        """Wait till blank screen exists
        Assert if blank screen is gone within waittime
        Return text displayed
        """
        isBlankScreenGone = False
        output = str()
        optim_output = ''

        max_waittime_in_sec, interval_waittime_in_sec = 60, 2
        total_iteration = int(max_waittime_in_sec / interval_waittime_in_sec)
        time.sleep(2.0)

        # for it in range(total_iteration):
        # while not channel.recv_ready():
        #     continue

        # while True:
        #     if channel.recv_ready():
        #         output_temp = channel.recv(10240).decode()
        #         output_temp = re.sub(r'(\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))|\x08|\x1b=|\x0f)', '', output_temp)
        #         if output_temp.replace('\n', '').replace(' ', '').strip() != '':
        #             output += '\n' + output_temp
        #             isBlankScreenGone = True
        #             break
        #         else:
        #             time.sleep(interval_waittime_in_sec)

        for it in range(total_iteration):
            if channel.recv_ready():
                output = channel.recv(10240).decode('ascii')

                optim_output = re.sub(r'(\x1b(\[.*?[@-~]|\\].*?(\x07|\x1b\\))|\x08|\x1b=|\x0f)', '', output)
                optim_output = self._optimizeOutput(optim_output, dataSent)
                optim_output_temp = optim_output.replace('\n', '').replace('*', '').replace(' ', '').strip()
                if optim_output_temp != '':
                    isBlankScreenGone = True
                    break
                else:
                    time.sleep(interval_waittime_in_sec)

        if not isBlankScreenGone:
            printit('Ssh blank screen didnt go within max waittime')
            printit('output: ', output)
        assert isBlankScreenGone, 'Ssh blank screen didnt go within max waittime'
        return optim_output, output

    def _optimizeOutput(self, s, excludeStr):
        """Remove sent data from 1st line
        """
        lines = s.split('\n')
        lines = ['' if line.replace(' ', '') == '' else line for line in lines]
        first_line = lines[0].replace(excludeStr, '', 1)
        lines[0] = first_line
        return '\n'.join(lines)

    def readScreen(self):
        return self.output

    def sendData(self, data, channel: Channel = None, isConfidential=False, isEnter: bool = False, isEnterIfLT20: bool = False):
        if data is not None:
            if not isConfidential:
                valToPrint = ascii(str(data)) if ascii(str(data)).startswith("'\\") else str(data)
            else:
                valToPrint = '*****'
            printit('Sending', str(valToPrint))

        try:
            channel = channel if channel else self.channel
            channel.sendall(str(data).encode('utf-8'))
            # channel.sendall(chr(0x1b) + "[B")
            if isEnter:
                channel.sendall(self.KEY_ENTER.encode('utf-8'))
            elif isEnterIfLT20 and len(data) < 20:
                channel.sendall(self.KEY_ENTER.encode('utf-8'))
        except Exception as e:
            assert False, 'Exception found while sending data in ssh channel: ' + str(e)

        if data not in (self.KEY_UP, self.KEY_DOWN):
            channel = channel if channel else self.channel
            all_texts, orig_texts = self._assertWaitForNextScreen(channel, data)
            if all_texts != data:
                self.output = all_texts
                printit(all_texts)
                # printit('mmmmmmmmmmmmmmmmmmmm')
        else:
            printit(f"data: {data}, self.output {self.output}")

    # def press(key, channel):
    #     print(f"'{key}' pressed")
    #     channel.send(key)
    #
    # def release(key):
    #     print(f"'{key}' released")

    def assertScreenTextExist(self, expectedTxts: Union[str, list]):
        allTxtsFound = True
        missing_texts = []
        expTextsList = expectedTxts if type(expectedTxts) == list else [expectedTxts]

        screentxt = self.readScreen()
        for t in expTextsList:
            if screentxt.count(t) == 0:
                allTxtsFound = False
                missing_texts.append(t)
            else:
                printit('Found', t)
        assert allTxtsFound, f"<RF> Terminal texts not found, expected: {missing_texts}"

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
        assert rfUser and rfPwd, 'RF creds not provided'

        self.assertScreenTextExist('User ID: _')
        self.sendData(rfUser, isEnter=True)
        # self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('User ID: ' + rfUser)
        self.assertScreenTextExist('Password: _')
        self.sendData(rfPwd, isEnter=True)
        # self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Choice:_')

    def readDataForTextInLine(self, textAsKeyInLine) -> str:
        final_data_from_ln = str()
        final_ln = str()
        screentxt = self.readScreen()
        screenlns = screentxt.split('\n')
        for ln in screenlns:
            if ln.count(textAsKeyInLine) > 0:
                final_ln = ln
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
            if ln.count(startTxtInLine) > 0 and ln.count(endTxtInLine, ln.index(startTxtInLine) + len(startTxtInLine)) > 0:
                final_ln = ln
                break
        if final_ln == '':
            assert False, 'Didnt find line having texts: ' + startTxtInLine + ' & ' + endTxtInLine
        else:
            startLineTextEndIndex = final_ln.index(startTxtInLine) + len(startTxtInLine)
            endLineTextStartIndex = final_ln.index(endTxtInLine, startLineTextEndIndex)
            final_data_from_ln = final_data_from_ln[startLineTextEndIndex:endLineTextStartIndex]
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