import inspect
import re
from typing import Union

from selenium.webdriver import Keys

from apps.wms.app_db_lib import DBLib, TaskPath, LocnType
from apps.wms.app_status import DOStat, TaskHdrStat, LPNFacStat, AllocStat, TaskDtlStat
from core.config_service import RF_TRAN, ENV_CONST, ENV_CONFIG
from core.file_service import DataHandler
from core.log_service import Logging, printit
from core.rf_paramiko_service import RFParamikoService
from core.rf_xtermjs_service import RFXtermService

RF_SERVICE_PROVIDER = RFXtermService if ENV_CONST.get('rf', 'rf_service_provider') == 'xtermjs' else RFParamikoService


class RFPage(RF_SERVICE_PROVIDER):
    logger = Logging.get(__qualname__)
    # _TITLE_XPATH = "//h1[contains(.,'xterm.js: A terminal')]"

    RF_USER = None
    RF_PWD = None

    def __init__(self, driver, isRFWeighUser: bool = None, isPageOpen: bool = False):
        printit('RF_SERVICE_PROVIDER:', RF_SERVICE_PROVIDER.__name__)

        sshHost = ENV_CONFIG.get('rf', 'ssh_host')
        if isRFWeighUser:
            sshUser = ENV_CONFIG.get('rf', 'rfweigh_ssh_user')
            sshPwd = ENV_CONFIG.get('rf', 'rfweigh_ssh_pwd_encrypted')
        else:
            sshUser = ENV_CONFIG.get('rf', 'ssh_user')
            sshPwd = ENV_CONFIG.get('rf', 'ssh_pwd_encrypted')
        sshPwd = DataHandler.decrypt_it(sshPwd)

        self.sshHost = sshHost
        self.sshUser = sshUser
        self.sshPwd = sshPwd

        if RF_SERVICE_PROVIDER.__name__ == RFXtermService.__name__:
            super().__init__(driver, sshHost, sshUser, sshPwd, isPageOpen)
        elif RF_SERVICE_PROVIDER.__name__ == RFParamikoService.__name__:
            super().__init__(sshHost, sshUser, sshPwd)

    # def connect_rf(self, host=None, user=None, pwd=None) -> str:
    #     return self.connect_ssh(self.host, self.user, self.pwd)

    def loginRF(self, isRfUser2: bool = None):
        if isRfUser2:
            rfUser = ENV_CONFIG.get('rf', 'rf_user2')
            rfPwd = ENV_CONFIG.get('rf', 'rf_pwd2_encrypted')
        else:
            rfUser = ENV_CONFIG.get('rf', 'rf_user')
            rfPwd = ENV_CONFIG.get('rf', 'rf_pwd_encrypted')
        rfPwd = DataHandler.decrypt_it(rfPwd)

        self.RF_USER = rfUser
        self.RF_PWD = rfPwd
        self._loginRF(rfUser, rfPwd)

        # self.assertScreenTextExist('User ID: _')
        # self.send_data(self.user)
        # self.assertScreenTextExist('Password: _')
        # self.send_data(self.pwd)
        # self.assertScreenTextExist('Choice: _')

    def goToHomeScreen(self, isRFUser2: bool = None) -> bool:
        isItHomeScreen = False

        screentxt = self.readScreen()
        homeScreenIndicator = 'Choice:'
        if screentxt.count(homeScreenIndicator) > 0:
            isItHomeScreen = True
        elif screentxt.count('User ID: _') > 0 and screentxt.count('Password: _') > 0:
            self.loginRF(isRfUser2=isRFUser2)
            isItHomeScreen = True
        elif screentxt.endswith('>'):
            self._connect_ssh(self.sshHost, self.sshUser, self.sshPwd)
            self.loginRF(isRfUser2=isRFUser2)
            isItHomeScreen = True
        else:
            for i in range(0, 2):
                self.sendData(self.KEY_CTRL_W_GoBack)
                screentxt = self.readScreen()
                if screentxt.count(homeScreenIndicator) > 0:
                    isItHomeScreen = True
                    break
                elif screentxt.count('User ID: _') > 0 and screentxt.count('Password: _') > 0:
                    self.loginRF(isRfUser2=isRFUser2)
                    isItHomeScreen = True
                    break
        assert isItHomeScreen, 'Didnt go to RF home screen'
        return isItHomeScreen

    def goToTransaction(self, tranName, dispModule=None):
        """Ex: SORT iLPN (Inboun
        tranName = SORT iLPN, displayedModule = Inboun"""
        self.sendData(self.KEY_CTRL_F_SearchTran)
        screentxt = self.readScreen()
        if screentxt.count('Transaction Search') == 0:
            assert False, 'Transaction search screen didnt come'
        else:
            self.sendData(tranName, isEnter=True)
            screentxt = self.readScreen()
            if screentxt.count('Choice:') == 0:
                assert False, 'Transaction choice/option screen didnt come'
            else:
                dispTranName = tranName if dispModule is None else tranName + ' (' + dispModule
                optionNum = self._fetchTranOptionNumber(screentxt, dispTranName)
                if optionNum == '':
                    self.sendData(self.KEY_CTRL_D_GoPageDown)
                    screentxt = self.readScreen()
                    optionNum = self._fetchTranOptionNumber(screentxt, dispTranName)
                self.sendData(optionNum, isEnter=True)

    def _fetchTranOptionNumber(self, screenTexts, tranName) -> str:
        tran_option_num = str()
        if screenTexts == '':
            assert False, 'Screen is empty'
        screenTxtLines = screenTexts.split('\n')
        for i in screenTxtLines:
            if i.strip() != '' and str(i.strip()[0:1]).isdigit():
                i = str(i.strip()).replace(' ', ' ')
                opt_name = str(i).split(' ', 1)
                printit(opt_name)
                if str(opt_name[1]).strip().startswith(tranName):
                    tran_option_num = opt_name[0]
                    break
        assert tran_option_num != '', 'RF transaction not found ' + tranName
        return tran_option_num

    def _decideFinalTaskGrp(self, providedTaskGrp=None, forTaskId=None, forIntType: int = None):
        taskGrp = providedTaskGrp

        if forTaskId is None:
            if providedTaskGrp is None:
                taskGrp = 'ALL'
        else:
            allTaskGrps = DBLib().getAllTaskGroupFromTask(taskId=str(forTaskId), intType=forIntType)
            if providedTaskGrp is None:
                if 'ALL' in allTaskGrps:
                    taskGrp = 'ALL'
                else:
                    taskGrp = allTaskGrps[0]
            else:
                assert taskGrp in allTaskGrps, f"Provided taskGrp {taskGrp} for task {forTaskId} not found in {allTaskGrps}"

        self.logger.info(f"Task group {taskGrp}")
        return taskGrp

    def receivePalletTran(self, workGrp: str, workArea: str, asn: str, itemBrcd: str, qty: int,
                          isMatchRFLocn: bool = False, dropLocn: str = None, actLocn:str=None, resvLocn:str=None, noLocn:str=None,
                          dockDoor: str = None, blindIlpn: str = None,
                          o_asnStatus:int=None, o_po:str=None, o_intType:int=None, o_destLocnType:LocnType=None,
                          isCubiscanNeed: bool = False, palletFacStat: LPNFacStat = None, isItemForCrossDock:bool=None,
                          isNewRcv:bool=True, isCloseTrailer:bool=True):
        """Receive 1 SKU to 1 pallet iLPN from an ASN, Put to drop/actv locn
        Mandatory: Pass any of this (dropLocn, actLocn, resvLocn)
        """
        tran_name = RF_TRAN.get('rf', 'receivePallet')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        isCubiScanConfigSet = DBLib().isCubiScanWarnConfigSetForReceive()
        if dockDoor is None:
            dockDoor, dbDockDoor = DBLib().getOpenDockDoor(workGrp=workGrp, workArea=workArea)
        else:
            dockDoor, dbDockDoor = DBLib().getDockDoorName(locnBrcd=dockDoor)
        if blindIlpn is None:
            blindIlpn = DBLib().getNewILPNNum()

        if isNewRcv:
            self.goToHomeScreen()
            self.goToTransaction(tran_name, 'Inbou')
            self.assertScreenTextExist('Dock Door:')
            self.sendData(dockDoor, isEnter=True)
            self.assertScreenTextExist('ASN:')
            self.sendData(asn, isEnter=True)
            self.assertScreenTextExist('LPN:')

        self.sendData(blindIlpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        if isItemForCrossDock:
            self.assertScreenTextExist(['iLPN allocated to', 'Cross Dock'])
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        if isCubiscanNeed:
            if isCubiScanConfigSet:
                self.assertScreenTextExist('Send to Cubiscan.')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            else:
                printit("'Send To Cubiscan' warn config for receiving not set")
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)

        ''''''
        if noLocn is not None:
            self.acceptMsgIfExist(['Field cannot be', 'blank'])
            screentxt = self.readScreen()
            if screentxt.count("Invalid Barcode") > 0 and screentxt.count("prefix") > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.assertScreenTextExist('Qty:')
                self.sendData(str(qty), isEnter=True)

        if dropLocn is not None:
            self.assertScreenTextExist('Rloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('WG/WA:' + dropLocn)
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn, isEnter=True)
        elif actLocn is not None:
            self.assertScreenTextExist('Aloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('Aloc:' + actLocn)
            self.sendData(actLocn, isEnter=True)
        elif resvLocn is not None:
            self.assertScreenTextExist('Rloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('Rloc:' + resvLocn)
            self.sendData(resvLocn, isEnter=True)
        elif noLocn is not None:
            self.assertScreenTextExist('Suggested Location:')
            self.assertScreenTextExist(noLocn)
            self.sendData(noLocn, isEnter=True)
        self.assertScreenTextExist('LPN:')

        if isCloseTrailer:
            self.sendData(self.KEY_CTRL_B_CloseTrailer)
            self.assertScreenTextExist('ASN:')
            self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        currLocn = actLocn if actLocn else dropLocn if dropLocn is not None else resvLocn if resvLocn is not None else noLocn
        if palletFacStat is None:
            palletFacStat = LPNFacStat.ILPN_CONSUMED_TO_ACTV if actLocn else LPNFacStat.ILPN_ALLOCATED
        # finalAsnStatus = 30 if o_isFullAsnReceive else None
        pullLocn = dbDockDoor
        # taskGenRefNbr = blindIlpn if actLocn else None
        # taskCmplRefNbr = None if actLocn else blindIlpn
        allocStatCode = AllocStat.TASK_DETAIL_CREATED
        taskStatCode = TaskHdrStat.IN_DROP_ZONE if dropLocn else TaskHdrStat.COMPLETE
        lpnDtlQty = 0 if actLocn else qty

        if o_asnStatus is not None:
            DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
        DBLib().assertASNDtls(i_asn=asn, i_itemBrcd=itemBrcd, i_po=o_po, o_receivedQty=qty, o_dtlStatus=16)
        DBLib().assertLPNHdr(i_lpn=blindIlpn, o_facStatus=palletFacStat, o_currLocn=currLocn, o_destLocnType=o_destLocnType)
        DBLib().assertLPNDtls(i_lpn=blindIlpn, i_itemBrcd=itemBrcd, o_qty=lpnDtlQty, o_receivedQty=qty)

        '''Alloc, task validation'''
        if noLocn is None:
            DBLib().assertAllocDtls(i_cntr=blindIlpn, i_itemBrcd=itemBrcd, i_intType=o_intType, o_taskPriority=50, o_statCode=allocStatCode,
                                    o_pullLocn=pullLocn)
            DBLib().assertTaskDtls(i_cntrNbr=blindIlpn, i_itemBrcd=itemBrcd, i_intType=o_intType, o_pullLocn=pullLocn)
            taskId = DBLib().getTaskIdByORCond(taskGenRefNbr=blindIlpn, taskCmplRefNbr=blindIlpn, cntr=blindIlpn, intType=o_intType)
            DBLib().assertTaskHdr(i_task=taskId, o_intType=o_intType, o_status=taskStatCode)

        '''Pix validation 100, 617, 606'''
        if not isCubiscanNeed:
            DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindIlpn, i_tranType='617')
            final_tranType = '606' if isItemForCrossDock else '100'
            DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindIlpn, i_tranType=final_tranType)

        '''LM validation (labor_msg_id 63746374)'''
        lmRefNbr = asn  # if actLocn else blindIlpn
        DBLib().assertLaborMsgHdr(i_refNbr=lmRefNbr, i_actName='RCV PLT')  # RCV PLT
        DBLib().assertLaborMsgDtl(i_refNbr=lmRefNbr, i_actName='RCV PLT', i_lpn=blindIlpn, i_itemBrcd=itemBrcd)

        return blindIlpn, dbDockDoor

    def receiveILPNTran(self, asn: str, itemBrcd: str, qty: int, iLPN: str = None,
                        o_asnStatus: int = None, o_po: str = None, isItemNotOnASN: bool = None,
                        isRcvQtyExceedASNQty:bool=None, isRcvQtyOverideASNQty:bool=None, isInvalidBarcode:bool=None):
        """Receive 1 SKU to 1 iLPN from an ASN
        Currently isRcvQtyOverideASNQty is used by admin user
        """
        tran_name = RF_TRAN.get('rf', 'recieveILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inbound')
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        if iLPN is None:
            iLPN = DBLib().getNewILPNNum()
        self.sendData(iLPN, isEnterIfLT20=True)

        if isInvalidBarcode:
            self.assertScreenTextExist('Invalid Barcode')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        else:
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemBrcd, isEnter=True)

        if isItemNotOnASN:
            self.assertScreenTextExist('Item not for this')
            self.assertScreenTextExist('ASN')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        else:
            warningText = self.readScreen()  # TODO check why warning
            if warningText.count('Max QTY') > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

            if not isInvalidBarcode:
                self.assertScreenTextExist('Qty:')
                self.sendData(str(qty), isEnter=True)

            if isRcvQtyExceedASNQty:
                # self.assertScreenTextExist('Warn: Qty >')
                # self.assertScreenTextExist('exceeds PO qty')
                # self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.acceptMsgIfExist(['Warn: Qty >', 'exceeds PO qty'])

                self.assertScreenTextExist('Warn: Qty >')
                self.assertScreenTextExist('exceeds ASN qty')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            elif isRcvQtyOverideASNQty:
                # self.assertScreenTextExist('Overide: Qty >')
                # self.assertScreenTextExist('exceeds PO qty')
                # self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.acceptMsgIfExist(['Overide: Qty >', 'exceeds PO qty'])

                self.assertScreenTextExist('Overide: Qty >')
                self.assertScreenTextExist('exceeds ASN qty')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

                self.assertScreenTextExist('Warn: Qty >')
                self.assertScreenTextExist('exceeds ASN qty')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if not isItemNotOnASN and not isInvalidBarcode:
            DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
            DBLib().assertLPNDtls(i_lpn=iLPN, i_itemBrcd=itemBrcd, o_qty=qty, o_receivedQty=qty)

            if o_asnStatus is not None:
                DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
            DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemBrcd, o_receivedQty=qty, o_dtlStatus=16)

            DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='100')
            DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='617')

            '''LM validation (labor_msg_id 63747220)'''
            DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV ILPN CA')
            DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV ILPN CA', i_lpn=iLPN, i_itemBrcd=itemBrcd)

        return iLPN

    def receiveSortMixILPNTran(self, asn: str, itemsIn1ActvZone: list[str], qty: list[int],
                               sortZone: str, blindIlpn: str = None, blindPallet: str = None, o_po: str = None,
                               o_intType: int = None, isCubiscanNeed: bool = False, o_facStatus: LPNFacStat = None):
        """Receive multiple items(assigned to same active zone locns) from an ASN to 1 iLPN
        and sort to 1 blind pallet -> Does not end pallet
        """
        tran_name = RF_TRAN.get('rf', 'receiveSortMixILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        isCubiScanConfigSet = DBLib().isCubiScanWarnConfigSetForReceive()

        itemList = itemsIn1ActvZone if type(itemsIn1ActvZone) == list else [itemsIn1ActvZone]
        qtyList = qty if type(qty) == list else [qty]
        fetchedSortLocn = None
        defaultIlpnPerItem = []

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        for i in range(len(itemList)):
            _tempDfltIlpn = self.readDataBetween2LineTexts('LPN:', 'Item Barcode:').strip()
            defaultIlpnPerItem.append(_tempDfltIlpn)

            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemList[i], isEnter=True)

            if isCubiscanNeed:
                if isCubiScanConfigSet:
                    self.assertScreenTextExist('Send to Cubiscan.')
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                else:
                    printit("'Send To Cubiscan' warn config for receiving not set")

            self.assertScreenTextExist('Qty:')
            self.sendData(qtyList[i], isEnter=True)

            if i == 0:
                self.assertScreenTextExist('Sorting Zone:')
                self.sendData(sortZone)

            if isCubiscanNeed:
                self.assertScreenTextExist('LPN Directed to')
                self.assertScreenTextExist('reserve location')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.assertScreenTextExist('Sorting Zone:')
            else:
                self.assertScreenTextExist('iLPN:')
                self.assertScreenTextExist('Rloc:')
                if i == 0:
                    fetchedSortLocn = self.readDataForTextInLine('Rloc:').strip()
                else:
                    self.assertScreenTextExist('Rloc:' + fetchedSortLocn)
                if blindIlpn is None:
                    blindIlpn = DBLib().getNewILPNNum()
                self.sendData(blindIlpn, isEnterIfLT20=True)
                if i == 0:
                    self.assertScreenTextExist('Pallet:')
                    if blindPallet is None:
                        blindPallet = DBLib().getNewInPalletNum()
                    self.sendData(blindPallet, isEnterIfLT20=True)
                if i == 0:
                    screentxt = self.readScreen()
                    if screentxt.count("Warning!") > 0 and screentxt.count("Pallet does not") > 0:
                        self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.assertScreenTextExist('Item Barcode:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asn, o_status=30)
        for i in range(len(itemList)):
            DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemList[i], o_receivedQty=qtyList[i], o_dtlStatus=16)

        if isCubiscanNeed:
            for i in range(len(defaultIlpnPerItem)):
                DBLib().assertLPNHdr(i_lpn=defaultIlpnPerItem[i], o_facStatus=o_facStatus, o_parentLpn=blindPallet)
                DBLib().assertLPNDtls(i_lpn=defaultIlpnPerItem[i], i_itemBrcd=itemList[i], o_qty=qtyList[i], o_receivedQty=qtyList[i])
        else:
            DBLib().assertLPNHdr(i_lpn=blindIlpn, o_facStatus=o_facStatus, o_parentLpn=blindPallet)
            for i in range(len(itemList)):
                DBLib().assertLPNDtls(i_lpn=blindIlpn, i_itemBrcd=itemList[i], o_qty=qtyList[i], o_receivedQty=qtyList[i])
                DBLib().assertAllocDtls(i_cntr=blindIlpn, i_intType=o_intType, i_itemBrcd=itemList[i], o_taskPriority=50,
                                        o_pullLocn=fetchedSortLocn)
                '''Pix validation'''
                defaultLpn = DBLib().getDefaultLpnFromASNItem(asn=asn, itemBrcd=itemList[i])
                DBLib().assertPix(i_itemBrcd=itemList[i], i_caseNbr=defaultLpn, i_tranType='100')
                DBLib().assertPix(i_itemBrcd=itemList[i], i_caseNbr=defaultLpn, i_tranType='617')

        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV PLT MIX')
        for i in range(0, len(itemList)):
            DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV PLT MIX', i_itemBrcd=itemList[i])

        return blindIlpn, blindPallet, fetchedSortLocn

    def sortILPNTran(self, sortZone: str, ilpns: list[str], pallet: str = None, o_items: list[list[str]] = None,
                     o_facStatus: LPNFacStat = None, o_intType: int = None, o_isAssertTask: bool = False, isPalletInUse: bool = None,
                     isPalletClosed: bool = None):
        """Sort multiple iLPNs to 1 blind pallet in 1 sort locn
        """
        tran_name = RF_TRAN.get('rf', 'sortILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        sortLocn = ''
        ilpnList = ilpns

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Sorting Zone:')
        self.sendData(sortZone)
        for i in ilpnList:
            self.assertScreenTextExist('iLPN:')
            self.sendData(i, isEnterIfLT20=True)
            self.assertScreenTextExist('Pallet:')
            self.assertScreenTextExist('Rloc:')
            sortLocn = self.readDataForTextInLine('Rloc:').strip()
            if pallet is None:
                pallet = DBLib().getNewInPalletNum()
            self.sendData(pallet, isEnterIfLT20=True)
            if isPalletInUse:
                self.assertScreenTextExist('Pallet currently')
                self.assertScreenTextExist('in use')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            if isPalletClosed:
                self.assertScreenTextExist('Pallet is already')
                self.assertScreenTextExist('closed!')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            screentxt = self.readScreen()
            if screentxt.count("Warning!") > 0 and screentxt.count("Pallet does not") > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(ilpnList)):
            final_parentLpn = None if isPalletInUse or isPalletClosed else pallet
            final_currLocn = None if isPalletInUse or isPalletClosed else sortLocn
            final_lpnFacStat = LPNFacStat.ILPN_ALLOCATED if o_facStatus is None else o_facStatus

            DBLib().assertLPNHdr(i_lpn=ilpnList[i], o_facStatus=final_lpnFacStat, o_parentLpn=final_parentLpn, o_currLocn=final_currLocn)

            for j in range(len(o_items[i])):
                DBLib().assertAllocDtls(i_cntr=ilpnList[i], i_or_taskRefNbr=ilpnList[i], i_intType=o_intType,
                                        i_itemBrcd=o_items[i][j], o_taskPriority=50, o_pullLocn=final_currLocn)
                if o_isAssertTask:
                    DBLib().assertTaskDtls(i_cntrNbr=ilpnList[i], i_itemBrcd=o_items[i][j], i_intType=o_intType,
                                           o_pullLocn=final_currLocn, o_statCode=TaskDtlStat.UNASSIGNED)
            if o_isAssertTask:
                DBLib().assertTaskHdr(i_taskGenRefNbr=ilpnList[i], o_intType=o_intType)

        '''LM validation'''
        if not (isPalletInUse or isPalletClosed):
            for i in range(len(ilpnList)):
                DBLib().assertLaborMsgHdr(i_refNbr=ilpnList[i], i_actName='SORT LPN CA')
                DBLib().assertLaborMsgDtl(i_refNbr=ilpnList[i], i_actName='SORT LPN CA', i_lpn=ilpnList[i])

        return sortLocn, pallet

    def endPalletInSortILPNTran(self, sortZone: str, pallet: str, sortLocn: str, o_lpns: list[str] = None,
                                o_items: list[list[str]] = None, o_intType: int = None, isTaskCreatedForAnyLpn: bool = None):
        """In sorting zone, end the pallet
        """
        tran_name = RF_TRAN.get('rf', 'sortILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Sorting Zone:')
        self.sendData(sortZone)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_E_EndPallet)
        self.assertScreenTextExist('Container:')
        self.sendData(pallet, isEnterIfLT20=True)
        self.assertScreenTextExist('Info')
        self.assertScreenTextExist('Putaway Task')
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=pallet)
        for i in range(len(o_lpns)):
            for j in range(len(o_items[i])):
                DBLib().assertTaskDtls(i_cntrNbr=o_lpns[i], i_itemBrcd=o_items[i][j], i_intType=o_intType,
                                       o_pullLocn=sortLocn, o_statCode=TaskDtlStat.UNASSIGNED)

            if isTaskCreatedForAnyLpn is None:
                DBLib().assertTaskHdr(i_cntr=o_lpns[i], o_intType=o_intType)

        '''LM validation'''
        # DBLib().assertLaborMsgHdr(i_refNbr=pallet, i_actName='SORT END PLT')

    def _changeTaskGroupByCtrl(self, taskGroup='ALL'):
        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_T_ChangeTaskGrp)
        self.assertScreenTextExist('Update Task Group')
        self.assertScreenTextExist('Task Group:')
        self.sendData(taskGroup)
        if len(taskGroup) < 3:
            self.sendData(self.KEY_ENTER)
        self.sendData(self.KEY_ENTER)
        self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Choice:')

    def fetchTaskSeqNumber(self, screenText, taskNum) -> str:
        seqNum = str()
        screenTxtLines = screenText.split('\n')
        for i in screenTxtLines:
            task_num = str(i).split('\t')
            printit(task_num)
            if task_num[1] == taskNum:
                seqNum = task_num[0]
                break
        return seqNum

    def replenFromResvToActvByCtrlTask(self, taskId, fromIlpn: list[str],
                                       itemToPull: list[list[str]], qtyToPull: list[list[str]],
                                       toActvLocn: list[list[str]], blindPallet: str = None,
                                       isReplenFromWave: bool = None, waveNbr: str = None, isReplenFromLT:bool=None, taskGrp: str = None):
        """Pull all iLPNs(from all task dtls) to 1 blind pallet, Fill all iLPNs in diff active locn"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        # fromResvLocnlist = fromResvLocn if type(fromResvLocn) == list else [fromResvLocn]
        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]
        # actvLocnList = actvLocn if type(actvLocn) == list else [actvLocn]

        if taskId is None:
            if isReplenFromWave:
                taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
            elif isReplenFromLT:
                taskId = DBLib().getTaskIdFromTaskDtl(cntrNbr=fromIlpn[0], isLTRTask=True)
        assert taskId is not None, 'Task not found for replen execution'

        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(taskId, isEnter=True)
        self.assertScreenTextExist('Pallet:')
        if blindPallet is None:
            blindPallet = DBLib().getNewInPalletNum()
        self.sendData(blindPallet, isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            self.assertScreenTextExist(str(taskId))
            self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))
            self.sendData(fromIlpnList[i], isEnter=True)

        for i in range(len(toActvLocnList)):
            for j in range(len(toActvLocnList[i])):
                self.assertScreenTextExist('Fill Actv')
                self.assertScreenTextExist(['Aloc:' + toActvLocnList[i][j], 'Quantity:'])
                self.assertScreenTextExist('Aloc:')
                # self.readDataBetweenTextInLine('Quantity: ', 'Unit')
                # toActLocn = self.readDataForTextInLine('Aloc: ')
                self.sendData(toActvLocnList[i][j], isEnter=True)

        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(0, len(fromIlpnList)):
            DBLib().assertLPNHdr(i_lpn=fromIlpnList[i], o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV)
            for j in range(0, len(itemFromIlpnList[i])):
                DBLib().assertAllocDtls(i_taskGenRefNbr=waveNbr, i_intType=1, i_itemBrcd=itemFromIlpnList[i][j],
                                        o_qtyAlloc=int(qtyForItemList[i][j]), o_statCode=AllocStat.TASK_DETAIL_CREATED)
                DBLib().assertTaskHdr(i_task=taskId, o_status=TaskHdrStat.COMPLETE)

        return blindPallet

    def replenFromDropToActvByCtrlTask(self, taskId: str, pallet: str,
                                       fromIlpn: list[str], itemToFill: list[list[str]], qtyToFill: list[list[int]],
                                       toActvLocn: list[list[str]], taskGrp: str = None):
        """Replen 1 pallet with 1 or more Lpns from drop to multiple active locns """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToFill if type(itemToFill) == list else [itemToFill]
        qtyForItemList = qtyToFill if type(qtyToFill) == list else [qtyToFill]
        actvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]
        if pallet is None:
            pallet = DBLib().getNewInPalletNum()

        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Task:' + str(taskId))
        self.assertScreenTextExist(pallet)
        self.assertScreenTextExist('Container:')
        self.sendData(pallet, isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Aloc:' + actvLocnList[i][j])
                self.assertScreenTextExist(itemFromIlpnList[i][j])
                self.assertScreenTextExist(str(qtyForItemList[i][j]))
                self.sendData(actvLocnList[i][j], isEnter=True)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=TaskHdrStat.COMPLETE)
        for i in range(len(fromIlpnList)):
            DBLib().assertLPNHdr(i_lpn=fromIlpnList[i], o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV)

    def replenFromResvToDropByCtrlTask(self, fromResvLocn: str, fromIlpn: list[str], itemToPull: list[list[str]], qtyToPull: list[list[int]],
                                       isReplenFromWave: bool = None, waveNbr: str = None, isReplenFromLT:bool = None,
                                       toDropLocn: str = None, taskGrp: str = None, isToWrongDrop:bool=None):
        """pull 1 or more lpns in a pallet and drops in a drop location"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        blindPallet = DBLib().getNewInPalletNum()
        taskId = None
        if isReplenFromWave:
            taskId = str(DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr))
        elif isReplenFromLT:
            taskId = str(DBLib().getTaskIdFromTaskDtl(cntrNbr=fromIlpn[0], isLTRTask=True))
        assert taskId is not None, 'Task not found for replen execution'

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]

        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(taskId, isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(blindPallet, isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + taskId)
                self.assertScreenTextExist(['Item:' + itemFromIlpnList[i][j], 'Qty:' + str(qtyForItemList[i][j])])
                self.assertScreenTextExist('iLPN:\n' + fromIlpnList[i])
                self.sendData(fromIlpnList[i], isEnter=True)
        self.assertScreenTextExist('Task#:' + taskId)
        self.assertScreenTextExist(blindPallet)
        if toDropLocn is None:
            self.assertScreenTextExist('Rloc:')
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            if isToWrongDrop:
                toDropLocn = DBLib().getLocnByWGWA(workGrp=workG, ignoreWorkArea=[workA])
            else:
                toDropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
        self.sendData(toDropLocn, isEnter=True)

        if isToWrongDrop:
            self.assertScreenTextExist(['Location Not In', 'Path For Task!'])
        else:
            self.assertScreenTextExist('Task#:')

        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if not isToWrongDrop:
            for i in range(len(fromIlpnList)):
                DBLib().assertLPNHdr(i_lpn=fromIlpnList[i], o_facStatus=LPNFacStat.ILPN_ALLOCATED_AND_PULLED,
                                     o_prevLocn=fromResvLocn, o_currLocn=toDropLocn)

        return blindPallet, taskId

    def replenFromDropToNextDropByCtrlTask(self, taskId:str, taskGrp:str, iLpns:list[str], pallet:str, prevResvLocn:str, nextDropLocn:str):
        """Replen 1 pallet with 1 or more Lpns from one drop to next drop
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')
        
        currDropLocn = None
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Task:' + str(taskId))
        self.assertScreenTextExist(pallet)
        self.assertScreenTextExist('Rloc:'+str(prevResvLocn))
        self.assertScreenTextExist('Container:')
        self.sendData(pallet, isEnterIfLT20=True)
        if nextDropLocn is not None:
            self.assertScreenTextExist('Task#:' + str(taskId))
            self.assertScreenTextExist(pallet)
            self.assertScreenTextExist('Rloc:')
            self.assertScreenTextExist('WG/WA:' + nextDropLocn)
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            nextDropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            currDropLocn = nextDropLocn
            self.sendData(nextDropLocn, isEnter=True)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=TaskHdrStat.IN_DROP_ZONE)
        for i in range(len(iLpns)):
            DBLib().assertLPNHdr(i_lpn=iLpns[i], o_parentLpn=pallet, o_prevLocn=prevResvLocn, o_currLocn=currDropLocn)

        return currDropLocn
    
    def executeReplenTask(self, fromResvLocn: list[str], fromIlpn: list[str], itemToPull: list[list[str]],
                          qtyToPull: list[list[int]],
                          toActvLocn: list[list[str]] = None, toDropLocn: str = None,
                          willTaskComplete: bool = None, isReplenFromWave: bool = None, waveNbr: str = None,
                          isPartialPull: bool = None, isCompleteShortPull: bool = None,
                          isSubstituteLpn: bool = None, substituteLpn: str = None,
                          isHaltAfterLpnPull: bool = None, isHaltAfterQtyPull: bool = None,
                          isAssertNewTask: bool = None, lpnsForNewTask: list[str] = None,
                          isAssertCycleCnt: bool = None, o_taskPriority: int = None, isCancelReplenTask: bool = None,
                          taskGrp: str = None):
        """Pull 1 or more lpns from resv to drop/actv"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]

        originalILpns = []
        finalIlpns = []
        pallet = DBLib().getNewInPalletNum()
        blindILPN = DBLib().getNewILPNNum()
        taskId = None
        if isReplenFromWave:
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
        assert taskId is not None, 'Task not found for replen execution'

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + str(taskId))
                self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))

                if isCancelReplenTask:
                    self.sendData(self.KEY_CTRL_G_CancelTask)
                    self.assertScreenTextExist('will be canceled.')
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                    finalIlpns.append(fromIlpnList[i])

                if isCompleteShortPull:
                    self.sendData(self.KEY_CTRL_S_ShortPull)

                if isSubstituteLpn is True:
                    self.sendData(self.KEY_CTRL_T_SubstituteLpn)
                    self.assertScreenTextExist('iLPN:')
                    self.sendData(str(substituteLpn), isEnter=True)
                    originalILpns.append(fromIlpnList[i])
                    finalIlpns.append(substituteLpn)
                elif isHaltAfterQtyPull:
                    self.sendData(self.KEY_CTRL_E_Halt)
                elif isCancelReplenTask is None and isCompleteShortPull is None:
                    self.sendData(str(fromIlpnList[i]), isEnter=True)
                    originalILpns.append(fromIlpnList[i])
                    finalIlpns.append(fromIlpnList[i])

                if isPartialPull or isHaltAfterQtyPull:
                    self.assertScreenTextExist('Qty:')
                    self.sendData(str(qtyForItemList[i][j]))
                    self.assertScreenTextExist('iLPN:')

                if isPartialPull:
                    self.sendData(str(blindILPN))
                if isHaltAfterLpnPull:
                    self.sendData(self.KEY_CTRL_E_Halt)

        screentxt = self.readScreen()
        if screentxt.count('Aloc:') > 0:
            for i in range(len(toActvLocnList)):
                for j in range(len(toActvLocnList[i])):
                    self.assertScreenTextExist('Fill Actv')
                    self.sendData(toActvLocnList[i][j], isEnter=True)
        elif screentxt.count('Rloc:') > 0 and screentxt.count('WG/WA:') > 0:
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        taskHdrStatus = TaskHdrStat.CANCELLED if isCompleteShortPull or isCancelReplenTask else TaskHdrStat.COMPLETE if willTaskComplete else TaskHdrStat.RELEASED
        DBLib().assertTaskHdr(i_task=taskId, o_status=taskHdrStatus)

        lpnFacStatus = LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY if isCompleteShortPull else LPNFacStat.ILPN_CONSUMED_TO_ACTV if toActvLocn else LPNFacStat.ILPN_PUTAWAY if isCancelReplenTask else LPNFacStat.ILPN_ALLOCATED
        taskDtlStatus = TaskDtlStat.TASK_COMPLETE if toActvLocn else TaskDtlStat.DELETED if isCancelReplenTask else TaskDtlStat.RELEASED
        taskGenRefNbr = waveNbr if waveNbr is not None else None

        for i in range(len(finalIlpns)):
            DBLib().assertLPNHdr(i_lpn=finalIlpns[i], o_facStatus=lpnFacStatus)
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=finalIlpns[i], o_statCode=taskDtlStatus)

            for j in range(len(itemFromIlpnList[i])):
                DBLib().assertAllocDtls(i_itemBrcd=itemFromIlpnList[i][j], i_cntr=finalIlpns[i],
                                        i_taskGenRefNbr=taskGenRefNbr, i_intType=1,
                                        o_taskPriority=o_taskPriority, o_statCode=AllocStat.TASK_DETAIL_CREATED)
        if isAssertNewTask:
            newTaskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr, i_ignoreTaskId=taskId)
            for i in range(len(lpnsForNewTask)):
                DBLib().assertTaskHdr(i_task=newTaskId, i_currTaskPrty=50, i_taskGenRefNbr=waveNbr,
                                      i_cntr=lpnsForNewTask[i], o_status=TaskHdrStat.RELEASED)

        if isAssertCycleCnt:
            for i in range(len(fromResvLocn)):
                locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=fromResvLocn[i])
                DBLib().assertTaskHdr(i_taskGenRefNbr=locnId, o_intType=101, o_status=TaskHdrStat.COMPLETE)
                # DBLib().assertCycleCountStatus(i_locnBrcd=fromResvLocn[i], i_intType=101, o_statCode=10)

        return taskId

    def executeReplenTaskWithSubstitute(self, fromResvLocn: list[str], fromIlpn: list[str], itemToPull: list[list[str]],
                                        qtyToPull: list[list[int]], toActvLocn: list[list[str]] = None,
                                        toDropLocn: str = None, willTaskComplete: bool = None,
                                        isReplenFromWave: bool = None, waveNbr: str = None,
                                        isSubstituteLpn: bool = None, substituteLpn: str = None, o_taskPriority: int = None, taskGrp: str = None):
        """Pull the substituted lpn from resv to drop/actv"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]

        originalILpns = []
        finalIlpns = []
        pallet = DBLib().getNewInPalletNum()
        blindILPN = DBLib().getNewILPNNum()
        taskId = None
        if isReplenFromWave:
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
        assert taskId is not None, 'Task not found for replen execution'

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + str(taskId))
                self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))

                if isSubstituteLpn:
                    self.sendData(self.KEY_CTRL_T_SubstituteLpn)
                    self.assertScreenTextExist('iLPN:')
                    self.sendData(str(substituteLpn), isEnter=True)
                    originalILpns.append(fromIlpnList[i])
                    finalIlpns.append(substituteLpn)

        screentxt = self.readScreen()
        if screentxt.count('Aloc:') > 0:
            for i in range(len(toActvLocnList)):
                for j in range(len(toActvLocnList[i])):
                    self.assertScreenTextExist('Fill Actv')
                    self.sendData(toActvLocnList[i][j], isEnter=True)

        elif screentxt.count('Rloc:') > 0 and screentxt.count('WG/WA:') > 0:
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        taskHdrStatus = TaskHdrStat.COMPLETE if willTaskComplete else TaskHdrStat.RELEASED
        DBLib().assertTaskHdr(i_task=taskId, o_status=taskHdrStatus)

        lpnFacStatus = LPNFacStat.ILPN_CONSUMED_TO_ACTV if toActvLocn else LPNFacStat.ILPN_ALLOCATED
        taskDtlStatus = TaskDtlStat.TASK_COMPLETE if toActvLocn else TaskDtlStat.RELEASED
        taskGenRefNbr = waveNbr if waveNbr is not None else None

        for i in range(len(finalIlpns)):
            DBLib().assertLPNHdr(i_lpn=finalIlpns[i], o_facStatus=lpnFacStatus)
            DBLib().assertLPNHdr(i_lpn=originalILpns[i], o_facStatus=LPNFacStat.ILPN_PUTAWAY)
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=finalIlpns[i], o_statCode=taskDtlStatus)
            for j in range(len(itemFromIlpnList[i])):
                DBLib().assertAllocDtls(i_itemBrcd=itemFromIlpnList[i][j], i_cntr=finalIlpns[i],
                                        i_taskGenRefNbr=taskGenRefNbr,
                                        i_intType=1,
                                        o_taskPriority=o_taskPriority, o_statCode=AllocStat.TASK_DETAIL_CREATED)

        return taskId

    def executeReplenTaskWithHalt(self, fromResvLocn: list[str], fromIlpn: list[str], itemToPull: list[list[str]],
                                  qtyToPull: list[list[int]],
                                  toActvLocn: list[list[str]] = None, toDropLocn: str = None,
                                  willTaskComplete: bool = None, isReplenFromWave: bool = None, waveNbr: str = None,
                                  isHaltAfterLpnPull: bool = None, isHaltAfterQtyPull: bool = None,
                                  isAssertNewTask: bool = None, lpnsForNewTask: list[str] = None,
                                  o_taskPriority: int = None, taskGrp: str = None):
        """Halt the lpns from resv to drop/actv"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]

        pallet = DBLib().getNewInPalletNum()
        # blindILPN = DBLib().getNewILPNNum()
        taskId = None
        if isReplenFromWave:
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
        assert taskId is not None, 'Task not found for replen execution'

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + str(taskId))
                self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))

                if isHaltAfterQtyPull:
                    self.sendData(self.KEY_CTRL_E_Halt)
                    self.assertScreenTextExist('Qty:')
                    self.sendData(str(qtyForItemList[i][j]))
                    self.assertScreenTextExist('iLPN:')
                    # self.sendData(str(blindILPN))
                if isHaltAfterLpnPull:
                    self.sendData(str(fromIlpnList[i]), isEnter=True)
                    self.sendData(self.KEY_CTRL_E_Halt)

        screentxt = self.readScreen()
        if screentxt.count('Aloc:') > 0:
            for i in range(len(toActvLocnList)):
                for j in range(len(toActvLocnList[i])):
                    self.assertScreenTextExist('Fill Actv')
                    self.sendData(toActvLocnList[i][j], isEnter=True)
        elif screentxt.count('Rloc:') > 0 and screentxt.count('WG/WA:') > 0:
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        taskHdrStatus = TaskHdrStat.COMPLETE if willTaskComplete else TaskHdrStat.RELEASED
        DBLib().assertTaskHdr(i_task=taskId, o_status=taskHdrStatus)

        taskDtlStatus = TaskDtlStat.TASK_COMPLETE if toActvLocn else TaskDtlStat.RELEASED
        taskGenRefNbr = waveNbr if waveNbr is not None else None

        for i in range(len(fromIlpnList)):
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=fromIlpnList[i], o_statCode=taskDtlStatus,
                                   o_taskPriority=o_taskPriority)

            for j in range(len(itemFromIlpnList[i])):
                DBLib().assertAllocDtls(i_itemBrcd=itemFromIlpnList[i][j], i_cntr=fromIlpnList[i],
                                        i_taskGenRefNbr=taskGenRefNbr, i_intType=1,
                                        o_taskPriority=o_taskPriority, o_statCode=AllocStat.TASK_DETAIL_CREATED)
        if isAssertNewTask:
            newTaskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr, i_ignoreTaskId=taskId)
            for i in range(len(lpnsForNewTask)):
                DBLib().assertTaskHdr(i_task=newTaskId, i_currTaskPrty=50, i_taskGenRefNbr=waveNbr,
                                      i_cntr=lpnsForNewTask[i],
                                      o_intType=1, o_status=TaskHdrStat.RELEASED)

        return taskId

    def executeReplenTaskWithCancel(self, fromResvLocn: list[str], fromIlpn: list[str], itemToPull: list[list[str]],
                                    qtyToPull: list[list[int]],
                                    toActvLocn: list[list[str]] = None, toDropLocn: str = None,
                                    isReplenFromWave: bool = None, waveNbr: str = None,
                                    isCancelReplenTask: bool = None, o_taskStatus: TaskHdrStat = None, taskGrp: str = None):
        """cancel replen task from resv to drop/actv"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]

        pallet = DBLib().getNewInPalletNum()
        # blindILPN = DBLib().getNewILPNNum()
        taskId = None
        if isReplenFromWave:
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
        assert taskId is not None, 'Task not found for replen execution'

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + str(taskId))
                self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))

                if isCancelReplenTask:
                    self.sendData(self.KEY_CTRL_G_CancelTask)
                    self.assertScreenTextExist('will be canceled.')
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)

        screentxt = self.readScreen()
        if screentxt.count('Aloc:') > 0:
            for i in range(len(toActvLocnList)):
                for j in range(len(toActvLocnList[i])):
                    self.assertScreenTextExist('Fill Actv')
                    self.sendData(toActvLocnList[i][j], isEnter=True)
        elif screentxt.count('Rloc:') > 0 and screentxt.count('WG/WA:') > 0:
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=o_taskStatus)
        taskGenRefNbr = waveNbr if waveNbr is not None else None

        for i in range(len(fromIlpnList)):
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=fromIlpnList[i], o_statCode=o_taskStatus)

        return taskId

    def executeReplenTaskWithShortPull(self, fromResvLocn: list[str], fromIlpn: list[str], itemToPull: list[list[str]],
                                       qtyToPull: list[list[int]],
                                       toActvLocn: list[list[str]] = None, toDropLocn: str = None,
                                       isReplenFromWave: bool = None, waveNbr: str = None,
                                       isPartialPull: bool = None, isCompleteShortPull: bool = None,
                                       isAssertCycleCnt: bool = None, taskGrp: str = None):
        """short pull lpn during replen"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        fromIlpnList = fromIlpn if type(fromIlpn) == list else [fromIlpn]
        itemFromIlpnList = itemToPull if type(itemToPull) == list else [itemToPull]
        qtyForItemList = qtyToPull if type(qtyToPull) == list else [qtyToPull]
        toActvLocnList = toActvLocn if type(toActvLocn) == list else [toActvLocn]

        pallet = DBLib().getNewInPalletNum()
        blindILPN = DBLib().getNewILPNNum()
        taskId = None
        if isReplenFromWave:
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr)
        assert taskId is not None, 'Task not found for replen execution'

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)
        for i in range(len(fromIlpnList)):
            for j in range(len(itemFromIlpnList[i])):
                self.assertScreenTextExist('Task:' + str(taskId))
                self.assertScreenTextExist('iLPN:\n' + str(fromIlpnList[i]))
                if isCompleteShortPull:
                    self.sendData(self.KEY_CTRL_S_ShortPull)
                    self.assertScreenTextExist('Warning! Task')
                    self.assertScreenTextExist('will be canceled')
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)

                if isPartialPull:
                    self.assertScreenTextExist('Qty:')
                    self.sendData(str(qtyForItemList[i][j]))
                    self.assertScreenTextExist('iLPN:')
                    self.sendData(str(blindILPN))

        screentxt = self.readScreen()
        if screentxt.count('Aloc:') > 0:
            for i in range(len(toActvLocnList)):
                for j in range(len(toActvLocnList[i])):
                    self.assertScreenTextExist('Fill Actv')
                    self.sendData(toActvLocnList[i][j], isEnter=True)
        elif screentxt.count('Rloc:') > 0 and screentxt.count('WG/WA:') > 0:
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        taskHdrStatus = TaskHdrStat.CANCELLED if isCompleteShortPull else TaskHdrStat.RELEASED
        DBLib().assertTaskHdr(i_task=taskId, o_status=taskHdrStatus)

        lpnFacStatus = LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY if isCompleteShortPull else LPNFacStat.ILPN_ALLOCATED
        taskGenRefNbr = waveNbr if waveNbr is not None else None

        for i in range(len(fromIlpnList)):
            DBLib().assertLPNHdr(i_lpn=fromIlpnList[i], o_facStatus=lpnFacStatus)

        if isAssertCycleCnt:
            ccINTType = 100 if isCompleteShortPull else 101
            for i in range(len(fromResvLocn)):
                locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=fromResvLocn[i])
                DBLib().assertTaskHdr(i_taskGenRefNbr=locnId, o_intType=ccINTType, o_status=TaskHdrStat.RELEASED)
                # DBLib().assertCycleCountStatus(i_locnBrcd=fromResvLocn[i], i_intType=101, o_statCode=10)

        return taskId

    def executeReplenTaskFromResvToDropOrActvByLpn(self, fromResvLocn: str, fromIlpn: str,
                                                   itemToPull: str, qtyToPull: int, waveNbr: str = None,
                                                   isToDropLocn: bool = None, toActvLocn: str = None,
                                                   isLpnPartiallyAllocated: bool = None):
        """Execute Ctrl+E by lpn,pull 1 lpn/partial lpn in a pallet and drops in a drop location / fill the actv locn
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        blindIlpn = toDropLocn = None
        blindPallet = DBLib().getNewInPalletNum()
        if isLpnPartiallyAllocated:
            blindIlpn = DBLib().getNewILPNNum()
        taskId = str(DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr))

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('LPN:')
        self.sendData(fromIlpn, isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(blindPallet, isEnterIfLT20=True)
        self.assertScreenTextExist(taskId)
        self.assertScreenTextExist(['Item:' + itemToPull, 'Qty:' + str(qtyToPull)])
        self.assertScreenTextExist('iLPN:\n' + fromIlpn)
        self.sendData(fromIlpn, isEnter=True)
        if isLpnPartiallyAllocated:
            self.assertScreenTextExist(taskId)
            self.assertScreenTextExist(['Item:' + itemToPull, 'Qty:' + str(qtyToPull)])
            self.sendData(str(qtyToPull), isEnter=True)
            self.assertScreenTextExist('iLPN:')
            self.sendData(blindIlpn, isEnter=True)
        if isToDropLocn:
            self.assertScreenTextExist('Rloc:')
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            toDropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(toDropLocn, isEnter=True)
        elif toActvLocn is not None:
            self.assertScreenTextExist('Aloc:' + str(toActvLocn))
            self.sendData(toActvLocn, isEnter=True)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        finalCurrLocn = toDropLocn if isToDropLocn else toActvLocn
        finalLpn = blindIlpn if isLpnPartiallyAllocated else fromIlpn
        finalTaskStatus = TaskHdrStat.IN_DROP_ZONE if isToDropLocn else TaskHdrStat.COMPLETE
        DBLib().assertTaskHdr(i_task=taskId, i_cntr=finalLpn, o_status=finalTaskStatus)
        finalLpnFacStat = LPNFacStat.ILPN_CONSUMED_TO_ACTV if toActvLocn is not None else LPNFacStat.ILPN_ALLOCATED_AND_PULLED
        DBLib().assertLPNHdr(i_lpn=finalLpn, o_facStatus=finalLpnFacStat, o_prevLocn=fromResvLocn, o_currLocn=finalCurrLocn)

        return finalLpn

    def receiveFullASNTran(self,asn:str, o_ilpn:str, o_item:str, o_qty:int, o_po:str, dockDoor:str=None):
        """"""
        tran_name = RF_TRAN.get('rf', 'receiveFullASN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if dockDoor is None:
            dockDoor, dbDockDoor = DBLib().getOpenDockDoor(workGrp='RECV', workArea='REC1')

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Dock Door:')
        self.sendData(dockDoor, isEnter=True)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist(['ASN submitted for', 'Recieving'])
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('ASN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=o_ilpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_asn=asn)
        DBLib().assertLPNDtls(i_lpn=o_ilpn, i_itemBrcd=o_item, o_qty=o_qty, o_receivedQty=o_qty)
        DBLib().assertASNHdr(i_asn=asn, o_status=30)
        DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=o_item, o_dtlStatus=16, o_shippedQty=o_qty, o_receivedQty=o_qty)
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=o_ilpn, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=o_item, i_caseNbr=o_ilpn, i_tranType='617')
        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV FULL ASN')
        DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV FULL ASN', i_lpn=o_ilpn, i_itemBrcd=o_item)

    def packILPNFromActiveTran(self, aLoc: str, qty: int, iLPN: str = None, locationEmpty: str = None):
        """"""
        tran_name = RF_TRAN.get('rf', 'packILPNFromActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('iLPN:')
        if iLPN is None:
            iLPN = DBLib().getNewILPNNum()
        self.sendData(iLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('Aloc:')
        self.sendData(aLoc, isEnter=True)
        self.assertScreenTextExist('Qty Pckd:')
        self.sendData(qty, isEnter=True)

        screentxt = self.readScreen()
        if screentxt.count('Location Empty(Y/N)') > 0:
            self.sendData(locationEmpty)

        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY, o_prevLocn=aLoc)

        return iLPN

    def palletizeILPN(self, iLPN: list[str], o_item: list[list[str]], o_qty: list[list[int]], palletId: str = None):
        """Palletize multiple ilpns to 1 pallet
        """
        tran_name = RF_TRAN.get('rf', 'palletizeILpn')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Pallet:')
        if palletId is None:
            palletId = DBLib().getNewInPalletNum()
        self.sendData(palletId, isEnter=True)
        self.assertScreenTextExist('iLPN:')
        for i in range(0, len(iLPN)):
            self.sendData(iLPN[i], isEnterIfLT20=True)
            self.assertScreenTextExist('Previous iLPN:')
            self.assertScreenTextExist(iLPN[i])
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)
        # self.sendData(Keys.CONTROL + 'o')  # To close the pallet
        # self.assertScreenTextExist('xiLPN not alloc to  x')  # asserting the error text

        '''Validation'''
        for i in range(len(iLPN)):
            DBLib().assertLPNHdr(i_lpn=iLPN[i], o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY,
                                 o_parentLpn=palletId)
            for j in range(0, len(o_item[i])):
                DBLib().assertLPNDtls(i_lpn=iLPN[i], i_itemBrcd=o_item[i][j], o_qty=o_qty[i][j],
                                      o_receivedQty=o_qty[i][j])

        return palletId

    def splitMoveILPN(self, fromILPN: str, qty: int, toILPN: str = None, o_item: str = None):
        tran_name = RF_TRAN.get('rf', 'splitMoveILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN :')
        self.sendData(fromILPN, isEnter=True)
        self.assertScreenTextExist('Move Qty:')
        self.sendData(qty, isEnter=True)
        self.assertScreenTextExist('Move To LPN :')
        if toILPN is None:
            toILPN = DBLib().getNewILPNNum()
        self.sendData(toILPN, isEnter=True)
        self.assertScreenTextExist('Move Qty:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        '''From ilpn'''
        DBLib().assertLPNHdr(i_lpn=fromILPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        DBLib().assertLPNDtls(i_lpn=fromILPN, i_itemBrcd=o_item, o_qty=qty)
        '''To ilpn'''
        DBLib().assertLPNHdr(i_lpn=toILPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        DBLib().assertLPNDtls(i_lpn=toILPN, i_itemBrcd=o_item, o_qty=qty)

        return toILPN

    def splitCombineOLPN(self, fromOLpns: list[str], toOLpn: str, items: list[list[str]] = None,
                         qtys: list[list[int]] = None, toOLpnFacStat: LPNFacStat = None):
        """Split/combine the olpns"""
        tran_name = RF_TRAN.get('rf', 'splitCombineOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(len(fromOLpns)):
            self.assertScreenTextExist('From oLPN#:')
            self.sendData(fromOLpns[i])
            self.assertScreenTextExist('To oLPN #:')
            self.sendData(toOLpn)
            for j in range(len(items[i])):
                self.assertScreenTextExist('Item Barcode:')
                self.sendData(items[i][j], isEnter=True)
                self.assertScreenTextExist('Qty:')
                self.sendData(qtys[i][j], isEnter=True)
            self.sendData(self.KEY_CTRL_E_EndOlpn)
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=toOLpn, o_facStatus=toOLpnFacStat)
        for i in range(len(fromOLpns)):
            DBLib().assertLPNHdr(i_lpn=fromOLpns[i], o_facStatus=LPNFacStat.OLPN_CANCELLED)
            for j in range(len(items[i])):
                DBLib().assertLPNDtls(i_lpn=toOLpn, i_itemBrcd=items[i][j], o_initialQty=qtys[i][j])

    def moveAllOLPNTran(self, fromOLpns: list[str], toOLpn: str, o_items: list[str]=None, o_qtys: list[int]=None,
                    isOLPNFromDiffDOs:bool=None, isDiffConsolLocn:bool=None,o_facStatus: LPNFacStat = None):
        tran_name = RF_TRAN.get('rf', 'moveAllOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(len(fromOLpns)):
            self.assertScreenTextExist('From oLPN#:')
            self.sendData(fromOLpns[i])
            self.assertScreenTextExist('To oLPN #:')
            self.sendData(toOLpn)
            if isOLPNFromDiffDOs:
                self.assertScreenTextExist('Container For')
                self.assertScreenTextExist('Different Order!')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            if isDiffConsolLocn:
                self.assertScreenTextExist(['Two LPNs have', 'different', 'consolidation', 'attributes'])
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if not isOLPNFromDiffDOs and not isDiffConsolLocn:
            for i in range(len(fromOLpns)):
                DBLib().assertLPNHdr(i_lpn=fromOLpns[i], o_facStatus=LPNFacStat.OLPN_CANCELLED)
            DBLib().assertLPNHdr(i_lpn=toOLpn, o_facStatus=o_facStatus, o_totalLpnQty=sum(o_qtys))
            for i in range(len(o_items)):
                finalPackedQty = o_qtys[i] if o_facStatus == LPNFacStat.OLPN_PACKED else None
                finalInitialQty = o_qtys[i] if o_facStatus == LPNFacStat.OLPN_PRINTED else None
                DBLib().assertLPNDtls(i_lpn=toOLpn, i_itemBrcd=o_items[i], o_qty=finalPackedQty, o_initialQty=finalInitialQty)

    def moveAllILPNTran(self, fromILPN:str, item:list[str], qty:list[int], toILPN:str=None,
                        isFromLpnMultiSku:bool=None, o_item:list[str]=None, o_qty:list[int]=None):
        """Move existing lpn to new lpn
        """
        tran_name = RF_TRAN.get('rf', 'moveAllILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if toILPN is None:
            toILPN = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN :')
        self.sendData(fromILPN, isEnter=True)
        self.assertScreenTextExist(fromILPN)
        self.sendData(toILPN, isEnter=True)
        warningList = ['iLPN Shipment not', 'Shipment in iLPN.', 'Different PO in']
        for i in warningList:
            warningscrntxt = self.readScreen()
            if warningscrntxt.count(i) > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                if warningscrntxt.count('LPN :') > 0:
                    break
        self.assertScreenTextExist('LPN :')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        '''From ilpn'''
        DBLib().assertLPNHdr(i_lpn=fromILPN, o_facStatus=LPNFacStat.ILPN_CONSUMED)
        if not isFromLpnMultiSku:
            DBLib().assertLPNDtls(i_lpn=fromILPN, i_itemBrcd=item[0], o_qty=0)
        '''To ilpn'''
        final_items = item if o_item is None else o_item
        final_qtys = qty if o_qty is None else o_qty
        DBLib().assertLPNHdr(i_lpn=toILPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        for i in range(len(final_items)):
            DBLib().assertLPNDtls(i_lpn=toILPN, i_itemBrcd=final_items[i], o_qty=final_qtys[i])

        return toILPN

    def shuttleAnchorPalletize(self, oLPN, palletLPN):
        tran_name = RF_TRAN.get('rf', 'shuttleAnchorPalletize')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN #:')
        self.sendData(oLPN)
        self.assertScreenTextExist('Pallet:')
        self.sendData(palletLPN)

    def unloadOLPN(self, oLPN:str, o_lpnFacStat:LPNFacStat=None):
        """"""
        tran_name = RF_TRAN.get('rf', 'unloadOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN:')
        self.sendData(oLPN)
        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=oLPN, o_facStatus=o_lpnFacStat)

    def inductLocOLPN(self, order: str, palletId: str, o_olpns: list, o_prevLocn: str, inductLocn:str=None):
        """"""
        tran_name = RF_TRAN.get('rf', 'inductLocOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs(orders=[order])[0]

        olpnList = o_olpns if type(o_olpns) == list else [o_olpns]

        if inductLocn is None:
            inductLocn, inductLocnId = DBLib().getInductLocn(zone='OB')

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN #:')
        self.sendData(palletId)
        screentxt = self.readScreen()
        if screentxt.count('oLPN is already') > 0:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Locn:')
        self.sendData(inductLocn, isEnter=True)
        self.assertScreenTextExist('oLPN #:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertDOHdr(i_order=pc_order, o_status=DOStat.STAGED)
        for i in range(len(olpnList)):
            DBLib().assertLPNHdr(i_lpn=str(olpnList[i]), o_prevLocn=o_prevLocn, o_currLocn=str(inductLocn))

        return inductLocn

    def lpnDispositionTran(self, iLPN: list[str], o_items: list[str], o_qty: list[int]):
        """For multiple lpns with 1 item each
        """
        tran_name = RF_TRAN.get('rf', 'lpnDisposition')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        resvLocn = []
        lpns = []

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(len(iLPN)):
            self.assertScreenTextExist('LPN:')
            self.sendData(iLPN[i], isEnterIfLT20=True)
            self.assertScreenTextExist(iLPN[i])
        self.sendData(self.KEY_CTRL_C_EndScanning)  # to end scanning

        it = 0
        while it < 10:
            screentxt = self.readScreen()
            if screentxt.count('LPN Disposition')==0 and screentxt.count('in progress')==0:
                break
            self.wait_for(1)
            it += 1

        for i in range(len(iLPN)):
            if len(iLPN)-1>i:
                self.assertScreenTextExist('LPN:')
                lpn = self.readDataFromLine(lineNum=3)
                assert lpn in iLPN, f"Ilpn {lpn} not found in list {iLPN}"
                lpns.append(lpn)
                self.sendData(lpn, isEnterIfLT20=True)
            if i==len(iLPN)-1:
                lpn = self.readDataBetween2LineTexts('LPN:','Rloc:').strip()
                assert lpn in iLPN, f"Ilpn {lpn} not found in list {iLPN}"
                lpns.append(lpn)
            self.assertScreenTextExist('Rloc:')
            resvLocn.append(self.readDataForTextInLine('Rloc:').strip())
            self.sendData(resvLocn[i], isEnter=True)
            
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(iLPN)):
            DBLib().assertLPNHdr(i_lpn=lpns[i], o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=resvLocn[i])
            DBLib().assertLPNDtls(i_lpn=iLPN[i], i_itemBrcd=o_items[i], o_qty=o_qty[i])

    def packOLPNFromFullILPNByCtrl(self, taskId, blindPallet, resvLocn, iLPNInResv, qty):
        """INT2: Pack 1 oLPN w/ 1 sku from full iLPN in reserve to 1 pallet"""
        # self._changeTaskGroup(taskGrp)
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:_')
        self.assertScreenTextExist('LPN:')
        self.sendData(taskId, isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(blindPallet, isEnterIfLT20=True)
        self.assertScreenTextExist('Task:' + str(taskId))
        self.assertScreenTextExist('Rloc:' + str(resvLocn))
        self.assertScreenTextExist(str(iLPNInResv))
        self.sendData(iLPNInResv, isEnterIfLT20=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(qty, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def packOLPNFromActiveTran(self, oLPN: str, fromActLocn: str, itemBrcd: str, qty: int, order: str, i_waveNum=None,
                               isShorting: bool = None, isPartialShorting:bool=None, pickQtyIfShort:int=None, isFullShorting:bool=None,
                               willLocnEmpty: bool = False, isAssertCycleCnt: bool = None, isAssertNoCycleCnt:bool=None,
                               o_doStatus: DOStat = None, o_doDtlStatus: int = None,
                               isAssertAlloc: bool = None, o_allocStatus: AllocStat = None,
                               isSkipPick: bool = None, isSkipPickUpdatesReplenTask: bool = None,
                               isAssertReplenTask: bool = None, o_replenTaskPrty: int = None,
                               isAssertNewReplenTask: bool = None, existngTaskId: str = None,
                               isSkipPickCreatesReplenTask: bool = None):
        """INT50"""
        tran_name = RF_TRAN.get('rf', 'packOLPNActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        finalPickQty = pickQtyIfShort if isPartialShorting else 0 if isFullShorting else qty
        isLocnEmptyFlag = 'Y' if willLocnEmpty is True else 'N'

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN:')
        self.sendData(oLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('Aloc:' + str(fromActLocn))
        self.assertScreenTextExist('Item:' + str(itemBrcd))
        self.assertScreenTextExist('Qty:' + str(qty))
        self.assertScreenTextExist('Item Barcode:')

        if isSkipPick:
            self.sendData(self.KEY_CTRL_S_Skip)
            if isSkipPickUpdatesReplenTask:
                self.assertScreenTextExist('Replen updated')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('Item:' + str(itemBrcd))
            self.sendData(self.KEY_CTRL_X_ExitTran)
        else:
            self.sendData(itemBrcd, isEnter=True)
            self.assertScreenTextExist('Qty:')
            if not isFullShorting:
                self.sendData(str(finalPickQty), isEnter=True)

        if isPartialShorting or isFullShorting:
            self.assertScreenTextExist('Qty:')
            self.sendData(self.KEY_CTRL_D_ShortPick)
            self.assertScreenTextExist('Unpacked Qty')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            
        screentxt = self.readScreen()
        if screentxt.count('Location Empty(Y'):
            self.sendData(isLocnEmptyFlag)
            
        if isSkipPick is None:
            self.assertScreenTextExist(['Info', 'End of oLPN!'])
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('oLPN:')
            self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        # finalDOStatus = 140 if isOLpnsPending is True or pickQtyIfShorting is not None else 150
        if o_doStatus is not None:
            DBLib().assertDOHdr(i_order=order, o_status=o_doStatus)

        if isSkipPick is None:
            final_lpnStat = LPNFacStat.OLPN_CANCELLED if isFullShorting else LPNFacStat.OLPN_PACKED
            DBLib().assertLPNHdr(i_lpn=oLPN, o_facStatus=final_lpnStat)
            DBLib().assertLPNDtls(i_lpn=oLPN, i_itemBrcd=itemBrcd, o_qty=finalPickQty)

        if isAssertAlloc:
            DBLib().assertAllocDtls(i_itemBrcd=itemBrcd, i_taskGenRefNbr=i_waveNum, i_intType=50, o_qtyPulled=finalPickQty, o_statCode=o_allocStatus)

        usrCancelQty = int(qty) - int(pickQtyIfShort) if pickQtyIfShort else qty if isFullShorting else None

        if o_doDtlStatus is not None:
            DBLib().assertDODtls(i_order=order, i_itemBrcd=itemBrcd, o_dtlStatus=o_doDtlStatus, o_usrCancldQty=usrCancelQty)

        if isPartialShorting or isFullShorting:
            DBLib().assertPickShortItemDtls(i_item=itemBrcd, i_order=order, i_lpn=oLPN, o_qty=usrCancelQty, o_statCode=0)

        if isAssertCycleCnt:
            locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=fromActLocn)
            DBLib().assertTaskHdr(i_taskGenRefNbr=locnId, o_intType=101, o_status=TaskHdrStat.RELEASED)
        elif isAssertNoCycleCnt:
            locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=fromActLocn)
            DBLib().assertNoTaskExist(i_cntrNbr=locnId, i_itemBrcd=itemBrcd, i_intType=101)

        if isAssertReplenTask:
            DBLib().assertTaskHdr(i_taskGenRefNbr=i_waveNum, o_intType=1, o_currTaskPrty=o_replenTaskPrty)

        if isAssertNewReplenTask:
            DBLib().assertTaskHdr(i_taskGenRefNbr=i_waveNum, i_ignoreTaskId=existngTaskId, o_intType=1, o_currTaskPrty=10)

        if isSkipPickCreatesReplenTask:
            taskId = DBLib().getTaskIdByItemName(itemName=itemBrcd, intType=1)
            DBLib().assertTaskHdr(i_task=taskId, i_currTaskPrty=o_replenTaskPrty, o_intType=1, o_status=TaskHdrStat.RELEASED)

    def packOLPNFromResvByActvTran(self, oLPN:str, fromResvLocn:str, itemBrcd:str, qty:int, order:str, i_waveNum:str=None, iLPN:str=None,
                                   o_doStatus: DOStat = None, o_doDtlStatus: int = None,
                                   isAssertAlloc: bool = None, o_allocStatus: AllocStat = None,
                                   isShorting:bool=None, pickQtyIfShort:int=None, isAssertCycleCnt:bool=None,
                                   isSubstituteLpn:bool=None, iLpnToBeSubstituted:str=None, isAssertSubLpnInOLpn:bool=None):
        """INT2"""
        tran_name = RF_TRAN.get('rf', 'packOLPNActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        allocatedILpn = DBLib().getAllocatedLpnFromAllocInvDtl(waveNum=i_waveNum, item=str(itemBrcd))
        finalLpn = allocatedILpn if iLPN is None else iLPN
        finalPickQty = pickQtyIfShort if isShorting else qty

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN:')
        self.sendData(oLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('Rloc:' + str(fromResvLocn))
        self.assertScreenTextExist('Item:' + str(itemBrcd))
        self.assertScreenTextExist('Qty:' + str(qty))
        self.assertScreenTextExist('iLPN:')
        if isSubstituteLpn:
            self.sendData(self.KEY_CTRL_T_SubstituteLpn)
            self.assertScreenTextExist('iLPN:')
            finalLpn = iLpnToBeSubstituted
        self.sendData(finalLpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Item:')
        self.assertScreenTextExist('Qty:')
        self.sendData(str(finalPickQty), isEnter=True)

        if isShorting:
            self.assertScreenTextExist('Qty:')
            self.sendData(self.KEY_CTRL_D_ShortPick)
            self.assertScreenTextExist('Unpacked Qty')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        screentxt = self.readScreen()
        if screentxt.count('Location Empty(Y.N):_'):
            self.sendData('Y')
        self.assertScreenTextExist(['Info', 'End of oLPN!'])
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if o_doStatus is not None:
            DBLib().assertDOHdr(i_order=order, o_status=o_doStatus)
        DBLib().assertLPNHdr(i_lpn=oLPN, o_facStatus=LPNFacStat.OLPN_PACKED)
        if isAssertAlloc:
            DBLib().assertAllocDtls(i_itemBrcd=itemBrcd, i_taskGenRefNbr=i_waveNum, i_intType=2,
                                    o_statCode=o_allocStatus)
        if isShorting:
            usrCancldQty = qty - pickQtyIfShort
            DBLib().assertDODtls(i_order=order, i_itemBrcd=itemBrcd, o_dtlStatus=o_doDtlStatus, o_usrCancldQty=usrCancldQty)
            DBLib().assertPickShortItemDtls(i_item=itemBrcd, i_order=order, i_lpn=oLPN, o_qty=usrCancldQty, o_statCode=0)

        if isAssertCycleCnt:
            locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=fromResvLocn)
            DBLib().assertTaskHdr(i_taskGenRefNbr=locnId, o_intType=100, o_status=TaskHdrStat.RELEASED)

        if isAssertSubLpnInOLpn:
            DBLib().assertLPNHdr(i_lpn=allocatedILpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY)
            DBLib().assertLPNHdr(i_lpn=iLpnToBeSubstituted, o_facStatus=LPNFacStat.ILPN_CONSUMED)
            taskId = DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=i_waveNum)
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=allocatedILpn, i_intType=2, i_pullLocn=fromResvLocn, o_statCode=TaskDtlStat.DELETED)
            DBLib().assertTaskDtls(i_task=taskId, i_cntrNbr=finalLpn, i_intType=2, i_pullLocn=fromResvLocn, o_statCode=TaskDtlStat.TASK_COMPLETE)

    def makePickCartTran(self, i_waveNum: str, oLPNs: list[str], o_taskStatus: TaskHdrStat = None, blindCartId: str = None):
        """"""

        tran_name = RF_TRAN.get('rf', 'makePickCart')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        olpnList = oLPNs if type(oLPNs) == list else [oLPNs]
        if blindCartId is None:
            blindCartId = DBLib().getNewCartNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Pick Cart #:')
        self.sendData(blindCartId, isEnter=True)
        for i in range(0, len(olpnList)):
            self.assertScreenTextExist(blindCartId)
            self.assertScreenTextExist('oLPN:')
            self.sendData(olpnList[i], isEnterIfLT20=True)
            self.assertScreenTextExist('Slot:')
            self.sendData(str(i + 1), isEnter=True)
        self.sendData(Keys.CONTROL + "e")
        self.assertScreenTextExist('Task:')
        self.assertScreenTextExist('Number of oLPNs:')
        self.assertScreenTextExist('LPN Size:')
        self.assertScreenTextExist('LPN Type:')
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertTaskHdr(i_taskCmplRefNbr=blindCartId, o_status=o_taskStatus)
        return blindCartId

    def packPickCartTran(self, order: str, waveNum: str, cartId, slots: list[str], olpns: list[str]):
        """"""
        tran_name = RF_TRAN.get('rf', 'packPickCart')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        '''Get slot and olpns as it might be in different seq'''
        slots, olpns = DBLib().getSlotAndOlpnByPickSeqFromCartNum(cartId=cartId)

        slotList = slots if type(slots) == list else [slots]
        olpnList = olpns if type(olpns) == list else [olpns]
        assert len(slotList) == len(olpnList), 'No. of slots didnt match with no. of olpns'

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'O')
        self.assertScreenTextExist('Pick Cart #:')
        self.sendData(cartId, isEnter=True)
        for i in range(len(slotList)):
            self.assertScreenTextExist('Slot:' + str(slotList[i]))
            self.assertScreenTextExist('Item:')
            self.assertScreenTextExist('Item Barcode:')
            itemBrcd = self.readDataForTextInLine('Item:').strip()
            self.sendData(itemBrcd, isEnter=True)
            self.assertScreenTextExist('Qty:')
            qty = self.readDataBetweenTextInLine('Qty:', 'Unit').strip()
            self.sendData(qty, isEnter=True)
            self.assertScreenTextExist('oLPN:')
            self.sendData(olpnList[i])

            screenText = self.readScreen()
            if screenText.count('Location Empty(Y/N)'):
                self.sendData('Y')

            self.assertScreenTextExist('End of oLPN!')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('Pick Cart #:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertDOHdr(i_order=order, o_status=DOStat.PACKED)
        DBLib().assertTaskHdr(i_taskCmplRefNbr=cartId, o_status=TaskHdrStat.COMPLETE)
        for i in range(len(olpnList)):
            DBLib().assertLPNHdr(i_lpn=olpnList[i], o_facStatus=LPNFacStat.OLPN_PACKED)

    def anchorPalletizeOLPNTran(self, olpns: list, order: str, isLastOlpnIncluded: bool, blindPallet: str = None,
                                isReAnchorSameOLpn:bool=None, isUpdateStgInd: bool = None, o_doStatus: DOStat = None):
        """Palletize all oLPNs from 1 Order going to same consol locn to 1 blind pallet
        """
        tran_name = RF_TRAN.get('rf', 'anchorPltzOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        isParentDOExist = DBLib()._isParentDOExist(order=order)
        pc_order = DBLib()._getParentDOsIfExistElseChildDOs(orders=[order])[0]

        consLocn = stagingLocn = None
        if isReAnchorSameOLpn:
            dbRows = DBLib().getStagingLocn(noOfLocn=1)
            stagingLocn = dbRows[0].get('LOCN_BRCD')

        if blindPallet is None:
            blindPallet = DBLib().getNewOutPalletNum()
        if not isReAnchorSameOLpn:
            consLocn, consLocnId = DBLib().get1ConsolLocnFromOLPNs(oLPNs=olpns)
        DBLib()._updateOLPNCntrTypeFromSCTForDO(order=pc_order, isUpdateStgInd=isUpdateStgInd)

        olpnList = olpns if type(olpns) == list else [olpns]
        # orderList = orders if type(orders) == list else [orders]
        # consLocnList = consLocn if type(consLocn) == list else [consLocn]

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(0, len(olpnList)):
            self.assertScreenTextExist('oLPN #:')
            self.sendData(olpnList[i], isEnterIfLT20=True)
            if not isReAnchorSameOLpn:
                self.assertScreenTextExist('CnLc:' + str(consLocn))
            self.assertScreenTextExist('Pallet:')
            self.sendData(blindPallet, isEnterIfLT20=True)

            if isReAnchorSameOLpn:
                self.assertScreenTextExist('Locn:')
                self.sendData(str(stagingLocn), isEnter=True)
                
            if isLastOlpnIncluded and i == len(olpnList)-1:
                self.assertScreenTextExist('Order Completed:')
                self.sendData(self.KEY_ENTER)

        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        currLocn = stagingLocn if isReAnchorSameOLpn else consLocn
        for i in range(len(olpnList)):
            # finalDOStat = 165 if isLastOlpnAvail and i == (len(olpnList) - 1) else 150
            DBLib().assertLPNHdr(i_lpn=str(olpnList[i]), o_currLocn=str(currLocn), o_parentLpn=blindPallet)
        DBLib().assertDOHdr(i_order=str(pc_order), o_status=o_doStatus)

        return blindPallet, currLocn

    def weighPalletTran(self, order:str, olpns:list, palletdID, isContainer:bool, o_doStatus:DOStat=None, hazmatItem:str=None):
        """isContainer: True for pallet, False for carton
        ssh_user: rfwpln, rf_user: PLNADMIN
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs(orders=[order])[0]

        unNumber = None
        if hazmatItem is not None:
            unNumber = DBLib().getUNNbrFromItem(itemBrcd=hazmatItem)

        rfUser = ENV_CONFIG.get('rf', 'rf_user')
        isContainerFlag = 'Y' if isContainer else 'N'
        actWeight = DBLib().getTotalEstWeightForPallet(palletId=palletdID)

        self.assertScreenTextExist('Enter User ID:')
        self.sendData(rfUser, isEnter=True)
        self.assertScreenTextExist('Scan oLPN/Pallet:')
        self.sendData(palletdID, isEnter=True)
        self.assertScreenTextExist('Enter Weight:')
        self.sendData(actWeight, isEnter=True)
        self.assertScreenTextExist('Container a Pallet?')
        self.sendData(isContainerFlag, isEnter=True)

        if hazmatItem is not None:
            self.assertScreenTextExist(unNumber)
            self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertDOHdr(i_order=pc_order, o_status=o_doStatus)  # DOStat.STAGED
        for i in range(len(olpns)):
            DBLib().assertLPNHdr(i_lpn=olpns[i], o_facStatus=LPNFacStat.OLPN_WEIGHED, isActWeightPresent=True)
            DBLib().assertNoInvLockForLpn(i_lpn=olpns[i], i_lockCode='WC')

    def loadOLPNTran(self, order: str, olpns: list, isFirstOLPNIncluded: bool = None, dockDoor: str = None,
                     isLoadPallet: bool = None, o_prevLocn: str = None, o_parentLpn: str = None,
                     o_doStatus: DOStat = None, isPartialLoad: bool = None):
        """isLoadPallet: True for 'y', False for 'n'"""

        tran_name = RF_TRAN.get('rf', 'loadOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        pc_order = DBLib()._getParentDOsIfExistElseChildDOs(orders=[order])[0]

        olpnList = olpns if type(olpns) == list else [olpns]
        loadPalletFlag = 'y' if isLoadPallet else 'n'
        trailerNum = DBLib().getNewTrailerNum()
        if dockDoor is None:
            dockDoor, systemDockDoor = DBLib().getOpenDockDoor(workGrp='RECV', workArea='REC1')

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(len(olpnList)):
            self.assertScreenTextExist('oLPN:')
            self.sendData(olpnList[i], isEnterIfLT20=True)
            self.assertScreenTextExist('Load Pallet?')
            self.sendData(loadPalletFlag)
            if isFirstOLPNIncluded and i == 0:
                self.assertScreenTextExist('Trailer:')
                self.sendData(trailerNum, isEnter=True)
                warningTxt = self.readScreen()
                if warningTxt.count('Assign Shipment') > 0:
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.assertScreenTextExist('Dock Door:')
                self.sendData(dockDoor, isEnter=True)
            else:
                self.assertScreenTextExist('Dock Door:' + str(dockDoor))
                self.sendData(dockDoor, isEnter=True)

        if isPartialLoad:
            self.assertScreenTextExist('oLPN:')
            self.sendData(self.KEY_CTRL_E_EndShipment)
            self.assertScreenTextExist(o_parentLpn)
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('UnassgnOrCancel')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            screentxt = self.readScreen()
            if screentxt.count('Shipment does not') > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        final_lpnFacStatus = LPNFacStat.OLPN_SHIPPED if isPartialLoad else LPNFacStat.OLPN_LOADED_ON_TRUCK
        final_parentLpn = None if isPartialLoad else o_parentLpn

        for i in range(len(olpnList)):
            DBLib().assertLPNHdr(i_lpn=olpns[i], o_facStatus=final_lpnFacStatus, o_parentLpn=final_parentLpn, o_prevLocn=o_prevLocn)
            # finalDOStat = 180 if isLastOLpnAvail and i == (len(olpnList) - 1) else 165
        if o_doStatus is not None:
            DBLib().assertDOHdr(i_order=pc_order, o_status=o_doStatus)

        return dockDoor

    def sysDirILPNPutawayToResvLocn(self, taskGrp, pallet, dropLocn):
        self._changeTaskGroupByCtrl(taskGrp)
        # self.send_data(Keys.CONTROL+'e')
        self.goToTransaction('Inbound')
        self.assertScreenTextExist('LPN:')
        self.sendData(pallet, isEnter=True)
        self.assertScreenTextExist('Rloc:')
        self.sendData(dropLocn, isEnter=True)

    def sysDirPutawayToDropLPNByCtrl(self, pallet: str, o_item: list[str], o_qty: list[int], taskGrp: str = None,
                                     o_currPullLocn: str = None, o_intType: int = None, rLoc: str = None):
        """Sys dir putaway 1 pallet lpn to drop locn
        Does validation"""

        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        taskGrp = self._decideFinalTaskGrp(providedTaskGrp=taskGrp)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('LPN:')
        self.sendData(pallet, isEnter=True)
        self.assertScreenTextExist('Rloc:')
        self.assertScreenTextExist('WG/WA:')
        if True:
            self.assertScreenTextExist('WG/WA:' + rLoc)
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            rLoc = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
        self.sendData(rLoc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.logger.info('Executed RF ' + inspect.currentframe().f_code.co_name + ': Ctrl E')

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=pallet, o_facStatus=LPNFacStat.ILPN_ALLOCATED, o_currLocn=rLoc)
        DBLib().assertWMInvnDtls(i_itemBrcd=o_item[0], i_locn=rLoc, i_lpn=pallet, o_onHandQty=o_qty[0], o_allocatedQty=o_qty[0])

    def sysDirPutawayToResvLPNByCtrl(self, pallet: str, o_item: list[str], o_qty: list[int],
                                     o_currPullLocn: str, o_intType: int = None, rLoc: str = None, taskGrp: str = None):
        """Sys dir putaway 1 pallet lpn to 1 resv
        Does validation"""

        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        palletList = [pallet]
        assert len(palletList) == len(o_item) == len(o_qty), 'Item/Qty for lpn missing'
        onHandQty = []

        taskGrp = self._decideFinalTaskGrp(providedTaskGrp=taskGrp)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        for i in range(len(palletList)):
            self.assertScreenTextExist('LPN:')
            self.sendData(palletList[i], isEnterIfLT20=True)
            self.assertScreenTextExist('Rloc:')
            if rLoc is not None:
                self.assertScreenTextExist('Rloc:' + rLoc)
            else:
                rLoc = self.readDataForTextInLine('Rloc:').strip()
            for j in range(len(o_item)):
                onHandQty.append(DBLib().getWMOnHandQty(itemBrcd=o_item[j], locnBrcd=rLoc))
            self.sendData(rLoc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.logger.info('Executed RF ' + inspect.currentframe().f_code.co_name + ': Ctrl E')

        '''Validation'''
        for i in range(len(palletList)):
            DBLib().assertLPNHdr(i_lpn=palletList[i], o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=rLoc)
            DBLib().assertAllocDtls(i_cntr=palletList[i], i_taskCmplRefNbr=palletList[i], i_itemBrcd=o_item[i], i_intType=o_intType,
                                    o_destLocn=rLoc)
            DBLib().assertTaskHdr(i_taskCmplRefNbr=palletList[i], o_intType=o_intType, o_status=TaskHdrStat.COMPLETE)
            DBLib().assertTaskDtls(i_cntrNbr=palletList[i], i_itemBrcd=o_item[i], i_intType=o_intType, o_pullLocn=o_currPullLocn)
            DBLib().assertWMInvnDtls(i_itemBrcd=o_item[i], i_locn=rLoc, i_lpn=palletList[i], o_onHandQty=o_qty[i])
            # LM validation (labor_msg_id )
            # DBLib().assertLaborMsgHdr(i_refNbr=palletList[i], i_actName='PTWY RSV CA')
            # DBLib().assertLaborMsgDtl(i_refNbr=palletList[i], i_actName='PTWY RSV CA', i_lpn=palletList[i],
            #                           i_itemBrcd=o_item[i])

    def sysDirPutawayToActvLPNByCtrl(self, pallet: str, o_item: list[str], o_qty: list[int],
                                     o_currPullLocn: str, o_intType: int = None, aLoc: str = None, taskGrp: str = None):
        """Sys dir putaway 1 pallet lpn to 1 actv locn
        Does validation"""

        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        palletList = pallet if type(pallet) == list else [pallet]
        onHandQty = DBLib().getWMOnHandQty(itemBrcd=o_item[0], locnBrcd=aLoc)

        taskGrp = self._decideFinalTaskGrp(providedTaskGrp=taskGrp)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        for i in range(len(palletList)):
            self.assertScreenTextExist('LPN:')
            self.sendData(palletList[i], isEnter=True)
            self.assertScreenTextExist('Aloc:' + aLoc)
            # aloc = self.readDataFromLine(3).split(":")[1].strip()
            self.sendData(aLoc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.logger.info('Executed RF ' + inspect.currentframe().f_code.co_name + ': Ctrl E')

        '''Validation'''
        for i in range(len(palletList)):
            DBLib().assertLPNHdr(i_lpn=palletList[i], o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV, o_currLocn=aLoc)
            DBLib().assertAllocDtls(i_cntr=palletList[i], i_taskGenRefNbr=palletList[i], i_itemBrcd=o_item[i],
                                    i_intType=o_intType, o_destLocn=aLoc)
            taskId = DBLib().getTaskIdByORCond(taskGenRefNbr=palletList[i], taskCmplRefNbr=palletList[i], cntr=palletList[i],
                                               intType=o_intType)
            DBLib().assertTaskDtls(i_task=taskId, i_intType=o_intType, o_pullLocn=o_currPullLocn)
            DBLib().assertTaskHdr(i_task=taskId, o_intType=o_intType, o_status=TaskHdrStat.COMPLETE)

            DBLib().assertWMInvnDtls(i_itemBrcd=o_item[i], i_locn=aLoc, o_onHandQty=int(onHandQty) + o_qty[0])
            # LM validation (labor_msg_id )
            # DBLib().assertLaborMsgHdr(i_refNbr=palletList[i], i_actName='PTWY RSV CA')
            # DBLib().assertLaborMsgDtl(i_refNbr=palletList[i], i_actName='PTWY RSV CA', i_lpn=palletList[i],
            #                           i_itemBrcd=o_item[i])

    def sysDirPutawayToActvLPNsInPalletByCtrl(self, palletID, iLPN, actvloc):
        """"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')
        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#')
        self.assertScreenTextExist('LPN:')
        self.sendData(palletID, isEnterIfLT20=True)
        self.assertScreenTextExist('Aloc:' + actvloc)
        self.sendData(actvloc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def sysDirPutawayToResvLPNsInPalletByCtrl(self, palletID, iLPN, Rloc):
        """"""
        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#')
        self.assertScreenTextExist('LPN:')
        self.sendData(palletID, isEnterIfLT20=True)
        self.assertScreenTextExist('Rloc:')
        self.sendData(Rloc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def sysDirPutawayToDropLPNsInPalletByCtrl(self, palletID: str, dropLocn: str,
                                              lpns: list[str], items: list[str], qtys: list[int], taskGrp: str = None):
        """"""
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        assert len(items) == len(qtys), 'No.of items and qtys didnt match'

        taskGrp = self._decideFinalTaskGrp(providedTaskGrp=taskGrp)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#')
        self.assertScreenTextExist('LPN:')
        self.sendData(palletID, isEnterIfLT20=True)
        self.assertScreenTextExist('Rloc:')
        self.sendData(dropLocn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(items)):
            DBLib().assertLPNHdr(i_lpn=lpns[i], o_facStatus=LPNFacStat.ILPN_ALLOCATED, o_currLocn=dropLocn)
            DBLib().assertLPNDtls(i_lpn=lpns[i], i_itemBrcd=items[i], o_qty=qtys[i], o_receivedQty=qtys[i])
            DBLib().assertWMInvnDtls(i_itemBrcd=items[i], i_locn=dropLocn, i_lpn=lpns[i])

    def userDirPutawayToResvTran(self, ilpn: str, o_items: list[str] = None, resvLocn: str = None, resvWG: str = 'RESV',
                                 taskPath: TaskPath = None, isResvWAFromTPathCurrWA: bool = None,
                                 isLocnWithTBF: bool = None, o_lpnLock: str = None, o_facStat: LPNFacStat = None,
                                 isPixCreated: bool = None, o_pixTranType: list[str] = None):
        """Usr dir putaway 1 iLPN to reserve locn
        """
        tran_name = RF_TRAN.get('rf', 'userDirPutawayResv')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if resvLocn is None:
            resvLocnRow = DBLib().getEmptyManualResvLocn(noOfLocn=1, resvWG=resvWG, taskPath=taskPath,
                                                         isResvWAInTPDCurrWA=isResvWAFromTPathCurrWA)
            resvLocn = resvLocnRow[0].get('LOCN_BRCD')

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN:')
        self.sendData(ilpn, isEnterIfLT20=True)

        if isLocnWithTBF:
            self.assertScreenTextExist(['iLPN', 'already allocated'])
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        else:
            self.acceptMsgIfExist(['iLPN Already', 'Allocated!'])
            self.acceptMsgIfExist(['Pallet is open', 'continue?'])
            self.acceptMsgIfExist(['iLPN', 'already allocated', 'for INT Repl'])

            self.assertScreenTextExist('Rloc:')
            self.sendData(resvLocn, isEnter=True)

            self.acceptMsgIfExist(['Exceed Max UOM', 'Location?'])
            self.acceptMsgIfExist(['Locn Temp', 'dedicated to a', 'diff Item'])

            self.assertScreenTextExist('LPN:')
            self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        final_currLocn = None if isLocnWithTBF else resvLocn
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=o_facStat, o_currLocn=final_currLocn)

        if not isLocnWithTBF:
            for i in range(len(o_items)):
                DBLib().assertWMInvnDtls(i_locn=resvLocn, i_itemBrcd=o_items[i], i_lpn=ilpn)
                if isPixCreated:
                    for j in range(len(o_pixTranType)):
                        DBLib().assertPix(i_caseNbr=ilpn, i_tranType=o_pixTranType[j], i_itemBrcd=o_items[i])

            if o_lpnLock is not None:
                DBLib().assertLpnLockPresent(i_lpn=ilpn, i_lockCode=o_lpnLock)
                for i in range(len(o_items)):
                    DBLib().assertPix(i_caseNbr=ilpn, i_tranType='606', i_itemBrcd=o_items[i])

        return resvLocn

    # def sysDirFillActive(self, pallet):
    #     tran_name = RF_TRAN.get('rf', 'SysDFillActive')
    #     self.goToTransaction(tran_name)
    #     self.assertScreenTextExist('LPN:')
    #     self.sendData(pallet)
    #     self.sendData(self.KEY_ENTER)
    #     self.assertScreenTextExist('Aloc:')
    #     Aloc = self.readDataFromLine(2).split(":")[1].strip()
    #     self.sendData(Aloc)
    #     self.sendData(self.KEY_ENTER)

    def fillActiveTran(self, lpn: str, aLoc: str, item: str, qty: int, isWrongActv: bool = None, o_lpnFacStat: LPNFacStat = None):
        """Sys dir fill active 1 lpn with 1 sku to 1 actv locn
        """
        tran_name = RF_TRAN.get('rf', 'fillActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        onHandQty = None
        if not isWrongActv:
            onHandQty = DBLib().getWMOnHandQty(itemBrcd=item, locnBrcd=aLoc)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inbo')

        self.assertScreenTextExist('LPN:')
        self.sendData(lpn, isEnterIfLT20=True)

        if isWrongActv:
            self.assertScreenTextExist('Aloc:')
        else:
            self.assertScreenTextExist('Aloc:' + aLoc)
        self.sendData(aLoc, isEnter=True)

        if isWrongActv:
            self.assertScreenTextExist("Wrong Locn")
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('Aloc:')
            self.sendData(self.KEY_CTRL_X_ExitTran)
            self.assertScreenTextExist("Not End Trk/iLPN")
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        else:
            self.assertScreenTextExist('LPN:')
            self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if isWrongActv:
            DBLib().assertLPNHdr(i_lpn=lpn, o_facStatus=o_lpnFacStat)
            DBLib().assertAllocDtls(i_cntr=lpn, i_itemBrcd=item, o_statCode=AllocStat.CANCELLED)
        else:
            DBLib().assertLPNHdr(i_lpn=lpn, o_facStatus=o_lpnFacStat, o_currLocn=aLoc)
            DBLib().assertWMInvnDtls(i_itemBrcd=item, i_locn=aLoc, o_onHandQty=int(onHandQty) + qty)

            '''LM validation'''
            DBLib().assertLaborMsgHdr(i_refNbr=lpn, i_actName='FILL ACTIVE CA')
            DBLib().assertLaborMsgDtl(i_refNbr=lpn, i_actName='FILL ACTIVE CA', i_lpn=lpn, i_itemBrcd=item)

    def fillActiveTranWithMultiLpn(self, palletLPN: str, lpns: list[str], o_itemList: list[list[str]], o_qtyList: list[list[int]],
                                   aLocList: list[list[str]]):
        """Sys dir fill active multi lpn with multi sku to Multi actv locn
        """
        tran_name = RF_TRAN.get('rf', 'fillActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        onHandQty = []  # Type of o_qtyList

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inbo')

        self.assertScreenTextExist('LPN:')
        self.sendData(palletLPN, isEnterIfLT20=True)

        for i in range(len(lpns)):

            temp_onHandQty = []
            for j in range(len(aLocList[i])):
                temp_onHandQty.append(DBLib().getWMOnHandQty(itemBrcd=o_itemList[i][j], locnBrcd=aLocList[i][j]))

                screentxt = self.readScreen()
                if screentxt.count('Pallet is open') and screentxt.count('continue?'):
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)

                self.assertScreenTextExist('Aloc:')
                self.sendData(aLocList[i][j], isEnter=True)
                self.assertScreenTextExist('LPN :')
                self.sendData(lpns[i], isEnterIfLT20=True)

            onHandQty.append(temp_onHandQty)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(lpns)):
            for j in range(len(o_itemList[i])):
                DBLib().assertLPNHdr(i_lpn=lpns[i], o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV, o_currLocn=aLocList[i][j])
                DBLib().assertWMInvnDtls(i_itemBrcd=o_itemList[i][j], i_locn=aLocList[i][j], o_onHandQty=onHandQty[i][j] + o_qtyList[i][j])

        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=palletLPN, i_actName='FILL ACTIVE CA')
        for i in range(len(lpns)):
            for j in range(len(o_itemList[i])):
                DBLib().assertLaborMsgDtl(i_refNbr=palletLPN, i_actName='FILL ACTIVE CA', i_lpn=lpns[i], i_itemBrcd=o_itemList[i][j])

    def userDirFillActiveTran(self, lpn: str, aLoc: str, item: str, qty: int):
        """Usr dir fill active 1 lpn with 1 sku to 1 actv locn
        """
        tran_name = RF_TRAN.get('rf', 'usrDirFillActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN:')
        self.sendData(lpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Item:' + item)
        self.assertScreenTextExist('Quantity:')
        self.sendData(qty, isEnter=True)
        self.assertScreenTextExist('Aloc:')
        self.sendData(aLoc, isEnter=True)
        screentxt = self.readScreen()
        if screentxt.count("Max dynamic") > 0 and screentxt.count("Active locns") > 0:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=lpn, o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV, o_currLocn=aLoc)
        DBLib().assertWMInvnDtls(i_itemBrcd=item, i_locn=aLoc, o_onHandQty=qty)

        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=lpn, i_actName='FILL ACTIVE CA')
        DBLib().assertLaborMsgDtl(i_refNbr=lpn, i_actName='FILL ACTIVE CA', i_lpn=lpn, i_itemBrcd=item)

    def sysDirPutawayByFillActvCartTran(self, lpn: list[str], aLoc: list[str]):
        """Sys dir putaway: multiple lpns to same/diff actv locns
        Does validation"""

        tran_name = RF_TRAN.get('rf', 'fillActiveCart')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        lpnList = lpn if type(lpn) == list else [lpn]
        aLocList = aLoc if type(aLoc) == list else [aLoc]
        assert len(lpnList) == len(aLocList), 'No. of lpn and actv locn didnt match'

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(len(lpnList)):
            self.assertScreenTextExist('LPN:')
            self.sendData(lpnList[i], isEnterIfLT20=True)
            warningList = ['No cap. in locn', 'Qty exceeds the']
            for j in warningList:
                screentxt = self.readScreen()
                if screentxt.count(j) > 0:
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                    if screentxt.count('LPN:') > 0:
                        break
            if i == len(lpnList) - 1:
                self.sendData(self.KEY_CTRL_A_EndTruck)
                for k in range(len(aLocList)):
                    self.assertScreenTextExist('Aloc:' + aLocList[k])
                    self.sendData(aLocList[k], isEnter=True)
                    self.assertScreenTextExist(lpnList[k])
                    self.assertScreenTextExist('LPN:')
                    self.sendData(lpnList[k], isEnterIfLT20=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(lpnList)):
            DBLib().assertLPNHdr(i_lpn=lpnList[i], o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV, o_currLocn=aLocList[i])

            '''LM validate LM'''
            DBLib().assertLaborMsgDtl(i_refNbr=lpnList[0], i_actName='FILLACT CART CA', i_lpn=lpnList[i])
        DBLib().assertLaborMsgHdr(i_refNbr=lpnList[0], i_actName='FILLACT CART CA')

    def sysDirFillActiveCart(self, pallet):
        tran_name = RF_TRAN.get('rf', 'fillActiveCart')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN:')
        self.sendData(pallet, isEnter=True)
        self.assertScreenTextExist('Aloc:')
        Aloc = self.readDataFromLine(2).split(":")[1].strip()
        self.sendData(Aloc, isEnter=True)
        self.assertScreenTextExist('LPN:')
        lpn = self.readDataFromLine(2).split(":")[1].strip()
        self.sendData(lpn, isEnterIfLT20=True)

    def sysDirPutawayByFillActvMixBoxTran(self, iLPN: str, item: list[str], qty: list[int],
                                          actLocnForItem: list[str] = None, o_currPullLocn: str = None):
        """Sys dir putaway: 1 iLPN with multi sku, going to same/different actv locns (INT1)
        Does validation
        """
        tran_name = RF_TRAN.get('rf', 'fillActiveMixBox')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        itemList = item if type(item) == list else [item]
        qtyList = qty if type(qty) == list else [qty]
        actLocnListForItem = [] if actLocnForItem is None else actLocnForItem

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        for i in range(0, len(itemList)):
            screentxt = self.readScreen()
            if 'LPN:' in screentxt:
                # self.assertScreenTextExist('LPN:')
                self.sendData(iLPN, isEnterIfLT20=True)
            # if isLpnHasUnallocLock:
            #     self.assertScreenTextExist('Unallocable Lock')
            #     self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('Aloc:')
            if len(actLocnListForItem) > 0:
                self.assertScreenTextExist('Aloc:' + str(actLocnListForItem[i]))
            else:
                aloc = self.readDataForTextInLine('Aloc:').strip()
                actLocnListForItem.append(aloc)
            self.assertScreenTextExist(['Item:' + str(itemList[i]), 'Quantity:' + str(qtyList[i]) + ' '])
            self.sendData(actLocnForItem[i], isEnter=True)
            self.assertScreenTextExist(['Quantity:' + str(qtyList[i]) + ' ', 'Quantity:'])
            self.sendData(qtyList[i], isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV)
        # TODO below
        # for i in range(0, len(itemList)):
        #     DBLib().assertWMInvnDtls(i_itemBrcd=itemList[i], i_locn=actLocnListForItem[i], o_onHandQty=int(onHandQty) + qty)
        # LM validation (labor_msg_id 63747405)
        DBLib().assertLaborMsgHdr(i_refNbr=iLPN, i_actName='FILL ACT MIX CA')
        DBLib().assertLaborMsgDtl(i_refNbr=iLPN, i_actName='FILL ACT MIX CA', i_lpn=iLPN)

    def loadTrailer(self, oLPN, loadPalletY, trailerNum, dockDoor):
        tran_name = RF_TRAN.get('rf', 'loadTrailer')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LOAD oLPN:')
        self.sendData(oLPN)
        self.assertScreenTextExist('Load Pallet:')
        self.sendData(loadPalletY)
        self.assertScreenTextExist('Trailer :')
        self.sendData(trailerNum)
        self.assertScreenTextExist('Assign Shipment to Dock/Door')
        self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Dock Door')
        self.sendData(dockDoor)

    def closeTrailerTran(self, dockDoor: str, orders: list[str], olpns: list[list[str]],
                         missingContainer: list[str] = None,
                         shipment: str = None, numOfStops: int = None, isBOLGenerated: bool = None):
        """"""
        tran_name = RF_TRAN.get('rf', 'closeTrailer')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if shipment is None:
            pc_orders = DBLib()._getParentDOsIfExistElseChildDOs(orders=orders)
            shipment = DBLib().get1ShipmentNumFromDOs(orders=pc_orders)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Dock Door:')
        self.sendData(dockDoor, isEnter=True)
        screentxt = self.readScreen()
        if screentxt.count('Shipment does not') and screentxt.count('have Ship Via'):
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        if missingContainer:
            self.assertScreenTextExist('Missing Container')
            for i in missingContainer:
                self.assertScreenTextExist(i)
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
            self.assertScreenTextExist('UnassgnOrCancel')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('Next Trailer Request')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(orders)):
            order = orders[i]
            pc_order = DBLib()._getParentDOsIfExistElseChildDOs([order])[0]

            DBLib().assertDOHdr(i_order=pc_order, o_status=DOStat.SHIPPED)
            DBLib().assertShipConfirmXmlMsgExist(order=order)
            for j in range(len(olpns[i])):
                DBLib().assertLPNHdr(i_lpn=olpns[i][j], o_facStatus=LPNFacStat.OLPN_SHIPPED, isBOLGenerated=isBOLGenerated)

        if shipment is not None:
            DBLib().assertShipmentStatus(i_shipment=shipment, o_status=80, o_noOfStops=numOfStops)

    def consumeILPN(self, iLpn: str, reasonCode: str, items: list[str], isAssertNoLockForILPN:bool=None, o_lockCode:str=None):
        """Consume 1 ilpn"""
        #
        tran_name = RF_TRAN.get('rf', 'consumeILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('LPN:')
        self.sendData(iLpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Reference')
        self.sendData('Test', isEnter=True)
        self.assertScreenTextExist('Reason Code :')
        self.sendData(reasonCode)
        if len(reasonCode) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLpn, o_facStatus=LPNFacStat.ILPN_CONSUMED)
        for i in range(len(items)):
            # DBLib().assertWMInvnDtls(i_itemBrcd=str(items[i]), i_lpn=iLpn)
            # DBLib().assertLPNDtls(i_lpn=iLpn, i_itemBrcd=str(items[i]))
            DBLib().assertPix(i_itemBrcd=str(items[i]), i_caseNbr=iLpn, i_tranType='300')
        if isAssertNoLockForILPN:
            DBLib().assertNoInvLockForLpn(i_lpn=iLpn, i_lockCode=o_lockCode)

    def consumeILPN08Tran(self, iLpn: str, items: list[str], reasonCode: str = '08'):
        """Consume 1 ilpn using tran 08
        """
        tran_name = RF_TRAN.get('rf', 'consumeILPN08')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('LPN:')
        self.sendData(iLpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Reference')
        self.sendData('Test', isEnter=True)
        self.assertScreenTextExist('Reason Code :')
        self.sendData(reasonCode)
        if len(reasonCode) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLpn, o_facStatus=LPNFacStat.ILPN_CONSUMED)
        DBLib().assertNoWMInvRecForLpn(i_lpn=iLpn)
        for i in range(len(items)):
            DBLib().assertPix(i_itemBrcd=str(items[i]), i_caseNbr=iLpn, i_tranType='300', i_tranCode='01', i_invnAdjType='S')

    def ilpnInquiryTran(self, iLpn: str, items: list[str], qty: list[int], locn: str, facStatDesc: str):
        """Ilpn inquiry
        """
        tran_name = RF_TRAN.get('rf', 'ilpnInquiry')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('iLPN:')
        self.sendData(iLpn, isEnterIfLT20=True)
        for i in range(len(items)):
            self.assertScreenTextExist('Item:' + items[i])
            self.assertScreenTextExist('Qty:' + str(qty[i]))
            self.assertScreenTextExist('loc:' + locn)
            self.assertScreenTextExist('Sts:' + facStatDesc)
            if i < len(items) - 1:
                self.sendData(self.KEY_CTRL_D_GoPageDown)

        # if True:
        #     self.assertScreenTextExist(['No More Details', 'To Display'])
        #     self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('LPN :')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def asnInquiryTran(self, asn:str, po:list[str], poLineId:list, items:list[list[str]], shipQty:list[list[int]],
                   recvQty:list[list[int]], status:str):
        """ASN Inquiry"""
        tran_name = RF_TRAN.get('rf', 'asnInquiry')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn,isEnter=True)
        self.assertScreenTextExist('ASN:')
        self.assertScreenTextExist(asn)
        self.assertScreenTextExist('Status:')
        self.assertScreenTextExist(status)
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        for i in range(len(po)):
            self.assertScreenTextExist('PO:')
            self.assertScreenTextExist(po[i] + '-' + str(poLineId[i]))
            for j in range(len(items[i])):
                self.assertScreenTextExist('Item:'+items[i][j])
                self.assertScreenTextExist('Ship Qty:'+ str(shipQty[i][j]))
                self.assertScreenTextExist('Recv Qty:'+ str(recvQty[i][j]))
                if j < len(items[i]) - 1:
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def itemInquiryTranForInbound(self, item: str, sequence: int, loc: list[str], qty: list[int], itemUOM: str):
        """"""
        tran_name = RF_TRAN.get('rf', 'itemInquiryForInbound')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'In')

        self.assertScreenTextExist('Item Barcode:')
        self.sendData(item, isEnter=True)

        screentxt = self.readScreen()
        assert screentxt.count(itemUOM) >= 1, "itemUOM not found " + itemUOM

        self.assertScreenTextExist('Select Seq:')
        self.sendData(sequence, isEnter=True)

        if True:  # TODO for loop for each locn
            self.assertScreenTextExist('Item:' + item)
            self.assertScreenTextExist('loc:' + loc[0])
            self.assertScreenTextExist('Qty:' + str(qty[0]))

        self.sendData(self.KEY_CTRL_X_ExitTran)

    def itemInquiryTranForOutbound(self, item: str, sequence: int, loc: list[str], qty: list[int], itemUOM: str):
        """"""
        tran_name = RF_TRAN.get('rf', 'itemInquiryForOutbound')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Out')

        self.assertScreenTextExist('Item Barcode:')
        self.sendData(item, isEnter=True)

        screentxt = self.readScreen()
        assert screentxt.count(itemUOM) >= 1, "itemUOM not found " + itemUOM

        self.assertScreenTextExist('Select Seq:')
        self.sendData(sequence, isEnter=True)

        if True:  # TODO for loop for each locn
            self.assertScreenTextExist('Item:' + item)
            self.assertScreenTextExist('loc:' + loc[0])
            self.assertScreenTextExist('Qty:' + str(qty[0]))

        self.sendData(self.KEY_CTRL_X_ExitTran)

    def ocLocationInquiryTran(self, consolLocn, o_order, o_olpn):
        """"""
        tran_name = RF_TRAN.get('rf', 'ocLocationInquiry')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('Location:')
        self.sendData(consolLocn, isEnter=True)
        screentxt = ''
        while True:
            lastscreentxt = self.readScreen()
            screentxt += lastscreentxt
            self.sendData(self.KEY_CTRL_D_GoPageDown)
            if lastscreentxt == self.readScreen():
                break
        # screentxt = screentxt.replace('\n', '').replace('x', '').replace(' ', '')
        # screentxt = re.sub(r'[^A-Z0-9]', '', screentxt)
        # screentxt = screentxt.replace('\n', '').replace(' ││', '')
        screentxt = re.sub(r'[^A-Za-z0-9-]+', '', screentxt.replace('\n', ''))

        '''Validation'''
        # Trim first two characters of olpn
        o_olpn = {element[2:] for element in o_olpn}

        # Create a copy of the order and olpn set to iterate over
        orders = o_order.copy()
        olpns = o_olpn.copy()

        # Remove orders and olpns found in screentxt from the original lists
        pending_order = [order for order in orders if order not in screentxt]
        pending_olpn = [olpn for olpn in olpns if olpn not in screentxt]
        printit(f"Pending orders {pending_order}, olpns {pending_olpn}")
        
        assert not pending_order and not pending_olpn, 'Consol locn inquiry failed for few orders/olpns'

    def executeCycleCountActiveTaskByCtrl(self, taskId: str, itemList: list[str], actvLocn: str, qtys: list[int],
                                          taskGrp: str = None):

        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        assert len(itemList) == len(qtys), "no.of items and qtys didnt match"

        if taskGrp is None:
            taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_UP)
        self.sendData(taskId, isEnter=True)
        self.assertScreenTextExist('Aloc:')
        self.sendData(actvLocn, isEnter=True)
        self.assertScreenTextExist('Aloc:' + actvLocn)
        for i in range(0, len(itemList)):
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemList[i], isEnter=True)
            self.assertScreenTextExist('Item:' + itemList[i])
            self.assertScreenTextExist('Qty:')
            self.sendData(str(qtys[i]), isEnter=True)
        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.assertScreenTextExist('Choice:')
        self.logger.info('Executed RF ' + inspect.currentframe().f_code.co_name + ': Ctrl E')

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=TaskHdrStat.COMPLETE)
        for i in range(len(itemList)):
            DBLib().assertWMInvnDtls(i_itemBrcd=itemList[i], i_locn=actvLocn, o_onHandQty=qtys[i])
            DBLib().assertNoPixPresent(i_itemBrcd=itemList[i])
            DBLib().assertLaborMsgHdr(i_taskNbr=taskId, i_actName='CYC CNT ACT')
            DBLib().assertLaborMsgDtl(i_taskNbr=taskId, i_actName='CYC CNT ACT', i_itemBrcd=itemList[i], o_qty=qtys[i])

    def cycleCountActiveTran(self, actvLocn, isCCPending: bool, items: list[str], qtys: list[int],
                             newQtys: list[list[int]], o_statCode: int, o_taskId: str, isEndLocnWithNoUnit: bool = None):
        """"""
        tran_name = RF_TRAN.get('rf', 'cycleCountActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        assert len(items) == len(qtys) == len(newQtys), 'No. of items and qtys didnt match'

        isSameQtyList = [False for i in items]
        finalCountedQtys = []

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'I')
        self.assertScreenTextExist('Aloc:_')
        self.sendData(actvLocn, isEnter=True)
        if isCCPending:
            self.assertScreenTextExist(['Warning!', 'Location prev', 'designated for', 'cycle cnt'])
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        for i in range(len(items)):
            self.assertScreenTextExist('Aloc:' + actvLocn)
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(items[i], isEnter=True)

            self.assertScreenTextExist('Qty:')

            if isEndLocnWithNoUnit:
                finalCountedQtys.extend(0 for i in range(len(qtys)))
                break
            else:
                prevCountedQty = qtys[i]
                for j in range(len(newQtys[i])):
                    self.sendData(newQtys[i][j], isEnter=True)
                    isSameQtyList[i] = True if j == 0 and prevCountedQty == newQtys[i][j] else isSameQtyList[i]
                    if prevCountedQty == newQtys[i][j]:
                        break
                    elif qtys[i] != newQtys[i][j]:
                        self.assertScreenTextExist(['Error', 'Qty Mimsmatch', 'Recount Required'])
                        self.sendData(self.KEY_CTRL_A_AcceptWarning)
                        self.assertScreenTextExist('Qty:')
                        prevCountedQty = newQtys[i][j]
                finalCountedQtys.append(prevCountedQty)

        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)
        screenTxt = self.readScreen()
        if screenTxt.count('Not all Items') > 0:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('Aloc:_')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertTaskHdr(i_task=str(o_taskId), o_status=TaskHdrStat.CANCELLED)
        locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=str(actvLocn))
        DBLib().assertTaskHdr(i_taskGenRefNbr=locnId, o_intType=101, o_status=TaskHdrStat.CANCELLED)
        # DBLib().assertCycleCountStatus(i_locnBrcd=str(actvLocn), o_statCode=int(o_statCode), i_intType=101)
        for i in range(len(items)):
            DBLib().assertWMInvnDtls(i_locn=actvLocn, i_itemBrcd=items[i], o_onHandQty=int(finalCountedQtys[i]))
            if not isSameQtyList[i]:
                DBLib().assertPix(i_itemBrcd=items[i], i_tranType='300', i_rsnCode='84')

    def cycleCountResvBulk(self, resvLocn: str, isCCPending: bool, iLpns: Union[str, list], qtys: Union[int, list],
                           isAddILpn: bool = None, iLpnsToBeAdded: list[str] = None, qtysToBeAdded: list[int] = None,
                           isOmitLpn: bool = None, iLpnsToBeOmitted: list[str] = None, qtysToBeOmitted: list[int] = None,
                           taskId: str = None):
        """Counts the number of lpns in the resv locns
        """
        tran_name = RF_TRAN.get('rf', 'cycleCountResvBulk')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if isOmitLpn:
            iLpns = [item for item in iLpns if item not in iLpnsToBeOmitted]
            qtys = [qty for qty in qtys if qty not in qtysToBeOmitted]
        elif isAddILpn:
            iLpns.extend(item for item in iLpnsToBeAdded)
            qtys.extend(qty for qty in qtysToBeAdded)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Rloc:_')
        self.sendData(resvLocn, isEnter=True)
        if isCCPending:
            self.acceptMsgIfExist(['Warning!', 'Location prev', 'designated', 'cycle cnt'])
        self.assertScreenTextExist('Rloc:' + resvLocn)
        self.assertScreenTextExist('Number of LPNs:')
        numOfLpns = len(iLpns)
        self.sendData(str(numOfLpns), isEnter=True)
        self.acceptMsgIfExist(['Qty Entered Diff', 'Than Expctd iLPN', 'Qty'])
        for i in range(len(iLpns)):
            self.assertScreenTextExist('iLPN:')
            self.sendData(iLpns[i], isEnter=True)
            if isAddILpn:
                self.acceptMsgIfExist(['iLPN In Different', 'Location'])
        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)

        if isOmitLpn:
            self.acceptMsgIfExist(['Not All iLPNs', 'Counted In', 'Location!'])
        self.assertScreenTextExist('Rloc:_')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if taskId is not None:
            locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=str(resvLocn))
            DBLib().assertTaskHdr(i_task=taskId, i_taskGenRefNbr=locnId, i_currTaskPrty=50, o_status=TaskHdrStat.CANCELLED, o_intType=100)

        finalILpns = iLpnsToBeAdded if isAddILpn else iLpnsToBeOmitted if isOmitLpn else iLpns
        finalQtys = qtysToBeAdded if isAddILpn else qtysToBeOmitted if isOmitLpn else qtys
        finalLpnFacStat = LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY if isOmitLpn else LPNFacStat.ILPN_PUTAWAY
        for i in range(len(finalILpns)):
            DBLib().assertLPNHdr(i_lpn=finalILpns[i], o_facStatus=finalLpnFacStat)
            DBLib().assertCCVariance(i_locnBrcd=resvLocn, i_iLpn=finalILpns[i], isILpnAdded=isAddILpn,
                                     isILpnOmitted=isOmitLpn, o_qty=finalQtys[i])
            if isOmitLpn:
                DBLib().assertLpnLockPresent(i_lpn=finalILpns[i], i_lockCode='LC')

    def cycleCountResvSummary(self, taskId:str, resvLocn:str, isCCPending: bool, iLpn: Union[str, list], isAddILpn:bool=None,
                              iLpnsToBeAdded:list[str]=None, isOmitLpn:bool=None, iLpnsToBeOmitted:list[str]=None):
        """"""
        tran_name = RF_TRAN.get('rf', 'cycleCountResvSummary')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        iLpnList = iLpn if type(iLpn) == list else [iLpn]
        if isOmitLpn:
            iLpnList = [x for x in iLpnList if x not in iLpnsToBeOmitted]

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Rloc:_')
        self.sendData(resvLocn, isEnter=True)
        if isCCPending:
            self.acceptMsgIfExist(['Warning!', 'Location prev', 'designated', 'cycle cnt'])
        for i in range(len(iLpnList)):
            self.assertScreenTextExist('Rloc:' + resvLocn)
            self.assertScreenTextExist('iLPN:')
            self.sendData(iLpnList[i], isEnterIfLT20=True)
            if isAddILpn:
                self.acceptMsgIfExist(['iLPN In Different', 'Location'])
        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)
        if isOmitLpn:
            self.acceptMsgIfExist(['Not All iLPNs', 'Counted In', 'Location!'])

        self.assertScreenTextExist('Rloc:_')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=str(resvLocn))
        DBLib().assertTaskHdr(i_task=taskId, i_taskGenRefNbr=locnId, i_currTaskPrty=50, o_status=TaskHdrStat.CANCELLED, o_intType=100)

        finalILpns = iLpnsToBeAdded if isAddILpn else iLpnsToBeOmitted if isOmitLpn else iLpnList
        finalLpnFacStat = LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY if isOmitLpn else LPNFacStat.ILPN_PUTAWAY
        for i in range(len(finalILpns)):
            DBLib().assertLPNHdr(i_lpn=finalILpns[i], o_facStatus=finalLpnFacStat)
            if isOmitLpn:
                DBLib().assertLpnLockPresent(i_lpn=finalILpns[i], i_lockCode='LC')

    def lockILPNTran(self, lockCode, iLPN: str, items: list[str], o_lpnStat: LPNFacStat):
        """Lock 1 ilpn with 1 lock code
        """
        tran_name = RF_TRAN.get('rf', 'lockILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('Lock Container')
        self.assertScreenTextExist('Lock Code:')
        self.sendData(lockCode)
        if len(lockCode) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('iLPN:')
        self.sendData(iLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=o_lpnStat)
        DBLib().assertLpnLockPresent(i_lpn=iLPN, i_lockCode=lockCode)
        for i in range(len(items)):
            DBLib().assertPix(i_itemBrcd=items[i], i_caseNbr=iLPN, i_tranType='300', i_tranCode='01', i_invnAdjType='S')
            DBLib().assertPix(i_itemBrcd=items[i], i_caseNbr=iLPN, i_tranType='606', i_tranCode='02', i_invnAdjType='A')

    def unlockILPNTran(self, lockCode, iLPN: str, items: list[str], o_lpnStat: LPNFacStat):
        """Unlock 1 ilpn with 1 lock code
        """
        tran_name = RF_TRAN.get('rf', 'unlockILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('Unlock Container')
        self.assertScreenTextExist('Lock Code:')
        self.sendData(lockCode)
        if len(lockCode) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('iLPN:')
        self.sendData(iLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=o_lpnStat)
        DBLib().assertNoInvLockForLpn(i_lpn=iLPN, i_lockCode=lockCode)
        for i in range(len(items)):
            DBLib().assertPix(i_itemBrcd=items[i], i_caseNbr=iLPN, i_tranType='300', i_tranCode='01', i_invnAdjType='A')
            DBLib().assertPix(i_itemBrcd=items[i], i_caseNbr=iLPN, i_tranType='606', i_tranCode='02', i_invnAdjType='S')

    def createILPNTran(self, reasonCode, item: list[str], qty: list[int], ilpn: str = None):
        """Create 1 iLPN with 1 or more items
        """
        #
        tran_name = RF_TRAN.get('rf', 'createILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        itemList = item if type(item) == list else [item]
        qtyList = qty if type(qty) == list else [qty]
        if ilpn is None:
            ilpn = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('CREATE iLPN')
        self.assertScreenTextExist('iLPN:')
        self.sendData(ilpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Reason Code:')
        self.sendData(str(reasonCode))
        if len(str(reasonCode)) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Reference:')
        self.sendData('Test', isEnter=True)
        for i in range(0, len(itemList)):
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemList[i], isEnter=True)
            self.assertScreenTextExist('Item:' + itemList[i])
            self.assertScreenTextExist('Qty Pckd:')
            self.sendData(str(qtyList[i]), isEnter=True)
        self.sendData(self.KEY_CTRL_E_EndIlpn)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        for i in range(len(item)):
            # DBLib().assertWMInvnDtls(i_itemBrcd=str(item[i]), i_lpn=ilpn)
            DBLib().assertLPNDtls(i_lpn=ilpn, i_itemBrcd=str(item[i]), o_qty=qty[i])
            DBLib().assertWMInvnDtls(i_lpn=ilpn, i_itemBrcd=str(item[i]), o_onHandQty=qty[i], o_allocatableFlag='Y')
            DBLib().assertPix(i_itemBrcd=str(item[i]), i_caseNbr=ilpn, i_tranType='300', i_invnAdjQty=qty[i],
                              i_invnAdjType='A', i_rsnCode=reasonCode, o_any_procStatCode=(10, 90))
        return ilpn

    def modifyILPNTran(self, ilpn:str, reasonCode, items:list[str]=None, currQty:list[int]=None, newQty:list[int]=None,
                       isAddNewItem:bool=None, itemsToAdd:list[str]=None, qtysToAdd:list[int]=None):
        """Modify 1 ilpn with 1 item
        """
        tran_name = RF_TRAN.get('rf', 'modifyILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('iLPN #:')
        self.sendData(ilpn, isEnterIfLT20=True)

        if newQty is not None:
            self.assertScreenTextExist('Qty:' + str(currQty[0]))
            self.assertScreenTextExist('New Qty:')
            self.sendData(newQty[0], isEnter=True)

        if isAddNewItem:
            for i in range(len(itemsToAdd)):
                self.sendData(self.KEY_CTRL_Y_AddDetail)
                self.assertScreenTextExist('Item Barcode:')
                self.sendData(itemsToAdd[i], isEnter=True)
                self.assertScreenTextExist('Qty:')
                self.sendData(qtysToAdd[i], isEnter=True)
            self.assertScreenTextExist('Qty:')
            self.sendData(self.KEY_ENTER)
            
        self.assertScreenTextExist('Reason Code:')
        self.sendData(reasonCode)
        if len(reasonCode) < 2:
            self.sendData(self.KEY_ENTER)

        self.assertScreenTextExist('Reference #:')
        self.sendData('Test', isEnter=True)

        self.assertScreenTextExist('iLPN #:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        if isAddNewItem:
            for i in range(len(itemsToAdd)):
                DBLib().assertLPNDtls(i_lpn=ilpn, i_itemBrcd=itemsToAdd[i], o_qty=qtysToAdd[i])
                DBLib().assertWMInvnDtls(i_lpn=ilpn, i_itemBrcd=itemsToAdd[i], o_onHandQty=qtysToAdd[i])
                DBLib().assertPix(i_itemBrcd=itemsToAdd[i], i_tranType='300', i_rsnCode='84')
        else:
            DBLib().assertLPNDtls(i_lpn=ilpn, i_itemBrcd=items[0], o_qty=newQty[0])
            DBLib().assertWMInvnDtls(i_lpn=ilpn, i_itemBrcd=items[0], o_onHandQty=newQty[0])

    # def sendToVLMTran(self, ilpn, o_item: str, o_qty: int):
    #     """1 sorted ilpn in 50 state, allocated to VLM locn"""
    #
    #     tran_name = RF_TRAN.get('rf', 'sendToVlm')
    #     Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)
    #
    #     self.goToHomeScreen()
    #     self.goToTransaction(tran_name, 'Inbo')
    #     self.assertScreenTextExist('iLPN:')
    #     self.sendData(ilpn, isEnterIfLT20=True)
    #     self.assertScreenTextExist('iLPN:')
    #     self.sendData(self.KEY_CTRL_X_ExitTran)
    #
    #     '''Validation'''
    #     # Verify VLM Message got generated in this format : SequenceNumber^REPLENISHMENT^TLPNNumber^ItemName^ItemQty^1
    #     DBLib().assertVLMReplenMsg(ilpn=ilpn, o_item=o_item, o_qty=o_qty, o_status='Ready')

    def sendToVLMTran(self, ilpn, o_item: str, o_qty: int):
        self.sendToMHETran(ilpn=ilpn, o_item=o_item, o_qty=o_qty)

    def sendToMHETran(self, ilpn, o_item: str, o_qty: int, o_locn:str=None):
        """1 sorted ilpn in 50 state, allocated to VLM/AS/ASRS locn
        """
        tran_name = RF_TRAN.get('rf', 'sendToVlm')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inbo')
        self.assertScreenTextExist('iLPN:')
        self.sendData(ilpn, isEnterIfLT20=True)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        # Verify message to VLM/AS/ASRS got generated
        final_statName = DBLib()._decide_clm_statusName_forMheMsg()
        DBLib()._assertPutwyReplenMsgToAutoLocn(ilpn=ilpn, o_locn=o_locn, o_item=o_item, o_qty=o_qty, o_status=final_statName)

    def sortMixILPNTran(self, sortZone, iLPN: list[str], o_item: list[list[str]], o_qty: list[list[int]],
                        newIlpn: str = None, pallet: str = None):
        """Mix multiple lpns to 1 new lpn -> then sort to 1 pallet"""

        tran_name = RF_TRAN.get('rf', 'sortMix')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        assert len(iLPN) == len(o_item), 'Set of items for each ilpn missing'
        if newIlpn is None:
            newIlpn = DBLib().getNewILPNNum()
        if pallet is None:
            pallet = DBLib().getNewInPalletNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Sorting Zone:')
        self.sendData(sortZone)
        for i in range(len(iLPN)):
            self.assertScreenTextExist('iLPN:')
            self.sendData(iLPN[i], isEnterIfLT20=True)
            self.assertScreenTextExist(iLPN[i])
            self.assertScreenTextExist('iLPN:')
            self.sendData(newIlpn, isEnterIfLT20=True)
            if i == 0:
                self.assertScreenTextExist('Pallet:')
                self.sendData(pallet, isEnterIfLT20=True)
            if i == 0:
                screentxt = self.readScreen()
                if screentxt.count("Warning!") > 0 and screentxt.count("Pallet does not") > 0:
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=newIlpn, o_facStatus=LPNFacStat.ILPN_ALLOCATED)
        for i in range(len(iLPN)):
            DBLib().assertLPNHdr(i_lpn=iLPN[i], o_facStatus=LPNFacStat.ILPN_CONSUMED)
            for j in range(len(o_item[i])):
                # DBLib().assertLPNDtlsForItem()
                DBLib().assertPix(i_itemBrcd=o_item[i][j], i_caseNbr=iLPN[i], i_tranType='100')
                DBLib().assertPix(i_itemBrcd=o_item[i][j], i_caseNbr=iLPN[i], i_tranType='617')

        return newIlpn, pallet

    # def shortPullReplenByCtrl(self,taskGrp:str,taskId:str,itemList:list[list[str]],qtyList:list[list[str]],
    #                           fromIlpnList:list[str],o_reservLocn:str,palletId:str=None):
    #
    #     if palletId is None:
    #         palletId=DBLib().getNewInPalletNum()
    #     self.goToHomeScreen()
    #     self._changeTaskGroupByCtrl(taskGroup=taskGrp)
    #     self.assertScreenTextExist('Task#:')
    #     self.sendData(self.KEY_UP)
    #     self.sendData(taskId)
    #     self.sendData(self.KEY_ENTER)
    #     self.assertScreenTextExist('Pallet:')
    #     self.sendData(palletId)
    #     self.sendData(self.KEY_ENTER)
    #     for i in range(0,len(itemList)):
    #         for j in range(0,len(itemList[i])):
    #             self.assertScreenTextExist(
    #                 ['Task:' + taskId, 'Item:' + fromIlpnList[i][j], 'Qty:' + qtyList[i][j]])
    #             self.assertScreenTextExist('iLPN:\n' + fromIlpnList[i])
    #             self.sendData(fromIlpnList[i])
    #             self.sendData(self.KEY_CTRL_S_ShortTask)
    #             self.assertScreenTextExist("Warning! Task")
    #             self.assertScreenTextExist("will be canceled.")
    #             self.sendData(self.KEY_CTRL_A_AcceptWarning)
    #
    #     '''Validation'''
    #     DBLib().assertTaskHdr(i_task=taskId,o_status=99)
    #     DBLib().assertCycleCountStatus(i_locnBrcd=o_reservLocn, i_intType=101, o_statCode=10)
    #     for i in range(0, len(itemList)):
    #         for j in range(0, len(itemList[i])):
    #             DBLib().assertLPNHdr(i_lpn=fromIlpnList[i][j], o_facStatus=10)

    def auditOLPNTran(self, olpn: str, items: list[str], qtys: list[int], isRFUser2: bool = None,
                      o_qaFlag: int = None, isVarianceFound:bool=None, isWrongItem:bool=None):
        """Audit 1 olpn with 1/more items
        """
        tran_name = RF_TRAN.get('rf', 'auditOLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if isVarianceFound:
            printReqstr = DBLib().getPrintRequestor(codeDesc='Audit oLPN variance report')
        else:
            printReqstr = None

        self.goToHomeScreen(isRFUser2=isRFUser2)
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('oLPN:')
        self.sendData(olpn, isEnterIfLT20=True)

        scrntxt = self.readScreen()
        if scrntxt.count('You packed this') > 0:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        scrntxt = self.readScreen()
        if scrntxt.count('oLPN does not') > 0:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        for i in range(len(items)):
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(items[i], isEnter=True)

            if isWrongItem:
                self.assertScreenTextExist('Unexpected Item')
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

            self.assertScreenTextExist('Item:' + str(items[i]))
            self.assertScreenTextExist('Qty:')
            self.sendData(qtys[i], isEnter=True)
            scrntxt = self.readScreen()
            if scrntxt.count('Quantity greater') > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.sendData(self.KEY_CTRL_E_EndIlpn)

        if isVarianceFound:
            scrntxt = self.readScreen()
            if scrntxt.count('Re-scan contents') > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.sendData(self.KEY_CTRL_E_EndIlpn)
            scrntxt = self.readScreen()
            if scrntxt.count('Variance Exists!') > 0:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                self.assertScreenTextExist(str(olpn))
                self.assertScreenTextExist('Variance Exists!')
                self.sendData(printReqstr, isEnter=True)

        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=olpn, o_qaFlag=o_qaFlag)
        if isVarianceFound:
            DBLib().assertLRFReport(printReqstr=printReqstr)

    def loadPltzTran(self, oLpn: str, blindPallet: str = None, order: str = None, o_doStatus: DOStat = None):
        """"""
        tran_name = RF_TRAN.get('rf', 'loadPltz')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if blindPallet is None:
            blindPallet = DBLib().getNewOutPalletNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('OB Container')
        self.sendData(oLpn)
        self.assertScreenTextExist('Pallet:')
        self.sendData(blindPallet)
        self.assertScreenTextExist('OB Container')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=str(oLpn), o_parentLpn=blindPallet)
        DBLib().assertDOHdr(i_order=str(order), o_status=o_doStatus)

        return blindPallet

    def packOLpnFromActiveTranWithMultiSKU(self, order: str, oLpn: str, fromActvLocns: list[str],
                                           items: list[str], qtys: list[int],
                                           waveNum: str = None, willLocnEmpty: bool = False, o_doStatus: DOStat = None,
                                           o_allocStatus: AllocStat = None,
                                           isSkipLocn: bool = None, locnsToSkip: list[str] = None):
        """Pack multi sku olpn from active
        """
        tran_name = RF_TRAN.get('rf', 'packOLPNActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        isLocnEmptyFlag = 'Y' if willLocnEmpty else 'N'

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN:')
        self.sendData(oLpn, isEnterIfLT20=True)

        i = 0
        while i < len(fromActvLocns):
            self.assertScreenTextExist('Aloc:' + str(fromActvLocns[i]))
            self.assertScreenTextExist('Item:' + str(items[i]))
            self.assertScreenTextExist('Qty:' + str(qtys[i]))
            self.assertScreenTextExist('Item Barcode:')

            if isSkipLocn and locnsToSkip.count(fromActvLocns[i]) > 0:
                self.sendData(self.KEY_CTRL_K_SkipDtl)
                fromActvLocns.append(fromActvLocns[i])
                items.append(items[i])
                qtys.append(qtys[i])
                locnsToSkip.remove(fromActvLocns[i])
                i = i + 1
            else:
                self.sendData(items[i], isEnter=True)
                self.assertScreenTextExist('Qty:')
                self.sendData(str(qtys[i]), isEnter=True)
                screentxt = self.readScreen()
                if screentxt.count('Location Empty(Y'):
                    self.sendData(isLocnEmptyFlag)
                i = i + 1

        self.assertScreenTextExist(['Info', 'End of oLPN!'])
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertDOHdr(i_order=order, o_status=o_doStatus)
        DBLib().assertLPNHdr(i_lpn=oLpn, o_facStatus=LPNFacStat.OLPN_PACKED)
        for i in range(len(items)):
            DBLib().assertAllocDtls(i_itemBrcd=items[i], i_taskGenRefNbr=waveNum, i_taskCmplRefNbr=oLpn, i_intType=50,
                                    o_statCode=o_allocStatus)

    def packMultiSkuOLpnFromActiveTranWithSplit(self, order: str, waveNum: str = None, oLpn: str = None,
                                                fromActvLocns: list[list[str]] = None, blindOLpnCnt: int = None, isPickToOrigOLpn: bool = None,
                                                items: list[list[str]] = None, qtys: list[list[int]] = None, willLocnEmpty: bool = None,
                                                o_doStatus: DOStat = None, o_allocStatus: AllocStat = None):
        """Pack multi skus from orig olpn to new olpns
        """
        tran_name = RF_TRAN.get('rf', 'packOLPNActive')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        isLocnEmptyFlag = 'Y' if willLocnEmpty else 'N'
        oLpns = []
        if isPickToOrigOLpn:
            oLpns.append(oLpn)
        k = 1
        for k in range(blindOLpnCnt):
            blindOLpn = DBLib().getNewOLPNNum()
            oLpns.append(blindOLpn)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('oLPN:')
        self.sendData(oLpn, isEnterIfLT20=True)
        for i in range(len(oLpns)):
            for j in range(len(items[i])):
                if oLpns[i] != oLpn:
                    self.sendData(self.KEY_CTRL_E_EndOlpn)
                    self.assertScreenTextExist('New oLPN:')
                    self.sendData(oLpns[i], isEnterIfLT20=True)
                self.assertScreenTextExist('Aloc:' + str(fromActvLocns[i][j]))
                self.assertScreenTextExist('Item:' + str(items[i][j]))
                self.assertScreenTextExist('Qty:' + str(qtys[i][j]))
                self.assertScreenTextExist('Item Barcode:')

                self.sendData(items[i][j], isEnter=True)
                self.assertScreenTextExist('Qty:')
                self.sendData(str(qtys[i][j]), isEnter=True)
                screentxt = self.readScreen()
                if screentxt.count('Location Empty(Y'):
                    self.sendData(isLocnEmptyFlag)
        self.assertScreenTextExist(['Info', 'End of oLPN!'])
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('oLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertDOHdr(i_order=order, o_status=o_doStatus)
        DBLib().assertOLPNCountForDO(i_order=order, o_totalOLPNs=len(oLpns))
        for i in range(len(oLpns)):
            DBLib().assertLPNHdr(i_lpn=oLpns[i], o_facStatus=LPNFacStat.OLPN_PACKED)
            for j in range(len(items[i])):
                DBLib().assertAllocDtls(i_itemBrcd=items[i][j], i_taskGenRefNbr=waveNum, i_taskCmplRefNbr=oLpns[i],
                                        i_intType=50, o_statCode=o_allocStatus)

    def executeReplenTaskWithSkip(self, fromResvLocn: str, fromIlpns: list[str], itemsToPull: list[str],
                                  taskGrp: str = None, waveNbr: str = None, iLpnsToSkip: list[str] = None, o_taskDtlStat:list[TaskDtlStat]=None):
        """Skip lpns during task completion and leave task
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        pallet = DBLib().getNewInPalletNum()
        taskId = str(DBLib().getTaskIdFromGenRefNbr(taskGenRefNbr=waveNbr))

        # if taskGrp is None:
        #     taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)
        taskGrp = self._decideFinalTaskGrp(taskGrp, taskId, 1)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.assertScreenTextExist('Pallet:')
        self.sendData(str(pallet), isEnterIfLT20=True)

        i = 0
        while i < len(fromIlpns):
            self.assertScreenTextExist('Task:' + str(taskId))
            self.assertScreenTextExist('Rloc:' + str(fromResvLocn))
            self.assertScreenTextExist(str(fromIlpns[i]))
            self.assertScreenTextExist('Item:' + str(itemsToPull[i]))

            if iLpnsToSkip is not None and iLpnsToSkip.count(fromIlpns[i]) > 0:
                self.sendData(self.KEY_CTRL_K_SkipDtl)
            i = i + 1

        self.assertScreenTextExist(fromIlpns[0])
        self.sendData(self.KEY_CTRL_X_ExitTran)
        self.assertScreenTextExist('Leaving Task')
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('Choice:_')

        '''Validation'''
        if o_taskDtlStat:
            for i in range(len(iLpnsToSkip)):
                DBLib().assertTaskDtls(i_cntrNbr=iLpnsToSkip[i], o_statCode=o_taskDtlStat[i])

    def cycleCountResvDetailTran(self, resvLocn: str, taskId: str = None, isCCPending: bool = None,
                                 isLocnHasAllocatedLpn: bool = None, iLpns: list[str] = None,
                                 items: list[list[str]] = None, qtys: list[list[int]] = None, newQtys: list[list[list[int]]]=None,
                                 isOmitLpn:bool=None, iLpnsToBeOmitted:list[str]=None,
                                 isAddILpn:bool=None, iLpnsToBeAdded:list[str]=None,
                                 o_statCode: int = None, isCCZero: bool = None):
        """CYL CNT-RSV DETAI
        """
        tran_name = RF_TRAN.get('rf', 'cycleCountResvDtl')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)


        isOmitSameOnlyLpn = True if (isOmitLpn and (len(iLpns) and len(iLpnsToBeOmitted)) == 1) else False
        finalCountedQtys = []
        
        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Rloc:_')
        self.sendData(resvLocn, isEnter=True)

        if isCCPending:
            self.assertScreenTextExist(['Warning!', 'Location prev', 'designated for', 'cycle cnt'])
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        if not isOmitSameOnlyLpn:
            for i in range(len(iLpns)):
                self.assertScreenTextExist('Rloc:' + resvLocn)
                self.assertScreenTextExist('iLPN:')
                self.sendData(iLpns[i], isEnterIfLT20=True)
                if isAddILpn and iLpns[i] in iLpnsToBeAdded:
                    self.assertScreenTextExist(['iLPN In Different', 'Location'])
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                if isLocnHasAllocatedLpn:
                    self.assertScreenTextExist(['Warning!', 'iLPN is allocated', 'or partially ', 'alloc!'])
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
                    break
                for j in range(len(items[i])):
                    self.assertScreenTextExist('Item Barcode:')
                    self.sendData(items[i][j], isEnter=True)
                    self.assertScreenTextExist('Item:' + str(items[i][j]))
                    self.assertScreenTextExist('Qty:')

                    prevCountedQty = qtys[i][j]
                    for k in range(len(newQtys[i][j])):
                        self.sendData(newQtys[i][j][k], isEnter=True)
                        if prevCountedQty == newQtys[i][j][k]:
                            break
                        if qtys[i][j] != newQtys[i][j][k]:
                            self.acceptMsgIfExist(['Error', 'Qty Mimsmatch', 'Recount Required'])
                            self.assertScreenTextExist('Qty:')
                            prevCountedQty = newQtys[i][j][k]
                    finalCountedQtys.append(prevCountedQty)
                self.sendData(self.KEY_CTRL_E_EndIlpn)

        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)
        if isOmitLpn:
            self.acceptMsgIfExist(['Not All iLPNs', 'Counted In', 'Location!'])
        self.acceptMsgIfExist(['Not all Items'])
        
        self.assertScreenTextExist('Rloc:_')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        locnId = DBLib().getLocnIdByLocnBrcd(locnBrcd=str(resvLocn))
        DBLib().assertTaskHdr(i_task=taskId, i_taskGenRefNbr=locnId, i_currTaskPrty=50, o_status=TaskHdrStat.CANCELLED, o_intType=100)
        if not (isLocnHasAllocatedLpn or isOmitLpn or isAddILpn):
            for i in range(len(iLpns)):
                for j in range(len(items[i])):
                    DBLib().assertWMInvnDtls(i_locn=resvLocn, i_lpn=iLpns[i], i_itemBrcd=items[i][j], o_onHandQty=int(finalCountedQtys[i]))
                    DBLib().assertPix(i_itemBrcd=items[i][j], i_tranType='300', i_rsnCode='84')
        if isOmitLpn or isAddILpn:
            finalILpns = iLpnsToBeOmitted if isOmitLpn else iLpnsToBeAdded
            finalLpnFacStat = LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY if isOmitLpn else LPNFacStat.ILPN_PUTAWAY
            for i in range(len(finalILpns)):
                DBLib().assertLPNHdr(i_lpn=finalILpns[i], o_facStatus=finalLpnFacStat)
                if isOmitLpn:
                    DBLib().assertLpnLockPresent(i_lpn=finalILpns[i], i_lockCode='LC')

    def executeCycleCountResvTaskByCtrl(self, taskId: str, taskGrp: str = None, resvLocn: str = None,
                                        iLpns: list[str] = None, items: list[list[str]] = None, qtys: list[list[int]] = None):
        """Execute cc by task for resv locn
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        itemList = items if type(items) == list else [items]
        qtyList = qtys if type(qtys) == list else [qtys]

        if taskGrp is None:
            taskGrp = DBLib().getTaskGroupFromTask(taskId=taskId)

        self.goToHomeScreen()
        self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_UP)
        self.sendData(taskId, isEnter=True)
        self.assertScreenTextExist('Rloc:_')
        self.sendData(resvLocn, isEnter=True)
        for i in range(len(iLpns)):
            self.assertScreenTextExist('Rloc:' + resvLocn)
            self.assertScreenTextExist('iLPN:')
            self.sendData(iLpns[i], isEnterIfLT20=True)
            for j in range(len(itemList[i])):
                self.assertScreenTextExist('Item Barcode:')
                self.sendData(itemList[i][j], isEnter=True)
                self.assertScreenTextExist('Item:' + str(itemList[i][j]))
                self.assertScreenTextExist('Qty:')
                self.sendData(qtyList[i][j], isEnter=True)
            self.sendData(self.KEY_CTRL_E_EndIlpn)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_N_EndLocnDuringCC)

        '''Validation'''
        DBLib().assertTaskHdr(i_task=taskId, o_status=TaskHdrStat.COMPLETE)
        for i in range(len(iLpns)):
            for j in range(len(itemList[i])):
                DBLib().assertWMInvnDtls(i_lpn=iLpns[i], i_itemBrcd=itemList[i][j], i_locn=resvLocn, o_onHandQty=qtys[i][j])
                DBLib().assertNoPixPresent(i_itemBrcd=itemList[i][j])

        DBLib().assertLaborMsgHdr(i_taskNbr=taskId, i_actName='CYC CNT BULK')
        for i in range(len(iLpns)):
            for j in range(len(itemList[i])):
                DBLib().assertLaborMsgDtl(i_taskNbr=taskId, i_actName='CYC CNT BULK', i_itemBrcd=itemList[i][j], o_qty=qtys[i][j])

    def allocateILPN(self, ilpn: str):
        """Allocate 1 ilpn
        """
        tran_name = RF_TRAN.get('rf', 'allocateILPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'In')

        self.assertScreenTextExist('LPN:')
        self.sendData(ilpn, isEnterIfLT20=True)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_ALLOCATED)

    def putawayCaseTran(self, ilpn: list[str], o_item: list[list[str]], o_qty: list[list[int]],
                        dropLocn: str = None, actLocn: str = None, resvLocn: str = None, o_intType: int = None):
        """Putaway multiple ilpns
        """
        tran_name = RF_TRAN.get('rf', 'putawayCase')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        final_locns = []
        final_lpnStats = []

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inb')

        for i in range(len(ilpn)):
            self.assertScreenTextExist('LPN:')
            self.sendData(ilpn[i], isEnterIfLT20=True)

            if dropLocn is not None:
                self.assertScreenTextExist('Rloc:')
                self.assertScreenTextExist('WG/WA:')
                wgwa = self.readDataForTextInLine('WG/WA:').strip()
                wgwaSplit = wgwa.split('/')
                workG = wgwaSplit[0].strip()
                workA = wgwaSplit[1].strip()
                _resvLoc = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
                final_locns.append(_resvLoc)
                self.sendData(_resvLoc, isEnter=True)
                final_lpnStats.append(LPNFacStat.ILPN_ALLOCATED)
            elif actLocn is not None:
                self.assertScreenTextExist('Aloc:')
                final_locns.append(actLocn)
                self.sendData(actLocn, isEnter=True)
                final_lpnStats.append(LPNFacStat.ILPN_CONSUMED_TO_ACTV)
            elif resvLocn is not None:
                self.assertScreenTextExist('Rloc:')
                final_locns.append(resvLocn)
                self.sendData(resvLocn, isEnter=True)
                final_lpnStats.append(LPNFacStat.ILPN_PUTAWAY)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(ilpn)):
            DBLib().assertLPNHdr(i_lpn=ilpn[i], o_facStatus=final_lpnStats[i], o_currLocn=final_locns[i])
            for j in range(len(o_item[i])):
                DBLib().assertLPNDtls(i_lpn=ilpn[i], i_itemBrcd=o_item[i][j], o_receivedQty=o_qty[i][j])
                DBLib().assertAllocDtls(i_cntr=ilpn[i], i_intType=o_intType, i_itemBrcd=o_item[i][j], o_taskPriority=50)
            taskID = DBLib().getTaskIdByORCond(taskCmplRefNbr=ilpn[i], taskGenRefNbr=ilpn[i], cntr=ilpn[i], intType=o_intType)
            DBLib().assertTaskHdr(i_task=taskID, o_status=TaskHdrStat.COMPLETE)

        '''LM validation'''
        # DBLib().assertLaborMsgHdr(i_refNbr=, i_actName=)

    def putawayPalletTran(self, ilpn: str, taskGrp: str = None, o_item: str = None, o_qty: int = None,
                          resvLocn: str = None, isSubstitueLPN: bool = None, isToWarnLocnForDiffItem: bool = None,
                          isAssertCCTask: bool = None):
        """Putaway 1 pallet ILPN
        isSubstitueLPN means isSubstitueLocn
        """
        tran_name = RF_TRAN.get('rf', 'putawayPallet')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        sysDestResvLocn = None
        if isSubstitueLPN:
            sysDestResvLocn = DBLib().getDestLocnForLpn(lpn=ilpn)
            DBLib()._updateLocnCCPending(i_locnBrcd=sysDestResvLocn, u_isCCPending=False)

        self.goToHomeScreen()
        if taskGrp is not None:
            self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'I')
        self.assertScreenTextExist('LPN:')
        self.sendData(ilpn, isEnterIfLT20=True)

        screentxt = self.readScreen()
        if screentxt.count('Pallet is open,') and screentxt.count('continue?'):
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('Rloc:')
        if isSubstitueLPN:
            self.sendData(self.KEY_CTRL_T_SubstituteLpn)
        
        self.assertScreenTextExist('Rloc:')
        if resvLocn is None:
            resvLocn = self.readDataForTextInLine('Rloc:').strip()
        self.sendData(resvLocn, isEnter=True)

        if isToWarnLocnForDiffItem:
            self.assertScreenTextExist('Locn Temp')
            self.assertScreenTextExist('dedicated to a')
            self.assertScreenTextExist('diff Item')
            self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.acceptMsgIfExist(['Locn Temp', 'dedicated to a', 'diff Item'])
        self.acceptMsgIfExist(['Exceed Max UOM', 'Location?'])

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=resvLocn)
        DBLib().assertLPNDtls(i_lpn=ilpn, i_itemBrcd=o_item, o_qty=o_qty, o_receivedQty=o_qty)
        DBLib().assertAllocDtls(i_cntr=ilpn, i_intType=11, i_itemBrcd=o_item, o_taskPriority=50, o_statCode=AllocStat.TASK_DETAIL_CREATED)
        if isAssertCCTask:
            DBLib().assertCCTask(i_locnBrcd=sysDestResvLocn, i_intType=101, o_status=90)

    def userDirKeepPallet(self, palletLpn: str, resvLoc: str):
        """Putaway pallet to Resv
        """
        tran_name = RF_TRAN.get('rf', 'userDirKeepPallet')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('LPN:')
        self.sendData(palletLpn, isEnter=True)
        self.assertScreenTextExist('Pallet is open')
        self.assertScreenTextExist('continue?')
        self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('Rloc:')
        self.sendData(resvLoc, isEnter=True)

        probable_warn_msgs = [['Exceed Max UOM', 'Location?'], ['Locn Temp', 'dedicated to a', 'diff Item']]
        for i in probable_warn_msgs:
            screentxt = self.readScreen()
            msg_found = False
            for j in i:
                msg_found = True if j in screentxt else False
                if not msg_found:
                    break
            if msg_found:
                self.sendData(self.KEY_CTRL_A_AcceptWarning)
                    
        # screentxt = self.readScreen()
        # if 'Exceed Max UOM' in screentxt and 'Location?' in screentxt:
        #     self.sendData(self.KEY_CTRL_A_AcceptWarning)
        #
        # screentxt = self.readScreen()
        # if screentxt.count('Locn Temp') and screentxt.count('dedicated to a') and screentxt.count('diff Item'):
        #     self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=palletLpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=resvLoc)

    def makingSpotsActvTran(self, palletId: str, o_itemBrcd: str, o_qty: int, o_pullLocn: str, actvLocn: str = None):
        """Scan pallet to temp actv locn
        """
        tran_name = RF_TRAN.get('rf', 'makingSpotsActv')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('LPN:')
        self.sendData(palletId, isEnter=True)
        if actvLocn is None:
            actvLocn = self.readDataForTextInLine('Aloc:')
        self.sendData(actvLocn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=palletId, o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV, o_currLocn=actvLocn)
        DBLib().assertLPNDtls(i_lpn=palletId, i_itemBrcd=o_itemBrcd, o_receivedQty=o_qty)
        DBLib().assertTaskHdr(i_cntr=palletId, o_intType=1)
        DBLib().assertTaskDtls(i_cntrNbr=palletId, i_itemBrcd=o_itemBrcd, i_intType=1)
        DBLib().assertAllocDtls(i_cntr=palletId, i_intType=1, i_itemBrcd=o_itemBrcd, o_taskPriority=50)
        # DBLib().assertLaborMsgHdr()

    def makingSpotsResvTran(self, palletId: str, o_itemBrcd: str, o_qty: int, resvLoc: str = None):
        """Scan pallet to temp reserve locn
        """
        tran_name = RF_TRAN.get('rf', 'makingSpotsResv')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if resvLoc is None:
            resvLocnRow = DBLib().getResvLocn(noOfLocn=1)
            resvLoc = resvLocnRow[0].get('LOCN_BRCD')

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('LPN:')
        self.sendData(palletId, isEnter=True)
        self.sendData(resvLoc, isEnter=True)
        screentxt = self.readScreen()
        if screentxt.count('Exceed Max UOM') and screentxt.count('Location?')>0:
            self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=palletId, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=resvLoc)
        DBLib().assertLPNDtls(i_lpn=palletId, i_itemBrcd=o_itemBrcd, o_qty=o_qty)
        
    def xferReceiveILPNTran(self, asn: str, itemBrcd: str, qty: int, o_po: str, iLPN: str = None):
        """Transfer Receive iLPN
        """
        tran_name = RF_TRAN.get('rf', 'xferRcvLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        if iLPN is None:
            iLPN = DBLib().getNewILPNNum()
        self.sendData(iLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        DBLib().assertLPNDtls(i_lpn=iLPN, i_itemBrcd=itemBrcd, o_qty=qty, o_receivedQty=qty)

        DBLib().assertASNHdr(i_asn=asn, o_status=30)
        DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemBrcd, o_receivedQty=qty, o_dtlStatus=16)

        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='617')

        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV ILPN CA')
        DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV ILPN CA', i_lpn=iLPN, i_itemBrcd=itemBrcd)

    def xferReceivePalletTran(self, workGrp: str, workArea: str, asn: str, itemBrcd: str, qty: int,
                          isMatchRFLocn: bool = False, dropLocn: str = None, actLocn: str = None, resvLocn: str = None,
                          dockDoor: str = None, blindIlpn: str = None,
                          o_asnStatus: int = None, o_po: str = None, o_intType: int = None, palletFacStat: LPNFacStat = None):
        """Receive 1 SKU to 1 pallet iLPN from an ASN, Put to drop/actv locn
        """
        tran_name = RF_TRAN.get('rf', 'xferRcvPallet')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        dockDoor, dbDockDoor = DBLib().getOpenDockDoor(workGrp=workGrp, workArea=workArea)
        if blindIlpn is None:
            blindIlpn = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Dock Door:')
        self.sendData(dockDoor, isEnter=True)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(blindIlpn, isEnter=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)
        if dropLocn is not None:
            self.assertScreenTextExist('Rloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('WG/WA:' + dropLocn)
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn, isEnter=True)
        elif actLocn is not None:
            self.assertScreenTextExist('Aloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('Aloc:' + actLocn)
            self.sendData(actLocn, isEnter=True)
        elif resvLocn is not None:
            self.assertScreenTextExist('Rloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('Rloc:' + resvLocn)
            self.sendData(resvLocn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_B_CloseTrailer)
        self.assertScreenTextExist('ASN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        allocStatCode = AllocStat.TASK_DETAIL_CREATED
        pullLocn = dbDockDoor
        currLocn = actLocn if actLocn else dropLocn if dropLocn is not None else resvLocn
        lpnDtlQty = 0 if actLocn else qty
        taskStatCode = TaskHdrStat.IN_DROP_ZONE if dropLocn else TaskHdrStat.COMPLETE

        if o_asnStatus is not None:
            DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
        DBLib().assertASNDtls(i_asn=asn, i_itemBrcd=itemBrcd, i_po=o_po, o_receivedQty=qty, o_dtlStatus=16)
        DBLib().assertLPNHdr(i_lpn=blindIlpn, o_facStatus=palletFacStat, o_currLocn=currLocn)
        DBLib().assertLPNDtls(i_lpn=blindIlpn, i_itemBrcd=itemBrcd, o_qty=lpnDtlQty, o_receivedQty=qty)
        '''Alloc, task validation'''
        DBLib().assertAllocDtls(i_cntr=blindIlpn, i_itemBrcd=itemBrcd, i_intType=o_intType, o_taskPriority=50, o_statCode=allocStatCode,
                                o_pullLocn=pullLocn)
        DBLib().assertTaskDtls(i_cntrNbr=blindIlpn, i_itemBrcd=itemBrcd, i_intType=o_intType, o_pullLocn=pullLocn)
        taskId = DBLib().getTaskIdByORCond(taskGenRefNbr=blindIlpn, taskCmplRefNbr=blindIlpn, cntr=blindIlpn, intType=o_intType)
        DBLib().assertTaskHdr(i_task=taskId, o_intType=o_intType, o_status=taskStatCode)
        '''Pix validation 100, 617, 606'''
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindIlpn, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindIlpn, i_tranType='617')

        return blindIlpn, dbDockDoor

    def xferReceiveSortMixILPNTran(self, asn: str, items: list[str], qty: list[int],
                                   sortZone: str, blindIlpn: list[str] = None, blindPallet: str = None,
                                   o_po: str = None, o_intType: int = None):
        """Transfer Rcv and Srt Mix ILPN
        """
        tran_name = RF_TRAN.get('rf', 'xferRcvSortMixLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        itemList = items
        qtyList = qty if type(qty) == list else [qty]
        fetchedSortLocn = None

        self.goToHomeScreen()
        self.goToTransaction(tran_name)

        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        for i in range(0, len(itemList)):
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemList[i], isEnter=True)
            self.assertScreenTextExist('Qty:')
            self.sendData(qtyList[i], isEnter=True)
            if i == 0:
                self.assertScreenTextExist('Sorting Zone:')
                self.sendData(sortZone)
            self.assertScreenTextExist('iLPN:')
            self.assertScreenTextExist('Rloc:')
            if blindIlpn is None:
                blindIlpn = DBLib().getNewILPNNum()
            self.sendData(blindIlpn, isEnterIfLT20=True)
            if i == 0:
                self.assertScreenTextExist('Pallet:')
                if blindPallet is None:
                    blindPallet = DBLib().getNewInPalletNum()
                self.sendData(blindPallet, isEnterIfLT20=True)
            if i == 0:
                screentxt = self.readScreen()
                if screentxt.count("Warning!") > 0 and screentxt.count("Pallet does not") > 0:
                    self.sendData(self.KEY_CTRL_A_AcceptWarning)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=blindIlpn, o_facStatus=LPNFacStat.ILPN_ALLOCATED, o_parentLpn=blindPallet)
        DBLib().assertLPNHdr(i_lpn=blindPallet, o_currLocn=fetchedSortLocn)
        DBLib().assertASNHdr(i_asn=asn, o_status=30)
        for i in range(0, len(itemList)):
            DBLib().assertLPNDtls(i_lpn=blindIlpn, i_itemBrcd=itemList[i], o_qty=qtyList[i], o_receivedQty=qtyList[i])
            DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemList[i], o_receivedQty=qtyList[i], o_dtlStatus=16)
            DBLib().assertAllocDtls(i_cntr=blindIlpn, i_intType=o_intType, i_itemBrcd=itemList[i], o_taskPriority=50, o_pullLocn=fetchedSortLocn)

            defaultLpn = DBLib().getDefaultLpnFromASNItem(asn=asn, itemBrcd=itemList[i])
            DBLib().assertPix(i_itemBrcd=itemList[i], i_caseNbr=defaultLpn, i_tranType='100')
            DBLib().assertPix(i_itemBrcd=itemList[i], i_caseNbr=defaultLpn, i_tranType='617')

        '''LM validation (labor_msg_id 63747360 63747359)'''
        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV PLT MIX')
        for i in range(0, len(itemList)):
            DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV PLT MIX', i_itemBrcd=itemList[i])

        return blindIlpn, blindPallet, fetchedSortLocn

    def packLocateLPN(self, aloc: str, qty: int, rloc: str, o_item: str, iLpn: str = None):
        """Pack and locate ilpn to resv locn
        """
        tran_name = RF_TRAN.get('rf', 'packLocateLPN')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        if iLpn is None:
            iLpn = DBLib().getNewILPNNum()
        self.assertScreenTextExist('iLPN:')
        self.sendData(iLpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Aloc:')
        self.sendData(aloc, isEnter=True)
        self.assertScreenTextExist('Qty Pckd:')
        self.sendData(qty, isEnter=True)
        self.assertScreenTextExist('Rloc:')
        self.sendData(rloc, isEnter=True)

        self.acceptMsgIfExist(['Locn Temp', 'dedicated to a', 'diff Item'])

        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY, o_currLocn=rloc)
        DBLib().assertLPNDtls(i_lpn=iLpn, i_itemBrcd=o_item, o_qty=qty)

        return iLpn

    def receivePalletFLRTran(self, currWG: str, currWA: str, asn: str,
                             itemBrcd: str, qty: int, o_po: str, dockDoor: str = None,
                             blindPallet: str = None, dropLocn: str = None, actvLocn: str = None,
                             isMatchRFLocn: bool = False, o_intType: int = None):
        """Receive 1 SKU to 1 pallet iLPN from an ASN, Put to drop locn
        """
        tran_name = RF_TRAN.get('rf', 'rcvPalletFLR')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        dbDockDoor = None
        if dockDoor is None:
            dockDoor, dbDockDoor = DBLib().getOpenDockDoor(workGrp=currWG, workArea=currWA)

        if blindPallet is None:
            blindPallet = DBLib().getNewInPalletNum()
            
        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'In')

        self.assertScreenTextExist('Dock Door:')
        self.sendData(dockDoor, isEnter=True)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(blindPallet, isEnter=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)
        if dropLocn is not None:
            self.assertScreenTextExist('Rloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('WG/WA:' + dropLocn)
            self.assertScreenTextExist('WG/WA:')
            wgwa = self.readDataForTextInLine('WG/WA:').strip()
            wgwaSplit = wgwa.split('/')
            workG = wgwaSplit[0].strip()
            workA = wgwaSplit[1].strip()
            dropLocn = DBLib().getLocnByWGWA(workGrp=workG, workArea=workA)
            self.sendData(dropLocn, isEnter=True)
        elif actvLocn is not None:
            self.assertScreenTextExist('Aloc:')
            if isMatchRFLocn:
                self.assertScreenTextExist('Aloc:' + actvLocn)
            self.sendData(actvLocn, isEnter=True)

        '''Validation'''
        DBLib().assertASNHdr(i_asn=asn, o_status=30)
        DBLib().assertASNDtls(i_asn=asn, i_itemBrcd=itemBrcd, i_po=o_po, o_receivedQty=qty, o_dtlStatus=16)
        DBLib().assertLPNHdr(i_lpn=blindPallet, o_facStatus=LPNFacStat.ILPN_ALLOCATED, o_currLocn=dropLocn)
        DBLib().assertLPNDtls(i_lpn=blindPallet, i_itemBrcd=itemBrcd, o_receivedQty=qty)
        DBLib().assertAllocDtls(i_cntr=blindPallet, i_itemBrcd=itemBrcd, i_intType=o_intType, o_taskPriority=50, o_statCode=AllocStat.TASK_DETAIL_CREATED)
        pullLocn = dbDockDoor
        DBLib().assertTaskDtls(i_cntrNbr=blindPallet, i_itemBrcd=itemBrcd, i_intType=o_intType, o_pullLocn=pullLocn)
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindPallet, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=blindPallet, i_tranType='617')
        lmRefNbr = asn
        DBLib().assertLaborMsgHdr(i_refNbr=lmRefNbr, i_actName='RCV PLT FLR')
        DBLib().assertLaborMsgHdr(i_refNbr=lmRefNbr, i_actName='RCV PLT FLR')
        DBLib().assertLaborMsgDtl(i_refNbr=lmRefNbr, i_actName='RCV PLT FLR', i_lpn=blindPallet, i_itemBrcd=itemBrcd)

    def locnInquiryTran(self, locn: str, items: list[list[str]], qty: list[list[int]], lpn: list[str] = None):
        """Locn inquiry
        """
        tran_name = RF_TRAN.get('rf', 'locnInquiry')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tranName=tran_name, dispModule='In')

        self.assertScreenTextExist('Locn:')
        self.sendData(locn, isEnter=True)
        for i in range(0, len(items)):
            for j in range(0, len(items[i])):
                self.assertScreenTextExist('loc:' + locn)
                if lpn is not None:
                    self.assertScreenTextExist('iLPN:')
                    self.assertScreenTextExist(lpn[i])
                self.assertScreenTextExist('Item:' + items[i][j])
                self.assertScreenTextExist('Qty:' + str(qty[i][j]))
                if j < len(items[i]) - 1:
                    self.sendData(self.KEY_CTRL_D_GoPageDown)

        self.assertScreenTextExist('Item:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def olpnInquiryTran(self, oLpn: str, items: list[str], qty: list[int], facStatDesc: str, order: str):
        """Olpn inquiry
        """
        tran_name = RF_TRAN.get('rf', 'olpnInquiry')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Out')

        self.assertScreenTextExist('oLPN:')
        self.sendData(oLpn)
        for i in range(len(items)):
            self.assertScreenTextExist('Item:' + items[i])
            self.assertScreenTextExist('Qty:' + str(qty[i]))
            self.assertScreenTextExist('Sts:' + facStatDesc)
            self.assertScreenTextExist(str(order))
            if i < len(items) - 1:
                self.sendData(self.KEY_CTRL_D_GoPageDown)
        self.assertScreenTextExist('LPN :')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def packILpnTrsnlTran(self, itemBrcd: str, qty: int, tranInvnType: int):
        """Pack the transitional inventory to the blind iLpn
        """
        tran_name = RF_TRAN.get('rf', 'packILPNTrnsl')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        blindIlpn = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('iLPN:')
        self.sendData(str(blindIlpn), isEnter=True)
        self.assertScreenTextExist('TI Type:')
        self.sendData(str(tranInvnType))
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(str(itemBrcd), isEnter=True)
        self.assertScreenTextExist('Qty Pckd:')
        self.sendData(str(qty), isEnter=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(self.KEY_CTRL_E_EndIlpn)
        self.assertScreenTextExist('iLPN:')

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=blindIlpn, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY)
        DBLib().assertLPNDtls(i_lpn=blindIlpn, i_itemBrcd=itemBrcd, o_qty=qty)

    def exceptionHandlingTran(self, asn: str, itemBrcd: str, qty: int, sortZone: str, iLPN: str = None,
                              pallet: str = None, o_asnStatus: int = None, o_po: str = None,
                              o_facStatus: LPNFacStat = None, o_intType: int = None, o_assertTask: bool = None):
        """Receive and Sort 1 SKU to 1 iLPN from an ASN
        """
        tran_name = RF_TRAN.get('rf', 'exceptionHandling')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if iLPN is None:
            iLPN = DBLib().getNewILPNNum()
        if pallet is None:
            pallet = DBLib().getNewInPalletNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)
        self.assertScreenTextExist('Sorting Zone:')
        self.sendData(sortZone)
        self.assertScreenTextExist('iLPN:')
        sortLocn = self.readDataForTextInLine('Rloc:').strip()
        self.sendData(iLPN, isEnterIfLT20=True)
        self.sendData(pallet, isEnterIfLT20=True)
        self.assertScreenTextExist('Pallet does not')
        self.assertScreenTextExist('exist. Create?')
        self.sendData(self.KEY_CTRL_A_AcceptWarning)

        self.assertScreenTextExist('Item Barcode:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=o_facStatus, o_currLocn=sortLocn, o_parentLpn=pallet)
        DBLib().assertLPNDtls(i_lpn=iLPN, i_itemBrcd=itemBrcd, o_qty=qty, o_receivedQty=qty)
        DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
        DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemBrcd, o_receivedQty=qty, o_dtlStatus=16)
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_tranType='617')
        DBLib().assertAllocDtls(i_cntr=iLPN, i_intType=o_intType, i_itemBrcd=itemBrcd, o_taskPriority=50, o_pullLocn=sortLocn)
        if o_assertTask:
            DBLib().assertTaskDtls(i_cntrNbr=iLPN, i_itemBrcd=itemBrcd, i_intType=o_intType, o_pullLocn=sortLocn, o_statCode=TaskDtlStat.UNASSIGNED)
            DBLib().assertTaskHdr(i_taskGenRefNbr=iLPN, o_intType=o_intType)
        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='EXCEPT HNDL')
        DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='EXCEPT HNDL', i_itemBrcd=itemBrcd, o_qty=qty)

    def receiveILPNExceptionHandlingTran(self, asn: str, itemBrcd: str, qty: int, iLPN: str = None,
                                         o_asnStatus: int = None, o_lpnFacStat: LPNFacStat = None, o_po: str = None):
        """Receive 1 SKU to 1 iLPN from an ASN
        """
        tran_name = RF_TRAN.get('rf', 'rcviLPNExcpHndl')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if iLPN is None:
            iLPN = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('ASN:')
        self.sendData(asn, isEnter=True)
        self.assertScreenTextExist('LPN:')
        self.sendData(iLPN, isEnterIfLT20=True)
        self.assertScreenTextExist('Item Barcode:')
        self.sendData(itemBrcd, isEnter=True)
        self.assertScreenTextExist('Qty:')
        self.sendData(str(qty), isEnter=True)

        self.assertScreenTextExist('LPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        DBLib().assertLPNHdr(i_lpn=iLPN, o_facStatus=o_lpnFacStat)
        DBLib().assertLPNDtls(i_lpn=iLPN, i_itemBrcd=itemBrcd, o_qty=qty, o_receivedQty=qty)
        DBLib().assertASNHdr(i_asn=asn, o_status=o_asnStatus)
        DBLib().assertASNDtls(i_asn=asn, i_po=o_po, i_itemBrcd=itemBrcd, o_receivedQty=qty, o_dtlStatus=16)
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='100')
        DBLib().assertPix(i_itemBrcd=itemBrcd, i_caseNbr=iLPN, i_tranType='617')
        '''LM validation'''
        DBLib().assertLaborMsgHdr(i_refNbr=asn, i_actName='RCV ILPN EX HND')
        DBLib().assertLaborMsgDtl(i_refNbr=asn, i_actName='RCV ILPN EX HND', i_lpn=iLPN, i_itemBrcd=itemBrcd)

        return iLPN

    # def plateBuildByLocation(self, palletLpn, rLoc):
    #     tran_name = RF_TRAN.get('rf', 'plateBuildByLocation')
    #     Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)
    #
    #     self.goToTransaction(tran_name)
    #     self.assertScreenTextExist('Pallet:')
    #     self.sendData(palletLpn)
    #     self.assertScreenTextExist('iLpn:')
    #     self.sendData(Keys.CONTROL + 'b')
    #     self.assertScreenTextExist('RLoc:')
    #     self.sendData(rLoc)
    #     self.assertScreenTextExist('Pallet:')
    #     self.sendData(Keys.CONTROL + 'x')  # To main menu

    def plateBuildByLocnTran(self, lpns: list[str], palletid: str = None):
        """"""
        tran_name = RF_TRAN.get('rf', 'plateBuildByLocation')
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        if palletid is None:
            palletid = DBLib().getNewInPalletNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name)
        self.assertScreenTextExist('Pallet:')
        self.sendData(palletid,isEnter=True)
        for i in range(len(lpns)):
            self.assertScreenTextExist('iLPN:')
            self.sendData(lpns[i],isEnterIfLT20=True)
            self.assertScreenTextExist('Previous iLPN:')
            self.assertScreenTextExist(lpns[i])
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

        '''Validation'''
        for i in range(len(lpns)):
            DBLib().assertLPNHdr(o_parentLpn=palletid, o_facStatus=LPNFacStat.ILPN_IN_INVENTORY_NOT_PUTAWAY, i_lpn=lpns[i])

    def verifyTaskExistForUserByCtrl(self, taskId: str, isRFUser2: bool = None):
        """Verify task for the user by ctrl+s
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl S')

        self.goToHomeScreen(isRFUser2=isRFUser2)
        self.assertScreenTextExist('Inbound')
        isTaskFound = bool

        self.sendData(self.KEY_CTRL_S_TaskSel)
        self.assertScreenTextExist('Task')

        while True:
            screentxt = self.readScreen().replace('\n', ' ')
            if screentxt.count(taskId) == 1:
                isTaskFound = True
                break
            else:
                screentxt = self.readScreen()
                self.sendData(self.KEY_CTRL_D_GoPageDown)
                if screentxt == self.readScreen():
                    isTaskFound = False
                    break

        assert isTaskFound, f"Task {taskId} not found for user"

    def verifyTaskNotExistForUserByCtrl(self, taskId:str):
        """Verify task not for user with ctrl+e
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, 'Ctrl E')

        self.goToHomeScreen()
        self.assertScreenTextExist('Inbound')
        isTaskNotFound = False

        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_UP)
        self.sendData(taskId,isEnter=True)

        screentxt = self.readScreen()
        assert 'Error' not in screentxt, f"Error found in screen " + screentxt
        
        screentxt = screentxt.replace('\n','').replace('x','')
        if screentxt.count('Task is Already')==1 and screentxt.count('Assigned')==1:
            isTaskNotFound=True

        assert isTaskNotFound, f"Task {taskId} found for user"

    def paramikoFunc_createIlpn(self, reasonCode, item: list[str], qty: list[int], ilpn: str = None):
        """Create 1 iLPN with 1 or more items
        """
        #
        tran_name = RF_TRAN.createILPN
        Logging.capture_action_func_start(inspect.currentframe(), self.logger, tran_name)

        itemList = item if type(item) == list else [item]
        qtyList = qty if type(qty) == list else [qty]
        if ilpn is None:
            ilpn = DBLib().getNewILPNNum()

        self.goToHomeScreen()
        self.goToTransaction(tran_name, 'Inv')
        self.assertScreenTextExist('CREATE iLPN')
        self.assertScreenTextExist('iLPN:')
        self.sendData(ilpn, isEnterIfLT20=True)
        self.assertScreenTextExist('Reason Code:')
        self.sendData(str(reasonCode))
        if len(str(reasonCode)) < 2:
            self.sendData(self.KEY_ENTER)
        self.assertScreenTextExist('Reference:')
        self.sendData('Test', isEnter=True)
        for i in range(0, len(itemList)):
            self.assertScreenTextExist('Item Barcode:')
            self.sendData(itemList[i], isEnter=True)
            self.assertScreenTextExist('Item:' + itemList[i])
            self.assertScreenTextExist('Qty Pckd:')
            self.sendData(str(qtyList[i]), isEnter=True)
        self.sendData(self.KEY_CTRL_E_EndIlpn)
        self.assertScreenTextExist('iLPN:')
        self.sendData(self.KEY_CTRL_X_ExitTran)

    def paramikoFunc_checkTask(self, taskGrp, taskId, pallet):
        # self.goToHomeScreen()
        # self._changeTaskGroupByCtrl(taskGroup=taskGrp)

        self.goToHomeScreen()
        self.sendData(self.KEY_CTRL_E_EnterTask)
        self.assertScreenTextExist('Task#:')
        self.sendData(self.KEY_UP)
        self.sendData(self.KEY_DOWN)
        self.sendData(self.KEY_UP)
        self.assertScreenTextExist('Task#:')
        self.sendData(str(taskId), isEnter=True)
        self.sendData(self.KEY_CTRL_W_GoBack)
        self.assertScreenTextExist('Pallet:')
        # self.sendData(str(pallet), isEnterIfLT20=True)
