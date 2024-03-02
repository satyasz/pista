import inspect
import os
from typing import Union

from apps.wms.app_db_lib import DBLib, OrdConsRuleData
from apps.wms.page.wm_base_page import WMBasePage
from apps.wms.page.wm_home_page import WMHomePage
from core.config_service import ENV_CONFIG
from core.log_service import Logging
from apps.wms.app_status import DOStat, TaskHdrStat, LPNFacStat, AllocStat, TaskDtlStat
from core.thread_data_handler import RuntimeXL, RuntimeAttr


class WMRunWavePage(WMBasePage):
    logger = Logging.get(__qualname__)

    PAGE = 'Run Waves'
    MODULE = 'Distribution'
    _TITLE_XPATH = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Run Waves')]"

    '''Template page'''

    _DESC_SEARCH_TEXTBOX = "//input[@id = 'dataForm:listView:filterId:field10value1' and @type = 'text']"
    _SEARCH_APPLY_BTN = "//input[@class= 'btn  groupBtn']"
    _ALL_DISPLAYED_TEMPLATES_NAME = "//table[@id='dataForm:listView:dataTable_body']//tr//td/span[contains(@id,'dataForm:listView:dataTable') and contains(@id,'wvdesc')]"
    _CHECKBOX_FOR_TEMPLATE = "(//table[@id='dataForm:listView:dataTable_body']//tr//td/span[contains(@id,'dataForm:listView:dataTable') and contains(@id,'wvdesc') and text()='#TEMPLATE_NAME#'])[1]/parent::td/parent::tr//input[@type='checkbox' and contains(@id,'checkAll')]"
    _RUN_WAVE_BTN = "//input[@id = 'rmButton_1RunWave1_100197000' and @value = 'Run Wave' ]"

    '''Details page (Rule section)'''

    _TITLE_FOR_RULE_PAGE = "//div[contains(@class,'window-header-title')]//div[contains(@class,'title-text-default')][contains(text(),'Run Waves -')]"
    _NO_OF_RULES = "//div[contains(@id,'PANEL_ruleHdrPanel_top')]//div[contains(@class,'advtbl_contr_body')]//tr[contains(@class,'advtbl_row -dg_t')]"
    _RULE_NAME_IN_RULE_ROW = "//div[contains(@id,'PANEL_ruleHdrPanel_top')]//div[contains(@class,'advtbl_contr_body')]//tr[contains(@class,'advtbl_row -dg_t')][#ROW_NUM#]//td[contains(@class,'advtbl_col advtbl_body_col')]//span[contains(@id,'ruleNameText')]"
    _RADIO_BTN_IN_RULE_ROW = "//div[contains(@id,'PANEL_ruleHdrPanel_top')]//div[contains(@class,'advtbl_contr_body')]//tr[contains(@class,'advtbl_row -dg_t')][#ROW_NUM#]//td[contains(@class,'advtbl_col advtbl_body_col')]//input[@type='radio' and contains(@id,'checkAll')]"
    _USE_CHECKBOX_IN_RULE_ROW = "//div[contains(@id,'PANEL_ruleHdrPanel_top')]//div[contains(@class,'advtbl_contr_body')]//tr[contains(@class,'advtbl_row -dg_t')][#ROW_NUM#]//td[contains(@class,'advtbl_col advtbl_body_col')]//input[@type='checkbox' and contains(@id,'Checkbox')]"

    '''Details page (Definition section)'''

    _NO_OF_SELECTION_ROWS = "//table[contains(@id,'ruleSelDtlDataTable_body')]//tr[contains(@class,'advtbl_row -dg_tr')]"
    _START_BRACKET_IN_FIRST_SEL_ROW = "(//table[contains(@id,'ruleSelDtlDataTable_body')]//tr//input[@type='text'])[1]"
    _END_BRACKET_IN_LAST_SEL_ROW = "(//table[contains(@id,'ruleSelDtlDataTable_body')]//tr//input[@type='text'])[last()]"
    _LEFT_EXPR_DROPDOWN_IN_SEL_ROW = "(//table[contains(@id,'ruleSelDtlDataTable_body')]//tr[contains(@class,'-dg_')][#ROW_NUM#]//td[3]//select)[last()]"
    _RIGHT_EXPR_TB_IN_SEL_ROW = "(//table[contains(@id,'ruleSelDtlDataTable_body')]//tr[contains(@class,'-dg_')][#ROW_NUM#]//td[5]//input)[last()]"
    _OPERATOR_IN_SEL_ROW = "(//table[contains(@id,'ruleSelDtlDataTable_body')]//tr[contains(@class,'-dg_')][#ROW_NUM#]//td[4]//select)[last()]"
    _AND_OR_DD_IN_SEL_ROW = "//table[contains(@id,'ruleSelDtlDataTable_body')]//tr[contains(@class,'-dg_')][#ROW_NUM#]//td[7]//select"
    _ADD_SELECTION_ROW_BTN = "//input[contains(@id,'dataForm:ruleSelAddButton')]"

    '''Details page'''

    _WAVE_SUBMIT_BTN = "//input[@type='button' and contains(@id,'SubmitWave')]"
    _GENERATED_WAVE_NBR = "//a[contains(@id,'dataForm:AwvNbrRun')]"

    def __init__(self, driver, isPageOpen: bool = False):
        """Opens run wave UI if not opened and maximize it"""
        super().__init__(driver, None)
        if not isPageOpen:
            WMHomePage(driver, isPageOpen=isPageOpen).openMenuPage(self.PAGE, self.MODULE)
            super().__init__(driver, self._TITLE_XPATH)
            self.wait_for(5)
            self.maximizeMenuPage()

    def filterByTemplate(self, template: str):
        # self.switch_default_content()
        # self.switch_frame(0)
        self.fill_by_xpath(self._DESC_SEARCH_TEXTBOX, template)
        self.click_by_xpath(self._SEARCH_APPLY_BTN)
        self.wait_for(2)
        self._assertTemplateDisplay(template)
        # self.switch_default_content()

    def _assertTemplateDisplay(self, template: str):
        is_template_found = False
        all_displayed_templates = self.get_webelements(self._ALL_DISPLAYED_TEMPLATES_NAME)
        displayed_templates_cnt = len(all_displayed_templates)
        for i in range(0, displayed_templates_cnt):
            if all_displayed_templates[i - 1].text == template:
                is_template_found = True
                break
        assert is_template_found, 'Template not found: ' + template

    def _selectRuleInDetailPage(self, ruleName):
        no_of_rules = len(self.get_webelements(self._NO_OF_RULES))
        rule_row_num = ''
        for i in range(1, no_of_rules + 1):
            rule_in_row = self.get_webelement(self._RULE_NAME_IN_RULE_ROW.replace('#ROW_NUM#', str(i)))

            '''Find provided rule row num'''
            if rule_in_row.text == ruleName:
                rule_row_num = str(i)
            else:
                '''Do not use other rules'''
                if self.is_checked_by_xpath(self._USE_CHECKBOX_IN_RULE_ROW.replace('#ROW_NUM#', str(i))):
                    self.click_by_xpath(self._USE_CHECKBOX_IN_RULE_ROW.replace('#ROW_NUM#', str(i)))
        assert rule_row_num != '', f"Provided wave rule {ruleName} not found in UI"

        '''Use provided rule'''
        self.click_by_xpath(self._RADIO_BTN_IN_RULE_ROW.replace('#ROW_NUM#', rule_row_num))
        if not self.is_checked_by_xpath(self._USE_CHECKBOX_IN_RULE_ROW.replace('#ROW_NUM#', rule_row_num)):
            self.click_by_xpath(self._USE_CHECKBOX_IN_RULE_ROW.replace('#ROW_NUM#', rule_row_num))

    def _addOrdersForSelection(self, orders: Union[str, list]):
        order_list = orders if type(orders) == list else [orders]
        no_of_sel_rows = len(self.get_webelements(self._NO_OF_SELECTION_ROWS))
        assert no_of_sel_rows == 1, 'Default no. of selection rows should be 1, actual: ' + str(no_of_sel_rows)

        for i in range(0, len(order_list)):
            if i > 0:
                self.click_by_xpath(self._ADD_SELECTION_ROW_BTN)
                if i < len(order_list) - 1:
                    self.select_in_dropdown_by_xpath(self._AND_OR_DD_IN_SEL_ROW.replace('#ROW_NUM#', str(i)), 'Or')
            self.select_in_dropdown_by_xpath(self._LEFT_EXPR_DROPDOWN_IN_SEL_ROW.replace('#ROW_NUM#', str(i + 1)),
                                             'Order number')
            self.select_in_dropdown_by_xpath(self._OPERATOR_IN_SEL_ROW.replace('#ROW_NUM#', str(i + 1)), 'equals')
            self.clear_textbox_by_value(self._RIGHT_EXPR_TB_IN_SEL_ROW.replace('#ROW_NUM#', str(i + 1)))
            self.fill_by_xpath(self._RIGHT_EXPR_TB_IN_SEL_ROW.replace('#ROW_NUM#', str(i + 1)), order_list[i])

        if len(order_list) > 1:
            self.fill_by_xpath(self._START_BRACKET_IN_FIRST_SEL_ROW, '(')
            self.fill_by_xpath(self._END_BRACKET_IN_LAST_SEL_ROW, ')')

    def _runWave(self, template: str, rule: str, orders: list[str]) -> str:
        """Run 1 wave for list of orders"""
        ordersList = [orders] if type(orders) == str else orders

        DBLib()._printWaveTemplateConfig(template)
        DBLib()._presetWaveTemplateRuleForOrders(template, rule, ordersList)

        self.switch_frame(0)
        self.filterByTemplate(template)
        self.click_by_xpath(self._CHECKBOX_FOR_TEMPLATE.replace('#TEMPLATE_NAME#', template))
        self.click_by_xpath(self._RUN_WAVE_BTN)
        self._selectRuleInDetailPage(rule)
        # self._addOrdersForSelection(order_list)
        self.click_by_xpath(self._WAVE_SUBMIT_BTN)
        waveNbr = self.get_text_by_xpath(self._GENERATED_WAVE_NBR)
        self.switch_default_content()
        self.logger.info('Wave nbr ' + waveNbr)

        return waveNbr

    def _decide_aid_statCode_forWaveType(self, waveTemplate:str, providedVal:AllocStat):
        """Decides alloc_invn_dtl.stat_code.
        TODO Get from DB if waveType has task creation configured.
        If task int50 creation is configured, statCode should be 91 if provided val is 0.
        """
        _ENV = os.environ['env']
        _ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')

        final_statCode = None
        if _ENV_TYPE in ['EXP']:
            if waveTemplate in ['UPS Wave', 'UPS Final'] and providedVal == AllocStat.CREATED:
                final_statCode = AllocStat.TASK_DETAIL_CREATED
        else:
            final_statCode = providedVal

        return final_statCode

    def runPickingWave(self, template:str, rule:str, orders:list[str], u_lineLpnTypes:list[list[str]]=None, u_forceNoOfOlpn:list[int]=None,
                       u_ordConsData: list[OrdConsRuleData] = None,
                       o_lineItems: list[list[str]] = None, o_qtys: list[list[int]] = None,
                       o_ordStatus: list[DOStat] = None, o_ordLineStatus: list[list[int]] = None,
                       # Use below line if partial DO selected
                       o_selectedDO: list[str] = None, o_selLnItems: list[list[str]] = None, o_selQtys: list[list[int]] = None,
                       o_totalLines: int = None, o_totalOlpns: int = None, o_totalOlpnPerDO: list[int] = None,
                       o_consolLocns: list[str] = None,
                       o_intTypes: list[list[int]] = None, o_allocStatus: list[list[AllocStat]] = None, o_allocPullLocns: list[list[str]] = None,
                       isAssertNoTask: bool = False, isAssertReplen: bool = False,
                       o_replenItems: list[str] = None, o_replenQtys: list[int] = None, o_replnPrty:list[int] = None,
                       isAssertConsolLocnExist: bool = None, isAssertNoConsolLocn: bool = None, isIgnoreCanceledDO:bool=None,
                       o_olpnStatus:LPNFacStat=None) -> str:
        """Run picking wave for list of orders
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Running picking wave: template ' + template + ', rule ' + rule)

        RuntimeXL.createThreadLockFile(maxWaitSec=300)
        try:
            if u_forceNoOfOlpn is not None:
                DBLib()._forceNbrOfOlpnCreationForOrders(orders=orders, ordLineItems=o_lineItems, u_forceNoOfOlpn=u_forceNoOfOlpn)
            # if u_lineLpnTypes is not None:
            #     for i in range(len(orders)):
            #         for j in range(len(o_lineItems[i])):
            #             DBLib().updateDOLineLpnType(dO=orders[i], lineItem=o_lineItems[i][j], u_lpnType=u_lineLpnTypes[i][j])

            '''Check if enough consol locn exist, else clear'''
            # doTypeCntRows = DBLib().getOrderTypeCountFromDO(orders=orders)
            # for i in range(len(doTypeCntRows)):
            #     orderType = str(doTypeCntRows[i].get('ORDER_TYPE'))
            #     totalCnt = int(doTypeCntRows[i].get('REQ_CONSOL_COUNT'))
            #     if orderType in ('N', 'J'):
            #         DBLib().getEmptyConsolLocn(noOfLocns=totalCnt, orderType=orderType)
            consLocnList = None
            if u_ordConsData is not None:
                consLocnList = DBLib().getEmptyConsLocnByRuleData(u_ordConsData)

            waveNbr = self._runWave(template, rule, orders)

            '''Validation'''
            o_selectedDO = o_selectedDO if o_selectedDO is not None else orders
            o_selLnItems = o_selLnItems if o_selLnItems is not None else o_lineItems
            o_selQtys = o_selQtys if o_selQtys is not None else o_qtys

            DBLib().assertWaitWaveStatus(i_wave=waveNbr, i_status=90)
            DBLib().assertParentDOsForOrdersByGroupAttr(orders=o_selectedDO)
            DBLib()._printWaveMsgLogs(refValue1=waveNbr, refValue2=o_selectedDO)
            DBLib()._printConsolLocnData(consolLocnList=consLocnList)
            # TODO DBLib()._printItemShortageConfig(consolLocnList=consLocnList)

            '''Update runtime thread data file'''
            consolLocnRecs = DBLib().getAllConsolLocnsFromWave(wave=waveNbr)
            if consolLocnRecs is not None:
                consolLocnsAsCsv = ','.join(i['LOCN_BRCD'] for i in consolLocnRecs if i['LOCN_BRCD'] is not None)
                RuntimeXL.updateThisAttrForThread(RuntimeAttr.CONSOL_LOCNS, consolLocnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        '''Validation'''
        if o_totalLines is not None:
            DBLib().assertDOLineInWave(i_wave=waveNbr, o_totalLines=o_totalLines)
        if o_totalOlpns is not None:
            DBLib().assertOLPNCountForWave(i_wave=waveNbr, o_totalOLPNs=o_totalOlpns)
        # if o_totalLines is not None:
        DBLib().assertDOInWave(i_wave=waveNbr, o_totalOrders=len(o_selectedDO), o_orders=o_selectedDO, isIgnoreCanceledDO=isIgnoreCanceledDO)
        if o_totalOlpnPerDO is not None:
            for i in range(len(o_selectedDO)):
                DBLib().assertOLPNCountForDO(i_order=o_selectedDO[i], o_totalOLPNs=o_totalOlpnPerDO[i])

        for i in range(len(orders)):
            DBLib().assertDOHdr(i_order=orders[i], o_status=o_ordStatus[i])
        if o_ordLineStatus is not None:
            for i in range(len(orders)):
                for j in range(len(o_lineItems[i])):
                    DBLib().assertDODtls(i_order=orders[i], i_itemBrcd=o_lineItems[i][j], o_dtlStatus=o_ordLineStatus[i][j])

        olpns = DBLib().getAllOLPNsFromWave(wave=waveNbr)
        for i in range(len(olpns)):
            final_olpnStat = o_olpnStatus if o_olpnStatus is not None else LPNFacStat.OLPN_PRINTED
            DBLib().assertLPNHdr(i_lpn=olpns[i], o_facStatus=final_olpnStat)
            if isAssertConsolLocnExist:
                DBLib().assertLPNHdr(i_lpn=olpns[i], isConsLocnUpdated=True)
            if isAssertNoConsolLocn:
                DBLib().assertLPNHdr(i_lpn=olpns[i], o_destLocn='null')

        temp_allocStatusList = []
        for i in range(len(o_selectedDO)):
            for j in range(len(o_selLnItems[i])):
                final_allocPullLocn = None if o_allocPullLocns is None else o_allocPullLocns[i][j]
                curr_allocStatus = self._decide_aid_statCode_forWaveType(waveTemplate=template, providedVal=o_allocStatus[i][j])
                DBLib().assertAllocDtls(i_taskGenRefNbr=waveNbr, i_intType=o_intTypes[i][j], i_itemBrcd=o_selLnItems[i][j],
                                        o_qtyAlloc=o_selQtys[i][j], o_statCode=curr_allocStatus, o_pullLocn=final_allocPullLocn)
                temp_allocStatusList.append(curr_allocStatus)

        '''If any aid.stat_code is 91, then dont assert no task creation'''
        if isAssertNoTask and AllocStat.TASK_DETAIL_CREATED not in temp_allocStatusList:
            DBLib().assertNoTaskPresent(taskRefNum=waveNbr)
        elif AllocStat.TASK_DETAIL_CREATED in temp_allocStatusList:
            DBLib().assertTaskExist(taskRefNum=waveNbr)

        if o_consolLocns is not None:
            for i in range(len(o_consolLocns)):
                DBLib().assertConsolLocnForOLPNs(i_order=o_selectedDO[i], o_consolLocn=o_consolLocns[i])

        if isAssertReplen:
            taskId = DBLib().getTaskIdByORCond(taskGenRefNbr=waveNbr, taskCmplRefNbr=waveNbr, cntr=waveNbr, intType=1)
            DBLib().assertTaskHdr(i_task=taskId, o_intType=1, o_status=TaskHdrStat.RELEASED)
            for i in range(len(o_replenItems)):
                DBLib().assertAllocDtls(i_taskGenRefNbr=waveNbr, i_intType=1, i_itemBrcd=o_replenItems[i],
                                        o_qtyAlloc=o_replenQtys[i], o_statCode=AllocStat.TASK_DETAIL_CREATED)
                finalTaskPrty = o_replnPrty[i] if o_replnPrty is not None else None
                DBLib().assertTaskDtls(i_task=taskId, i_itemBrcd=o_replenItems[i], i_intType=1,
                                       o_taskPriority=finalTaskPrty)

        return waveNbr

    def runReplenWave(self, template: str, rule: str, orders: list[str],
                      o_lineItems: list[list[str]] = None, o_qtys: list[list[int]] = None, o_noOFTasks:int = None,
                      o_replenItems: list[str] = None, o_replenQtys: list[int] = None, o_replenPrty: int = None,
                      isAssertTaskSeq: bool = None, o_replenResvLocns: list[str] = None, o_replenILpns: list[str] = None) -> str:
        """Run replen wave for list of orders
        """
        Logging.capture_action_func_start(inspect.currentframe(), self.logger)
        self.logger.info('Running replen wave: template ' + template + ', rule ' + rule)

        RuntimeXL.createThreadLockFile()
        try:
            waveNbr = self._runWave(template, rule, orders)

            '''Validation'''
            DBLib().assertWaitWaveStatus(i_wave=waveNbr, i_status=90)
            DBLib()._printWaveMsgLogs(refValue1=waveNbr, refValue2=orders)
        finally:
            RuntimeXL.removeThreadLockFile()

        '''Validation'''
        taskIds = DBLib().getTaskIdsFromGenRefNbr(taskGenRefNbr=waveNbr)
        if o_noOFTasks is not None:
            assert o_noOFTasks == len(taskIds), 'number of Tasks validation failed'
        for i in range(len(taskIds)):
            DBLib().assertTaskHdr(i_task=taskIds[i], o_intType=1, o_status=TaskHdrStat.RELEASED, o_currTaskPrty=o_replenPrty)

        for i in range(len(o_replenItems)):
            finalILpn = None if o_replenILpns is None else o_replenILpns[i]
            DBLib().assertAllocDtls(i_taskGenRefNbr=waveNbr, i_intType=1, i_cntr=finalILpn, i_itemBrcd=o_replenItems[i], o_qtyAlloc=o_replenQtys[i],
                                    o_statCode=AllocStat.TASK_DETAIL_CREATED)
        if isAssertTaskSeq:
            for i in range(len(o_replenItems)):
                final_rpelenResvLocn = None if o_replenResvLocns is None else o_replenResvLocns[i]
                DBLib().assertTaskDtls(i_task=taskIds[0], i_intType=1, i_pullLocn=final_rpelenResvLocn, o_statCode=TaskDtlStat.UNASSIGNED,
                                       o_taskSeq=int(i + 1))
        return waveNbr

    # def clickOnWaveNumber(self):
    #     self.click_by_xpath(self._WAVE_NUMBER)
