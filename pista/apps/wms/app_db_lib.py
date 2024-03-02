# import inspect
import os
import threading
from collections import OrderedDict
from enum import Enum
from typing import Union, Dict

from apps.wms.app_db_admin import DBAdmin
from apps.wms.app_mhe_util import MHEUtil
from apps.wms.app_status import AllocStat, TaskDtlStat, DOStat, TaskHdrStat, LPNFacStat, POStat
from core.common_service import Commons
from core.config_service import ENV_CONFIG, ENV_CONST
from core.db_service import DBService
from core.log_service import Logging, printit
from core.thread_data_handler import RuntimeXL, RuntimeAttr


class ConsolInvnType(Enum):
    """Order consol attr defined in OB rule
    """
    # BIG = 'BIG'  # Not used yet
    # NXP = 'NXP'  # Not used yet
    # PLT = 'PLT'  # Not used yet
    SLP = 'SLP'
    WP = 'WP'
    PCK = 'PCK'
    # THM = 'THM'  # Not in OB rule for order consol


class MheEventType(Enum):
    CONTAINERSTATUS = 'CONTAINERSTATUS'
    VLM_ITEMSTATUS = 'VLM-ITEMSTATUS'
    FILE_ITEMSTATUS = 'FILE-ITEMSTATUS'
    INVENTORYADJ = 'INVENTORYADJ'


class LocnType(Enum):
    MANUAL_ACTV = 'MANUAL_ACTV'
    MANUAL_RESV = 'MANUAL_RESV'
    VLM_ACTV = 'VLM_ACTV'
    AS_ACTV = 'AS_ACTV'
    ASRS_RESV = 'ASRS_RESV'


class CCTaskRule(Enum):
    CYCLE_CNT_ACTV = 'Cycle Count Active'
    CYCLE_CNT_RESV = 'Cycle Count reserve'


class CCTrigger(Enum):
    CC_ACTV_PCK_TO_ZERO = 'Cycle count active - pick to zero'


class RecvASNTolConfig:
    ERROR = 'ERROR'
    OVERRIDE = 'OVERRIDE'
    WARNING = 'WARNING'
    REGULAR_RECV = 'REGULAR_RECV'
    
    @classmethod
    def isHardStopConfigSet(cls, warning, override, error):
        if error and error > 0:
            return True
        return False

    @classmethod
    def isOverrideWarnMsgConfigSet(cls, warning, override, error):
        if override and override > 0:
            return True
        return False

    @classmethod
    def isWarnMsgConfigSet(cls, warning, override, error):
        if warning and warning > 0:
            return True
        return False


class TaskPath:
    INT_TYPE = CURR_WG = CURR_WA = DEST_WG = DEST_WA = NEXT_WG = NEXT_WA = IGNORE_CURR_WA = IGNORE_DEST_WA = None

    def __init__(self, intType: int = None, currWG: str = None, currWA: Union[str, list] = None,
                 destWG: str = None, destWA: Union[str, list] = None,
                 nextWG: str = None, nextWA: Union[str, list] = None,
                 ignoreCurrWA: list[str] = None, ignoreDestWA: list[str] = None):
        """For empty, use * for curr and dest, use # for next.
        It just sets the task path params and returns a task path obj
        """
        self.INT_TYPE = intType
        if currWG is not None:
            self.CURR_WG = Commons.get_tuplestr(currWG)
        if currWA is not None:
            self.CURR_WA = Commons.get_tuplestr(currWA)
        if destWG is not None:
            self.DEST_WG = Commons.get_tuplestr(destWG)
        if destWA is not None:
            self.DEST_WA = Commons.get_tuplestr(destWA)
        if nextWG is not None:
            self.NEXT_WG = Commons.get_tuplestr(nextWG)
        if nextWA is not None:
            self.NEXT_WA = Commons.get_tuplestr(nextWA)
        if ignoreCurrWA is not None:
            self.IGNORE_CURR_WA = Commons.get_tuplestr(ignoreCurrWA)
        if ignoreDestWA is not None:
            self.IGNORE_DEST_WA = Commons.get_tuplestr(ignoreDestWA)


class OrdConsRuleData:
    """8 OB rules are defined for order consol
    """
    ORDER = ITEMS = LOCNS = None

    def __init__(self, order: str, items: list[str], locns: list[list]):
        self.ORDER = order
        self.ITEMS = items
        self.LOCNS = locns

    class GetSql:
        getEligLocnForBIG = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                               from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                               where pcl.pkt_consol_value_1='O' and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForNXP = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                               from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                               where pcl.pkt_consol_value_1='U' and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForPLT = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                               from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id
                               where pcl.pkt_consol_attr='PLT' and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForSLP = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                               from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                               where pcl.pkt_consol_value_1='N' and pcl.pkt_consol_attr in ('SLP','*') and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForWP = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                              from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                              where pcl.pkt_consol_value_1='N' and pcl.pkt_consol_attr in ('WP','*') and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForPCK = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                               from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                               where pcl.pkt_consol_value_1='N' and pcl.lock_pkt_consol_colm_1='Y' and pcl.pkt_consol_attr in ('PCK') and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForPCK2 = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                                from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                                where pcl.pkt_consol_value_1='J' and pcl.lock_pkt_consol_colm_1='Y' --and pcl.pkt_consol_attr in ('PCK','*') 
                                and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""
        getEligLocnForMarkFor = """select distinct lh.dsp_locn,lh.locn_brcd,pcl.prty_seq_nbr,pcl.rec_type,lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3,pcl.* 
                                   from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                                   where pcl.pkt_consol_value_1='M' and pcl.lock_pkt_consol_colm_1='Y' and pcl.pkt_consol_attr in ('*') and pcl.rec_type='#P_OR_W#' order by pcl.prty_seq_nbr"""


class DBLib:
    logger = Logging.get(__qualname__)

    '''ASN'''

    _fetchASNHdrByASNNum = """select * from asn where tc_asn_id in ('#TC_ASN_ID#')"""
    _fetchASNDtlsByASNDtls = """select a.tc_asn_id, ad.* from asn_detail ad inner join asn a on ad.asn_id = a.asn_id 
                                where a.tc_asn_id = '#TC_ASN_ID#' 
                                #CONDITION#"""

    def __init__(self, schema=None):
        if schema is None:
            schema = ENV_CONFIG.get('orcl_db', 'wm_schema')
        self.schema = schema
        DBService.connect_db(schema)

        self.IS_ALLOW_UPDATE_INVN = ENV_CONFIG.get('data', 'is_allow_update_invn')
        self.IS_ALLOW_CREATE_INVN = ENV_CONFIG.get('data', 'is_allow_create_invn')
        self.IS_ALLOW_CREATE_INVN_IN_VLM = ENV_CONFIG.get('data', 'is_allow_create_invn_in_vlm')
        self.IS_ALLOW_CREATE_INVN_IN_AUTOSTR = ENV_CONFIG.get('data', 'is_allow_create_invn_in_as')
        self.IS_ALLOW_CREATE_INVN_IN_ASRS = ENV_CONFIG.get('data', 'is_allow_create_invn_in_asrs')
        self.IS_ALLOW_CLEAR_INVN = ENV_CONFIG.get('data', 'is_allow_clear_invn')
        self.IS_ALLOW_CLEAR_INVN_IN_VLM = ENV_CONFIG.get('data', 'is_allow_clear_invn_in_vlm')
        self.IS_ALLOW_CLEAR_INVN_IN_AUTOSTR = ENV_CONFIG.get('data', 'is_allow_clear_invn_in_as')
        self.IS_ALLOW_CLEAR_INVN_IN_ASRS = ENV_CONFIG.get('data', 'is_allow_clear_invn_in_asrs')
        self.IS_ALLOW_CANCEL_TASK = ENV_CONFIG.get('data', 'is_allow_cancel_task')
        self.IS_ALLOW_UPDATE_TASK = ENV_CONFIG.get('data', 'is_allow_update_task')
        self.IS_ALLOW_CANCEL_ALLOC = ENV_CONFIG.get('data', 'is_allow_cancel_alloc')
        self.IS_ALLOW_UPDATE_LOCN = ENV_CONFIG.get('data', 'is_allow_update_locn')
        self.IS_ALLOW_CLEAR_CONS_LOCN = ENV_CONFIG.get('data', 'is_allow_clear_consol_locn')
        self.IS_ALLOW_CLEAR_DOCKDOOR = ENV_CONFIG.get('data', 'is_allow_clear_dockdoor')
        self.IS_ALLOW_CLOSE_MANIFEST = ENV_CONFIG.get('data', 'is_allow_close_manifest')
        self.IS_ALLOW_UPDATE_ILPN = ENV_CONFIG.get('data', 'is_allow_update_ilpn')
        self.IS_ALLOW_UPDATE_OLPN = ENV_CONFIG.get('data', 'is_allow_update_olpn')

        self.thread_id = threading.current_thread().native_id

        self._ENV = os.environ['env']
        self._ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')
        self._IS_FOR_ARC = os.environ['isForARC']

    def _clearBadInvnData(self):
        """Clear bad invn records created by automation
        This deletes in wm_inventory and pick_locn_dtl
        TODO Check CREATED_SOURCE from pick_locn_dtl instead of wm_inventory
        """
        sql = f"""select wi.item_id from wm_inventory wi 
                where wi.locn_class = 'A' group by wi.item_id having count(wi.item_id) > 1"""
        dbRows = DBService.fetch_rows(sql, self.schema)

        for i in range(len(dbRows)):
            itemId = dbRows[i]['ITEM_ID']
            sql = f"""select distinct ic.item_name,lh.locn_brcd,wi.created_dttm,wi.created_source
                    from wm_inventory wi inner join item_cbo ic on ic.item_id = wi.item_id inner join locn_hdr lh on lh.locn_id = wi.location_id
                    where wi.item_id = '{itemId}' and wi.locn_class = 'A' order by wi.created_dttm asc
                    """
            locnRecs = DBService.fetch_rows(sql, self.schema)
            for j in range(1, len(locnRecs)):
                itemName = locnRecs[j]['ITEM_NAME']
                locnBrcd = locnRecs[j]['LOCN_BRCD']
                if locnRecs[j]['CREATED_SOURCE'] == 'AUTOMATION':
                    DBAdmin._deleteFromActvInvnTables(self.schema, item=itemName, locnBrcd=locnBrcd)

    def getTaskPathDefs2(self, taskPath: TaskPath = None) -> list:
        """Returns list of distinct task path def records
        """
        sql = self._buildQueryForTaskPathDef2(taskPath)
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is not None and len(dbRows) > 0 and dbRows[0]['CURR_WORK_GRP'] is not None, '<TaskPath> No task path record found ' + sql
        return dbRows

    def _getCurrWAFromTaskPath2(self, taskPath: TaskPath = None) -> list:
        """Returns list of distinct curr work areas
        """
        sql = self._buildQueryForTaskPathDef2(taskPath)
        dbRows = DBService.fetch_rows(sql, self.schema)

        currWASet = set()
        currWASet.update(r.get('CURR_WORK_AREA') for r in dbRows)
        currWAList = list(currWASet)
        printit(f"Found TPD curr work areas {currWAList}")

        assert len(currWAList) > 0, '<TaskPath> No curr workarea found from taskpath ' + sql

        # valuesToRemove = ['*', '#']
        # for i in valuesToRemove:
        #     while i in currWAList:
        #         currWAList.remove(i)

        return currWAList

    def _getDestWAFromTaskPath2(self, taskPath: TaskPath = None) -> list:
        """Returns list of distinct dest work areas
        """
        sql = self._buildQueryForTaskPathDef2(taskPath)
        dbRows = DBService.fetch_rows(sql, self.schema)

        destWASet = set()
        destWASet.update(r.get('DEST_WORK_AREA') for r in dbRows)
        destWAList = list(destWASet)
        printit(f"Found TPD dest work areas {destWAList}")

        assert len(destWAList) > 0, '<TaskPath> No dest workarea found from taskpath ' + sql

        # valuesToRemove = ['*', '#']
        # for i in valuesToRemove:
        #     while i in destWAList:
        #         destWAList.remove(i)

        return destWAList

    def removeSpecialCharFromTaskPathVals(self, tpdVals: Union[str, list] = None, extraChar: list = ['*', '#']):
        """Return list or None"""
        tpdVals = tpdVals if type(tpdVals) == list else [tpdVals] if type(tpdVals) == str else None
        if tpdVals is not None and type(tpdVals) == list:
            tpdVals = Commons.remove_vals_from_list(tpdVals, extraChar)
            if len(tpdVals) == 0:
                tpdVals = None
        else:
            tpdVals = None
        return tpdVals

    def _decide_isAllowUpdateInvn(self, isVLMLocn: bool = None, isASLocn: bool = None, isASRSLocn: bool = None):
        """Decide based on if allowed to update invn in auto locns VLM/AS/ASRS"""
        final_isAllowUpdateInvn = True if 'true' in self.IS_ALLOW_UPDATE_INVN else False

        printit(f"final_isAllowUpdateInvn {final_isAllowUpdateInvn}")
        return final_isAllowUpdateInvn

    def _decide_isAllowCreateInvn(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None):
        """Decide based on if allowed to create invn in auto locns VLM/AS/ASRS"""
        final_isAllowCreateInvn = True if 'true' in self.IS_ALLOW_CREATE_INVN else False
        if isVLMLocn: final_isAllowCreateInvn = True if 'true' in self.IS_ALLOW_CREATE_INVN_IN_VLM else False
        elif isASLocn: final_isAllowCreateInvn = True if 'true' in self.IS_ALLOW_CREATE_INVN_IN_AUTOSTR else False
        elif isASRSLocn: final_isAllowCreateInvn = True if 'true' in self.IS_ALLOW_CREATE_INVN_IN_ASRS else False
        
        printit(f"final_isAllowCreateInvn {final_isAllowCreateInvn} (isVLMLocn {isVLMLocn} isASLocn {isASLocn} isASRSLocn {isASRSLocn})")
        return final_isAllowCreateInvn

    def _decide_isAllowClearInvn(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None):
        """Decide based on if allowed to clear invn in auto locns VLM/AS/ASRS"""
        final_isAllowClearInvn = True if 'true' in self.IS_ALLOW_CLEAR_INVN else False
        if isVLMLocn: final_isAllowClearInvn = True if 'true' in self.IS_ALLOW_CLEAR_INVN_IN_VLM else False
        elif isASLocn: final_isAllowClearInvn = True if 'true' in self.IS_ALLOW_CLEAR_INVN_IN_AUTOSTR else False
        elif isASRSLocn: final_isAllowClearInvn = True if 'true' in self.IS_ALLOW_CLEAR_INVN_IN_ASRS else False

        printit(f"final_isAllowClearInvn {final_isAllowClearInvn} (isVLMLocn {isVLMLocn} isASLocn {isASLocn} isASRSLocn {isASRSLocn})")
        return final_isAllowClearInvn

    def _decide_ifmw_putwyType_forItemType(self, isVLMItem:bool=None, isASItem:bool=None, isASRSItem:bool=None,
                                           providedVal=None, defaultVal:str=None):
        """item_facility_mapping_wms.putwy_type"""
        final_itemPutwyType = final_avoidItemPutwyType = None

        if isVLMItem:
            final_itemPutwyType = Commons.get_tuplestr('STD')
        elif isASItem:
            final_itemPutwyType = Commons.get_tuplestr('STD')
        elif isASRSItem:
            final_itemPutwyType = Commons.get_tuplestr('ASR')
        elif providedVal:
            final_itemPutwyType = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_itemPutwyType = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidItemPutwyType = Commons.get_tuplestr('ASR')

        return final_itemPutwyType, final_avoidItemPutwyType

    def _decide_ifmw_allocType_forItemType(self, isVLMItem:bool=None, isASItem:bool=None, isASRSItem:bool=None, isPromoItem:bool=None,
                                           providedVal=None, defaultVal:str=None):
        """item_facility_mapping_wms.alloc_type"""
        final_itemAllocType = final_avoidItemAllocType = None

        if isVLMItem:
            final_itemAllocType = Commons.get_tuplestr('STD')
        elif isASItem:
            final_itemAllocType = Commons.get_tuplestr('STD')
        elif isASRSItem:
            final_itemAllocType = Commons.get_tuplestr('PLR')
        elif isPromoItem:
            final_itemAllocType = Commons.get_tuplestr('PRO')
        elif providedVal:
            final_itemAllocType = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_itemAllocType = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidItemAllocType = Commons.get_tuplestr(['PLR', 'PRO'])
        
        return final_itemAllocType, final_avoidItemAllocType

    def _decide_iap_allocType_forItemType(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None, isPromoLocn:bool=None,
                                          providedVal=None, defaultVal:str=None):
        """invn_alloc_prty.alloc_type"""
        final_itemAllocType = final_avoidItemAllocType = None

        if isVLMLocn:
            final_itemAllocType = Commons.get_tuplestr('STD')
        elif isASLocn:
            final_itemAllocType = Commons.get_tuplestr('STD')
        elif isASRSLocn:
            final_itemAllocType = Commons.get_tuplestr('PLR')
        elif isPromoLocn:
            final_itemAllocType = Commons.get_tuplestr('PRO')
        elif providedVal:
            final_itemAllocType = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_itemAllocType = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidItemAllocType = Commons.get_tuplestr(['PLR', 'PRO'])

        return final_itemAllocType, final_avoidItemAllocType

    def _decide_lh_locnBrcd_forLocnType(self, isVLMLocn: bool = None, isASLocn: bool = None, isASRSLocn: bool = None):
        """locn_hdr.locn_brcd starts with val"""
        final_locnBrcd = final_avoidLocnBrcd = None

        sql = f"""select locn_brcd from locn_hdr where #CONDITION#"""

        sqlCond = sqlCondForAvoid = ''
        if isVLMLocn:
            sqlCond += " \n locn_brcd like 'VM%'"
        elif isASLocn:
            sqlCond += " \n locn_brcd like 'AS%' and locn_brcd not like 'ASRS%'"
        elif isASRSLocn is not None:
            sqlCond += " \n locn_brcd like 'ASRS%'"
        else:
            sqlCondForAvoid += " \n locn_brcd like 'VM%' or locn_brcd like 'AS%'"

        if sqlCond != '':
            sql = sql.replace('#CONDITION#', sqlCond)
            dbRows = DBService.fetch_rows(sql, self.schema)
            final_locnBrcd = [r['LOCN_BRCD'] for r in dbRows]
            final_locnBrcd = Commons.get_tuplestr(final_locnBrcd)
        elif sqlCondForAvoid != '':
            sql = sql.replace('#CONDITION#', sqlCondForAvoid)
            dbRows = DBService.fetch_rows(sql, self.schema)
            final_avoidLocnBrcd = [r['LOCN_BRCD'] for r in dbRows]
            final_avoidLocnBrcd = Commons.get_tuplestr(final_avoidLocnBrcd)

        return final_locnBrcd, final_avoidLocnBrcd

    def _decide_lh_zone_forLocnType(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None, isPromoLocn:bool=None,
                                    providedVal=None, defaultVal:str=None):
        """locn_hdr.zone"""
        final_zone = final_avoidZone = None

        if isVLMLocn:
            final_zone = Commons.get_tuplestr('VM')
        elif isASLocn:
            final_zone = Commons.get_tuplestr('AS')
        elif isASRSLocn is not None:
            final_avoidZone = Commons.get_tuplestr(['VM'])
        elif providedVal:
            final_zone = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_zone = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidZone = Commons.get_tuplestr(['VM', 'AS'])
        
        return final_zone, final_avoidZone

    def _decide_lh_pullZone_forLocnType(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None, isPromoLocn:bool=None,
                                        providedVal:str=None, defaultVal:str=None):
        """locn_hdr.pull_zone"""
        final_pullZone = final_avoidPullZone = None

        # if isVLMLocn:
        #     final_pullZone = Commons.get_tuplestr('VM')
        # elif isASLocn:
        #     final_pullZone = Commons.get_tuplestr('AS')
        if isASRSLocn is not None:
            final_pullZone = Commons.get_tuplestr('ASR')
        elif providedVal:
            final_pullZone = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_pullZone = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidPullZone = Commons.get_tuplestr('ASR')

        return final_pullZone, final_avoidPullZone

    def _decide_lh_pickDetrmZone_forLocnType(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None, isPromoLocn:bool=None,
                                             providedVal:str=None, defaultVal:str=None):
        """locn_hdr.pick_detrm_zone"""
        final_pickDetrmZone = final_avoidPickDetrmZone = None

        # if isVLMLocn:
        #     final_pullZone = Commons.get_tuplestr('VM')
        # elif isASLocn:
        #     final_pullZone = Commons.get_tuplestr('AS')
        if isPromoLocn:
            final_pickDetrmZone = Commons.get_tuplestr('PRO')
        elif providedVal:
            final_pickDetrmZone = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_pickDetrmZone = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidPickDetrmZone = Commons.get_tuplestr('PRO')

        return final_pickDetrmZone, final_avoidPickDetrmZone

    def _decide_pzp_putwyType_forLocnType(self, isVLMLocn:bool=None, isASLocn:bool=None, isASRSLocn:bool=None, isPromoLocn:bool=None,
                                          providedVal:str=None, defaultVal:str=None):
        """putwy_zone_prty.putwy_type"""
        final_pzpPtwyType = final_avoidPzpPtwyType = None

        # if isVLMLocn:
        #     final_pzpPtwyType = Commons.get_tuplestr('VM')
        # elif isASLocn:
        #     final_pzpPtwyType = Commons.get_tuplestr('AS')
        if isASRSLocn:
            final_pzpPtwyType = Commons.get_tuplestr('ASR')
        elif providedVal:
            final_pzpPtwyType = Commons.get_tuplestr(providedVal)
        elif defaultVal:
            final_pzpPtwyType = Commons.get_tuplestr(defaultVal)
        else:
            final_avoidPzpPtwyType = Commons.get_tuplestr('ASR')

        return final_pzpPtwyType, final_avoidPzpPtwyType

    def _decide_lh_workArea_forLocnType(self, locnType:LocnType):
        """Decide workArea by locnType
        Eg: MANUAL_RESV, VLM_ACTV
        """
        locnClass = sqlCond = ''
        if locnType == LocnType.VLM_ACTV:
            locnClass = 'A'
            sqlCond = " locn_brcd like 'VM%'"
        elif locnType == LocnType.AS_ACTV:
            locnClass = 'A'
            sqlCond = " locn_brcd like 'AS%'"
        elif locnType == LocnType.MANUAL_ACTV:
            locnClass = 'A'
            sqlCond =  " locn_brcd not like 'AS%' and locn_brcd not like 'VM%'"
        elif locnType == LocnType.ASRS_RESV:
            locnClass = 'R'
            sqlCond = " locn_brcd like 'ASR%'"
        elif locnType == LocnType.MANUAL_RESV:
            locnClass = 'R'
            sqlCond = " locn_brcd not like 'ASR%'"
        
        sql = f"""select distinct work_area from locn_hdr where {sqlCond} and locn_class= '{locnClass}'"""
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is not None, f"<Data> Work area not found for locntype {locnType} " + sql
        workAreaList = [i['WORK_AREA'] for i in dbRows]
        
        return workAreaList

    def _decide_ic_refField10_forItemType(self):
        """item_cbo.ref_field10"""
        final_refField10 = None
        if 'true' in self._IS_FOR_ARC:
            final_refField10 = Commons.get_tuplestr('ARC')

        return final_refField10

    def _decide_clm_statusName_forMheMsg(self):
        final_statName = None

        _FLAG = ENV_CONFIG.get('flag', 'mheMsgProcessStat_flag')

        printit(f">>> Using flag {_FLAG}")
        
        if _FLAG == '1':
            final_statName = 'Ready'
        elif _FLAG == '2':
            final_statName = 'Succeeded'

        return final_statName

    def assertCubiScanWarnConfigSetForReceive(self):
        """"""
        isConfigSet = self.isCubiScanWarnConfigSetForReceive()
        assert isConfigSet, "<Config> 'Send To Cubiscan' warn config for receiving not set"

    def isCubiScanWarnConfigSetForReceive(self):
        """Assert 'Send To Cubiscan' warning is configured for Receiving
        stat_code '0' means enabled,
        stat_code '90' means disabled"""
        sql = f"""select stat_code from rule_hdr where rule_name='CubiScan' and rule_type='RL' and rec_type='T'"""
        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None and len(dbRow) >= 1, "<Config> 'Send To Cubiscan' warn config for receiving not found " + sql
        statCode = dbRow.get('STAT_CODE')
        isConfigSet = True if statCode != 90 else False

        return isConfigSet

    def assertCCTaskRuleConfigEnabled(self, ccTaskRule: CCTaskRule):
        """Assert 'cc actv' & 'cc resv' is configured in cc task rules
        stat_code '0' means enabled,
        stat_code '90' means disabled"""
        # ruleName = ['Cycle Count Active', 'Cycle Count reserve']
        ruleName = ccTaskRule.value
        sql = f"""select tp.task_parm_id, rh.rule_id, tp.crit_nbr, rh.rule_name, rh.stat_code from task_parm tp 
                inner join task_rule_parm trp on tp.task_parm_id = trp.task_parm_id 
                inner join rule_hdr rh on trp.rule_id = rh.rule_id
                where rh.rule_type='CC' and tp.crit_nbr = 'CycleCount' and rh.rule_name = '{ruleName}'"""
        dbRow = DBService.fetch_row(sql, self.schema)

        statCode = dbRow.get('STAT_CODE')
        isConfigSet = True if statCode != 90 else False
        assert isConfigSet, f"<Config> CC task rule is not set for {ruleName} " + sql

    def assertCCTaskCreateTriggerConfigEnabled(self, ccTrigger: CCTrigger):
        """Assert CC task create trigger is enabled
        Y means enabled,
        N means disabled"""
        triggerDesc = ccTrigger.value
        sql = f"""select create_task from cycle_count_trig 
                    where trig_code in (select code_id from sys_code sc where rec_type='S' and code_type='867' and code_desc='{triggerDesc}') 
                    offset 0 rows fetch next 1 rows only"""
        dbRow = DBService.fetch_row(sql, self.schema)

        createTaskFlag = dbRow.get('CREATE_TASK')
        isConfigSet = True if createTaskFlag and createTaskFlag == 'Y' else False
        assert isConfigSet, f"<Config> CC task create trigger is not set for {triggerDesc} " + sql

    def getUNNbrFromItem(self, itemBrcd: str):
        # sql = f""" select un.un_number from item_cbo ic inner join un_number un on un.un_number_id=ic.un_number_id
        #         where ic.item_name = '{itemBrcd}'"""
        sql = f"""select un.unn_hazard_label from item_cbo ic inner join un_number un on un.un_number_id=ic.un_number_id 
                where ic.item_name = '{itemBrcd}'"""
        dbRow = DBService.fetch_row(sql, self.schema)

        unNumbr = str(dbRow.get('UNN_HAZARD_LABEL'))
        self.logger.info(f'UN number for {itemBrcd} is {unNumbr}')

        return unNumbr

    def getItemDtlsFrom(self, orItemId: str = None, orItemBrcd: str = None):
        assert orItemId is not None or orItemBrcd is not None, 'itemId and itemBrcd missing'

        sql = f"""select ic.*, iw.*, ifmw.* from item_cbo ic inner join item_wms iw on ic.item_id=iw.item_id 
                 inner join item_facility_mapping_wms ifmw on ic.item_id=ifmw.item_id
                 where #CONDITION#"""
        if orItemId is not None:
            sqlCond = " ic.item_id = '" + orItemId + "'"
        else:
            sqlCond = " ic.item_name = '" + orItemBrcd + "'"

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        return dbRow

    def getItemIdFromBrcd(self, itemBrcd):
        dbRow = self.getItemDtlsFrom(orItemBrcd=str(itemBrcd))
        itemId = dbRow.get("ITEM_ID")
        return itemId

    def _getManualActvLocnByItemToClear(self, itemBrcd: str):
        """Get the item slotted in manual actv pick locn"""
        sql = f""" select distinct lh.locn_brcd from wm_inventory wi,locn_hdr lh,item_cbo ic
                    where wi.location_id=lh.locn_id and wi.item_id=ic.item_id and lh.locn_class='A'
                    #CONDITION# 
                    and ic.item_name = '{itemBrcd}' """
        sqlCond = ''

        final_zone_vm, final_avoidZone_vm = self._decide_lh_zone_forLocnType(isVLMLocn=True)
        final_zone_as, final_avoidZone_as = self._decide_lh_zone_forLocnType(isASLocn=True)
        final_zone_vm = final_zone_vm if final_zone_vm is not None else ()
        final_zone_as = final_zone_as if final_zone_as is not None else ()
        avoidZones = tuple(set(final_zone_vm + final_zone_as))
        sqlCond += f" \n and lh.zone not in {avoidZones}"

        # '''Exclude runtime thread locns'''
        # threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        # if threadLocns is not None:
        #     sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)
        
        actvLocns = [i['LOCN_BRCD'] for i in dbRows]

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            # sqlCond += " \n and lh.locn_brcd not in " + threadLocns
            anyLocnToBeExcluded = [True if i in actvLocns else False for i in threadLocns]
            assert True not in anyLocnToBeExcluded, f"<Data> To be excluded actv locn found to clear. Test manually"

        return actvLocns

    def getAnyItem(self, noOfItem: int,
                   # isPCKItem: bool = None, isWPItem: bool = None,
                   consolInvnType: ConsolInvnType = None, isTHMItem: bool = None,
                   isItemWithBundleQty: bool = None, isItemForCrossDock: bool = None, isHazmatItem: bool = None,
                   itemAllocType: list[str] = None, isCubiscanNeed: bool = False, ignoreItems: list[str] = None,
                   isVLMItem: bool = None, isASItem: bool = None, isASRSItem: bool = None,
                   isFetchByMaxVol:bool=None):

        RuntimeXL.createThreadLockFile()
        try:
            sql = self._buildQueryForGetItems(noOfItem=noOfItem,
                                              # isPCKItem=isPCKItem, isWPItem=isWPItem,
                                              consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                              isItemWithBundleQty=isItemWithBundleQty, isItemForCrossDock=isItemForCrossDock, isHazmatItem=isHazmatItem,
                                              itemAllocType=itemAllocType, isCubiscanNeed=isCubiscanNeed, ignoreItems=ignoreItems,
                                              isVLMItem=isVLMItem, isASItem=isASItem, isASRSItem=isASRSItem,
                                              isFetchByMaxVol=isFetchByMaxVol)
            dbRows = DBService.fetch_rows(sql, self.schema)

            assert len(dbRows) == noOfItem, f"<Data> {noOfItem} no. of any item not found " + sql

            '''Print data'''
            for i in range(0, noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getItemsNotInAnyLocn(self, noOfItem: int,
                             # isPCKItem: bool = None, isWPItem: bool = None,
                             consolInvnType: ConsolInvnType = None, isTHMItem: bool = None,
                             isNotInOnlyResvActv: bool = None, isItemForCrossDock: bool = None,
                             isItemWithBundleQty: bool = None, itemAllocType: list[str] = None, isCubiscanNeed:bool=None,
                             isHazmatItem: bool = None, ignoreItems: list[str] = None,
                             isVLMItem: bool = None, isASItem: bool = None, isASRSItem: bool = None,
                             isFetchByMaxVol: bool = None):
        RuntimeXL.createThreadLockFile()
        try:
            sql = self._buildQueryForGetItems(noOfItem=noOfItem,
                                              # isPCKItem=isPCKItem, isWPItem=isWPItem,
                                              consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                              isNotInOnlyResvActv=isNotInOnlyResvActv, isNotInAnyLocn=True,
                                              isItemWithBundleQty=isItemWithBundleQty, isItemForCrossDock=isItemForCrossDock, isHazmatItem=isHazmatItem,
                                              itemAllocType=itemAllocType, isCubiscanNeed=isCubiscanNeed,
                                              isVLMItem=isVLMItem, isASItem=isASItem, isASRSItem=isASRSItem,
                                              ignoreItems=ignoreItems, isFetchByMaxVol=isFetchByMaxVol)
            dbRows = DBService.fetch_rows(sql, self.schema)

            isItemNotInAnyLocnFound = True if dbRows is not None and len(dbRows) >= noOfItem else False

            '''Get any item & clear invn'''
            if not isItemNotInAnyLocnFound:
                final_isAllowClearInvn = self._decide_isAllowClearInvn(isVLMLocn=isVLMItem, isASLocn=isASItem, isASRSLocn=isASRSItem)
                if final_isAllowClearInvn:
                    dbRows = self.getAnyItem(noOfItem=noOfItem,
                                             # isPCKItem=isPCKItem, isWPItem=isWPItem,
                                             consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                             isItemWithBundleQty=isItemWithBundleQty, isCubiscanNeed=isCubiscanNeed,
                                             isItemForCrossDock=isItemForCrossDock, isHazmatItem=isHazmatItem,
                                             itemAllocType=itemAllocType, ignoreItems=ignoreItems,
                                             isVLMItem=isVLMItem, isASItem=isASItem, isASRSItem=isASRSItem,
                                             isFetchByMaxVol=isFetchByMaxVol)
                    
                    '''Clear invn in both locn'''
                    for i in range(len(dbRows)):
                        itemBrcd = dbRows[i]['ITEM_NAME']
                        self._clearManualActvAndResvInvnForReplen(item=itemBrcd)
                else:
                    assert False, 'Clearing invn (clearing invn in actv/resv for replen) is not allowed. Test manually'

            assert len(dbRows) == noOfItem, f"<Data> {noOfItem} no. of items not in any locn not found " + sql

            '''Print data'''
            for i in range(0, noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItems(self, noOfItem: int,
                               # isPCKItem: bool = None, isWPItem: bool = None,
                               consolInvnType: ConsolInvnType = None, isTHMItem: bool = None,
                               isItemWithBundleQty: bool = None, isItemForCrossDock: bool = None, isHazmatItem: bool = None,
                               itemAllocType: list[str] = None, isCubiscanNeed: bool = False, ignoreItems: list[str] = None,
                               isNotInAnyLocn:bool=None, isNotInOnlyResvActv:bool=None,
                               isVLMItem: bool = None, isASItem: bool = None, isASRSItem: bool = None,
                               isFetchByMaxVol:bool=None):
        """"""
        sql = f"""select a.item_name,a.item_id,a.unit_volume,b.locn_count,a.alloc_type,a.putwy_type,a.size_uom 
                  from
                    (select ic.item_name,ic.std_bundl_qty,ic.item_id,ic.unit_volume,ic.ref_field10,ic.un_number_id,su.size_uom
                        ,ifmw.slot_misc_1,ifmw.slot_misc_2,ifmw.alloc_type,ifmw.putwy_type
                    from item_cbo ic 
                    inner join size_uom su on ic.base_storage_uom_id=su.size_uom_id and su.size_uom='EACH'
                    inner join item_facility_mapping_wms ifmw on ic.item_id=ifmw.item_id and ifmw.mark_for_deletion <> '1' 
                    and ic.item_id not in (select item_id from immd_needs where item_id is not null)) a
                  inner join 
                    (select ic.item_name,ic.unit_volume,count(wi.location_id) as locn_count from item_cbo ic 
                    left outer join wm_inventory wi on ic.item_id=wi.item_id group by ic.item_name,ic.unit_volume order by locn_count) b  
                  on a.item_name=b.item_name
                  #CONDITION# 
                  #ORDER_CONDITION#
                  offset 0 rows fetch next {noOfItem} rows only
              """
        sqlCond = ''
        # if isPCKItem:
        #     sqlCond += " \n and a.slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM')"
        # if isWPItem:
        #     sqlCond += " \n and a.slot_misc_1 in ('NW','NTH','NSE','CHT','SER','THM')"
        #     # sqlCond += " \n and a.item_id not in (select item_id from pick_locn_dtl where locn_id in (select locn_id from locn_hdr lh where  (lh.zone = '40' and lh.aisle not in ('AA','BB','CC','DD','HH','II') or lh.zone not in('40','63','64','65','66')))) "
        if isTHMItem:
            sqlCond += " \n and a.slot_misc_1 in ('THM')"

        if consolInvnType is not None:
            if consolInvnType == ConsolInvnType.PCK:
                sqlCond += " \n and a.slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM')" + f" --{consolInvnType.value}"
            elif consolInvnType == ConsolInvnType.WP:
                sqlCond += " \n and a.slot_misc_1 in ('NW','NTH','NSE','CHT','SER','THM')" + f" --{consolInvnType.value}"
                # sqlCond += " \n and a.item_id not in (select item_id from pick_locn_dtl where locn_id in (select locn_id from locn_hdr lh where  (lh.zone = '40' and lh.aisle not in ('AA','BB','CC','DD','HH','II') or lh.zone not in('40','63','64','65','66')))) " + f" --{consolInvnType.value}"

        if isItemForCrossDock:
            sqlCond += " \n and a.slot_misc_2 in ('Active-NS', 'Unreleased')"
        else:
            sqlCond += " \n and a.slot_misc_2 not in ('Active-NS', 'Unreleased')"
        if isItemWithBundleQty:
            sqlCond += " \n and a.std_bundl_qty > 1"
        if isHazmatItem:
            sqlCond += " \n and a.un_number_id is not null"
        if isCubiscanNeed:
            sqlCond += " \n and a.unit_volume = '.01'"
        else:
            sqlCond += " \n and a.unit_volume > '.01'"

        if isNotInOnlyResvActv:
            sqlCond += " \n and a.item_id not in (select item_id from wm_inventory where locn_class in ('A','R'))"
        elif isNotInAnyLocn:
            sqlCond += """ \n and a.item_id not in (select item_id from wm_inventory where item_id is not null)
                                            and a.item_id not in (select item_id from pick_locn_dtl where item_id is not null)"""
        if ignoreItems is not None:
            sqlCond += f" \n and a.item_name not in {Commons.get_tuplestr(ignoreItems)}"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and a.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (a.ref_field10 is null or (a.ref_field10 is not null and a.ref_field10 not in ('ARC')))"

        final_itemPutwyType, final_avoidItemPutwyType = \
            self._decide_ifmw_putwyType_forItemType(isVLMItem=isVLMItem, isASItem=isASItem, isASRSItem=isASRSItem, defaultVal='STD')
        if final_itemPutwyType is not None:
            sqlCond += f" \n and a.putwy_type in {final_itemPutwyType}"

        final_itemAllocType, final_avoidItemAllocType = \
            self._decide_ifmw_allocType_forItemType(isVLMItem=isVLMItem, isASItem=isASItem, isASRSItem=isASRSItem,
                                                    providedVal=itemAllocType, defaultVal='STD')
        if final_itemAllocType is not None:
            sqlCond += f" \n and a.alloc_type in {final_itemAllocType}"

        orderCond = ''
        if isFetchByMaxVol:
            orderCond += ' \n order by a.unit_volume desc'
        else:
            orderCond += ' \n order by b.locn_count asc, a.unit_volume asc'

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += f""" \n and a.item_id not in (select item_id from wm_inventory where location_id is not null 
                                                and location_id in (select locn_id from locn_hdr where locn_brcd in {threadLocns}))"""

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += " \n and a.item_name not in " + threadItems

        sql = sql.replace('#CONDITION#', sqlCond)
        sql = sql.replace('#ORDER_CONDITION#', orderCond)

        return sql

    def getItemsForResvPutwy(self, noOfItem: int, resvWG: str = 'RESV', minAvailUnit: int = None,
                             taskPath: TaskPath = None, isResvWAFromTPathDestWA: bool = None,
                             isItemForCrossDock: bool = False,
                             isZoneInPutawayDir: bool = None, isItemIn1Resv: bool = None,
                             isFetchByMaxVol: bool = None,
                             isPutwyLocnASRS: bool = None):
        """(Generic method) This makes sure any resv.
        """
        RuntimeXL.createThreadLockFile()
        try:
            isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = False

            '''Get item in resv, but not in actv'''
            sql = self._buildQueryForGetItemsForResvPutwy(noOfItem=noOfItem, resvWG=resvWG, minAvailUnit=minAvailUnit,
                                                          taskPath=taskPath, isResvWAFromTPathDestWA=isResvWAFromTPathDestWA,
                                                          isItemForCrossDock=isItemForCrossDock,
                                                          isZoneInPutawayDir=isZoneInPutawayDir, isItemNotInActv=True,
                                                          isItemIn1Resv=isItemIn1Resv, isFetchByMaxVol=isFetchByMaxVol,
                                                          isASRSItem=isPutwyLocnASRS)

            dbRows = DBService.fetch_rows(sql, self.schema)
            isItemFoundFromOrigQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

            '''Get item present in both actv and resv '''
            if not isItemFoundFromOrigQuery:
                sql = self._buildQueryForGetItemsForResvPutwy(noOfItem=noOfItem, resvWG=resvWG, minAvailUnit=minAvailUnit,
                                                              taskPath=taskPath, isResvWAFromTPathDestWA=isResvWAFromTPathDestWA,
                                                              isItemForCrossDock=isItemForCrossDock,
                                                              isZoneInPutawayDir=isZoneInPutawayDir,
                                                              isItemIn1Resv=isItemIn1Resv, isFetchByMaxVol=isFetchByMaxVol,
                                                              isASRSItem=isPutwyLocnASRS)

                dbRows = DBService.fetch_rows(sql, self.schema)
                isItemFoundFromRevisedQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

                '''Clear invn in actv'''
                if isItemFoundFromRevisedQuery:
                    final_isAllowClearInvn = self._decide_isAllowClearInvn(isASRSLocn=isPutwyLocnASRS)
                    if final_isAllowClearInvn:
                        for i in range(noOfItem):
                            item = dbRows[i]['ITEM_NAME']
                            actvLocnList = self._getManualActvLocnByItemToClear(itemBrcd=item)
                            for actvLocn in actvLocnList:
                                DBAdmin._deleteFromActvInvnTables(self.schema, locnBrcd=actvLocn, item=item)
                    else:
                        assert False, 'Clearing invn (clearing invn in actv for putaway to resv) is not allowed. Test manually'
                else:
                    '''Creating item in resv locn'''
                    final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isASRSLocn=isPutwyLocnASRS)
                    if final_isAllowCreateInvn:
                        dbRows = []
                        for i in range(noOfItem):
                            finalLpnQty = minAvailUnit if minAvailUnit is not None else 10
                            # taskPath = TaskPath(currWG='RESV', currWA=resvWA)
                            if taskPath is None:
                                taskPath = TaskPath(intType=11, destWG='RESV')
                            dbRowsNew = self._createInvnForResvPutwy(itemBrcd=None, noOfResvLocn=1, noOfLpn=1, lpnQty=[finalLpnQty],
                                                                     taskPath=taskPath, isResvWAInTPDDestWA=True,
                                                                     isItemForCrossDock=isItemForCrossDock,
                                                                     isASRSItem=isPutwyLocnASRS)
                            dbRows.extend(dbRowsNew)
                    else:
                        assert False, 'Creating invn (assigning item in resv locn) is not allowed. Test manually'

            assert len(dbRows) == noOfItem, f"<Data> {noOfItem} no. of items for putwy to resv not found " + sql

            '''Print data'''
            for i in range(0, noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'LOCN_BRCD'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItemsForResvPutwy(self, noOfItem: int, resvWG: str = 'RESV', minAvailUnit: int = None,
                                           taskPath: TaskPath = None, isResvWAFromTPathDestWA: bool = None, isItemForCrossDock: bool = False,
                                           isZoneInPutawayDir:bool=None, isItemIn1Resv:bool=None, isFetchByMaxVol:bool=None,
                                           isItemNotInActv:bool=None, isASRSItem:bool=None):
        sql = f"""select distinct ic.item_name,ic.unit_volume,lh.locn_brcd,lh.zone,lh.aisle,lh.putwy_zone,pzp.prty
                    ,rld.max_vol 
                    from wm_inventory wi inner join item_cbo ic on wi.item_id = ic.item_id
                    inner join item_facility_mapping_wms ifmw on ic.item_id = ifmw.item_id 
                    inner join size_uom su on ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
                    inner join locn_hdr lh on wi.location_id = lh.locn_id inner join resv_locn_hdr rld on lh.locn_id=rld.locn_id
                    left outer join putwy_zone_prty pzp on lh.putwy_zone=pzp.putwy_zone and pzp.putwy_method='D'  
                    where 0=0
                    #ACTV_LOCN_COND#
                    and ic.item_id not in (select item_id from wm_inventory where locn_class = 'A')
                    and ic.item_id not in (select item_id from pick_locn_dtl where item_id is not null)
                    and ic.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion = '1')
                    and ic.item_id not in (select item_id from immd_needs where item_id is not null)
                    and ic.unit_volume > '.01' and lh.work_grp='{resvWG}'
                    #CONDITION#
                    #ORDER_CONDITION#  
                    offset 0 rows fetch next {noOfItem} rows only
                """
        sqlCond = ''

        actvLocnCond = ''
        if isItemNotInActv:
            actvLocnCond += """ \n and ic.item_id not in (select item_id from wm_inventory where locn_class='A') 
                                                and ic.item_id not in (select item_id from pick_locn_dtl where item_id is not null) """
        else:
            if isASRSItem:
                vmActvWAs = self._decide_lh_workArea_forLocnType(locnType=LocnType.VLM_ACTV)
                asActvWAs = self._decide_lh_workArea_forLocnType(locnType=LocnType.AS_ACTV)
                final_avoidWorkAreaList = list(set(vmActvWAs + asActvWAs))
                actvLocnCond += """ \n and ic.item_id in (select item_id from wm_inventory where locn_class='A')"""
                actvLocnCond += f""" \n and ic.item_id not in (select item_id from wm_inventory wi,locn_hdr lh where wi.location_id=lh.locn_id and lh.locn_class='A'
                                                    and lh.work_area in {Commons.get_tuplestr(final_avoidWorkAreaList)} )"""  # avoid VM and AutoStr locns
        sql = sql.replace('#ACTV_LOCN_COND#', actvLocnCond)

        if isResvWAFromTPathDestWA:
            destWAList = self._getDestWAFromTaskPath2(taskPath)
            resvWA = destWAList
            resvWA = self.removeSpecialCharFromTaskPathVals(resvWA)
            if resvWA is not None:
                sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(resvWA)

        if isItemForCrossDock:
            sqlCond += " \n and ifmw.slot_misc_2 in ('Active-NS', 'Unreleased')"
        else:
            sqlCond += " \n and ifmw.slot_misc_2 not in ('Active-NS', 'Unreleased')"

        final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isASRSLocn=isASRSItem)
        if final_zone is not None:
            sqlCond += f" \n and lh.zone in {final_zone}"
        elif final_avoidZone is not None:
            sqlCond += f" \n and lh.zone not in {final_avoidZone}"

        final_pullZone, final_avoidPullZone = self._decide_lh_pullZone_forLocnType(isASRSLocn=isASRSItem)
        if final_pullZone is not None:
            sqlCond += f" \n and lh.pull_zone in {final_pullZone}"
        elif final_avoidPullZone is not None:
            sqlCond += f" \n and lh.pull_zone not in {final_avoidPullZone}"

        final_itemPutwyType, final_avoidItemPutwyType = self._decide_ifmw_putwyType_forItemType(isASRSItem=isASRSItem, defaultVal='STD')
        if final_itemPutwyType is not None:
            sqlCond += f" \n and ifmw.putwy_type in {final_itemPutwyType}"

        final_pzpPtwyType, final_avoidPzpPtwyType = self._decide_pzp_putwyType_forLocnType(isASRSLocn=isASRSItem, defaultVal='STD')
        if final_pzpPtwyType is not None:
            sqlCond += f" \n and pzp.putwy_type in {final_pzpPtwyType}"

        if isZoneInPutawayDir:
            sqlCond += f" \n and lh.putwy_zone in (select putwy_zone from putwy_zone_prty where putwy_type in {final_pzpPtwyType} and putwy_method='D')"
        if isItemIn1Resv:
            sqlCond += " \n and ic.item_id in (select item_id from wm_inventory where locn_class='R' group by item_id having count(item_id)=1)"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += " \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems})) """

        orderCond = ''
        if isFetchByMaxVol:
            orderCond += ' \n order by pzp.prty asc, ic.unit_volume desc'
        else:
            orderCond += ' \n order by pzp.prty asc'

        sql = sql.replace('#CONDITION#', sqlCond)
        sql = sql.replace('#ORDER_CONDITION#', orderCond)

        return sql

    def getItemsForManualActvPutwyToSameZone(self, noOfItem:int, actvWG:str= 'ACTV', minAvailCap:int=None, minAvailUnit:int=None, isCrossDockItem:bool=None):
        """Get items that are present in same actv zone, might present in resv
        """
        RuntimeXL.createThreadLockFile()
        try:
            isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = False

            '''Get item with unit'''
            sql = self._buildQueryForGetItemsForManualActvPutwyToSameZone(noOfItem=noOfItem, actvWG=actvWG, minAvailCap=minAvailCap, minAvailUnit=minAvailUnit,
                                                                          isCrossDockItem=isCrossDockItem)
            dbRows = DBService.fetch_rows(sql, self.schema)
            isItemFoundFromOrigQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False

            '''Get item without unit'''
            if not isItemFoundFromOrigQuery:
                sql = self._buildQueryForGetItemsForManualActvPutwyToSameZone(noOfItem=noOfItem, actvWG=actvWG, isCrossDockItem=isCrossDockItem)
                dbRows = DBService.fetch_rows(sql, self.schema)
                isItemFoundFromRevisedQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False

            '''Update invn for unit'''
            if isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery:
                final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn()
                if final_isAllowUpdateInvn:
                    noOfItem = noOfItem
                    final_availUnit = minAvailUnit if minAvailUnit is not None else 1
                    final_availCap = minAvailCap if minAvailCap is not None else 999

                    for i in range(noOfItem):
                        itemBrcd = dbRows[i]['ITEM_NAME']
                        locnBrcd = dbRows[i]['LOCN_BRCD']
                        final_onHand = self._presetInvnInActvLocn(i_locnBrcd=locnBrcd, i_itemBrcd=itemBrcd, f_availUnit=final_availUnit,
                                                                  f_availCap=final_availCap)
                        dbRows[i]['ON_HAND_QTY'] = final_onHand
                else:
                    assert False, 'Updating invn (updating units in actv locn) is not allowed. Test manually'
            else:
                '''Create invn in same actv zone/locn grp locns'''
                final_isAllowCreateInvn = self._decide_isAllowCreateInvn()
                if final_isAllowCreateInvn:
                    noOfActvLocn = 1
                    # zone = self.getActvLocnZoneHavingEmptyLocns(noOfLocns=noOfItem)
                    locnGrpAttr = self._getManualActvLocnGrpAttrHavingEmptyLocns(noOfLocns=noOfItem)
                    final_onHand = minAvailUnit if minAvailUnit is not None else 1
                    final_availCap = minAvailCap if minAvailCap is not None else 999
                    final_maxInvQty = int(final_onHand + final_availCap)
                    dbRows = []
                    for i in range(noOfItem):
                        dbRecs = self._createInvnForActvPutwy(noOfActvLocn=noOfActvLocn, locnGrpAttrs=[locnGrpAttr],
                                                              actvQty=final_onHand, maxInvQty=final_maxInvQty,
                                                              isCheckTaskPath=True, intType=50, isCrossDockItem=isCrossDockItem)
                        dbRows.extend(dbRecs)
                else:
                    assert False, 'Creating invn (slotting item in actv locn) is not allowed. Test manually'

            assert len(dbRows) >= noOfItem, f"<Data> {noOfItem} no. of items in same actv zone not found " + sql

            '''Print data'''
            for i in dbRows:
                self._logDBResult(i, ['ZONE', 'LOCN_BRCD', 'ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItemsForManualActvPutwyToSameZone(self, noOfItem:int, actvWG:str=None, minAvailCap:int=None, minAvailUnit:int=None, isCrossDockItem:bool=None):
        """Build query for get item present in same actv zone, might be present in resv"""
        # sql = f"""select * from (
        #             select rank() over(partition by lh.zone order by lh.zone,lh.locn_brcd) rn,lh.zone,lh.locn_brcd,lh.work_grp,lh.work_area,ic.item_name
        #             from wm_inventory wi inner join locn_hdr lh on wi.location_id = lh.locn_id
        #             inner join pick_locn_dtl pld on wi.location_id = pld.locn_id inner join item_cbo ic on wi.item_id = ic.item_id
        #             inner join size_uom su on ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
        #             where ic.item_id in (select item_id from wm_inventory where locn_class = 'A')
        #             and ic.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion = '1')
        #             and lh.work_grp = '{actvWG}'
        #             #CONDITION#
        #             order by zone)
        #         where zone in (
        #             select zone from wm_inventory wi,locn_hdr lh,pick_locn_dtl pld,item_cbo ic,size_uom su
        #             where wi.location_id=lh.locn_id and wi.location_id=pld.locn_id and wi.item_id=ic.item_id and ic.base_storage_uom_id=su.size_uom_id
        #             and lh.locn_class = 'A' and su.size_uom = 'EACH'
        #             and lh.work_grp = '{actvWG}'
        #             #CONDITION#
        #             group by zone having count(distinct wi.item_id)>={noOfItem} order by zone
        #             offset 0 rows fetch next {noOfItem} rows only)
        #         order by zone
        #         offset 0 rows fetch next {noOfItem} rows only"""
        sql = f"""select * from (
                    select rank() over(partition by lg.grp_attr order by lg.grp_attr,lh.locn_brcd) rn,lg.grp_attr,lh.zone,lh.locn_brcd,lh.work_grp,lh.work_area,ic.item_name 
                    from wm_inventory wi inner join locn_hdr lh on wi.location_id = lh.locn_id inner join locn_grp lg on lh.locn_id=lg.locn_id
                    inner join pick_locn_dtl pld on wi.location_id=pld.locn_id inner join item_cbo ic on wi.item_id=ic.item_id inner join item_facility_mapping_wms ifmw on ic.item_id=ifmw.item_id
                    inner join size_uom su on ic.base_storage_uom_id=su.size_uom_id and su.size_uom='EACH'
                    where ic.item_id in (select item_id from wm_inventory where locn_class='A')
                    and ic.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion='1')
                    and lh.work_grp='{actvWG}' and lh.sku_dedctn_type='P'
                    and lh.locn_id in (select plh.locn_id from pick_locn_hdr plh inner join putwy_method_prty pmp on plh.putwy_type=pmp.putwy_type
                                        inner join sys_code sc1 on pmp.putwy_type=sc1.code_id inner join sys_code sc2 on sc2.code_id=pmp.putwy_method and sc2.code_desc='Direct to active')
                    #CONDITION#
                    order by lg.grp_attr)
                where grp_attr in (
                    select lg.grp_attr from wm_inventory wi,locn_hdr lh,locn_grp lg,pick_locn_dtl pld,item_cbo ic,size_uom su,item_facility_mapping_wms ifmw
                    where wi.location_id=lh.locn_id and lh.locn_id=lg.locn_id and wi.location_id=pld.locn_id and wi.item_id=ic.item_id and ic.base_storage_uom_id=su.size_uom_id and ic.item_id=ifmw.item_id
                    and lh.locn_class='A' and su.size_uom='EACH' 
                    and lh.work_grp='{actvWG}' and lh.sku_dedctn_type='P'
                    and lh.locn_id in (select plh.locn_id from pick_locn_hdr plh inner join putwy_method_prty pmp on plh.putwy_type=pmp.putwy_type
                                        inner join sys_code sc1 on pmp.putwy_type=sc1.code_id inner join sys_code sc2 on sc2.code_id=pmp.putwy_method and sc2.code_desc='Direct to active')
                    #CONDITION#
                    group by lg.grp_attr having count(distinct wi.item_id)>={noOfItem} order by lg.grp_attr
                    offset 0 rows fetch next {noOfItem} rows only) 
                order by grp_attr
                offset 0 rows fetch next {noOfItem} rows only"""

        sqlCond = ''
        if minAvailCap is not None:
            sqlCond += f" \n and (wi.on_hand_qty + wi.to_be_filled_qty + {minAvailCap}) <= pld.max_invn_qty"
        if minAvailUnit is not None:
            sqlCond += f" \n and (wi.on_hand_qty - wi.wm_allocated_qty + wi.to_be_filled_qty) >= {minAvailUnit}"

        if isCrossDockItem:
            sqlCond += " \n and ifmw.slot_misc_2 in ('Active-NS', 'Unreleased')"
        else:
            sqlCond += " \n and ifmw.slot_misc_2 not in ('Active-NS', 'Unreleased')"

        final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType()
        if final_locnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
        elif final_avoidLocnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

        final_zone, final_avoidZone = self._decide_lh_zone_forLocnType()
        if final_zone is not None:
            sqlCond += f" \n and lh.zone in {final_zone}"
        elif final_avoidZone is not None:
            sqlCond += f" \n and lh.zone not in {final_avoidZone}"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns
            # sqlCond += f""" \n and lg.grp_attr not in (select grp_attr from locn_grp
            #                                 where locn_id in (select locn_id from locn_hdr where locn_brcd in {threadLocns}))"""

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='A' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', sqlCond)

        return sql

    def getManualActvZoneHavingEmptyLocns(self, noOfLocns):
        """TODO Before using this method, exclude thread locns
        """
        sql = f"""select lh.zone, count(*) as locn_count
                from locn_hdr lh where lh.locn_id not in (select location_id from wm_inventory where location_id is not null)
                and lh.locn_id not in (select locn_id from pick_locn_dtl where locn_id is not null)
                and lh.work_grp='ACTV' and lh.locn_class='A' group by lh.zone having count(*) >= {noOfLocns}
                """
        dbRow = DBService.fetch_row(sql, self.schema)

        assert len(dbRow) > 0, f"<Data> Empty actv locn zone not found " + sql
        zone = str(dbRow['ZONE'])

        return zone

    def _getManualActvLocnGrpAttrHavingEmptyLocns(self, noOfLocns):
        sql = f"""select lg.grp_attr, count(*) from locn_hdr lh,locn_grp lg 
                    where lh.locn_id=lg.locn_id 
                    and lh.work_grp='ACTV'
                    and lh.locn_id not in (select location_id from wm_inventory where location_id is not null)
                    and lh.locn_id not in (select locn_id from pick_locn_dtl where locn_id is not null)
                    and lh.sku_dedctn_type='P'
                    #CONDITION#
                    group by lg.grp_attr having count(lg.grp_attr)>={noOfLocns} order by lg.grp_attr
                """
        sqlCond = ''

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += f""" \n and lg.grp_attr not in (select grp_attr from locn_grp 
                                        where locn_id in (select locn_id from locn_hdr where locn_brcd in {threadLocns}))"""

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        assert len(dbRow) > 0, '<Data> Locn grp attr with empty actv locns not found ' + sql
        locnGrpAttr = str(dbRow['GRP_ATTR'])

        return locnGrpAttr

    def getItemsForActvPutwy(self, noOfItem: int, actvWG: str = 'ACTV', actvWA: str = None,
                             minAvailCap: int = 1, availCap: int = None, availUnit: int = None, zone: str = None,
                             isItemIn1Actv:bool=True,
                             taskPath: TaskPath = None, isActvWAFromTPathDestWA: bool = None, ignoreZone: str = None,
                             isCrossDockItem:bool=None, isPutwyLocnVLM:bool=None, isPutwyLocnAS:bool=None, isASRSItem:bool=None,
                             isForPutawayToActv:bool=True,
                             isFetchByMaxVol:bool=None):
        """(Generic method) This makes sure any actv.
        """
        RuntimeXL.createThreadLockFile()
        try:
            isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = isCreateInvnByDefault = False

            '''Get item with unit'''
            sql = self._buildQueryForGetItemsForActvPutwy(noOfItem=noOfItem, actvWG=actvWG, actvWA=actvWA,
                                                          minAvailCap=minAvailCap, availCap=availCap, availUnit=availUnit,
                                                          zone=zone, ignoreZone=ignoreZone,
                                                          isItemIn1Actv=isItemIn1Actv,
                                                          taskPath=taskPath, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
                                                          isCrossDockItem=isCrossDockItem,
                                                          isPutwyLocnVLM=isPutwyLocnVLM, isPutwyLocnAS=isPutwyLocnAS, isASRSItem=isASRSItem,
                                                          isForPutawayToActv=isForPutawayToActv,
                                                          isFetchByMaxVol=isFetchByMaxVol)
            dbRows = DBService.fetch_rows(sql, self.schema)
            isItemFoundFromOrigQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

            '''Get item without unit'''
            if not isItemFoundFromOrigQuery:
                sql = self._buildQueryForGetItemsForActvPutwy(noOfItem=noOfItem, actvWG=actvWG, actvWA=actvWA,
                                                              zone=zone, ignoreZone=ignoreZone,
                                                              isItemIn1Actv=isItemIn1Actv,
                                                              taskPath=taskPath, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
                                                              isCrossDockItem=isCrossDockItem,
                                                              isPutwyLocnVLM=isPutwyLocnVLM, isPutwyLocnAS=isPutwyLocnAS, isASRSItem=isASRSItem,
                                                              isForPutawayToActv=isForPutawayToActv,
                                                              isFetchByMaxVol=isFetchByMaxVol)
                dbRows = DBService.fetch_rows(sql, self.schema)
                isItemFoundFromRevisedQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

            '''Update invn for unit'''
            if isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery:
                final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn(isVLMLocn=isPutwyLocnVLM, isASLocn=isPutwyLocnAS, isASRSLocn=isASRSItem)
                if final_isAllowUpdateInvn:
                    noOfItem = noOfItem
                    final_onHand = availUnit if availUnit is not None else None
                    final_availCap = availCap if availCap is not None else minAvailCap if minAvailCap is not None else None
                    for i in range(noOfItem):
                        itemBrcd = dbRows[i]['ITEM_NAME']
                        locnBrcd = dbRows[i]['LOCN_BRCD']
                        final_onHand = self._presetInvnInActvLocn(i_locnBrcd=locnBrcd, i_itemBrcd=itemBrcd,
                                                                  f_onHand=final_onHand, f_availCap=final_availCap)
                        dbRows[i]['ON_HAND_QTY'] = final_onHand
                else:
                    assert False, 'Updating invn (updating units in actv locn) is not allowed. Test manually'
            else:
                '''Create item in actv locn'''
                final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isVLMLocn=isPutwyLocnVLM, isASLocn=isPutwyLocnAS, isASRSLocn=isASRSItem)
                if final_isAllowCreateInvn:
                    noOfActvLocn = 1
                    final_onHand = availUnit if availUnit is not None else 1
                    final_availCap = availCap if availCap is not None else minAvailCap if minAvailCap is not None else 999
                    final_maxInvQty = int(final_onHand + final_availCap)
                    dbRows = []
                    for i in range(noOfItem):
                        dbRecs = self._createInvnForActvPutwy(noOfActvLocn=noOfActvLocn, zone=zone, actvWA=actvWA,
                                                              actvQty=final_onHand, maxInvQty=final_maxInvQty,
                                                              taskPath=taskPath, isActvWAInTPDDestWA=isActvWAFromTPathDestWA,
                                                              isCrossDockItem=isCrossDockItem,
                                                              isPutwyLocnVLM=isPutwyLocnVLM, isPutwyLocnAS=isPutwyLocnAS, isASRSItem=isASRSItem,
                                                              isForPutawayToActv=isForPutawayToActv,
                                                              isFetchByMaxVol=isFetchByMaxVol)
                        dbRows.extend(dbRecs)
                else:
                    assert False, 'Creating invn (slotting item in actv locn) is not allowed. Test manually'

            assert len(dbRows) >= noOfItem, f"<Data> {noOfItem} no. of items in actv not found " + sql

            '''Print data'''
            for i in range(0, noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'LOCN_BRCD', 'ON_HAND_QTY', 'MAX_INVN_QTY', 'UNIT_VOLUME'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItemsForActvPutwy(self, noOfItem: int, actvWG: str = None, actvWA: str = None,
                                           minAvailCap: int = None, availCap: int = None, availUnit: int = None,
                                           zone: str = None, ignoreZone: str = None,
                                           isItemIn1Actv: bool = True,
                                           taskPath:TaskPath=None, isActvWAFromTPathDestWA:bool=None, isCrossDockItem:bool=None,
                                           isPutwyLocnVLM:bool=None, isPutwyLocnAS:bool=None, isASRSItem:bool=None,
                                           isForPutawayToActv:bool=True, isFetchByMaxVol:bool=None):
        """Build query for get item in actv locn for putway
        """
        sql = f"""select /*+ PARALLEL(wi,8) */ lh.locn_brcd,lh.work_grp,lh.work_area,lh.zone,ic.item_name,ic.unit_volume,pld.max_invn_qty, wi.* from wm_inventory wi 
                    inner join item_cbo ic on wi.item_id=ic.item_id
                    inner join item_facility_mapping_wms ifmw on ic.item_id=ifmw.item_id 
                    inner join size_uom su on ic.base_storage_uom_id=su.size_uom_id and su.size_uom='EACH'
                    inner join locn_hdr lh on wi.location_id=lh.locn_id
                    inner join pick_locn_dtl pld on wi.location_id=pld.locn_id and wi.item_id=pld.item_id 
                    where 0=0
                    #PUTAWAY_METHOD_COND#                   
                    and ic.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion = '1')
                    and ic.item_id not in (select item_id from immd_needs where item_id is not null)
                    and ic.unit_volume > '.01' and lh.sku_dedctn_type='P'
                    #CONDITION#
                    #ORDER_CONDITION#
                    offset 0 rows fetch next {noOfItem} rows only
              """
        sqlCond = ''
        if actvWG is not None:
            sqlCond += f" \n and lh.work_grp = '{actvWG}'"

        if actvWA is None:
            if isActvWAFromTPathDestWA:
                destWAList = self._getDestWAFromTaskPath2(taskPath)
                actvWA = destWAList
        actvWA = self.removeSpecialCharFromTaskPathVals(actvWA)
        if actvWA is not None:
            sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(actvWA)

        if isItemIn1Actv:
            sqlCond += " \n and ic.item_id in (select item_id from wm_inventory where locn_class='A' group by item_id having count(item_id)=1)"

        putwyMethodCond = ''
        if isForPutawayToActv:
            putwyMethodCond += """ \n and lh.locn_id in (select plh.locn_id from pick_locn_hdr plh inner join putwy_method_prty pmp on plh.putwy_type=pmp.putwy_type
                                                inner join sys_code sc1 on pmp.putwy_type=sc1.code_id inner join sys_code sc2 on sc2.code_id = pmp.putwy_method and sc2.code_desc='Direct to active')"""

        if availCap is not None:
            sqlCond += f" \n and (wi.on_hand_qty+wi.to_be_filled_qty + {availCap}) = pld.max_invn_qty"
        elif minAvailCap is not None:
            sqlCond += f" \n and (wi.on_hand_qty+wi.to_be_filled_qty + {minAvailCap}) <= pld.max_invn_qty"
        if availUnit is not None:
            sqlCond += f" \n and (wi.on_hand_qty-wi.wm_allocated_qty) >= {availUnit}"

        '''Default min max_invn_qty check'''
        maxOfAllUnits = max(minAvailCap or 0, availCap or 0, availUnit or 0)
        sqlCond += f" \n and pld.max_invn_qty >= {maxOfAllUnits}"

        if isCrossDockItem:
            sqlCond += " \n and ifmw.slot_misc_2 in ('Active-NS', 'Unreleased')"
        else:
            sqlCond += " \n and ifmw.slot_misc_2 not in ('Active-NS', 'Unreleased')"

        final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isVLMLocn=isPutwyLocnVLM, isASLocn=isPutwyLocnAS, isASRSLocn=isASRSItem, providedVal=zone)
        if final_zone is not None:
            sqlCond += f" \n and lh.zone in {final_zone}"
        elif final_avoidZone is not None:
            sqlCond += f" \n and lh.zone not in {final_avoidZone}"

        if ignoreZone is not None:
            sqlCond += " \n and lh.zone not in ('" + str(ignoreZone) + "')"

        final_itemPutwyType, final_avoidItemPutwyType = \
            self._decide_ifmw_putwyType_forItemType(isVLMItem=isPutwyLocnVLM, isASItem=isPutwyLocnAS, isASRSItem=isASRSItem,
                                                    defaultVal='STD')
        if final_itemPutwyType is not None:
            sqlCond += f" \n and ifmw.putwy_type in {final_itemPutwyType}"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='A' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        orderCond = ''
        if isFetchByMaxVol:
            orderCond += ' \n order by ic.unit_volume desc'
        else:
            orderCond += ' \n order by lh.locn_pick_seq asc'

        sql = sql.replace('#PUTAWAY_METHOD_COND#', putwyMethodCond)
        sql = sql.replace('#CONDITION#', sqlCond)
        sql = sql.replace('#ORDER_CONDITION#', orderCond)

        return sql

    def getItemsForActvPick2(self, noOfItem: int,
                             # isPCKItem: bool = None, isSLPItem:bool=None, isWPItem:bool=None,
                             consolInvnType: ConsolInvnType = None, isTHMItem:bool=None,
                             actvWG: str = 'ACTV', actvWA: Union[str, list] = None, zone: Union[str, list] = None, area: list[str] = None, aisle:list[str]=None,
                             taskPath: TaskPath = None, isActvWAInTPDDestWA: bool = None,
                             minAvailCap: int = None, minAvailUnit: int = None, availUnit: int = None,
                             minOnHand: int = None, onHand: int = None,
                             isCcPending: bool = None, isItemIn1Actv: bool = None, isItemNotInResv: bool = None,
                             itemAllocType: list[str] = None, pickDetrmZone: list[str] = None, ignoreActvLocn: list[str] = None,
                             isLocnConveyable:bool = None, isHazmatItem:bool=None, isCrossDockItem:bool=None,
                             isPromoLocn:bool=None, isPickLocnVLM:bool=None, isPickLocnAS:bool=None):
        """(Generic method) This makes sure any actv.
        Get items in actv locn based on diff params
        If not found, create new one or clear existing one.
        isLocnConveyable: Needed for manifest -> cntrType as BOX(dim is within UPS cap) instead PLT(dim beyond UPS cap).
        """
        RuntimeXL.createThreadLockFile()
        try:
            isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = isCreateInvnByDefault = False

            '''Get item with unit, cc'''
            sql = self._buildQueryForGetItemsForActvPick(noOfItem=noOfItem,
                                                         # isPCKItem=isPCKItem, isSLPItem=isSLPItem, isWPItem=isWPItem,
                                                         consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                                         actvWG=actvWG, actvWA=actvWA, zone=zone, area=area, aisle=aisle,
                                                         taskPath=taskPath, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                                         minAvailCap=minAvailCap, minAvailUnit=minAvailUnit, availUnit=availUnit,
                                                         minOnHand=minOnHand, onHand=onHand, isCcPending=isCcPending,
                                                         isItemIn1Actv=isItemIn1Actv, isItemNotInResv=isItemNotInResv,
                                                         itemAllocType=itemAllocType, pickDetrmZone=pickDetrmZone,
                                                         ignoreActvLocn=ignoreActvLocn, isLocnConveyable=isLocnConveyable, isHazmatItem=isHazmatItem,
                                                         isPromoLocn=isPromoLocn, isCrossDockItem=isCrossDockItem, isPickLocnVLM=isPickLocnVLM, isPickLocnAS=isPickLocnAS)
            dbRows = DBService.fetch_rows(sql, self.schema)
            isItemFoundFromOrigQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

            '''Get item without unit, cc'''
            if not isItemFoundFromOrigQuery:
                dbRows = None
                sql = self._buildQueryForGetItemsForActvPick(noOfItem=noOfItem,
                                                             # isPCKItem=isPCKItem, isSLPItem=isSLPItem, isWPItem=isWPItem,
                                                             consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                                             actvWG=actvWG, actvWA=actvWA, zone=zone, area=area, aisle=aisle,
                                                             taskPath=taskPath, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                                             isItemIn1Actv=isItemIn1Actv, isItemNotInResv=isItemNotInResv,
                                                             itemAllocType=itemAllocType, pickDetrmZone=pickDetrmZone,
                                                             ignoreActvLocn=ignoreActvLocn, isLocnConveyable=isLocnConveyable, isHazmatItem=isHazmatItem,
                                                             isPromoLocn=isPromoLocn, isCrossDockItem=isCrossDockItem, isPickLocnVLM=isPickLocnVLM, isPickLocnAS=isPickLocnAS)
                dbRows = DBService.fetch_rows(sql, self.schema)
                isItemFoundFromRevisedQuery = False if len(dbRows) < noOfItem or dbRows[0]['ITEM_NAME'] is None else True

            '''Update invn for unit, cc'''
            if isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery:
                final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn(isVLMLocn=isPickLocnVLM, isASLocn=isPickLocnAS)
                if final_isAllowUpdateInvn:
                    noOfItem = noOfItem
                    final_availUnit = availUnit if availUnit is not None else minAvailUnit if minAvailUnit is not None else None
                    final_onHand = onHand if onHand is not None else minOnHand if minOnHand is not None else None

                    for i in range(noOfItem):
                        itemBrcd = dbRows[i]['ITEM_NAME']
                        locnBrcd = dbRows[i]['LOCN_BRCD']

                        final_onHand = self._presetInvnInActvLocn(i_locnBrcd=locnBrcd, i_itemBrcd=itemBrcd, f_onHand=final_onHand,
                                                                  f_availUnit=final_availUnit, f_isCCPending=isCcPending)
                        dbRows[i]['ON_HAND_QTY'] = final_onHand
                        dbRows[i]['AVAIL_UNIT'] = final_availUnit
                else:
                    assert False, 'Updating invn (updating units in actv locn) is not allowed. Test manually'
            else:
                '''Create invn'''
                isCreateInvnByDefault = True
                if isCreateInvnByDefault:
                    final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isVLMLocn=isPickLocnVLM, isASLocn=isPickLocnAS)
                    if final_isAllowCreateInvn:
                        noOfActvLocn = 1
                        final_onHand = onHand if onHand is not None else availUnit if availUnit is not None else 1
                        final_availCap = 999
                        final_maxInvQty = int(final_onHand + final_availCap)

                        dbRows = []
                        for i in range(noOfItem):
                            ignoreItems = None if len(dbRows) == 0 else {r['ITEM_NAME'] for r in dbRows}
                            dbRecs = self._createInvnForActvPick(noOfActvLocn=noOfActvLocn, zone=zone, area=area, aisle=aisle, actvQty=final_onHand,
                                                                 maxInvQty=final_maxInvQty,
                                                                 # isPCKItem=isPCKItem, isSLPItem=isSLPItem, isWPItem=isWPItem,
                                                                 consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                                                 isCheckTaskPath=True, intType=50,
                                                                 itemAllocType=itemAllocType, pickDetrmZone=pickDetrmZone,
                                                                 isLocnConveyable=isLocnConveyable, isHazmatItem=isHazmatItem, ignoreItems=ignoreItems,
                                                                 isPromoLocn=isPromoLocn, isCrossDockItem=isCrossDockItem, isPickLocnVLM=isPickLocnVLM, isPickLocnAS=isPickLocnAS)
                            dbRows.extend(dbRecs)
                    else:
                        assert False, 'Creating invn (slotting item in actv locn) is not allowed. Test manually'

            assert len(dbRows) >= noOfItem, f"<Data> {noOfItem} no. of items present only in actv not found " + sql

            '''Print data'''
            for i in range(noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'LOCN_BRCD', 'ON_HAND_QTY', 'AVAIL_UNIT', 'MAX_INVN_QTY'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getItemsInAutoActvLocn(self, noOfItem: int, isVLMItem: bool = None, isASItem: bool = None,
                               isFetchByMaxVol: bool = None):
        RuntimeXL.createThreadLockFile()
        try:
            sql = self._buildQueryForGetItemsForActvPick(noOfItem=noOfItem, isPickLocnVLM=isVLMItem, isPickLocnAS=isASItem)
            dbRows = DBService.fetch_rows(sql, self.schema)

            assert len(dbRows) == noOfItem, f"<Data> {noOfItem} no. of items in auto actv locn not found " + sql

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getManualActvLocnWithItem(self, noOfLocn: int, isLocnWith1Item: bool = None, isItemNotInResv: bool = None, minOnHand: int = None,
                                  ignoreLocn: list[str] = None):
        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select lh.locn_brcd, lh.locn_id,ic.item_name, ic.item_id, pld.max_invn_qty, pld.min_invn_qty,su.size_uom 
                     ,wm.on_hand_qty, wm.wm_allocated_qty, wm.to_be_filled_qty, lh.locn_id, lh.zone, lh.aisle
                     from wm_inventory wm inner join item_cbo ic on wm.item_id=ic.item_id
                     inner join locn_hdr lh on wm.location_id=lh.locn_id inner join pick_locn_dtl pld on lh.locn_id=pld.locn_id
                     inner join size_uom su on ic.base_storage_uom_id = su.size_uom_id and su.size_uom='EACH'
                     where wm.locn_class='A' and lh.sku_dedctn_type='P' 
                     #CONDITION#  
                     order by lh.locn_brcd
                     offset 0 rows fetch next {noOfLocn} rows only
                  """
            sqlCond = ''
            if isLocnWith1Item:
                sqlCond += " \n and wm.location_id in (select location_id from wm_inventory where locn_class='A' group by location_id having count(location_id)=1)"
            if isItemNotInResv:
                sqlCond += " \n and ic.item_id not in (select item_id from wm_inventory where locn_class='R')"
            if minOnHand is not None:
                sqlCond += f" \n and wm.on_hand_qty >= {minOnHand}"
            if ignoreLocn is not None:
                sqlCond += " \n and lh.locn_brcd not in " + Commons.get_tuplestr(ignoreLocn)

            final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isVLMLocn=False, isASLocn=False)
            if final_zone is not None:
                sqlCond += f" \n and lh.zone in {final_zone}"
            elif final_avoidZone is not None:
                sqlCond += f" \n and lh.zone not in {final_avoidZone}"

            final_refField10 = self._decide_ic_refField10_forItemType()
            if final_refField10 is not None:
                sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
            else:
                sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            '''Exclude runtime thread items'''
            threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
            if threadItems is not None:
                sqlCond += f" \n and ic.item_name not in " + threadItems
                sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='A' 
                                                and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)
            assert len(dbRows) > 0, f"<Data> {noOfLocn} no.of actv locn not found " + sql

            '''Update runtime thread data file'''
            locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getEmptyManualActvLocn3(self, noOfLocn:int, actvWG:str= 'ACTV', actvWA:Union[str, list]=None,
                                taskPath:TaskPath=None, isActvWAInTPDCurrWA:bool=None, isActvWAInTPDDestWA:bool=None,
                                zone:Union[str, list]=None, area:list[str]=None, aisle:list[str]=None, locnGrpAttrs:list=None,
                                # isPCKLocn:bool=None, isSLPLocn:bool=None, isWPLocn:bool=None,
                                consolInvnType: ConsolInvnType = None, isTHMLocn:bool=None,
                                pickDetrmZone:list[str]=None, locnType:str=None, isReplenElig:bool=None,
                                isLocnConveyable:bool=None, isForPutawayToActv:bool=None, isPromoLocn:bool=None,
                                f_isClearLocnIfNotFound:bool=True, f_isAssertResult:bool=True):
        """Return empty actv locn if found.
        Else if allowed to clear invn, then get any actv locn and clear it
        """
        RuntimeXL.createThreadLockFile()
        try:
            dbRows = None
            isEmptyActvLocnFound = isActvLocnFoundToClear = False

            '''Get empty actv locn'''
            sql = self._buildQueryForGetActvLocn(noOfLocn=noOfLocn, actvWG='ACTV', actvWA=actvWA,
                                                 taskPath=taskPath, isActvWAInTPDCurrWA=isActvWAInTPDCurrWA, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                                 zone=zone, area=area, aisle=aisle, locnGrpAttrs=locnGrpAttrs, pickDetrmZone=pickDetrmZone,
                                                 locnType=locnType, isReplenElig=isReplenElig,
                                                 # isPCKLocn=isPCKLocn, isSLPLocn=isSLPLocn, isWPLocn=isWPLocn,
                                                 consolInvnType=consolInvnType, isTHMLocn=isTHMLocn,
                                                 isLocnConveyable=isLocnConveyable, isForPutawayToActv=isForPutawayToActv, isLocnWithNoItem=True,
                                                 isPromoLocn=isPromoLocn)
            dbRows = DBService.fetch_rows(sql, self.schema)
            isEmptyActvLocnFound = True if dbRows is not None and len(dbRows) >= noOfLocn else False

            '''Get actv locn to clear'''
            if f_isAssertResult and not isEmptyActvLocnFound:
                dbRows = self._getManualActvLocnToClear3(noOfLocn=noOfLocn, actvWG='ACTV', actvWA=actvWA,
                                                         taskPath=taskPath, isActvWAInTPDCurrWA=isActvWAInTPDCurrWA, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                                         zone=zone, area=area, aisle=aisle, locnGrpAttrs=locnGrpAttrs, pickDetrmZone=pickDetrmZone,
                                                         locnType=locnType, isReplenElig=isReplenElig,
                                                         # isPCKLocn=isPCKLocn, isSLPLocn=isSLPLocn, isWPLocn=isWPLocn,
                                                         consolInvnType=consolInvnType, isTHMLocn=isTHMLocn,
                                                         isLocnConveyable=isLocnConveyable, isForPutawayToActv=isForPutawayToActv,
                                                         isPromoLocn=isPromoLocn)

                isActvLocnFoundToClear = True if dbRows is not None and len(dbRows) == noOfLocn else False

                '''Clear actv locn records (wm and pld)'''
                if f_isClearLocnIfNotFound:
                    final_isAllowClearInvn = self._decide_isAllowClearInvn()
                    if final_isAllowClearInvn:
                        if isActvLocnFoundToClear:
                            for i in range(noOfLocn):
                                item = dbRows[i]['ITEM_NAME']
                                actvLocn = dbRows[i]['LOCN_BRCD']
                                DBAdmin._deleteFromActvInvnTables(self.schema, locnBrcd=actvLocn, item=item)
                    else:
                        assert False, 'Clearing actv invn (deslotting item in actv locn) is not allowed. Test manually'

            if f_isAssertResult:
                assert len(dbRows) == noOfLocn, f"<Data> {noOfLocn} no. of empty manual actv locns not found " + sql

            if isEmptyActvLocnFound or isActvLocnFoundToClear:
                '''Update runtime thread data file'''
                locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
                RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _getManualActvLocnToClear3(self, noOfLocn:int, actvWG:str= 'ACTV', actvWA:Union[str, list]=None,
                                   taskPath:TaskPath=None, isActvWAInTPDCurrWA:bool=None, isActvWAInTPDDestWA:bool=None,
                                   zone:Union[str, list]=None, area:list[str]=None, aisle:list[str]=None, locnGrpAttrs:list=None,
                                   pickDetrmZone:list[str]=None, locnType:str=None, isReplenElig:bool=None,
                                   # isPCKLocn:bool=None, isSLPLocn:bool=None, isWPLocn:bool=None,
                                   consolInvnType: ConsolInvnType = None, isTHMLocn:bool=None,
                                   isLocnWithItem:bool=None, isLocnHas1item:bool=None, isLocnWithNoItem:bool=None,
                                   isLocnConveyable:bool=None, isForPutawayToActv:bool=None, isPromoLocn:bool=None):
        """Return actv locn created by automation
        Else get actv locn created by any
        """
        actvLocnRows=None
        isLocnFoundCreatedByAuto = isLocnFoundCreatedByAny = False

        '''Fetch locn created by automation'''
        sql = self._buildQueryForGetActvLocn(noOfLocn=noOfLocn, actvWG='ACTV', actvWA=actvWA,
                                             taskPath=taskPath, isActvWAInTPDCurrWA=isActvWAInTPDCurrWA, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                             zone=zone, area=area, aisle=aisle, pickDetrmZone=pickDetrmZone, locnGrpAttrs=locnGrpAttrs,
                                             locnType=locnType, isReplenElig=isReplenElig,
                                             # isPCKLocn=isPCKLocn, isSLPLocn=isSLPLocn, isWPLocn=isWPLocn,
                                             consolInvnType=consolInvnType, isTHMLocn=isTHMLocn,
                                             isLocnWithItem=True, isLocnHas1item=True,
                                             isLocnConveyable=isLocnConveyable, isForPutawayToActv=isForPutawayToActv,
                                             isPromoLocn=isPromoLocn,
                                             isCreatedByAutom=True)
        actvLocnRows = DBService.fetch_rows(sql, self.schema)
        isLocnFoundCreatedByAuto = True if actvLocnRows and len(actvLocnRows) >= noOfLocn and actvLocnRows[0]['LOCN_BRCD'] is not None else False

        '''Fetch locn created by any'''
        if not isLocnFoundCreatedByAuto:
            sql = self._buildQueryForGetActvLocn(noOfLocn=noOfLocn, actvWG='ACTV', actvWA=actvWA,
                                                 taskPath=taskPath, isActvWAInTPDCurrWA=isActvWAInTPDCurrWA, isActvWAInTPDDestWA=isActvWAInTPDDestWA,
                                                 zone=zone, area=area, aisle=aisle, pickDetrmZone=pickDetrmZone, locnGrpAttrs=locnGrpAttrs,
                                                 locnType=locnType, isReplenElig=isReplenElig,
                                                 # isPCKLocn=isPCKLocn, isSLPLocn=isSLPLocn, isWPLocn=isWPLocn,
                                                 consolInvnType=consolInvnType, isTHMLocn=isTHMLocn,
                                                 isLocnWithItem=True, isLocnHas1item=True,
                                                 isLocnConveyable=isLocnConveyable, isForPutawayToActv=isForPutawayToActv,
                                                 isPromoLocn=isPromoLocn)
            actvLocnRows = DBService.fetch_rows(sql, self.schema)
            isLocnFoundCreatedByAny = True if actvLocnRows and len(actvLocnRows) >= noOfLocn and actvLocnRows[0]['LOCN_BRCD'] is not None else False

        assert isLocnFoundCreatedByAuto or isLocnFoundCreatedByAny, f"<Data> {noOfLocn} no. of manual actv locn not found to clear " + sql

        return actvLocnRows

    def getAutoActvLocn(self, noOfLocn: int, actvWG: str = 'ACTV', isVLMLocn:bool=None, isASLocn:bool=None):
        """Get automated actv locns present in wm_inventory and pick location dtl
        eg: VLM, AutoStore
        """
        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select distinct lh.* from locn_hdr lh
                     where lh.locn_id  in (select location_id from wm_inventory where location_id is not null)
                     and lh.locn_id  in (select locn_id from pick_locn_dtl where locn_id is not null)
                     and lh.locn_class='A' and lh.work_grp='{actvWG}'  
                     #CONDITION#
                     offset 0 rows fetch next {noOfLocn} rows only
                  """
            sqlCond = ''

            final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isVLMLocn=isVLMLocn, isASLocn=isASLocn)
            if final_zone is not None:
                sqlCond += f" \n and lh.zone in {final_zone}"
            elif final_avoidZone is not None:
                sqlCond += f" \n and lh.zone not in {final_avoidZone}"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)

            # assert len(dbRows) == noOfLocn, str(noOfLocn) + ' no. of actv locns not present in wm not found ' + sql

            '''Update runtime thread data file'''
            locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getEmptyManualResvLocn(self, noOfLocn: int, resvWG: str = 'RESV', resvWA: Union[str, list] = None,
                               isPullZoneInAllocPrty: bool = None, pullZone: str = None, itemAllocType:list[str]=['STD'],
                               taskPath: TaskPath = None, isResvWAInTPDCurrWA: bool = None):
        """Get resv locns not present in wm_inventory
        """
        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select distinct lh.* from locn_hdr lh
                     where lh.locn_id not in (select location_id from wm_inventory where location_id is not null) 
                     and lh.locn_class='R' and lh.work_grp='{resvWG}'
                     #CONDITION#
                     offset 0 rows fetch next {noOfLocn} rows only
                  """
            sqlCond = ''

            if resvWA is None:
                if isResvWAInTPDCurrWA:
                    currWAList = self._getCurrWAFromTaskPath2(taskPath)
                    resvWA = currWAList
            resvWA = self.removeSpecialCharFromTaskPathVals(resvWA)
            if resvWA is not None:
                sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(resvWA)

            final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType()
            if final_locnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
            elif final_avoidLocnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

            final_itemAllocType, final_avoidItemAllocType = \
                self._decide_iap_allocType_forItemType(providedVal=itemAllocType[0], defaultVal='STD')
            if isPullZoneInAllocPrty:
                sqlCond += f""" \n and lh.pull_zone in (select pull_zone from invn_alloc_prty where invn_need_type='{taskPath.INT_TYPE}' 
                                            and alloc_type in {final_itemAllocType})"""
            final_pullZone, final_avoidPullZone = self._decide_lh_pullZone_forLocnType(providedVal=pullZone)
            if final_pullZone is not None:
                sqlCond += f" \n and lh.pull_zone in {final_pullZone}"
            elif final_avoidPullZone is not None:
                sqlCond += f" \n and lh.pull_zone not in {final_avoidPullZone}"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)

            assert len(dbRows) == noOfLocn, f"<Data> {noOfLocn} no. of manual resv locns not present in wm not found " + sql

            '''Update runtime thread data file'''
            locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getResvLocn(self, noOfLocn: int, resvWG: str = 'RESV', resvWA: Union[str, list] = None,
                    taskPath: TaskPath = None, isResvWAInTPDCurrWA: bool = None, isResvWAInTPDDestWA:bool=None,
                    isPullZoneInAllocPrty: bool = None, pullZone: str = None, itemAllocType:list[str]=['STD'], isLocnWithTBF: bool = None,
                    isLocnWith1Item: bool = None, ignoreItem: list[str] = None, ignoreLocn: list[str] = None,
                    isASRSLocn:bool=None):
        """(Generic method) This makes sure any resv.
        Get resv locns
        """
        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select /*+ PARALLEL(lh,8) */ distinct lh.* from locn_hdr lh 
                     --left outer join wm_inventory wm on lh.locn_id = wm.location_id
                     where lh.locn_class='R' and lh.work_grp='{resvWG}'
                     #CONDITION#
                     offset 0 rows fetch next {noOfLocn} rows only
                  """
            sqlCond = ''

            if resvWA is None:
                if isResvWAInTPDCurrWA:
                    currWAList = self._getCurrWAFromTaskPath2(taskPath)
                    resvWA = currWAList
                elif isResvWAInTPDDestWA:
                    destWAList = self._getDestWAFromTaskPath2(taskPath)
                    resvWA = destWAList
            resvWA = self.removeSpecialCharFromTaskPathVals(resvWA)
            if resvWA is not None:
                sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(resvWA)

            final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType(isASRSLocn=isASRSLocn)
            if final_locnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
            elif final_avoidLocnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

            final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isASRSLocn=isASRSLocn)
            if final_zone is not None:
                sqlCond += f" \n and lh.zone in {final_zone}"
            elif final_avoidZone is not None:
                sqlCond += f" \n and lh.zone not in {final_avoidZone}"

            final_pullZone, final_avoidPullZone = self._decide_lh_pullZone_forLocnType(isASRSLocn=isASRSLocn, providedVal=pullZone)
            if final_pullZone is not None:
                sqlCond += f" \n and lh.pull_zone in {final_pullZone}"
            elif final_avoidPullZone is not None:
                sqlCond += f" \n and lh.pull_zone not in {final_avoidPullZone}"

            final_itemAllocType, final_avoidItemAllocType = self._decide_iap_allocType_forItemType(isASRSLocn=isASRSLocn, providedVal=itemAllocType[0], defaultVal='STD')
            if isPullZoneInAllocPrty:
                sqlCond += f""" \n and lh.pull_zone in (select pull_zone from invn_alloc_prty where invn_need_type='{taskPath.INT_TYPE}' 
                                            and alloc_type in {final_itemAllocType})"""

            if isLocnWith1Item:
                sqlCond += " \n and locn_id in (select location_id from wm_inventory where locn_class='R' group by location_id having count(location_id)=1)"
            if ignoreItem is not None:
                sqlCond += f""" \n and locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                                and item_id in (select item_id from item_cbo where item_name in {Commons.get_tuplestr(ignoreItem)}))"""
            if ignoreLocn is not None:
                sqlCond += " \n and lh.locn_brcd not in " + Commons.get_tuplestr(ignoreLocn)
            if isLocnWithTBF:
                sqlCond += " \n and locn_id in (select location_id from wm_inventory where locn_class='R' and to_be_filled_qty > 0)"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            '''Exclude runtime thread items'''
            threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
            if threadItems is not None:
                sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                                and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

            sql = sql.replace('#CONDITION#', sqlCond)
            dbRows = DBService.fetch_rows(sql, self.schema)

            # assert len(dbRows) == noOfLocn, str(noOfLocn) + ' no. of resv locns not found ' + sql

            '''Update runtime thread data file'''
            locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def assertResvLocnHasNoInvnLock(self, locn):
        sql = f"""select lh.locn_brcd,rlh.locn_id,rlh.locn_putaway_lock,rlh.invn_lock_code 
                    from resv_locn_hdr rlh inner join locn_hdr lh on rlh.locn_id=lh.locn_id 
                    where lh.locn_brcd in ('{locn}')"""
        dbRow = DBService.fetch_row(sql, self.schema)
        lockCode = dbRow['INVN_LOCK_CODE']

        assert lockCode is None, f"<Lock> Resv locn {locn} has invn lock {lockCode} " + sql

    def getManualResvLocnWithNoLock(self, noOfLocn: int):
        """"""
        sql = f"""select a.locn_brcd,a.invn_lock_code, b.lpn_count from
                    (select lh.locn_brcd,rlh.invn_lock_code ,lh.locn_id from locn_hdr lh
                    inner join resv_locn_hdr rlh on lh.locn_id=rlh.locn_id
                    where rlh.invn_lock_code is null and lh.work_grp = 'RESV' and rlh.dedctn_item_id is null) a
                left outer join
                    (select lh.locn_brcd,lh.locn_id, count(wi.location_id) as lpn_count from locn_hdr lh 
                    left outer join wm_inventory wi on lh.locn_id = wi.location_id where lh.locn_class = 'R'
                    group by lh.locn_id, lh.locn_brcd) b
                on a.locn_id = b.locn_id 
                where 0=0 
                #CONDITION#
                order by b.lpn_count asc
                offset 0 rows fetch next {noOfLocn} rows only"""
        sqlCond = ''

        final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType()
        if final_locnBrcd is not None:
            sqlCond += f" \n and a.locn_brcd in {final_locnBrcd}"
        elif final_avoidLocnBrcd is not None:
            sqlCond += f" \n and a.locn_brcd not in {final_avoidLocnBrcd}"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and a.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f""" \n and a.locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        return dbRows

    def getMaxVolFromPutwyDirResvLocns(self, isASRSLocn:bool=None):
        """(Generic method) This makes sure any resv.
        """
        sql = f"""select max(rld.max_vol) max_vol from resv_locn_hdr rld 
                inner join locn_hdr lh on rld.locn_id = lh.locn_id
                inner join putwy_zone_prty pzp on lh.putwy_zone=pzp.putwy_zone and pzp.putwy_method='D' 
                #CONDITION# 
              """
        sqlCond = ''

        final_pzpPtwyType, final_avoidPzpPtwyType = self._decide_pzp_putwyType_forLocnType(isASRSLocn=isASRSLocn, defaultVal='STD')
        if final_pzpPtwyType is not None:
            sqlCond += f" \n and pzp.putwy_type in {final_pzpPtwyType}"

        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        maxVol = dbRow['MAX_VOL']
        printit(f">>> Max volume from putaway directed resv locns {maxVol}")

        return maxVol

    def getStagingLocn(self,noOfLocn:int):
        sql = f"""select locn_brcd from locn_hdr where locn_class='S' 
                    offset 0 rows fetch next {noOfLocn} rows only """
        dbRows = DBService.fetch_rows(sql,self.schema)

        return dbRows

    def getDockDoorName(self, locnBrcd: str):
        """Get dockdoor name from locn brcd"""
        sql = f"""select dd.dock_door_name, lh.locn_brcd from dock_door dd 
                    inner join locn_hdr lh on dd.dock_door_name='DOOR' || substr(lh.locn_brcd, 6, length(lh.locn_brcd))
                    where lh.locn_brcd = '{locnBrcd}'
              """
        dbRow = DBService.fetch_row(sql, self.schema)
        sysDockDoor = dbRow.get('DOCK_DOOR_NAME')
        dbDockDoor = dbRow.get('LOCN_BRCD')

        return sysDockDoor, dbDockDoor

    def getOpenDockDoor(self, workGrp: str, workArea: str):
        """Returns a dockdoor with specific WG/WA
        """
        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select dd.dock_door_name, lh.locn_brcd from dock_door dd inner join sys_code sc on sc.code_id = dd.dock_door_status
                     inner join locn_hdr lh on dd.dock_door_name='DOOR' || substr(lh.locn_brcd, 6, length(lh.locn_brcd))
                     where 0=0 and sc.code_type='Y04' and sc.code_desc='Open'
                     and dd.dock_door_name like 'DOOR%' and length(dd.dock_door_name) >= 6
                     and dd.dock_door_name not in ('DOOR55','DOOR00') --TODO temp fix, remove later
                     and lh.putwy_zone='IBS' 
                     and lh.work_grp like '#WORK_GRP#' and lh.work_area like '#WORK_AREA#'
                     #CONDITION#
                  """
            sql = sql.replace('#WORK_GRP#', workGrp).replace('#WORK_AREA#', workArea)

            sqlCond = ''

            '''Exclude runtime thread dockdoor'''
            threadDockdoors = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.DOCKDOOR, replaceFrom=',', replaceWith="','")
            if threadDockdoors is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadDockdoors
            sql = sql.replace('#CONDITION#', sqlCond)

            dbRow = DBService.fetch_row(sql, self.schema)

            # assert dbRow is not None and dbRow.get('DOCK_DOOR_NAME') is not None, 'Open dockdoor not found ' + sql
            isDDFound = dbRow is not None and dbRow.get('DOCK_DOOR_NAME') is not None
            if not isDDFound:
                dbRow = self._clearInUseDockDoor(workGrp, workArea)
            assert dbRow is not None and dbRow.get('DOCK_DOOR_NAME') is not None, '<Data> Open dockdoor not found ' + sql

            systemDockDoor = dbRow.get('DOCK_DOOR_NAME')
            dbDockDoor = dbRow.get('LOCN_BRCD')
            self.logger.info(f"Dockdoor {systemDockDoor} {dbDockDoor}")

            '''Update runtime thread data file'''
            dockDoorAsCsv = dbRow.get('DOCK_DOOR_NAME')
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.DOCKDOOR, dockDoorAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return systemDockDoor, dbDockDoor

    def _clearInUseDockDoor(self, workGrp: str, workArea: str):
        if 'true' in self.IS_ALLOW_CLEAR_DOCKDOOR:
            sql = f"""select dd.dock_door_id, dd.dock_door_name, lh.locn_brcd from dock_door dd inner join 
                     dock_door_ref drf on dd.dock_door_id=drf.dock_door_id inner join sys_code sc on sc.code_id=dd.dock_door_status
                     inner join locn_hdr lh on dd.dock_door_name='DOOR' || substr(LH.LOCN_BRCD, 6, length(LH.LOCN_BRCD))
                     where 0=0 and sc.code_type='Y04' and sc.code_desc <> 'Open'
                     and dd.dock_door_name like 'DOOR%' and length(dd.dock_door_name) >= 6
                     and dd.dock_door_name not in ('DOOR55','DOOR00') --TODO temp fix, remove later
                     and lh.putwy_zone='IBS'  
                     and lh.work_grp like '{workGrp}' and lh.work_area like '{workArea}'
                     #CONDITION#
                     order by dd.last_updated_dttm
                     offset 0 rows fetch next 1 rows only"""
            sqlCond = ''

            '''Exclude runtime thread dockdoor'''
            threadDockdoors = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.DOCKDOOR, replaceFrom=',', replaceWith="','")
            if threadDockdoors is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadDockdoors
            sql = sql.replace('#CONDITION#', sqlCond)

            dbRow = DBService.fetch_row(sql, self.schema)

            dockDoorId = dbRow.get('DOCK_DOOR_ID')
            systemDockDoor = dbRow.get('DOCK_DOOR_NAME')
            dbDockDoor = dbRow.get('LOCN_BRCD')

            assert dbRow is not None and dockDoorId is not None, '<Data> In use dockdoor not found ' + sql

            updQ1 = f"""update dock_door_ref set asn_id=null,shipment_id=null,appointment_id=null,trailer_number=null,
                       carrier_id=null,trailer_id=null,last_updated_dttm=systimestamp,last_updated_source='AUTOMATION'
                       where dock_door_id in ('{dockDoorId}')"""
            DBService.update_db(updQ1, self.schema)

            updQ2 = f"""update dock_door set dock_door_status=204,last_updated_dttm=systimestamp 
                       where dock_door_id in ('{dockDoorId}')"""
            DBService.update_db(updQ2, self.schema)

            dbRow = dict()
            dbRow['DOCK_DOOR_NAME'] = systemDockDoor
            dbRow['LOCN_BRCD'] = dbDockDoor

            return dbRow
        else:
            assert False, f"Dockdoor clear not allowed. Test manually"

    def getLocnByWGWA(self, workGrp: str, workArea: str=None, ignoreWorkArea:list[str]=None):
        sql = f"""select locn_brcd, locn_id from locn_hdr where locn_class='R' and work_grp='{workGrp}' 
                  #CONDITION#
              """
        sqlCond = ''
        if workArea is not None:
            sqlCond += f" \n and work_area = '{workArea}'"
        elif ignoreWorkArea is not None:
            sqlCond += f" \n and work_area not in {Commons.get_tuplestr(ignoreWorkArea)}"

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        return dbRow.get('LOCN_BRCD')

    def getLocnIdByLocnBrcd(self, locnBrcd: str):
        sql = f"select locn_id from locn_hdr where locn_brcd='{locnBrcd}'"
        dbRow = DBService.fetch_row(sql, self.schema)

        return dbRow.get('LOCN_ID')

    def sortLocnByPickSeq(self, locns: list[str]):
        """"""
        sql = f"""select locn_brcd from locn_hdr where locn_brcd in #LOCN_BRCD# order by locn_pick_seq asc"""
        sql = sql.replace('#LOCN_BRCD#', Commons.get_tuplestr(locns))

        dbRows = DBService.fetch_rows(sql, self.schema)
        sortedLocns = [row['LOCN_BRCD'] for row in dbRows]
        printit(f"Sorted locns {sortedLocns}")

        return sortedLocns

    def getNewPONum(self):
        barcode = ''
        poPrefix = ENV_CONFIG.get('data', 'po_prefix')
        poLength = ENV_CONFIG.get('data', 'po_length')

        for i in range(3):
            barcode = Commons.get_random_num(prefix=poPrefix, len_with_prefix=poLength)
            sql = "select tc_purchase_orders_id from purchase_orders where tc_purchase_orders_id = '" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TC_PURCHASE_ORDERS_ID') is None:
                break

        self.logger.info('PO number ' + barcode)
        assert len(barcode) == int(poLength), '<Data> New PO number length didnt match with ' + str(poLength)
        return barcode

    def getNewASNNum(self):
        barcode = ''
        asnPrefix = ENV_CONFIG.get('data', 'asn_prefix')
        asnLength = ENV_CONFIG.get('data', 'asn_length')

        for i in range(3):
            barcode = Commons.get_random_num(prefix=asnPrefix, len_with_prefix=asnLength)
            sql = "select tc_asn_id from asn where tc_asn_id = '" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TC_ASN_ID') is None:
                break

        self.logger.info('Asn number ' + barcode)
        assert len(barcode) == int(asnLength), '<Data> New ASN number length didnt match with ' + str(asnLength)
        return barcode

    def getNewILPNNum(self, lpnPrefix=None, lpnLength=None):
        barcode = ''
        receivedLpnPrefix = lpnPrefix
        if lpnPrefix is None:
            lpnPrefix = ENV_CONFIG.get('data', 'ilpn_prefix')
        if lpnLength is None:
            lpnLength = ENV_CONFIG.get('data', 'ilpn_length')

        for i in range(3):
            barcode = Commons.get_random_num(prefix=lpnPrefix, len_with_prefix=lpnLength)
            sql = "select tc_lpn_id from lpn where tc_lpn_id = '" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TC_LPN_ID') is None:
                break

        if receivedLpnPrefix is None:
            self.logger.info('Lpn number ' + barcode)
            assert len(barcode) == int(lpnLength), '<Data> New LPN number length didnt match with ' + str(lpnLength)
        return barcode

    def getNewCartNum(self):
        barcode = ''
        cartPrefix = ENV_CONFIG.get('data', 'cart_prefix')
        cartLen = ENV_CONFIG.get('data', 'cart_length')

        # barcode = self.getNewILPNNum(lpnPrefix=cartPrefix, lpnLength=cartLen)
        for i in range(3):
            barcode = Commons.get_random_num(prefix=cartPrefix, len_with_prefix=cartLen)
            # sql = "SELECT task_cmpl_ref_nbr FROM TASK_HDR WHERE TASK_TYPE='75' AND STAT_CODE<'90'"
            sql = f"""select distinct task_id from task_dtl where (task_genrtn_ref_nbr is not null and task_genrtn_ref_nbr = '#BARCODE#') or 
                    (task_cmpl_ref_nbr is not null and task_cmpl_ref_nbr = '#BARCODE#')"""
            sql = sql.replace('#BARCODE#', str(barcode))
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TASK_ID') is None:
                break

        self.logger.info('Cart number ' + barcode)
        assert len(barcode) == int(cartLen), '<Data> New Cart number length didnt match with ' + str(cartLen)
        return barcode

    def getNewInPalletNum(self):
        pltPrefix = ENV_CONFIG.get('data', 'in_pallet_prefix')
        pltLength = ENV_CONFIG.get('data', 'in_pallet_length')
        barcode = self.getNewILPNNum(pltPrefix, pltLength)
        self.logger.info('Inbound pallet number ' + barcode)
        assert len(barcode) == int(pltLength), '<Data> New Inbound pallet number length didnt match with ' + str(pltLength)
        return barcode

    def getNewOutPalletNum(self):
        pltPrefix = ENV_CONFIG.get('data', 'out_pallet_prefix')
        pltLength = ENV_CONFIG.get('data', 'out_pallet_length')
        barcode = self.getNewILPNNum(pltPrefix, pltLength)
        self.logger.info('Outbound pallet number ' + barcode)
        assert len(barcode) == int(pltLength), '<Data> New Outbound pallet number length didnt match with ' + str(pltLength)
        return barcode

    def getNewDONum(self):
        barcode = ''
        doPrefix = ENV_CONFIG.get('data', 'do_prefix')
        doLength = ENV_CONFIG.get('data', 'do_length')

        for i in range(3):
            barcode = Commons.get_random_num(prefix=doPrefix, len_with_prefix=doLength)
            sql = "select tc_order_id from orders where tc_order_id = '" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TC_ORDER_ID') is None:
                break

        self.logger.info('DO number ' + barcode)
        assert len(barcode) == int(doLength), '<Data> New DO number length didnt match with ' + str(doLength)
        return barcode

    def getNewOLPNNum(self, lpnPrefix=None, lpnLength=None):
        barcode = ''
        lpnPrefix = lpnPrefix or ENV_CONFIG.get('data', 'olpn_prefix')
        lpnLength = lpnLength or ENV_CONFIG.get('data', 'olpn_length')

        for i in range(3):
            barcode = Commons.get_random_num(prefix=lpnPrefix, len_with_prefix=lpnLength)
            sql = "select tc_lpn_id from lpn where tc_lpn_id = '" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TC_LPN_ID') is None:
                break

        self.logger.info('Olpn number ' + barcode)
        assert len(barcode) == int(lpnLength), f"<Data> New olpn number length didnt match with {lpnLength}"
        return barcode

    def getNewTrailerNum(self):
        barcode = ''
        trlrPrefix = ENV_CONFIG.get('data', 'trailer_prefix')
        trlrLength = ENV_CONFIG.get('data', 'trailer_length')

        # barcode = self.getNewILPNNum(trlrPrefix, trlrLength)
        for i in range(3):
            barcode = Commons.get_random_num(prefix=trlrPrefix, len_with_prefix=trlrLength)
            sql = "select trailer_number from shipment where trailer_number is not null and trailer_number='" + barcode + "'"
            dbRow = DBService.fetch_row(sql, self.schema)
            if dbRow is None or len(dbRow) == 0 or dbRow.get('TRAILER_NUMBER') is None:
                break

        self.logger.info('Trailer number: ' + barcode)
        assert len(barcode) == int(trlrLength), '<Data> New trailer number length didnt match with ' + str(trlrLength)
        return barcode

    def getDefaultLpnFromASNItem(self, asn: str, itemBrcd: str):
        sql = f"""select tc_lpn_id from lpn where tc_asn_id = '{asn}' 
                 and item_id in (select item_id from item_cbo where item_name='{itemBrcd}') and o_facility_id is null"""
        dbRow = DBService.fetch_row(sql, self.schema)

        defaultLpn = dbRow.get('TC_LPN_ID')
        assert len(dbRow) == 1 and defaultLpn is not None, '<ASN> Asn didnt find 1 default lpn ' + sql

        return defaultLpn

    def _getParentDOsIfExistElseChildDOs(self, orders: list):
        final_orders = []
        for order in orders:
            if self._isParentDOExist(order):
                final_orders.append(self.get1ParentDOFromOrders(orders=[order]))
            else:
                final_orders.append(order)
        final_orders = list(set(final_orders))  # Filter distinct DOs from final_orders
        printit(f"Child DOs {orders} Final parent/child DOs {final_orders}")

        return final_orders

    def get1ParentDOFromOrders(self, orders: list):
        """Get 1 parent order for list of orders. Pass all the orders that should have same parent order
        """
        sql = f"""select distinct parent_order_id from orders where tc_order_id in #ORDERS#"""

        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(orders))
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert len(dbRows) == 1, '<DO> Parent order count for orders didnt match ' + sql
        parentOrder = str(dbRows[0]['PARENT_ORDER_ID'])

        return parentOrder

    def getAllParentDOFromOrders(self, orders: list):
        """Get all parent orders for list of orders. Pass all the orders that has parent orders
        """
        sql = f"""select distinct parent_order_id from orders where tc_order_id in #ORDERS#"""

        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(orders))
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert len(dbRows) >= 1, '<DO> Parent order not found for orders ' + sql
        parentOrders = {str(r['PARENT_ORDER_ID']) for r in dbRows}

        return list(parentOrders)

    def assertDOsIn1ParentDO(self, i_parentDO: str, o_orders: list):
        """Pass 1 parent DO and list of orders belong to the same parent DO
        """
        sql = f"""select distinct tc_order_id from orders where parent_order_id = '#PARENT_ORDER#'"""
        sql = sql.replace('#PARENT_ORDER#', str(i_parentDO))

        dbRows = DBService.fetch_rows(sql, self.schema)

        assert len(dbRows) == len(o_orders), '<DO> Orders count didnt match with parent order ' + sql

        isDOMatchedList = []
        for i in range(len(dbRows)):
            isMatched = dbRows[i]['TC_ORDER_ID'] in o_orders
            isDOMatchedList.append(isMatched)

        assert False not in isDOMatchedList, '<DO> All orders not found in parent order ' + sql

    def _isParentDOExist(self, order:str):
        sql = f"select parent_order_id from orders where tc_order_id = '{order}'"
        dbRow = DBService.fetch_row(sql, self.schema)

        isParentDOExist = True if dbRow['PARENT_ORDER_ID'] is not None else False
        return isParentDOExist

    def assertParentDOExist(self, orders: list[str]):
        parentDO = self.get1ParentDOFromOrders(orders)
        self.logger.info(f"{orders} has parent DO {parentDO}")

        assert parentDO is not None and parentDO != '', f"<DO> No parent order found for orders {orders}"
        return parentDO

    def assertParentDONotExist(self, orders: list[str]):
        parentDO = self.get1ParentDOFromOrders(orders)
        self.logger.info(f"{orders} has parent DO {parentDO}")

        assert parentDO is None or parentDO == '', f"<DO> Parent order found for orders {orders}"

    def _categorizeParentDOsWithOrders(self, orders: list[str]):
        """Returns a dict with major grp attr with a list of child orders.
        Also returns a list with only child orders that does not have parent DO"""
        sql = f"""select tc_order_id, major_order_grp_attr, parent_order_id from orders where tc_order_id in {Commons.get_tuplestr(orders)}
                  order by major_order_grp_attr asc"""
        dbRows = DBService.fetch_rows(sql, self.schema)
        parentDODict, childDOList = {}, []

        for row in dbRows:
            majorGrpAttr = row.get('MAJOR_ORDER_GRP_ATTR')
            childDo = row.get('TC_ORDER_ID')
            if majorGrpAttr is not None:
                if majorGrpAttr in parentDODict.keys():
                    parentDODict[majorGrpAttr].append(childDo)
                else:
                    parentDODict[majorGrpAttr] = [childDo]
            else:
                childDOList.append(childDo)

        return parentDODict, childDOList

    def _forceNbrOfOlpnCreationForOrders(self, orders:list[str], ordLineItems:list[list[str]], u_forceNoOfOlpn:list[int]=None):
        """Update such that expected no. of olpn generated on running wave"""
        if u_forceNoOfOlpn is not None:
            assert len(orders) == len(ordLineItems) == len(u_forceNoOfOlpn), '<Data> Param mismatch for lineItems/expOlpns for orders'

            parentDODict, childDOList = self._categorizeParentDOsWithOrders(orders=orders)
            for pdo, dos in parentDODict.items():
                availLpnTypes = ['001', '003', '005', '002', '004']
                for do in dos:
                    doIndex = orders.index(do)
                    expOlpnCntForDo, lineItemsForDo = u_forceNoOfOlpn[doIndex], ordLineItems[doIndex]

                    if expOlpnCntForDo == 1:
                        updLpnType = availLpnTypes.pop(0)
                        for item in lineItemsForDo:
                            self.updateDOLineLpnType(dO=do, lineItem=item, u_lpnType=updLpnType)
                    elif expOlpnCntForDo == len(lineItemsForDo):
                        for item in lineItemsForDo:
                            updLpnType = availLpnTypes.pop(0)
                            self.updateDOLineLpnType(dO=do, lineItem=item, u_lpnType=updLpnType)

            for do in childDOList:
                availLpnTypes = ['001', '003', '005', '002', '004']
                doIndex = orders.index(do)
                expOlpnCntForDo, lineItemsForDo = u_forceNoOfOlpn[doIndex], ordLineItems[doIndex]

                if expOlpnCntForDo == 1:
                    updLpnType = availLpnTypes.pop(0)
                    for item in lineItemsForDo:
                        self.updateDOLineLpnType(dO=do, lineItem=item, u_lpnType=updLpnType)
                elif expOlpnCntForDo == len(lineItemsForDo):
                    for item in lineItemsForDo:
                        updLpnType = availLpnTypes.pop(0)
                        self.updateDOLineLpnType(dO=do, lineItem=item, u_lpnType=updLpnType)

    def assertParentDOsForOrdersByGroupAttr(self, orders: list[str]):
        """Assert based on orders.major_order_grp_attr"""
        sql = f"""select tc_order_id, major_order_grp_attr, parent_order_id from orders where tc_order_id in {Commons.get_tuplestr(orders)}
                  order by major_order_grp_attr asc
                """
        dbRows = DBService.fetch_rows(sql, self.schema)
        grpAttr_parentDo_dict = {}

        assertlist = []
        for row in dbRows:
            grpAttr = row.get('MAJOR_ORDER_GRP_ATTR')
            parentDo = row.get('PARENT_ORDER_ID')
            if grpAttr is None and parentDo is not None:  # If M_GRP_ATTR is None, PARENT_DO should also be None
                assertlist.append(False)
            else:  # If M_GRP_ATTR is not None, check for duplicate M_GRP_ATTR and PARENT_DO
                if grpAttr not in grpAttr_parentDo_dict:
                    grpAttr_parentDo_dict[grpAttr] = parentDo
                else:
                    if grpAttr_parentDo_dict[grpAttr] != parentDo:
                        assertlist.append(False)

        printit('^^^ Orders and major grp attr\n', Commons.get_table_data_aligned(table_data=dbRows))
        assert not assertlist.count(False), '<DO> Parent order validation failed for orders ' + sql

    def assertOLPNCountForDO(self, i_order: str = None, o_totalOLPNs: int = None):
        """Pass order or parent order"""
        sql = f"""select distinct tc_lpn_id from lpn l inner join lpn_detail ld on l.lpn_id=ld.lpn_id 
                inner join order_line_item oli on #DOCONDITION#
                inner join orders ord on ord.order_id=oli.order_id where 0=0  """

        isParentDOExistForOrder = self._isParentDOExist(order=i_order)
        if isParentDOExistForOrder:
            sqlDOCond = " ld.tc_order_line_id = oli.reference_line_item_id "
        else:
            sqlDOCond = " ld.distribution_order_dtl_id = oli.line_item_id "

        if i_order is not None:
            sql += f" \n and ord.tc_order_id = '{i_order}'"

        sql = sql.replace('#DOCONDITION#', sqlDOCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        isMatched = dbRows is not None and dbRows[0]['TC_LPN_ID'] is not None and len(dbRows) == o_totalOLPNs

        assert isMatched, f"<LPN> Olpn count {o_totalOLPNs} didnt match for order {i_order}, actual {len(dbRows)} " + sql

    def assertOLPNCountForWave(self, i_wave: str, o_totalOLPNs: int):
        """"""
        sql = f"""select distinct tc_lpn_id from lpn where wave_nbr='#WAVE_NBR#'"""
        sql = sql.replace('#WAVE_NBR#', str(i_wave))

        dbRows = DBService.fetch_rows(sql, self.schema)

        isMatched = DBService.compareEqual(len(dbRows), o_totalOLPNs, '<LpnHdr> Total olpns for wave')

        assert isMatched, f"<LPN> Olpn count {o_totalOLPNs} didnt match for wave {i_wave}, actual {len(dbRows)} " + sql

    def get1OLPNFromDO(self, order: str, item: str = None):
        """Pass child order
        """
        sql = f"""select distinct tc_lpn_id from lpn l inner join lpn_detail ld on l.lpn_id=ld.lpn_id 
                inner join order_line_item oli on #DOCONDITION#
                inner join orders ord on ord.order_id=oli.order_id
                inner join item_cbo ic on ic.item_id = ld.item_id
                where 0=0 #CONDITION# """
        sqlCond = ''

        isParentDOExistForOrder = self._isParentDOExist(order=order)
        if isParentDOExistForOrder:
            sqlDOCond = " ld.tc_order_line_id = oli.reference_line_item_id "
        else:
            sqlDOCond = " ld.distribution_order_dtl_id = oli.line_item_id "

        if order is not None:
            sqlCond += f" \n and ord.tc_order_id='{order}' "
        if item:
            sqlCond += f" \n and ic.item_name in ('{item}')) "

        sql = sql.replace('#DOCONDITION#', sqlDOCond)
        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        olpn = dbRow.get('TC_LPN_ID')

        return olpn

    def get1ConsolLocnFrom1DO(self, order: str):
        olpns = self.getAllOLPNsFrom1DO(order=order)
        consolLocn, consolLocnId = self.get1ConsolLocnFromOLPNs(oLPNs=olpns)

        return consolLocn

    def getAllConsolLocnsFromWave(self, wave: str):
        olpns = self.getAllOLPNsFromWave(wave=wave)

        consolLocnRecs = None
        if len(olpns) > 0:
            consolLocnRecs = self.getAllConsolLocnsFromOLPNs(oLPNs=olpns)

        return consolLocnRecs

    def get1ConsolLocnFromOLPNs(self, oLPNs: list):
        """Get 1 consolidation locn for list of olpns. Pass all the olpns that should have same consolidation locn
        """
        sql = f"""select distinct lh.locn_brcd,lh.locn_id from lpn l 
                 left outer join locn_hdr lh on l.dest_sub_locn_id = lh.locn_id and lh.locn_class='O'
                 where l.tc_lpn_id in #OLPNS#"""
        sql = sql.replace('#OLPNS#', Commons.get_tuplestr(oLPNs))
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert len(dbRows) == 1, '<LPN> Olpns didnt have same and 1 consol locn ' + sql
        consolLocn = dbRows[0]['LOCN_BRCD']
        consolLocnId = dbRows[0]['LOCN_ID']

        return consolLocn, consolLocnId

    def getAllConsolLocnsFromOLPNs(self, oLPNs: list):
        """Get list of all distinct consol locns for list of olpns. Pass all the olpns
        """
        sql = f"""select distinct lh.locn_brcd,lh.locn_id from lpn l 
                 left outer join locn_hdr lh on l.dest_sub_locn_id = lh.locn_id and lh.locn_class='O'
                 where l.tc_lpn_id in #OLPNS#"""
        sql = sql.replace('#OLPNS#', Commons.get_tuplestr(oLPNs))
        dbRows = DBService.fetch_rows(sql, self.schema)

        return dbRows

    def getAllConsolLocnsFromDOs(self, orders: list):
        """Get all consol locns from DOs
        """
        sql = f"""select distinct lh.locn_brcd,lh.locn_id from lpn l 
                    left outer join locn_hdr lh on l.dest_sub_locn_id=lh.locn_id and lh.locn_class='O'
                    where l.tc_order_id in #ORDERS#
              """
        pc_orders = self._getParentDOsIfExistElseChildDOs(orders=orders)
        orders = pc_orders

        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(orders))
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is not None and len(dbRows) > 0, f"<DO> Orders {orders} didnt have consol locn"
        consolLocnList = [i['LOCN_BRCD'] for i in dbRows]

        return consolLocnList

    def getConsolLocnWithLessOLPNs(self, noOfLocn:int=1):
        """Get consol locn with the least no of DOs and Olpns for inquiry
        """
        final_dbRows = []
        sql = f"""select lh.locn_brcd,lh.locn_id,count(tc_lpn_id) as lpn_count
                    from lpn l,locn_hdr lh
                    where l.dest_sub_locn_id=lh.locn_id and l.inbound_outbound_indicator='O' and lh.locn_class='O'
                    group by lh.locn_brcd,lh.locn_id
                    order by lpn_count asc
                    offset 0 rows fetch next {noOfLocn} rows only
                """
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is not None and len(dbRows) == noOfLocn, "<Data> Didnt find consol locn (with less olpns) " + sql
        for i in dbRows:
            destLocnId = i['LOCN_ID']
            lpnCnt = i['LPN_COUNT']
            sql2 = f"""select lh.locn_brcd,l.tc_order_id,l.tc_lpn_id from lpn l,locn_hdr lh
                        where l.dest_sub_locn_id=lh.locn_id and l.inbound_outbound_indicator='O' 
                        and l.dest_sub_locn_id='{destLocnId}'
                        --offset 0 rows fetch next {i['LPN_COUNT']} rows only
                    """
            dbRows2 = DBService.fetch_rows(sql2, self.schema)
            assert dbRows2 is not None and len(dbRows2) == int(lpnCnt), f"<Data> Didnt find {lpnCnt} olpns for consol locn " + sql2
            final_dbRows.extend(dbRows2)

        # consolLocnList = [i['LOCN_BRCD'] for i in dbRows][0]
        # lpns = [i['TC_LPN_ID'] for i in dbRows][0]
        # orders = [i['TC_ORDER_ID'] for i in dbRows][0]

        consLocnSet = {i['LOCN_BRCD'] for i in final_dbRows}
        orderSet = {i['TC_ORDER_ID'] for i in final_dbRows}
        olpnSet = {i['TC_LPN_ID'] for i in final_dbRows}

        for i in range(len(final_dbRows)):
            self._logDBResult(final_dbRows[i], ['LOCN_BRCD', 'TC_ORDER_ID', 'TC_LPN_ID'])

        return final_dbRows, consLocnSet, orderSet, olpnSet

    def assertConsolLocnForOLPNs(self, i_order: str, o_consolLocn: str):
        olpns = self.getAllOLPNsFrom1DO(order=i_order)
        consolLocn, consolLocnId = self.get1ConsolLocnFromOLPNs(oLPNs=olpns)
        isMatched = DBService.compareEqual(consolLocn, o_consolLocn, 'Consol locn for olpns in order ' + i_order)

        assert isMatched, f"<LPN> Consol locn {o_consolLocn} didnt match for order {i_order}"

    def assertNoOfConsolLocnforDO(self, i_order: str, noOfConsolLocn: int):
        """Validate the no. of locn for DO
        """
        consolLocns = DBLib().getAllConsolLocnsFromDOs([i_order])

        assert len(consolLocns) == noOfConsolLocn, f"<DO> No. of consol locn validation failed for do {i_order}"

    def assertPOHdr(self, i_po: str, o_status: POStat = None):

        assertlist = []

        sql = f"""select * from purchase_orders where tc_purchase_orders_id in ('{i_po}')"""
        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_PURCHASE_ORDERS_ID') == i_po, '<POHdr> PO hdr not found ' + sql

        if o_status is not None:
            dbVal = dbRow.get("PURCHASE_ORDERS_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_status.value, '<POHdr> PO status for ' + i_po, o_status.name)
            assertlist.append(isMatched)

        assert not assertlist.count(False), f"<POHdr> Few PO hdr validation failed for {i_po} " + sql

    def assertPODtls(self, i_po: str, i_itemBrcd: str, o_dtlStatus: int = None, o_origQty: int = None,
                     o_qty: int = None, o_receivedQty: int = None, o_shippedQty: int = None):

        assertlist = []

        sql = f"""select po.tc_purchase_orders_id, pol.* 
                    from purchase_orders_line_item pol inner join purchase_orders po on pol.purchase_orders_id = po.purchase_orders_id 
                    where po.tc_purchase_orders_id in ('{i_po}') 
                    #CONDITION#"""

        sqlCond = " and pol.sku = '" + i_itemBrcd + "'"
        sqlCond += " \n order by pol.purchase_orders_line_item_id asc"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_PURCHASE_ORDERS_ID') == i_po, '<PODtl> PO dtl not found ' + sql

        if o_dtlStatus is not None:
            dbVal = dbRow.get("PURCHASE_ORDERS_LINE_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_dtlStatus, '<PODtl> dtlStatus for ' + i_po + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_origQty is not None:
            dbVal = dbRow.get("ORIG_ORDER_QTY")
            isMatched = DBService.compareEqual(dbVal, o_origQty, '<PODtl> origQty for ' + i_po + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_qty is not None:
            dbVal = dbRow.get("ORDER_QTY")
            isMatched = DBService.compareEqual(dbVal, o_qty, '<PODtl> orderQty for ' + i_po + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_receivedQty is not None:
            dbVal = dbRow.get("RECEIVED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_receivedQty, '<PODtl> receivedQty for ' + i_po + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_shippedQty is not None:
            dbVal = dbRow.get("SHIPPED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_shippedQty, '<PODtl> shippedQty for ' + i_po + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)

        assert not assertlist.count(False), f"<PODtl> Few PO dtl validation failed for {i_po} " + sql

    def assertWaitASNRecord(self, i_asn: str):
        sql = self._fetchASNHdrByASNNum.replace('#TC_ASN_ID#', i_asn)
        DBService.wait_for_records(sql, expected_cnt=1, schema=self.schema)

    def assertWaitASNStatus(self, i_asn: str, o_status: int = None):
        sql = self._fetchASNHdrByASNNum.replace('#TC_ASN_ID#', i_asn)
        DBService.wait_for_value(sql, 'ASN_STATUS', str(o_status), self.schema)

    def assertASNHdr(self, i_asn: str, o_status: int = None):

        assertlist = []

        sql = self._fetchASNHdrByASNNum.replace('#TC_ASN_ID#', i_asn)
        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_ASN_ID') == i_asn, '<ASNHdr> ASN hdr not found ' + sql

        if o_status is not None:
            dbVal = dbRow.get("ASN_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_status, '<ASNHdr> asn status for ' + i_asn)
            assertlist.append(isMatched)

        assert not assertlist.count(False), f"<ASNHdr> Few asn hdr validation failed for {i_asn} " + sql

    def assertASNDtls(self, i_asn: str, i_po: str, i_itemBrcd: str, o_dtlStatus: int = None,
                      o_shippedQty=None, o_receivedQty=None):

        sql = self._fetchASNDtlsByASNDtls.replace('#TC_ASN_ID#', i_asn)
        sqlCond = " and ad.sku_name = '" + i_itemBrcd + "'"
        if i_po is not None:
            sqlCond += " \n and ad.tc_purchase_orders_id = '" + i_po + "'"
        sqlCond += " \n order by ad.asn_detail_id asc"
        sql = sql.replace('#CONDITION#', sqlCond)

        DBService.wait_for_value(sql, 'TC_PURCHASE_ORDERS_ID', str(i_po), self.schema, maxWaitInSec=15)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_ASN_ID') is not None, '<ASNDtl> ASN dtl not found ' + sql

        assertlist = []
        if o_dtlStatus is not None:
            dbVal = dbRow.get("ASN_DETAIL_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_dtlStatus, '<ASNDtl> dtlStatus for ' + i_asn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_shippedQty is not None:
            dbVal = dbRow.get("SHIPPED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_shippedQty, '<ASNDtl> shippedQty for ' + i_asn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_receivedQty is not None:
            dbVal = dbRow.get("RECEIVED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_receivedQty, '<ASNDtl> receivedQty for ' + i_asn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        assert not assertlist.count(False), f'<ASNDtl> Few asn dtl validation failed for {i_asn} ' + sql

    def assertPONotOnASN(self, i_asn, i_po):
        """Validate provided PO is not on ASN
        """
        sql = self._fetchASNDtlsByASNDtls.replace('#TC_ASN_ID#', str(i_asn))

        sqlCond = ''
        if i_po is not None:
            sqlCond += f" \n and ad.tc_purchase_orders_id = '{i_po}'"
        sqlCond += " \n order by ad.asn_detail_id asc"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        noRecPresent = True if dbRow is None or len(dbRow) == 0 else False

        assert noRecPresent, f"<PO> PO {i_po} found on ASN {i_asn}"

    def assertLPNHdr(self, i_lpn: str, o_facStatus:LPNFacStat=None,
                     o_prevLocn:str=None, o_currLocn:str=None, o_destLocn:str=None, o_destLocnType:LocnType=None,
                     o_parentLpn:str=None, o_asn:str=None, isActWeightPresent:bool=None,
                     isConsLocnUpdated:bool=None, o_totalLpnQty:int=None, o_qaFlag:int=None, isBOLGenerated:bool=None):

        sql = f"""select lhc.locn_brcd curr_sub_locn, lhd.locn_brcd dest_sub_locn,lhd.work_area dest_work_area, lhp.locn_brcd prev_sub_locn, l.* from lpn l 
                    left outer join locn_hdr lhc on l.curr_sub_locn_id = lhc.locn_id
                    left outer join locn_hdr lhd on l.dest_sub_locn_id = lhd.locn_id
                    left outer join locn_hdr lhp on l.prev_sub_locn_id = lhp.locn_id
                    where l.tc_lpn_id in ('{i_lpn}')"""

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_LPN_ID') is not None, '<LPNHdr> Lpn hdr not found ' + sql

        assertlist = []
        if o_facStatus is not None:
            dbVal = dbRow.get("LPN_FACILITY_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_facStatus.value, '<LPNHdr> lpn status for ' + i_lpn, o_facStatus.name)
            assertlist.append(isMatched)
        if o_prevLocn is not None:
            dbVal = dbRow.get("PREV_SUB_LOCN")
            isMatched = DBService.compareEqual(dbVal, o_prevLocn, '<LPNHdr> prevLocn for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_currLocn is not None:
            dbVal = dbRow.get("CURR_SUB_LOCN")
            isMatched = DBService.compareEqual(dbVal, o_currLocn, '<LPNHdr> currLocn for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_destLocn is not None:
            dbVal = dbRow.get("DEST_SUB_LOCN")
            dbVal = 'null' if dbVal is None else dbVal
            isMatched = DBService.compareEqual(dbVal, o_destLocn, '<LPNHdr> destLocn for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_destLocnType is not None:
            dbVal = dbRow.get("DEST_WORK_AREA")
            dbVal = 'null' if dbVal is None else dbVal
            elgblWAs = self._decide_lh_workArea_forLocnType(locnType=o_destLocnType)
            isMatched = DBService.compareIn(dbVal, elgblWAs, '<LPNHdr> destWorkArea for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_parentLpn is not None:
            dbVal = dbRow.get("TC_PARENT_LPN_ID")
            if dbVal is None:
                dbVal = 'null'
            isMatched = DBService.compareEqual(dbVal, o_parentLpn, '<LPNHdr> parentLpn for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_asn is not None:
            dbVal = dbRow.get("TC_ASN_ID")
            isMatched = DBService.compareEqual(dbVal, o_asn, '<LPNHdr> asn for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if isActWeightPresent is not None:
            dbVal = dbRow.get("WEIGHT")
            isMatched = True if dbVal is not None and str(dbVal) != '0' else False
            self.logger.info('<LPNHdr> Actual weight: ' + str(dbVal))
            assertlist.append(isMatched)
        if isConsLocnUpdated is not None:
            dbVal = dbRow.get('DEST_SUB_LOCN')
            isMatched = True if dbVal is not None else False
            self.logger.info('<LPNHdr> Consol locn for ' + i_lpn + ' is ' + str(dbVal))
            assertlist.append(isMatched)
        if o_totalLpnQty is not None:
            dbVal = dbRow.get("TOTAL_LPN_QTY")
            isMatched = DBService.compareEqual(dbVal, o_totalLpnQty, '<LPNHdr> total Qty for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if o_qaFlag is not None:
            dbVal = dbRow.get("QA_FLAG")
            isMatched = DBService.compareEqual(dbVal, o_qaFlag, '<LPNHdr> qa_flag for lpn ' + i_lpn)
            assertlist.append(isMatched)
        if isBOLGenerated:
            dbVal = dbRow.get("BOL_NBR")
            isMatched = True if dbVal is not None else False
            self.logger.info('<LPNHdr> BOL number: ' + str(dbVal))
            assertlist.append(isMatched)

        assert not assertlist.count(False), f'<LPNHdr> lpn hdr validation failed for {i_lpn} ' + sql

    def assertLPNDtls(self, i_lpn: str, i_itemBrcd: str, o_dtlStatus: int = None, o_qty: int = None,
                      o_receivedQty: int = None, o_initialQty: int = None):

        sql = f"""select l.tc_lpn_id, ic.item_name, ld.* from lpn_detail ld inner join lpn l on ld.lpn_id = l.lpn_id 
                    inner join item_cbo ic on ld.item_id = ic.item_id
                    where l.tc_lpn_id in ('{i_lpn}') 
                    #CONDITION#"""
        sqlCond = " and ic.item_name = '" + i_itemBrcd + "'"
        sqlCond += " \n order by ld.lpn_detail_id asc"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_LPN_ID') is not None, '<LPNDtl> Lpn dtl not found ' + sql

        assertlist = []
        if o_dtlStatus is not None:
            dbVal = dbRow.get("LPN_DETAIL_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_dtlStatus, '<LPNDtl> dtlStatus for ' + i_lpn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_qty is not None:
            dbVal = dbRow.get("SIZE_VALUE")
            isMatched = DBService.compareEqual(dbVal, int(o_qty), '<LPNDtl> qty for ' + i_lpn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_receivedQty is not None:
            dbVal = dbRow.get("RECEIVED_QTY")
            isMatched = DBService.compareEqual(dbVal, int(o_receivedQty), '<LPNDtl> receivedQty for ' + i_lpn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_initialQty is not None:
            dbVal = dbRow.get("INITIAL_QTY")
            isMatched = DBService.compareEqual(dbVal, int(o_initialQty), '<LPNDtl> initialQty for ' + i_lpn + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<LPNDtl> Few lpn dtl validation failed for {i_lpn} ' + sql

    def assertLPNHasItems(self, i_lpn: str, o_items: list[str]):
        """ """
        sql = f"""select l.tc_lpn_id, ic.item_name from lpn_detail ld inner join lpn l on ld.lpn_id = l.lpn_id 
                inner join item_cbo ic on ld.item_id = ic.item_id
                where l.tc_lpn_id in ('{i_lpn}') and ic.item_name in #ITEMS# """
        sql = sql.replace('#ITEMS#', Commons.get_tuplestr(o_items))

        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is not None and len(o_items) == len(dbRows), "<LPN> Lpn details with items not found " + sql

    def assertWaitDOStatus(self, i_order: str, o_status: int):
        sql = f"""select do_status from orders where tc_order_id = '#ORDER#'"""
        sql = sql.replace('#ORDER#', str(i_order))

        DBService.wait_for_value(sql, 'DO_STATUS', str(o_status), self.schema, maxWaitInSec=35)

    def assertDOHdr(self, i_order: str, o_status: DOStat = None, o_isParentDOExist: bool = None, o_parentDO: str = None,
                    o_shipVia: str = None):
        """"""
        sql = f"""select * from orders where tc_order_id in ('{i_order}')"""

        DBService.wait_for_value(sql, 'DO_STATUS', expected_value=str(o_status.value), schema=self.schema, maxWaitInSec=25)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_ORDER_ID') is not None, '<DOHdr> DO hdr not found ' + sql

        assertlist = []
        if o_status is not None:
            dbVal = dbRow.get("DO_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_status.value, '<DOHdr> order status for ' + i_order , o_status.name)
            assertlist.append(isMatched)
        if o_isParentDOExist is not None:
            dbVal = dbRow.get("PARENT_ORDER_ID")
            dbVal = False if dbVal is None else True
            isMatched = DBService.compareEqual(dbVal, o_isParentDOExist, '<DOHdr> isParentDOExist for order ' + i_order)
            assertlist.append(isMatched)
        if o_parentDO is not None:
            dbVal = dbRow.get("PARENT_ORDER_ID")
            isMatched = DBService.compareEqual(dbVal, o_parentDO, '<DOHdr> parentDO for order ' + i_order)
            assertlist.append(isMatched)
        if o_shipVia is not None:
            dbVal = dbRow.get("DSG_SHIP_VIA")
            isMatched = DBService.compareEqual(dbVal, o_shipVia, '<DOHdr> ship via for order ' + i_order)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<DOHdr> Few order hdr validation failed for {i_order} ' + sql

    def assertDODtls(self, i_order: str, i_itemBrcd: str,i_waveNum:str = None, o_dtlStatus: int = None, o_origQty: int = None,
                     o_qty: int = None, o_qtyAllocated: int = None, o_usrCancldQty: int = None):

        sql = f"""select o.tc_order_id, ic.item_name, oli.* from order_line_item oli inner join orders o on oli.order_id = o.order_id
                    inner join item_cbo ic on oli.item_id = ic.item_id
                    where o.tc_order_id = '{i_order}' 
                    #CONDITION#"""
        sqlCond = " and ic.item_name = '" + i_itemBrcd + "'"
        if i_waveNum is not None:
            sqlCond += " \n and oli.wave_nbr = '" + i_waveNum + "'"
        sqlCond += " \n order by oli.line_item_id asc"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_ORDER_ID') is not None, '<DODtl> DO dtl not found ' + sql

        assertlist = []
        if o_dtlStatus is not None:
            dbVal = dbRow.get("DO_DTL_STATUS")
            isMatched = DBService.compareEqual(dbVal, o_dtlStatus, '<DODtl> dtlStatus for ' + i_order + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_origQty is not None:
            dbVal = dbRow.get("ORIG_ORDER_QTY")
            isMatched = DBService.compareEqual(dbVal, o_origQty, '<DODtl> origQty for ' + i_order + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_qty is not None:
            dbVal = dbRow.get("ORDER_QTY")
            isMatched = DBService.compareEqual(dbVal, o_qty, '<DODtl> orderQty for ' + i_order + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_qtyAllocated is not None:
            dbVal = dbRow.get("ALLOCATED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_qtyAllocated, '<DODtl> allocatedQty for ' + i_order + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_usrCancldQty is not None:
            dbVal = dbRow.get("USER_CANCELED_QTY")
            isMatched = DBService.compareEqual(dbVal, o_usrCancldQty, '<DODtl> userCancelledQty for ' + i_order + ', item ' + i_itemBrcd)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<DODtl> Few order_line_item validation failed for {i_order} ' + sql

    def assertWaitShipmentStatus(self, i_shipment: str, o_status: int):
        sql = "select * from shipment where tc_shipment_id ='" + i_shipment + "'"

        DBService.wait_for_value(sql, 'SHIPMENT_STATUS', str(o_status), self.schema, maxWaitInSec=10)

    def assertShipmentStatus(self, i_shipment: str, o_status: int = None, o_noOfStops: int = None):
        """"""
        sql = "select * from shipment where tc_shipment_id ='" + i_shipment + "'"

        dbRow = DBService.fetch_row(sql, self.schema)

        assertList = []
        if o_status is not None:
            isMatched = DBService.compareEqual(dbRow.get('SHIPMENT_STATUS'), o_status, '<Ship> shipment status for ' + i_shipment)
            assertList.append(isMatched)
        if o_noOfStops is not None:
            isMatched = DBService.compareEqual(dbRow.get('NUM_STOPS'), o_noOfStops, '<Ship> Number of stops for ' + i_shipment)
            assertList.append(isMatched)

        assert False not in assertList, '<Shipment> shipment validation failed ' + sql

    def assertDOInShipment(self, i_shipment: str, o_orders: list[str]):
        sql = f"""select * from orders where tc_shipment_id = '{i_shipment}'"""
        dbRows = DBService.fetch_rows(sql, self.schema)

        assertlist = []
        isMatched = DBService.compareEqual(len(dbRows), len(o_orders), '<DO> No. of orders for shipment ' + i_shipment)
        assertlist.append(isMatched)

        allOrdersFound = True
        for row in dbRows:
            isOrderFound = row.get('TC_ORDER_ID') in o_orders
            if isOrderFound:
                self.logger.info(row.get('TC_ORDER_ID') + ' order found in shipment ' + i_shipment)
            else:
                allOrdersFound = False
                self.logger.info(row.get('TC_ORDER_ID') + ' order not found in shipment ' + i_shipment)

        assertlist.append(allOrdersFound)
        assert not assertlist.count(False), '<DO> shipment for orders validation failed for {i_shipment} ' + sql

    def assertShipmentForOLPNFromDO(self, i_shipment: str, i_orders: list[str]):
        # pc_orders = self._getParentDOsIfExistElseChildDOs(i_orders)
        # i_orders = pc_orders

        sql = f"""select * from lpn where tc_order_id in #ORDERS# and tc_shipment_id = '{i_shipment}'"""
        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(i_orders))

        dbRows = DBService.fetch_rows(sql, self.schema)
        assert dbRows is not None and len(dbRows) >= len(i_orders), '<LPN> Olpn from DO with shipment not matched ' + sql

    def assertWaitWaveStatus(self, i_wave: str, i_status: int = None):
        sql = f"""select * from ship_wave_parm where ship_wave_nbr = '#SHIP_WAVE_NBR#'"""
        sql = sql.replace('#SHIP_WAVE_NBR#', i_wave)
        DBService.wait_for_value(sql, 'STAT_CODE', str(i_status), self.schema, maxWaitInSec=90)

    def _printWaveMsgLogs(self, refValue1: str, refValue2: list[str]):
        """refValue1: wave, refValue2: orders
        """
        sql = "select * from msg_log where 0=0 "
        if refValue1 is not None:
            sql += " \n and ref_value_1 = '" + str(refValue1) + "'"
        if refValue2 is not None:
            sql += " \n and ref_value_2 in " + Commons.get_tuplestr(refValue2)
        sql += " \n order by 1 desc"
        dbRows = DBService.fetch_rows(sql, self.schema)

        msgLogs = ''
        if dbRows is not None and len(dbRows) > 0:
            for i in dbRows:
                msgLogs += i.get('MSG') + '\n'
        printit(f"^^^ Curr wave msg logs {msgLogs}")

    def assertDOInWave(self, i_wave: str, o_totalOrders: int, o_orders: list, isIgnoreCanceledDO:bool=None):
        """o_totalOrders is for only child orders.
        isIgnoreCanceledDO is for ignoring DO with status canceled while fetching from DB
        """
        sql = f"""select distinct o.tc_order_id, o.* from order_line_item oli inner join orders o on oli.order_id=o.order_id
                 where oli.wave_nbr='{i_wave}'
                 and (o.major_minor_order is null or o.major_minor_order='N')
                 #CONDITION#
              """
        sqlCond = ''
        if isIgnoreCanceledDO:
            sqlCond += " \n and o.do_status != 200"

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        assertlist = []
        isMatched = DBService.compareEqual(len(dbRows), o_totalOrders, '<DODtl> No. of orders in wave ' + i_wave)
        assertlist.append(isMatched)

        allOrdersFound = True
        for row in dbRows:
            isOrderFound = row.get('TC_ORDER_ID') in o_orders
            if isOrderFound:
                self.logger.info(row.get('TC_ORDER_ID') + ' order found in wave ' + i_wave)
            else:
                allOrdersFound = False
                self.logger.info(row.get('TC_ORDER_ID') + ' order not found in wave ' + i_wave)
        assertlist.append(allOrdersFound)
        assert not assertlist.count(False), f"<DO> No. of orders {o_totalOrders} didnt match for wave {i_wave}, actual {len(dbRows)} " + sql

    def assertDOLineInWave(self, i_wave: str, o_totalLines: int):
        sql = f"""select distinct o.tc_order_id, oli.* from order_line_item oli inner join orders o on oli.order_id = o.order_id
                    where oli.wave_nbr = '{i_wave}' and (o.major_minor_order is null or o.major_minor_order='N')
              """
        dbRows = DBService.fetch_rows(sql, self.schema)

        isMatched = DBService.compareEqual(len(dbRows), o_totalLines, '<DODtl> No. of order lines in wave ' + i_wave)
        assert isMatched, f"<DO> No. of order lines {o_totalLines} didnt match for wave {i_wave}, actual {len(dbRows)} " + sql

    def getWMOnHandQty(self, itemBrcd: str, locnBrcd: str):
        sql = f"""select lh.locn_brcd, ic.item_name, sum(nvl(wi.on_hand_qty, 0)) sum_on_hand from wm_inventory wi 
                    left outer join locn_hdr lh on wi.location_id = lh.locn_id 
                    inner join item_cbo ic on wi.item_id = ic.item_id
                    where ic.item_name = '{itemBrcd}' and lh.locn_brcd = '#LOCN_BRCD#'
                    group by ic.item_name, lh.locn_brcd"""
        sql = sql.replace('#LOCN_BRCD#', locnBrcd)

        dbRow = DBService.fetch_row(sql, self.schema)
        onHand = 0 if dbRow is None else dbRow.get('SUM_ON_HAND')

        return onHand

    def getWMAllocQty(self, itemBrcd: str, locnBrcd: str):
        sql = f"""select lh.locn_brcd, ic.item_name, sum(wi.wm_allocated_qty) sum_wm_alloc_qty from wm_inventory wi 
                    left outer join locn_hdr lh on wi.location_id = lh.locn_id 
                    inner join item_cbo ic on wi.item_id = ic.item_id
                    where ic.item_name = '{itemBrcd}' and lh.locn_brcd = '#LOCN_BRCD#'
                    group by ic.item_name, lh.locn_brcd"""
        sql = sql.replace('#LOCN_BRCD#', locnBrcd)

        dbRow = DBService.fetch_row(sql, self.schema)
        onHand = dbRow.get('SUM_WM_ALLOC_QTY')
        onHand = 0 if onHand is None else int(onHand)

        return onHand

    def getAvailUnitFromWM(self, itemBrcd: str, locnBrcd: str):
        sql = f"""select nvl(wi.on_hand_qty + wi.to_be_filled_qty - wi.wm_allocated_qty,0) as avail_units from 
                    wm_inventory wi where item_id in (select item_id from item_cbo where item_name = '{itemBrcd}')
                    and location_id in (select locn_id from locn_hdr where locn_brcd = '#LOCN_BRCD#')"""
        sql = sql.replace('#LOCN_BRCD#', locnBrcd)

        dbRow = DBService.fetch_row(sql, self.schema)
        availUnit = int(dbRow.get('AVAIL_UNITS'))

        return availUnit

    def getWMTranInvn(self, itemBrcd: str, tranInvnType: int):
        sql = f"""select wi.on_hand_qty from wm_inventory wi 
                     inner join item_cbo ic on wi.item_id = ic.item_id
                     where ic.item_name= '{itemBrcd}' and wi.transitional_inventory_type = '{tranInvnType}'
              """
        dbRow = DBService.fetch_row(sql, self.schema)
        tranInvn = 0 if dbRow is None else dbRow.get('ON_HAND_QTY')

        return int(tranInvn)

    def getActvQtyPercent(self, item: str, locnBrcd:str=None):
        """(Generic method) This makes sure any actv.
        """
        sql = f"""select round(((nvl(on_hand_qty,0)+nvl(to_be_filled_qty,0))/pld.max_invn_qty)*100) actv_qty_percent
                    ,wm.on_hand_qty,wm.to_be_filled_qty,pld.max_invn_qty,lh.locn_brcd 
                    from wm_inventory wm inner join item_cbo ic on wm.item_id=ic.item_id 
                    inner join locn_hdr lh on wm.location_id=lh.locn_id inner join pick_locn_dtl pld on wm.location_id=pld.locn_id
                    where wm.locn_class='A' and ic.item_name='{item}' 
              """
        if locnBrcd is not None:
            sql += f" \n and lh.locn_brcd='{locnBrcd}'"

        dbRow = DBService.fetch_row(sql, self.schema)

        actvLocn = dbRow.get('LOCN_BRCD')
        actvQtyPercent = int(dbRow.get('ACTV_QTY_PERCENT'))
        printit(f'Actv qty percent for item {item} in actvLocn {actvLocn} is {actvQtyPercent}%')

        return actvQtyPercent

    def getWmInvnDtls(self, itemBrcd: str = None, locnBrcd: str = None, lpn: str = None):
        # sql = self._fetchWMInvnDtls
        sql = f"""select lh.locn_brcd, ic.item_name, wi.* from wm_inventory wi 
                    left outer join locn_hdr lh on wi.location_id = lh.locn_id 
                    inner join item_cbo ic on wi.item_id = ic.item_id
                    inner join size_uom su on ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
                    where 0=0 
                    #CONDITION#"""
        sqlCond = ''
        if itemBrcd is not None:
            sqlCond += " \n and ic.item_name = '" + itemBrcd + "'"
        if locnBrcd is not None:
            sqlCond += " \n and lh.locn_brcd = '" + locnBrcd + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('ITEM_NAME') is not None, '<Data> WM invn dtl not found ' + sql

        return dbRow

    def assertWMInvnDtls(self, i_itemBrcd: str = None, i_locn: str = None, i_lpn: str = None, i_transInvnType: int = None,
                         o_onHandQty: int = None, o_toBeFilledQty: int = None, o_allocatedQty: int = None,
                         o_allocatableFlag: str = None):

        sql = f"""select lh.locn_brcd, ic.item_name, wi.* from wm_inventory wi 
                 left outer join locn_hdr lh on wi.location_id = lh.locn_id 
                 inner join item_cbo ic on wi.item_id = ic.item_id
                 where 0=0 
                 #CONDITION#
              """
        sqlCond = ''
        if i_itemBrcd is not None:
            sqlCond += " \n and ic.item_name = '" + i_itemBrcd + "'"
        if i_locn is not None:
            sqlCond += " \n and lh.locn_brcd = '" + i_locn + "'"
        if i_lpn is not None:
            sqlCond += " \n and wi.tc_lpn_id = '" + i_lpn + "'"
        if i_transInvnType is not None:
            sqlCond += " \n and wi.transitional_inventory_type = '" + str(i_transInvnType) + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('ITEM_ID') is not None, '<WMInvn> WM invn record not found ' + sql

        assertlist = []
        if o_onHandQty is not None:
            dbVal = dbRow.get("ON_HAND_QTY")
            isMatched = DBService.compareEqual(dbVal, int(o_onHandQty), '<WMInvn> wm onHandQty')
            assertlist.append(isMatched)
        if o_toBeFilledQty is not None:
            dbVal = dbRow.get("TO_BE_FILLED_QTY")
            isMatched = DBService.compareEqual(dbVal, int(o_toBeFilledQty), '<WMInvn> wm toBeFilledQty')
            assertlist.append(isMatched)
        if o_allocatedQty is not None:
            dbVal = dbRow.get("WM_ALLOCATED_QTY")
            isMatched = DBService.compareEqual(dbVal, int(o_allocatedQty), '<WMInvn> wm allocatedQty')
            assertlist.append(isMatched)
        if o_allocatableFlag is not None:
            dbVal = dbRow.get("ALLOCATABLE")
            isMatched = DBService.compareEqual(dbVal, o_allocatableFlag, '<WMInvn> wm allocatable Flag')
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<WMInvn> Few wm invn validation failed for {i_itemBrcd} ' + sql

    def assertNoWMInvRecForLpn(self, i_lpn: str):
        sql = f"""select * from wm_inventory where tc_lpn_id = '{i_lpn}'"""

        dbRow = DBService.fetch_row(sql, self.schema)
        noRecPresent = True if dbRow is None or len(dbRow) == 0 else False

        assert noRecPresent, '<WMInvn> WM record found for LPN ' + i_lpn

    def assertAllocDtls(self, i_itemBrcd: str, i_cntr: str = None, i_taskGenRefNbr: str = None,
                        i_taskCmplRefNbr: str = None,
                        i_intType: int = None, o_taskPriority: int = None, o_pullLocn: str = None,
                        o_destLocn: str = None, o_origReqmt: int = None, o_qtyAlloc: int = None,
                        o_qtyPulled: int = None, o_statCode: AllocStat = None, i_or_taskRefNbr: str = None):

        sql = f"""select ic.item_name, lhp.locn_brcd pull_locn_brcd, lhd.locn_brcd dest_locn_brcd, aid.* 
                 from alloc_invn_dtl aid 
                 inner join item_cbo ic on aid.item_id = ic.item_id
                 left outer join locn_hdr lhp on aid.pull_locn_id = lhp.locn_id
                 left outer join locn_hdr lhd on aid.dest_locn_id = lhd.locn_id
                 where 0=0 
                 #CONDITION#"""
        sqlCond = ''
        if i_cntr is not None:
            sqlCond += " \n and aid.cntr_nbr = '" + str(i_cntr) + "'"
        if i_or_taskRefNbr is not None:
            sqlCond += f" \n and (aid.task_genrtn_ref_nbr = '{i_or_taskRefNbr}' or aid.task_cmpl_ref_nbr = '{i_or_taskRefNbr}')"
        else:
            if i_taskGenRefNbr is not None:
                sqlCond += " \n and aid.task_genrtn_ref_nbr = '" + str(i_taskGenRefNbr) + "'"
            if i_taskCmplRefNbr is not None:
                sqlCond += " \n and aid.task_cmpl_ref_nbr = '" + str(i_taskCmplRefNbr) + "'"
        if i_itemBrcd is not None:
            sqlCond += " \n and ic.item_name = '" + str(i_itemBrcd) + "'"
        if i_intType is not None:
            sqlCond += " \n and aid.invn_need_type = '" + str(i_intType) + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('CREATE_DATE_TIME') is not None, '<Alloc> Alloc dtl not found ' + sql

        assertlist = []
        if o_taskPriority is not None:
            dbVal = dbRow.get("TASK_PRTY")
            isMatched = DBService.compareEqual(dbVal, o_taskPriority, '<Alloc> alloc taskPriority for item ' + str(i_itemBrcd))
            assertlist.append(isMatched)
        if o_pullLocn is not None:
            dbVal = dbRow.get("PULL_LOCN_BRCD")
            isMatched = DBService.compareEqual(dbVal, o_pullLocn, '<Alloc> alloc pullLocn for item ' + str(i_itemBrcd))
            assertlist.append(isMatched)
        if o_destLocn is not None:
            dbVal = dbRow.get("DEST_LOCN_BRCD")
            isMatched = DBService.compareEqual(dbVal, o_destLocn, '<Alloc> alloc destLocn for item ' + str(i_itemBrcd))
            assertlist.append(isMatched)
        if o_origReqmt is not None:
            dbVal = dbRow.get("ORIG_REQMT")
            isMatched = DBService.compareEqual(dbVal, o_origReqmt, '<Alloc> alloc origReqmt for item ' + str(i_itemBrcd))
            assertlist.append(isMatched)
        if o_qtyAlloc is not None:
            dbVal = dbRow.get("QTY_ALLOC")
            isMatched = DBService.compareEqual(dbVal, o_qtyAlloc, '<Alloc> alloc qtyAlloc for item ' + str(i_itemBrcd))
            assertlist.append(isMatched)
        if o_qtyPulled is not None:
            dbVal = dbRow.get("QTY_PULLD")
            isMatched = DBService.compareEqual(dbVal, o_qtyPulled, '<Alloc> qtyPulled for item ' + i_itemBrcd)
            assertlist.append(isMatched)
        if o_statCode is not None:
            dbVal = dbRow.get("STAT_CODE")
            isMatched = DBService.compareEqual(dbVal, o_statCode.value, '<Alloc> alloc statCode for item ' + str(i_itemBrcd), o_statCode.name)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<Alloc> alloc_invn_dtl validation failed ' + sql

    def getTaskIdsFromGenRefNbr(self, taskGenRefNbr:str, ignoreTaskId:str=None):
        sql = f"""select * from task_hdr th where task_genrtn_ref_nbr in ('{taskGenRefNbr}') 
                  #CONDITION#
              """
        sqlCond = ''
        if ignoreTaskId is not None:
            sqlCond += f" \n and th.task_id != '{ignoreTaskId}' order by th.create_date_time desc"

        sql = sql.replace('#CONDITION#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)
        assert dbRows is not None and len(dbRows) > 0 and dbRows[0]['TASK_ID'] is not None, '<Data> Task not found ' + sql
        taskIds = [i['TASK_ID'] for i in dbRows]

        return taskIds

    def getTaskIdFromGenRefNbr(self, taskGenRefNbr, i_ignoreTaskId:str = None):
        taskIds = self.getTaskIdsFromGenRefNbr(taskGenRefNbr, i_ignoreTaskId)
        taskId = taskIds[0]
        return taskId

    def getTaskIdFromTaskDtl(self, cntrNbr: str, isLTRTask: bool = None):
        sql = f"""select task_id from task_dtl where cntr_nbr = '#ILPN#'"""
        sql = sql.replace('#ILPN#', str(cntrNbr))

        if isLTRTask:
            sql += " \n and task_type='LT' and task_prty='70'"
        sql += " \n order by create_date_time desc"

        dbRow = DBService.fetch_row(sql, self.schema)

        taskId = dbRow.get('TASK_ID')
        return taskId

    def getTaskIdByORCond(self, taskGenRefNbr: str, taskCmplRefNbr: str, cntr: str, intType: int = None):
        taskId = self._getTaskIdByORCond_1by1(taskGenRefNbr, taskCmplRefNbr, cntr, intType)
        return taskId

    def _getTaskIdByORCond_all(self, taskGenRefNbr: str, taskCmplRefNbr: str, cntr: str, intType: int = None):
        """TODO Method not used yet
        """
        sql = f"""select * from task_hdr th 
                 where (th.task_genrtn_ref_nbr in (select task_genrtn_ref_nbr from alloc_invn_dtl where cntr_nbr='#CNTR#')
                        or th.task_genrtn_ref_nbr='#TASK_GEN_REFNBR#' 
                        or th.task_cmpl_ref_nbr='#TASK_CMPL_REFNBR#')
                 #CONDITION# 
                 order by create_date_time desc"""
        sql = sql.replace('#CNTR#', cntr)
        sql = sql.replace('#TASK_GEN_REFNBR#', taskGenRefNbr)
        sql = sql.replace('#TASK_CMPL_REFNBR#', taskCmplRefNbr)
        if intType is not None:
            sql = sql.replace('#CONDITION#', " and th.invn_need_type = '" + str(intType) + "'")

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None, '<Data> Task id not found ' + sql
        taskId = dbRow.get("TASK_ID")

        return taskId

    def _getTaskIdByORCond_1by1(self, taskGenRefNbr: str, taskCmplRefNbr: str, cntr: str, intType: int = None):
        """Get task id from multiple queries one by one
        th.task_id,th.task_genrtn_ref_nbr,th.task_cmpl_ref_nbr,th.invn_need_type,th.create_date_time
        """
        taskId = None
        sqlCond = ''
        if intType is not None:
            sqlCond += f" \n and th.invn_need_type = '{intType}'"

        sql = f"""select th.task_id from task_hdr th where th.task_genrtn_ref_nbr='{taskGenRefNbr}' 
                  {sqlCond} order by create_date_time desc"""
        dbRow = DBService.fetch_row(sql, self.schema)
        if dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None:
            taskId = dbRow.get("TASK_ID")

        if taskId is None:
            sql2 = f"""select th.task_id from task_hdr th where th.task_cmpl_ref_nbr='{taskCmplRefNbr}' 
                       {sqlCond} order by create_date_time desc"""
            dbRow = DBService.fetch_row(sql2, self.schema)
            if dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None:
                taskId = dbRow.get("TASK_ID")

        if taskId is None:
            sql3 = f"""select th.task_id from task_hdr th 
                       where th.task_genrtn_ref_nbr in (select task_genrtn_ref_nbr from alloc_invn_dtl where cntr_nbr='{cntr}') 
                       {sqlCond} order by create_date_time desc"""
            dbRow = DBService.fetch_row(sql3, self.schema)
            if dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None:
                taskId = dbRow.get("TASK_ID")

        assert taskId is not None, '<Data> Task id not found from multiple queries ' + sql

        return taskId

    def getTaskGroupFromTask(self, taskId=None):
        sql = f"""select task_grp from task_hdr td inner join int_path_defn ipd on td.start_curr_work_grp=ipd.curr_work_grp
                 and td.start_curr_work_area=ipd.curr_work_area and td.start_dest_work_grp=ipd.dest_work_grp
                 and td.start_dest_work_area=ipd.dest_work_area 
                 where td.invn_need_type=1 
                 and task_id=#TASKID#"""
        sql = sql.replace('#TASKID#', str(taskId))
        taskGrp = DBService.fetch_row(sql,self.schema)
        return taskGrp

    def getAllTaskGroupFromTask(self, taskId=None, intType: int = None):
        sql = f"""select task_grp from task_hdr td inner join int_path_defn ipd on td.start_curr_work_grp=ipd.curr_work_grp
                 and td.start_curr_work_area=ipd.curr_work_area and td.start_dest_work_grp=ipd.dest_work_grp
                 and ipd.dest_work_area in (td.start_dest_work_area , '*') 
                 where task_id = #TASKID#
                 #CONDITION# 
              """
        sql = sql.replace('#TASKID#', str(taskId))

        sqlCond = ''
        if intType is not None:
            sqlCond += f" \n and td.invn_need_type={intType}"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)
        allTaskGrps = [i['TASK_GRP'] for i in dbRows]
        printit(f"Task {taskId} with eligible taskGrps {allTaskGrps}")

        return allTaskGrps

    def getReplenTaskAssignedToUser(self, userId: str = '*'):
        """Get older task id assigned to user
        """
        sql = f"""select * from task_hdr where owner_user_id='{userId}' 
                    and task_id in (select td.task_id from task_hdr td inner join int_path_defn ipd on td.start_curr_work_grp=ipd.curr_work_grp and td.start_curr_work_area=ipd.curr_work_area 
                                    and td.start_dest_work_grp=ipd.dest_work_grp and td.start_dest_work_area=ipd.dest_work_area and ipd.task_grp='ALL')
                    and invn_need_type=1 and lower(task_desc) like '%replen%' and stat_code in ('10')
                    #CONDITION#
                    order by create_date_time offset 0 rows fetch next 1 rows only """
        sqlCond = ''

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += f""" \n and task_id not in (select task_id from task_dtl 
                                        where pull_locn_id in (select locn_id from locn_hdr where locn_brcd in {threadLocns})) """
            sqlCond += f""" \n and task_id not in (select task_id from task_dtl 
                                        where dest_locn_id in (select locn_id from locn_hdr where locn_brcd in {threadLocns})) """

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f""" \n and task_id not in (select task_id from task_dtl 
                                        where item_id in (select item_id from item_cbo where item_name in {threadItems})) """

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow, f"<Data> Replen task assigned to user {userId} not found"
        taskId = dbRow.get('TASK_ID')

        return taskId

    def _updateTaskWithOldestCreatedDate(self, taskId: str, user:str):
        """Update created date time for task to oldest
        """
        if 'true' in self.IS_ALLOW_UPDATE_TASK:
            updQ = f"""update task_hdr 
                        set create_date_time = (select create_date_time-1 as create_date_time from task_hdr 
                                                where owner_user_id='{user}' order by create_date_time asc offset 0 rows fetch next 1 rows only), 
                       user_id='AUTOMATION' 
                       where task_id='{taskId}'
                    """
            updQ = updQ.replace("#USER_ID#", user)

            DBService.update_db(updQ, self.schema)
        else:
            assert False, f"Update task not allowed. Test manually"

    def assertWaitForTask(self, i_cntrNbr:str , i_itemBrcd:str = None,  i_intType:int = None, i_destLocn:str = None, i_taskPrty:int = None):
        sql = f"""select ic.item_name, lhp.locn_brcd pull_locn_brcd, lhd.locn_brcd dest_locn_brcd, td.* 
                 from task_dtl td inner join task_hdr th on td.task_id = th.task_id
                 inner join item_cbo ic on td.item_id = ic.item_id
                 left outer join locn_hdr lhp on td.pull_locn_id = lhp.locn_id
                 left outer join locn_hdr lhd on td.dest_locn_id = lhd.locn_id
                 where 0=0 
                 #CONDITION#
              """
        sqlCond = ''
        if i_itemBrcd is not None:
            sqlCond += " \n and ic.item_name = '" + str(i_itemBrcd) + "'"
        if i_cntrNbr is not None:
            sqlCond += " \n and td.cntr_nbr = '" + str(i_cntrNbr) + "'"
        if i_intType is not None:
            sqlCond += " \n and td.invn_need_type = '" + str(i_intType) + "'"
        if i_destLocn is not None:
            sqlCond += " \n and lhd.locn_brcd = '" + str(i_destLocn) + "'"
        if i_taskPrty is not None:
            sqlCond += " \n and td.task_prty = '" + str(i_taskPrty) + "'"
        sqlCond += " \n and th.create_date_time >= sysdate-1 order by th.create_date_time desc "
        sql = sql.replace('#CONDITION#', sqlCond)

        DBService.wait_for_records(sql, 1, self.schema, 60)

    def assertTaskHdr(self, i_task=None, i_currTaskPrty=None, i_taskGenRefNbr: str = None, i_taskCmplRefNbr: str = None,
                      i_cntr: str = None, i_ignoreTaskId: str = None, o_status: TaskHdrStat = None,
                      o_intType: int = None, o_cmplRefNbr: str = None, o_currTaskPrty: int = None, isIgnoreDateCheck:bool=False, o_ownerUser: str = None):

        sql = "select * from task_hdr th where 0=0 #CONDITION# "
        sqlCond = ''
        if i_task is not None:
            sqlCond += " \n and th.task_id = '" + str(i_task) + "'"
        if i_currTaskPrty is not None:
            sqlCond += " \n and th.curr_task_prty = '" + str(i_currTaskPrty) + "'"
        if i_cntr is not None:
            sqlCond += f" \n and th.task_genrtn_ref_nbr in (select task_genrtn_ref_nbr from alloc_invn_dtl where cntr_nbr = '{i_cntr}')"
        if i_taskGenRefNbr is not None:
            sqlCond += " \n and th.task_genrtn_ref_nbr = '" + str(i_taskGenRefNbr) + "'"
        if i_taskCmplRefNbr is not None:
            sqlCond += " \n and th.task_cmpl_ref_nbr = '" + str(i_taskCmplRefNbr) + "'"
        if i_ignoreTaskId is not None:
            sqlCond += " \n and th.task_id != '" + str(i_ignoreTaskId) + "'"
        if not isIgnoreDateCheck:
            sqlCond += " \n and th.create_date_time >= sysdate-1 order by th.create_date_time desc "
        sql = sql.replace('#CONDITION#', sqlCond)

        DBService().wait_for_records(query=sql, expected_cnt=1, schema=self.schema)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None, '<TaskHdr> Task hdr not found ' + sql

        assertlist = []
        if o_status is not None:
            dbVal = dbRow.get("STAT_CODE")
            isMatched = DBService.compareEqual(dbVal, o_status.value, '<TaskHdr> task status',o_status.name)
            assertlist.append(isMatched)
        if o_intType is not None:
            dbVal = dbRow.get("INVN_NEED_TYPE")
            isMatched = DBService.compareEqual(dbVal, o_intType, '<TaskHdr> task intType')
            assertlist.append(isMatched)
        if o_cmplRefNbr is not None:
            dbVal = dbRow.get("TASK_CMPL_REF_NBR")
            isMatched = DBService.compareEqual(dbVal, o_cmplRefNbr, '<TaskHdr> task cmplRefNbr')
            assertlist.append(isMatched)
        if o_currTaskPrty is not None:
            dbVal = dbRow.get("CURR_TASK_PRTY")
            isMatched = DBService.compareEqual(dbVal, o_currTaskPrty, '<TaskHdr> current task priority')
            assertlist.append(isMatched)
        if o_ownerUser is not None:
            dbVal = dbRow.get("OWNER_USER_ID")
            isMatched = DBService.compareEqual(dbVal, o_ownerUser, '<TaskHdr> user ID')
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<TaskHdr> Few task_hdr validation failed ' + sql

    def assertTaskDtls(self, i_task: str = None, i_itemBrcd: str = None, i_cntrNbr: str = None,
                       i_intType: int = None, i_pullLocn: str = None, i_destLocn: str = None,
                       o_taskPriority: int = None, o_taskSeq: int = None,
                       o_pullLocn: str = None, o_destLocn: str = None, o_origReqmt: int = None,
                       o_qtyAlloc: int = None, o_qtyPulled: int = None, o_statCode: TaskDtlStat = None):
        """"""
        sql = f"""select ic.item_name, lhp.locn_brcd pull_locn_brcd, lhd.locn_brcd dest_locn_brcd, td.* 
                    from task_dtl td inner join task_hdr th on td.task_id = th.task_id
                    inner join item_cbo ic on td.item_id = ic.item_id
                    left outer join locn_hdr lhp on td.pull_locn_id = lhp.locn_id
                    left outer join locn_hdr lhd on td.dest_locn_id = lhd.locn_id
                    where 0=0 
                    #CONDITION#"""
        sqlCond = ''
        if i_task is not None:
            sqlCond += " \n and td.task_id = '" + str(i_task) + "'"
        if i_itemBrcd is not None:
            sqlCond += " \n and ic.item_name = '" + str(i_itemBrcd) + "'"
        if i_cntrNbr is not None:
            sqlCond += " \n and td.cntr_nbr = '" + str(i_cntrNbr) + "'"
        if i_intType is not None:
            sqlCond += " \n and td.invn_need_type = '" + str(i_intType) + "'"
        if i_pullLocn is not None:
            sqlCond += " \n and lhp.locn_brcd = '" + str(i_pullLocn) + "'"
        if i_destLocn is not None:
            sqlCond += " \n and lhd.locn_brcd = '" + str(i_destLocn) + "'"
        sqlCond += " \n and td.create_date_time > sysdate - interval '7' minute order by th.task_id desc "

        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TASK_ID') is not None, '<TaskDtl> Task dtl not found ' + sql

        assertlist = []
        if o_taskPriority is not None:
            dbVal = dbRow.get("TASK_PRTY")
            isMatched = DBService.compareEqual(dbVal, o_taskPriority, '<TaskDtl> task dtl taskPriority')
            assertlist.append(isMatched)
        if o_pullLocn is not None:
            dbVal = dbRow.get("PULL_LOCN_BRCD")
            isMatched = DBService.compareEqual(dbVal, o_pullLocn, '<TaskDtl> task dtl pullLocn')
            assertlist.append(isMatched)
        if o_destLocn is not None:
            dbVal = dbRow.get("DEST_LOCN_BRCD")
            isMatched = DBService.compareEqual(dbVal, o_destLocn, '<TaskDtl> task dtl destLocn')
            assertlist.append(isMatched)
        if o_origReqmt is not None:
            dbVal = dbRow.get("ORIG_REQMT")
            isMatched = DBService.compareEqual(dbVal, o_origReqmt, '<TaskDtl> task dtl origReqmt')
            assertlist.append(isMatched)
        if o_qtyAlloc is not None:
            dbVal = dbRow.get("QTY_ALLOC")
            isMatched = DBService.compareEqual(dbVal, o_qtyAlloc, '<TaskDtl> task dtl qtyAlloc')
            assertlist.append(isMatched)
        if o_qtyPulled is not None:
            dbVal = dbRow.get("QTY_PULLD")
            isMatched = DBService.compareEqual(dbVal, o_qtyPulled, '<TaskDtl> task dtl qtyPulled')
            assertlist.append(isMatched)
        if o_statCode is not None:
            dbVal = dbRow.get("STAT_CODE")
            isMatched = DBService.compareEqual(dbVal, o_statCode.value, '<TaskDtl> task dtl statCode', o_statCode.name)
            assertlist.append(isMatched)
        if o_taskSeq is not None:
            dbVal = dbRow.get("TASK_SEQ_NBR")
            isMatched = DBService.compareEqual(dbVal, o_taskSeq, '<TaskDtl> task dtl taskSeqNum', str(o_taskSeq))
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<TaskDtl> Few task_dtl validation failed ' + sql

    def assertTaskCount(self, i_taskGenRefNbr: str = None, i_taskCmplRefNbr: str = None, i_cntr: str = None, o_totalTasks: int = None,
                        o_totalTaskDtls: list[int] = None):
        """"""
        sql = f"""select * from task_hdr th where 0=0 
                 #CONDITION# 
              """
        sqlCond = ''
        if i_cntr is not None:
            sqlCond += f" \n and th.task_genrtn_ref_nbr in (select task_genrtn_ref_nbr from alloc_invn_dtl where cntr_nbr = '{i_cntr}')"
        if i_taskGenRefNbr is not None:
            sqlCond += f" \n and th.task_genrtn_ref_nbr = '{i_taskGenRefNbr}'"
        if i_taskCmplRefNbr is not None:
            sqlCond += f" \n and th.task_cmpl_ref_nbr = '{i_taskCmplRefNbr}'"
        sqlCond += " \n and th.create_date_time >= sysdate-1 order by th.create_date_time desc "

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        isMatched = DBService.compareEqual(len(dbRows), o_totalTasks, '<TaskHdr> Total tasks')
        assert isMatched, f"<Task> Task count {o_totalTasks} didnt match " + sql

    def assertLaborMsgHdr(self, i_actName: str, i_refNbr: str = None, i_user: str = None, i_taskNbr: str = None,
                          o_jobFunc: str = None):
        """"""
        sql = f"""select lm.labor_msg_id,lm.act_name,lm.login_user_id,lm.job_function,lm.ref_nbr,lm.* 
                 from labor_msg lm where 0=0 
                 #CONDITION#
              """
        if i_user is None:
            i_user = ENV_CONFIG.get('rf', 'rf_user')
        sqlCond = " and lm.login_user_id = '" + i_user + "'"
        if i_actName is not None:
            sqlCond += f" \n and (trim(lm.act_name) = '{i_actName}' or trim(lm.orig_act_name) = '{i_actName}')"
        if i_refNbr is not None:
            sqlCond += " \n and lm.ref_nbr = '" + i_refNbr + "'"
        if i_taskNbr is not None:
            sqlCond += " \n and lm.task_nbr = '" + str(i_taskNbr) + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('LOGIN_USER_ID') == i_user, '<LMHdr> LM hdr not found ' + sql
        self.logger.info(f"<LMHdr> LM hdr record found")

        assertlist = []
        if o_jobFunc is not None:
            dbVal = dbRow.get("JOB_FUNCTION")
            isMatched = DBService.compareEqual(dbVal, o_jobFunc, '<LMHdr> job func for ' + i_actName)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<LMHdr> LM hdr validation failed ' + sql

    def assertLaborMsgDtl(self, i_actName: str, i_refNbr: str = None, i_user: str = None, i_taskNbr: str = None,
                          i_lpn: str = None, i_itemBrcd: str = None, o_qty: int = None):
        """"""
        sql = f"""select * from labor_msg_dtl lmd inner join labor_msg lm on lmd.labor_msg_id = lm.labor_msg_id
                 where 0=0 
                 #CONDITION#
              """
        if i_user is None:
            i_user = ENV_CONFIG.get('rf', 'rf_user')
        sqlCond = " and lm.login_user_id = '" + i_user + "'"
        if i_actName is not None:
            sqlCond += f" \n and (trim(lm.act_name) = '{i_actName}' or trim(lm.orig_act_name) = '{i_actName}')"
        if i_refNbr is not None:
            sqlCond += " \n and lm.ref_nbr = '" + i_refNbr + "'"
        if i_taskNbr is not None:
            sqlCond += " \n and lm.task_nbr = '" + str(i_taskNbr) + "'"
        if i_lpn is not None:
            sqlCond += " \n and lmd.tc_ilpn_id = '" + i_lpn + "'"
        if i_itemBrcd is not None:
            sqlCond += " \n and lmd.item_name = '" + i_itemBrcd + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('LOGIN_USER_ID') is not None, '<LMDtl> LM dtl not found ' + sql
        self.logger.info(f"<LMDtl> LM dtl record found")

        assertlist = []
        if o_qty is not None:
            dbVal = dbRow.get("QTY")
            isMatched = DBService.compareEqual(dbVal, o_qty, '<LMDtl> qty for ' + i_actName)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<LMDtl> LM dtl validation failed ' + sql

    def assertWaitCLEndPointQueueStatus(self, i_msgId, o_status: int):
        sql = f"""select * from cl_endpoint_queue where msg_id is not null #CONDITION#"""

        sqlCond = " and msg_id = '" + str(i_msgId) + "'"
        sql = sql.replace('#CONDITION#', sqlCond)

        DBService.wait_for_value(sql, 'STAT_CODE', str(o_status), self.schema)

    def assertPix(self, i_itemBrcd: str, i_tranType: str, i_tranCode: str = None, i_caseNbr: str = None,
                  i_invnAdjQty: int = None, i_invnAdjType: str = None, i_rsnCode: str = None,
                  o_proc_stat_code: int = None, o_any_procStatCode: tuple = None):

        # sql = self._fetchPixDtlsByItemPixDtls.replace('#ITEM_NAME#', i_itemBrcd).replace('#TRAN_TYPE#', i_tranType)
        sql = f"""select pt.* from pix_tran pt where pt.item_name = '#ITEM_NAME#' and pt.tran_type = '#TRAN_TYPE#' 
                 #CONDITION#"""
        sql = sql.replace('#ITEM_NAME#', i_itemBrcd).replace('#TRAN_TYPE#', i_tranType)
        sqlCond = ''
        if i_caseNbr is not None:
            sqlCond += " \n and pt.case_nbr = '" + i_caseNbr + "'"
        if i_tranCode is not None:
            sqlCond += " \n and pt.tran_code = '" + i_tranCode + "'"
        if i_invnAdjQty is not None:
            sqlCond += " \n and pt.invn_adjmt_qty = '" + str(i_invnAdjQty) + "'"
        if i_invnAdjType is not None:
            sqlCond += " \n and pt.invn_adjmt_type = '" + i_invnAdjType + "'"
        sqlCond += " \n and pt.mod_date_time >= sysdate - interval '7' minute"
        sqlCond += " \n order by pt.mod_date_time desc"
        sql = sql.replace('#CONDITION#', sqlCond)

        # DBService().wait_for_records(query=sql, expected_cnt=1, schema=self.schema)
        DBService().wait_for_value(query=sql, column='ITEM_NAME', expected_value=i_itemBrcd, schema=self.schema)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TRAN_TYPE') is not None, '<Pix> Pix dtls not found ' + sql

        assertlist = []
        if i_tranType is not None:
            dbVal = dbRow.get("TRAN_TYPE")
            isMatched = DBService.compareEqual(dbVal, i_tranType, f"<Pix> {i_itemBrcd} with trantype {i_tranType}")
            assertlist.append(isMatched)
        if i_invnAdjQty is not None:
            dbVal = dbRow.get("INVN_ADJMT_QTY")
            isMatched = DBService.compareEqual(dbVal, int(i_invnAdjQty), f"<Pix> {i_itemBrcd} with invn adj qty {i_invnAdjQty}")
            assertlist.append(isMatched)
        if i_invnAdjType is not None:
            dbVal = dbRow.get("INVN_ADJMT_TYPE")
            isMatched = DBService.compareEqual(dbVal, i_invnAdjType, f"<Pix> {i_itemBrcd} with invn adj type {i_invnAdjType}")
            assertlist.append(isMatched)
        if i_rsnCode is not None:
            dbVal = dbRow.get("RSN_CODE")
            isMatched = DBService.compareEqual(dbVal, str(i_rsnCode), f"<Pix> {i_itemBrcd} with reason code {i_rsnCode}")
            assertlist.append(isMatched)
        if o_proc_stat_code is not None:
            dbVal = dbRow.get("PROC_STAT_CODE")
            isMatched = DBService.compareEqual(dbVal, int(o_proc_stat_code), f"<Pix> {i_itemBrcd} with processed status {o_proc_stat_code}")
            assertlist.append(isMatched)
        if o_any_procStatCode is not None:
            dbVal = dbRow.get("PROC_STAT_CODE")
            isMatched = DBService.compareIn(dbVal, o_any_procStatCode, f"<Pix> {i_itemBrcd} with processed status {o_any_procStatCode}")
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<Pix> Few pix validation failed for {i_itemBrcd} ' + sql

    def _updateWMInvn(self, i_locnBrcd: str, i_itemBrcd: str, u_onHandQty: int = None, u_wmAllocatedQty: int = None,
                      u_toBeFilledQty: int = None):
        sql = f"""update wm_inventory set #UPDATES#
                 where item_id in (select item_id from item_cbo where item_name = '{i_itemBrcd}')
                 and location_id in (select locn_id from locn_hdr where locn_brcd = '{i_locnBrcd}')
              """
        updList = []
        if u_onHandQty is not None:
            updList.append("on_hand_qty = '" + str(u_onHandQty) + "'")
        if u_wmAllocatedQty is not None:
            updList.append("wm_allocated_qty = '" + str(u_wmAllocatedQty) + "'")
        if u_toBeFilledQty is not None:
            updList.append("to_be_filled_qty = '" + str(u_toBeFilledQty) + "'")

        assert len(updList) > 0, '<Data> No WM data provided for update'

        sqlUpd = ', \n'.join(updList)
        sql = sql.replace('#UPDATES#', sqlUpd)

        DBService.update_db(sql, self.schema)

    def _updateWMInvnForLpn(self, i_locnBrcd: str, i_lpn:str, i_itemBrcd: str, u_onHandQty: int = None):
        sql = f"""update wm_inventory set #UPDATES#
                 where item_id in (select item_id from item_cbo where item_name = '{i_itemBrcd}')
                 and location_id in (select locn_id from locn_hdr where locn_brcd = '{i_locnBrcd}')
                 and tc_lpn_id = '{i_lpn}'
              """
        updList = []
        if u_onHandQty is not None:
            updList.append(f"on_hand_qty = '{u_onHandQty}'")

        assert len(updList) > 0, '<Data> No WM data provided for update'

        sqlUpd = ', \n'.join(updList)
        sql = sql.replace('#UPDATES#', sqlUpd)

        DBService.update_db(sql, self.schema)

    def _updateLpnQty(self, i_iLpn: str, i_item: str, u_lpnQty: int):
        sql = f"""Update lpn_detail set size_value='{u_lpnQty}' 
                    where lpn_id in (select lpn_id from lpn where tc_lpn_id = '{i_iLpn}') 
                    and item_id in (select item_id from item_cbo where item_name='{i_item}') """
        DBService.update_db(sql, self.schema)

    def getMaxInvnQty(self, i_locnBrcd: str, i_itemBrcd: str):
        sql = f"""select max_invn_qty from pick_locn_dtl
                 where locn_id in (select locn_id from locn_hdr where locn_brcd = '#LOCN_BRCD#')
                 and item_id in (select item_id from item_cbo where item_name = '#ITEM_NAME#')
              """
        sql = sql.replace('#ITEM_NAME#', i_itemBrcd).replace('#LOCN_BRCD#', i_locnBrcd)

        dbRow = DBService.fetch_row(sql, self.schema)
        maxInvnQty = int(dbRow.get('MAX_INVN_QTY'))

        return maxInvnQty

    def _updatePickLocnDtl_NOT_USED(self, i_locnBrcd: str, i_itemBrcd: str, u_maxInvn: int):
        """TODO Method not used yet
        """
        sql = f"""update pick_locn_dtl set max_invn_qty = '#MAX_INVN_QTY#'
                 where locn_id in (select locn_id from locn_hdr where locn_brcd = '#LOCN_BRCD#')
                 and item_id in (select item_id from item_cbo where item_name = '#ITEM_NAME#')
              """
        sql = sql.replace('#ITEM_NAME#', i_itemBrcd).replace('#LOCN_BRCD#', i_locnBrcd)
        sql = sql.replace('#MAX_INVN_QTY#', str(u_maxInvn))

        DBService.update_db(sql, self.schema)

    def getCCPendingFlagForLocn(self, locn:str):
        sql = f"select cycle_cnt_pending from locn_hdr where locn_brcd='{locn}' "

        dbRow = DBService.fetch_row(sql, self.schema)

        ccFlag = dbRow['CYCLE_CNT_PENDING']

        return ccFlag

    def _updateLocnCCPending(self, i_locnBrcd: str, u_isCCPending: bool):
        ccPendingFlag = 'Y' if u_isCCPending else 'N'
        ccFlag = self.getCCPendingFlagForLocn(locn=i_locnBrcd)
        ccFlag = 'N' if ccFlag is None else ccFlag

        isCCUpdateReqd = True if ccFlag != ccPendingFlag else False

        if isCCUpdateReqd:
            if 'true' in self.IS_ALLOW_UPDATE_LOCN:
                '''Exclude runtime thread locns'''
                threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
                if threadLocns is not None:
                    assert i_locnBrcd not in threadLocns, f"<Data> To be excluded locn {i_locnBrcd} found for update CC flag. Test manually"

                sql = f"""update locn_hdr set cycle_cnt_pending='{ccPendingFlag}' where locn_brcd='{i_locnBrcd}'"""

                DBService.update_db(sql, self.schema)

                if u_isCCPending is False:
                    self._cancelCCOpenTasks(locnBrcd=i_locnBrcd)
            else:
                assert False, f"<Data> Locn update not allowed. Test manually"
        else:
            printit(f"^^^ CC flag is already {ccFlag} for locn {i_locnBrcd}")

    def _cancelCCOpenTasks(self, locnBrcd: str):
        """Cancels open cc tasks for any locn"""
        if 'true' in self.IS_ALLOW_CANCEL_TASK:
            sql = f"""select th.task_id from task_hdr th inner join locn_hdr lh on th.task_genrtn_ref_nbr=lh.locn_id
                        where lh.locn_brcd='{locnBrcd}' and th.invn_need_type in ('100','101') and stat_code not in ('90','99')
                        #CONDITION#"""
            sqlCond = ''

            # '''Exclude runtime thread locns'''
            # threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            # if threadLocns is not None:
            #     sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                assert locnBrcd not in threadLocns, f"<Data> To be excluded locn {locnBrcd} found for CC task cancel. Test manually"

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)

            isOpenTaskFound = True if dbRows is not None and len(dbRows) > 0 and dbRows[0]['TASK_ID'] is not None else False
            allOpenTasks = [i['TASK_ID'] for i in dbRows]

            if isOpenTaskFound:
                updSql = "update task_hdr set stat_code='99' where task_id in " + Commons.get_tuplestr(allOpenTasks)
                DBService.update_db(updSql, self.schema)
        else:
            assert False, "<Data> CC task cancel not allowed. Test manually"

    def _presetInvnInActvLocn(self, i_locnBrcd: str, i_itemBrcd: str, f_onHand:int = None, f_availUnit: int = None,
                              f_availCap: int = None, f_isCCPending: bool = None):
        isUpdateOnHand = isUpdateMaxInvn = False
        final_onHandQty = None

        if f_availUnit is not None or f_onHand is not None or f_availCap is not None:
            currAvailUnit = self.getAvailUnitFromWM(itemBrcd=i_itemBrcd, locnBrcd=i_locnBrcd)
            currOnHand = self.getWMOnHandQty(itemBrcd=i_itemBrcd, locnBrcd=i_locnBrcd)
            currWmAllocQty = self.getWMAllocQty(itemBrcd=i_itemBrcd, locnBrcd=i_locnBrcd)
            currMaxInvnQty = self.getMaxInvnQty(i_locnBrcd=i_locnBrcd, i_itemBrcd=i_itemBrcd)

            final_onHandQty = final_wmAllocQty = final_tbfQty = None

            '''Approach 1'''
            # if currAvailUnit < f_availUnit:
            #     final_onHandQty = currOnHand + f_availUnit - currAvailUnit
            #     isUpdateOnHand = True
            # elif currAvailUnit > f_availUnit:
            #     final_onHandQty = f_availUnit
            #     final_wmAllocQty = 0
            #     final_tbfQty = 0
            #     isUpdateOnHand = True
            # else:
            #     isUpdateOnHand = False

            '''Approach 2'''
            # if True:
            #     final_wmAllocQty = currWmAllocQty  # get from DB
            #     final_tbfQty = 0
            #     final_onHandQty = f_availUnit + final_wmAllocQty
            #     isUpdateOnHand = True

            '''Approach 3'''
            if True:
                final_wmAllocQty = 0
                final_tbfQty = 0
                if f_onHand is not None:
                    final_onHandQty = f_onHand
                elif f_availUnit is not None:
                    final_onHandQty = f_availUnit
                elif f_availCap is not None:
                    final_onHandQty = max(0, currMaxInvnQty - f_availCap)
                else:
                    final_onHandQty = currOnHand
                isUpdateOnHand = True

            assert final_onHandQty is not None, '<Data> OnHandQty is None for update, Check TC'
            if isUpdateOnHand:
                self._updateWMInvn(i_locnBrcd=i_locnBrcd, i_itemBrcd=i_itemBrcd, u_onHandQty=final_onHandQty,
                                   u_wmAllocatedQty=final_wmAllocQty, u_toBeFilledQty=final_tbfQty)

        if f_isCCPending is not None:
            self._updateLocnCCPending(i_locnBrcd=i_locnBrcd, u_isCCPending=f_isCCPending)

        # TODO Code not ready
        # currMaxInvnQty = self.getMaxInvnQty(i_locnBrcd=i_locnBrcd, i_itemBrcd=i_itemBrcd)
        # if currMaxInvnQty < f_availCap:
        #     final_maxInvn = u_maxInvn
        #
        # if isUpdateMaxInvn:
        #     self._updatePickLocnDtl(i_locnBrcd=i_locnBrcd, i_itemBrcd=i_itemBrcd, u_maxInvn=final_maxInvn)
        return final_onHandQty

    def _presetWaveTemplateRuleForOrders(self, template: str, rule: str, orders: list):
        """Insert each order in the list in a separate row for selection"""
        assert type(orders) == list and rule == 'AUTOMATION', '<Config> Wave param is not proper for setup'

        sqlRule = f"""select rh.rule_id, rh.rule_hdr_id, rsd.sel_seq_nbr from ship_wave_parm swp 
                     inner join wave_rule_parm wrp on swp.ship_wave_parm_id = wrp.wave_parm_id 
                     inner join rule_hdr rh on wrp.rule_hdr_id = rh.rule_hdr_id and rh.rec_type = 'T'
                     left outer join rule_sel_dtl rsd on rh.rule_id = rsd.rule_id 
                     where swp.rec_type = 'T' and swp.wave_desc = '{template}' and rh.rule_name = '{rule}' 
                     order by rsd.sel_seq_nbr asc"""
        dbRuleResults = DBService.fetch_rows(sqlRule, self.schema)

        noOfSelCriteria = len(dbRuleResults)
        assert noOfSelCriteria >= 1, f"<Config> Wave rule {rule} not present for template {template} " + sqlRule

        ruleId = str(dbRuleResults[0].get('RULE_ID'))
        ruleHdrId = str(dbRuleResults[0].get('RULE_HDR_ID'))

        '''Delete all sel rules'''
        sqlDelRule = f"""delete from rule_sel_dtl where rule_id = '{ruleId}' and rule_hdr_id = '{ruleHdrId}' 
                            and sel_seq_nbr = #SEL_SEQ_NBR#"""
        for i in range(0, noOfSelCriteria):
            deleteQuery = sqlDelRule
            selSeqNbr = dbRuleResults[i].get('SEL_SEQ_NBR')
            if selSeqNbr is not None and selSeqNbr != '':
                deleteQuery = deleteQuery.replace('#SEL_SEQ_NBR#', str(selSeqNbr))
                DBService.update_db(deleteQuery, self.schema)

        '''Insert required sel rules'''
        sqlInsRule = f"""insert into rule_sel_dtl (rule_id,sel_seq_nbr,open_paran,tbl_name,colm_name,oper_code,rule_cmpar_value,and_or_or,
                            close_paran,create_date_time,mod_date_time,user_id,rule_sel_dtl_id,rule_hdr_id,wm_version_id,created_dttm,last_updated_dttm)
                        values ({ruleId},#SEL_SEQ_NBR#,'#OPEN_PARAN#','ORDERS','TC_ORDER_ID','=','#RULE_CMPAR_VALUE#','#AND_OR_OR#',
                            '#CLOSE_PARAN#',SYSDATE,SYSDATE,null,RULE_SEL_DTL_ID_SEQ.NEXTVAL,{ruleHdrId},1,SYSDATE,null)"""
        noOfOrders = len(orders)
        for i in range(0, noOfOrders):
            insertQuery = sqlInsRule
            openParan = '(' if noOfOrders > 1 and i == 0 else ''
            andOrFlag = 'O' if (noOfOrders > 1 and i >= 0 and i < noOfOrders - 1) else ''
            closeParan = ')' if noOfOrders > 1 and i == noOfOrders - 1 else ''
            insertQuery = insertQuery.replace('#OPEN_PARAN#', openParan)
            insertQuery = insertQuery.replace('#SEL_SEQ_NBR#', str(i + 1))
            insertQuery = insertQuery.replace('#RULE_CMPAR_VALUE#', orders[i])
            insertQuery = insertQuery.replace('#AND_OR_OR#', andOrFlag)
            insertQuery = insertQuery.replace('#CLOSE_PARAN#', closeParan)
            DBService.update_db(insertQuery, self.schema)

        '''Might be useful later'''
        # sqlUpdRule = """UPDATE RULE_SEL_DTL SET TBL_NAME='ORDERS', COLM_NAME='TC_ORDER_ID', OPER_CODE='=', RULE_CMPAR_VALUE='#RULE_CMPAR_VALUE#'
        #             WHERE RULE_ID IN (SELECT RH.RULE_ID FROM SHIP_WAVE_PARM SWP INNER JOIN WAVE_RULE_PARM WRP ON SWP.SHIP_WAVE_PARM_ID = WRP.WAVE_PARM_ID
        #             INNER JOIN RULE_HDR RH ON WRP.RULE_HDR_ID = RH.RULE_HDR_ID AND RH.REC_TYPE = 'T'
        #             INNER JOIN RULE_SEL_DTL RSD ON RH.RULE_ID = RSD.RULE_ID
        #             WHERE RH.RULE_ID = '#RULE_ID#' AND RSD.RULE_HDR_ID = '#RULE_HDR_ID#') AND SEL_SEQ_NBR = #SEL_SEQ_NBR#"""
        # sqlUpdRule = sqlUpdRule.replace('#RULE_ID#', ruleId).replace('#RULE_HDR_ID#', ruleHdrId)

    def _assertLeanTimeReplenRuleExist(self, ruleDesc:str='AUTOMATION'):
        sql = f"""select * from lean_time_repl_parm where parm_desc = '{ruleDesc}'"""
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows and len(dbRows) >= 1, f'<Config> LTR rule {ruleDesc} not found ' + sql

    def _assertReasonCodeForInvnAdjustExist(self, rsnCode:str):
        sql = f"""select code_id, code_desc from sys_code where rec_type='B' and code_type='051' and code_id='{rsnCode}' """
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows and len(dbRows) >= 1, f'<Config> Reason code {rsnCode} for invn adjust not found ' + sql

    def _decide_mheEventType_itemStatus_forPack(self, locnType: LocnType = None):
        _EVENT_TYPE = ENV_CONFIG.get('mhe', 'item_stat_event_type')

        final_eventType = _EVENT_TYPE

        # if self._ENV_TYPE in ['EXP']:
        #     final_eventType = MheEventType.FILE_ITEMSTATUS
        # else:
        #     final_eventType = MheEventType.VLM_ITEMSTATUS

        return final_eventType

    def _getMheEventId(self, mheEventType: MheEventType):
        eventDesc = mheEventType.value

        sql = f"""select event_id from cl_event_master where event_desc= '{eventDesc}'"""

        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None, f'<Data> Mhe event id not found for {eventDesc} ' + sql
        eventId = str(dbRow.get('EVENT_ID'))

        return eventId

    def _getEndPointId(self, mheEventType: MheEventType):
        name = mheEventType.value

        sql = f"""select endpoint_id from cl_endpoint where name= '{name}'"""

        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None, f'<Data> Mhe endpoint id not found for {name} ' + sql
        endPntId = str(dbRow.get('ENDPOINT_ID'))

        return endPntId

    def _insertCntrStatMsgFromAutoLocnVLM(self, ilpn: str, vlmLocn: str) -> str:
        """Insert VLM containerstatus record in cl message & endpoint queue
        """
        # cntrStatEventId = ENV_CONST.get('mhe_event_id', 'container_status')
        cntrStatEventId = self._getMheEventId(MheEventType.CONTAINERSTATUS)
        endPointId = self._getEndPointId(MheEventType.CONTAINERSTATUS)

        msg = MHEUtil.buildContainerSatusMsgFromVLM(ilpn, vlmLocn)
        msgId = DBAdmin._insertRecordInCLMessage(self.schema, msg=msg, eventId=cntrStatEventId)
        msgId = DBAdmin._insertRecordInCLEndpointQueue(self.schema, msgId, endPointId)

        return msgId

    def putwyReplenToAutoLocnVLM(self, ilpn: str, vlmLocn: str, o_items: list[str], o_qtys: list[int]) -> str:
        """"""
        assert len(o_items) == len(o_qtys), "<Data> No. of items and qty didnt match"

        msgId = self._insertCntrStatMsgFromAutoLocnVLM(ilpn, vlmLocn)

        '''Validation'''
        self.assertCLEndPointQueueStatus(msgId=msgId, o_status=5)
        self.assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV)
        for i in range(0, len(o_items)):
            self.assertLPNDtls(i_lpn=ilpn, i_itemBrcd=o_items[i], o_receivedQty=o_qtys[i])

            final_statName = self._decide_clm_statusName_forMheMsg()
            self._assertPutwyReplenMsgToAutoLocnVLM(ilpn=ilpn, o_locn=vlmLocn, o_item=o_items[i], o_qty=o_qtys[i], o_status=final_statName)

        return msgId

    def _assertPutwyReplenMsgToAutoLocnVLM(self, ilpn: str, o_item: str, o_qty: int, o_locn: str = None, o_status: str = None):
        self._assertPutwyReplenMsgToAutoLocn(ilpn=ilpn, o_item=o_item, o_qty=o_qty, o_locn=o_locn, o_status=o_status)

    def _assertPutwyReplenMsgToAutoLocn(self, ilpn: str, o_item: str, o_qty: int, o_locn:str=None, o_status: str = None):
        """"""
        locnId = self.getLocnIdByLocnBrcd(locnBrcd=o_locn) if o_locn is not None else None
        expMsg = MHEUtil.buildPutawayReplenMsgToAutoLocnForAssert(ilpn=ilpn, item=o_item, qty=o_qty, locn=o_locn, locnId=locnId)
        self.logger.info('Msg to verify for putaway/replen to auto locn ' + expMsg)

        clEndPtName = ENV_CONFIG.get('mhe', 'cl_endpoint_name_vlm_replen')  # VLM_REPLEN

        sql = f"""select clm.event_id, clm.data, clms.status_name from cl_endpoint cle 
                 inner join cl_endpoint_queue cleq on cleq.endpoint_id = cle.endpoint_id
                 inner join cl_message clm on clm.msg_id = cleq.msg_id 
                 inner join cl_message_status clms on clms.status_id = cleq.status
                 where cle.name = '{clEndPtName}'
                 and clm.data like '%{ilpn}%'"""

        DBService().wait_for_records(query=sql, expected_cnt=1, schema=self.schema, maxWaitInSec=120)

        dbRow = DBService().fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('EVENT_ID') is not None, '<CLMsg> CL msg dtl not found ' + sql

        assertlist = []
        dbVal = dbRow.get("DATA")
        isMatched = expMsg in str(dbVal)
        assertlist.append(isMatched)
        if o_status is not None:
            dbVal = dbRow.get("STATUS_NAME")
            isMatched = DBService.compareEqual(dbVal, o_status, '<CLMsg> Msg status for ' + ilpn)
            assertlist.append(isMatched)

        assert not assertlist.count(False), '<CLMsg> CL msg dtl validation failed ' + sql

    def putwyReplenToAutoLocnAS(self, pallet: str, autostoreLocn: str, o_items: list[str], o_qtys: list[int]) -> str:
        """Executes putaway to destined autostore locn"""
        assert len(o_items) == len(o_qtys), "<Data> No. of items and qty didnt match"

        msgId = self._insertCntrStatMsgFromAutoLocnAS(pallet, autostoreLocn)

        '''Validation'''
        self.assertCLEndPointQueueStatus(msgId=msgId, o_status=5)
        self.assertLPNHdr(i_lpn=pallet, o_facStatus=LPNFacStat.ILPN_CONSUMED_TO_ACTV)
        for i in range(0, len(o_items)):
            self.assertLPNDtls(i_lpn=pallet, i_itemBrcd=o_items[i], o_receivedQty=o_qtys[i])

            final_statName = self._decide_clm_statusName_forMheMsg()
            self._assertReplenMsgToAutoLocnAS(ilpn=pallet, o_locn=autostoreLocn, o_item=o_items[i], o_qty=o_qtys[i], o_status=final_statName)

        return msgId

    def _insertCntrStatMsgFromAutoLocnAS(self, pallet: str, autoStoreLocn: str) -> str:
        """Insert autostore containerstatus record in cl message & endpoint queue
        """
        cntrStatEventId = self._getMheEventId(MheEventType.CONTAINERSTATUS)
        endPointId = self._getEndPointId(MheEventType.CONTAINERSTATUS)

        msg = MHEUtil.buildContainerSatusMsgFromAutoStore(pallet, autoStoreLocn)
        msgId = DBAdmin._insertRecordInCLMessage(self.schema, msg=msg, eventId=cntrStatEventId)
        msgId = DBAdmin._insertRecordInCLEndpointQueue(self.schema, msgId, endPointId)

        return msgId

    def _assertReplenMsgToAutoLocnAS(self, ilpn:str, o_item:str, o_qty:int, o_locn:str=None, o_status:str=None):
        self._assertPutwyReplenMsgToAutoLocn(ilpn=ilpn, o_item=o_item, o_qty=o_qty, o_locn=o_locn, o_status=o_status)

    def putawayToAutoLocnASRS(self, ilpn: str, asrsLocn: str, o_items: list[str], o_qtys: list[int]) -> str:
        """"""
        assert len(o_items) == len(o_qtys), "<Data> No. of items and qty didnt match"

        msgId = self._insertCntrStatMsgFromAutoLocnASRS(ilpn, asrsLocn)

        '''Validation'''
        self.assertCLEndPointQueueStatus(msgId=msgId, o_status=5)
        self.assertLPNHdr(i_lpn=ilpn, o_facStatus=LPNFacStat.ILPN_PUTAWAY)
        for i in range(0, len(o_items)):
            self.assertLPNDtls(i_lpn=ilpn, i_itemBrcd=o_items[i], o_receivedQty=o_qtys[i])

            final_statName = self._decide_clm_statusName_forMheMsg()
            self._assertPutawyMsgToAutoLocnASRS(ilpn=ilpn, o_locn=asrsLocn, o_item=o_items[i], o_qty=o_qtys[i], o_status=final_statName)

        return msgId

    def _insertCntrStatMsgFromAutoLocnASRS(self, pallet: str, asrsLocn: str) -> str:
        """Insert asrs containerstatus record in cl message & endpoint queue
        """
        cntrStatEventId = self._getMheEventId(MheEventType.CONTAINERSTATUS)
        endPointId = self._getEndPointId(MheEventType.CONTAINERSTATUS)

        msg = MHEUtil.buildContainerSatusMsgFromASRS(pallet, asrsLocn)
        msgId = DBAdmin._insertRecordInCLMessage(self.schema, msg=msg, eventId=cntrStatEventId)
        msgId = DBAdmin._insertRecordInCLEndpointQueue(self.schema, msgId, endPointId)

        return msgId

    def _assertPutawyMsgToAutoLocnASRS(self, ilpn:str, o_item:str, o_qty:int, o_locn:str=None, o_status:str=None):
        self._assertPutwyReplenMsgToAutoLocn(ilpn=ilpn, o_item=o_item, o_qty=o_qty, o_locn=o_locn, o_status=o_status)

    def _buildQueryForTaskPathDef2(self, taskPath: TaskPath = None) -> str:
        """Returns only the task path query built from TaskPath obj
        """
        sql = f"""select distinct curr_work_grp, curr_work_area, dest_work_grp, dest_work_area, prty, nxt_work_grp, nxt_work_area 
                 from int_path_defn where 0=0
                 #CONDITION#
              """
        sqlCond = ''
        if taskPath.INT_TYPE is not None:
            sqlCond += " \n and invn_need_type = '" + str(taskPath.INT_TYPE) + "'"
        if taskPath.CURR_WG is not None:
            sqlCond += " \n and curr_work_grp in " + taskPath.CURR_WG
        if taskPath.CURR_WA is not None:
            sqlCond += " \n and curr_work_area in " + taskPath.CURR_WA
        if taskPath.DEST_WG is not None:
            sqlCond += " \n and dest_work_grp in " + taskPath.DEST_WG
        if taskPath.DEST_WA is not None:
            sqlCond += " \n and dest_work_area in " + taskPath.DEST_WA
        if taskPath.NEXT_WG is not None:
            sqlCond += " \n and nxt_work_grp in " + taskPath.NEXT_WG
        if taskPath.NEXT_WA is not None:
            sqlCond += " \n and nxt_work_area in " + taskPath.NEXT_WA
        if taskPath.IGNORE_CURR_WA is not None:
            sqlCond += " \n and curr_work_area not in " + taskPath.IGNORE_CURR_WA
        if taskPath.IGNORE_DEST_WA is not None:
            sqlCond += " \n and dest_work_area not in " + taskPath.IGNORE_DEST_WA

        sql = sql.replace('#CONDITION#', sqlCond)

        return sql

    def getEstWeightFromOLPN(self, oLpn: str):
        sql = f"""select estimated_weight,WAVE_NBR,tc_order_id,TC_LPN_ID,lpn_facility_status,tracking_nbr from lpn 
                 where TC_LPN_ID='#OLPNNUM#'"""
        sql = sql.replace('#OLPNNUM#', str(oLpn))

        dbRow = DBService.fetch_row(sql, self.schema)
        estWeight = dbRow.get('ESTIMATED_WEIGHT')

        return estWeight

    def getTotalEstWeightForPallet(self, palletId: str):
        sql = f"""select (sum(estimated_weight)) TOTAL_EST_WT from lpn where tc_parent_lpn_id='#PALLET#'"""
        sql = sql.replace('#PALLET#', str(palletId))

        dbRow = DBService.fetch_row(sql, self.schema)
        estWeight = dbRow.get('TOTAL_EST_WT')

        return estWeight

    def _closeAllOpenManifests(self, ignoreManifestIds: list[str] = None):
        """close all the open manifests
        """
        if 'true' in self.IS_ALLOW_CLOSE_MANIFEST:
            '''Get all open manifest IDs'''
            sql = f"""select tc_manifest_id from manifest_hdr where manifest_status_id = '10'"""
            dbRows = DBService.fetch_rows(sql, self.schema)
            openManifestIds = [i['TC_MANIFEST_ID'] for i in dbRows]

            if openManifestIds is not None:
                final_listToClose = openManifestIds
                if ignoreManifestIds is not None:
                    final_listToClose = list(set(openManifestIds).difference(ignoreManifestIds))

                self.logger.info(f"Closing open manifests {openManifestIds} ignoring {ignoreManifestIds}")

                '''Close above manifests IDs'''
                sql = f"""update manifest_hdr set manifest_status_id='90', last_updated_source='AUTOMATION' 
                         where manifest_id in #OPENMANIFESTIDS#"""
                sql = sql.replace('#OPENMANIFESTIDS#', Commons.get_tuplestr(final_listToClose))
                DBService.update_db(sql, self.schema)
        else:
            assert False, f"<Data> Close manifest not allowed. Test manually"

    def getManifestIdFromWaveNum(self, waveNum: str, order: str, oLpn: str = None):
        """"""
        assert waveNum is not None and order is not None and oLpn is not None, '<Data> waveNum/order/oLPN is missing'

        # pc_order = self._getParentDOsIfExistElseChildDOs([order])[0]
        # order = pc_order

        sql = f"""select manifest_status_id,tc_manifest_id,wave_nbr,tc_order_id,tc_lpn_id,lpn.tracking_nbr 
                    from manifest_hdr mh,lpn where lpn.manifest_nbr=mh.tc_manifest_id 
                    and wave_nbr='{waveNum}' and tc_order_id='{order}' and TC_LPN_ID='#OLPN#'"""
        sql = sql.replace('#OLPN#', oLpn)

        dbRow = DBService.fetch_row(sql, self.schema)
        manifestId = dbRow.get('TC_MANIFEST_ID')
        self.logger.info(f"Manifest nbr {manifestId}")

        return manifestId

    def assertWaitManifestStatus(self, i_manifestId: str, o_status: int):
        sql = f"""select manifest_status_id from manifest_hdr where tc_manifest_id = '#MANIFEST_NUM#'"""
        sql = sql.replace('#MANIFEST_NUM#', str(i_manifestId))

        DBService.wait_for_value(sql, 'MANIFEST_STATUS_ID', str(o_status), self.schema, maxWaitInSec=20)

    def assertManifestStatus(self, i_wave: str, i_order: str, o_manifestStatus: int):
        sql = f"""select manifest_status_id,tc_manifest_id from manifest_hdr mh,lpn 
                 where lpn.manifest_nbr=mh.tc_manifest_id and wave_nbr='#WAVENBR#' and tc_order_id='#ORDER#'"""
        sql = sql.replace('#WAVENBR#', i_wave).replace('#ORDER#', i_order)
        # DBService.wait_for_value(sql, 'MANIFEST_STATUS_ID', str(o_manifestStatus), self.schema, maxWaitInSec=20)

        dbRow = DBService.fetch_row(sql, self.schema)
        actManifestStat = dbRow.get('MANIFEST_STATUS_ID')
        isMatched = DBService.compareEqual(actManifestStat, o_manifestStatus, '<ManifestHdr> Manifest status for ' + i_wave)

        assert isMatched, f"<Manifest> Manifest status {o_manifestStatus} didnt match for wave {i_wave} " + sql

    def assertManifestEDIFile(self, i_manifestId: str):
        sql = f"""select edi_file_name from manifest_hdr where tc_manifest_id = '#MANIFEST_NUM#'"""
        sql = sql.replace('#MANIFEST_NUM#', str(i_manifestId))

        dbRow = DBService.fetch_row(sql, self.schema)
        ediFilename = dbRow.get('EDI_FILE_NAME')

        assert ediFilename.startswith(i_manifestId), "<Manifest> Manifest EDI file validation failed"
        self.logger.info('<Manifest> EDI file found for manifest id ' + str(i_manifestId))

    def assertNoInvLockForLpn(self, i_lpn: str, i_lockCode: str):
        sql = f"""select tc_order_id,inventory_lock_code,lpn.tc_lpn_id from lpn_lock ll,lpn where lpn.tc_lpn_id=ll.tc_lpn_id
                 and lpn.tc_lpn_id='#LPN#' and inventory_lock_code='#LOCK_CODE#'"""
        sql = sql.replace('#LPN#', i_lpn)
        sql = sql.replace('#LOCK_CODE#', i_lockCode)

        dbRow = DBService.fetch_row(sql, self.schema)
        noLockPresent = True if dbRow is None or len(dbRow) == 0 else False

        assert noLockPresent, i_lockCode + ' invn lock found for LPN ' + i_lpn
        self.logger.info(f"<LPNLock> No lpn lock {i_lockCode} present for {i_lpn}")

    def assertLpnLockPresent(self, i_lpn: str, i_lockCode: str):
        sql = f"""select l.lpn_facility_status,ll.inventory_lock_code, l.tc_lpn_id from lpn l inner join lpn_lock ll
                 on l.lpn_id=ll.lpn_id where l.tc_lpn_id = '{i_lpn}' and ll.inventory_lock_code = '{i_lockCode}'
                """
        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('TC_LPN_ID') is not None,  f'<LPNLock> invn lock {i_lockCode} not found for LPN ' + i_lpn
        self.logger.info(f"<LPNLock> Lpn lock {i_lockCode} present for {i_lpn}")

    def updateDOLineLpnType(self, dO, lineItem, u_lpnType):
        sql = f"""update order_line_item set lpn_type='{u_lpnType}' 
                 where order_id in (select order_id from orders where tc_order_id in ('{dO}'))
                 and item_id in (select item_id from item_cbo where item_name in ('{lineItem}'))"""
        DBService.update_db(sql, self.schema)

    def getAllOLPNsFrom1DO(self, order: str, item: str = None, isOrderByWaveSeqNbr: bool = None):
        """Pass parent order if available, else pass regular order
        """
        sql = f""" select l.tc_lpn_id from lpn l inner join lpn_detail ld on l.lpn_id=ld.lpn_id 
                inner join order_line_item oli on #DOCONDITION#
                inner join orders ord on ord.order_id=oli.order_id
                inner join item_cbo ic on ic.item_id = ld.item_id
                where 0=0  #CONDITION# """
        sqlCond = ''

        isParentDOExistForOrder = self._isParentDOExist(order=order)
        if isParentDOExistForOrder:
            sqlDOCond = " ld.tc_order_line_id = oli.reference_line_item_id "
        else:
            sqlDOCond = " ld.distribution_order_dtl_id = oli.line_item_id "

        if order is not None:
            sqlCond += f" \n and ord.tc_order_id = '{order}' "
        if item:
            sqlCond += f" \n and ic.item_id in ('{item}'))"
        if isOrderByWaveSeqNbr:
            sqlCond += " \n order by l.wave_seq_nbr asc"
        else:
            sqlCond += " \n order by l.lpn_id asc"

        sql = sql.replace('#DOCONDITION#', sqlDOCond)
        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        olpns = [sub['TC_LPN_ID'] for sub in dbRows]
        return olpns

    def getAllOLPNsFromWave(self, wave: str):
        """"""
        sql = f"""select distinct tc_lpn_id from lpn where wave_nbr='#WAVE_NBR#'"""
        sql = sql.replace('#WAVE_NBR#', str(wave))

        dbRows = DBService.fetch_rows(sql, self.schema)
        olpns = [sub['TC_LPN_ID'] for sub in dbRows]

        return olpns

    def _updateOLPNCntrTypeFromSCTForDO(self, order: str, isUpdateStgInd: bool = None):
        """Update olpn cntr type to a shipping container type from system code C-SCT
        """
        isOlpnHasNonSCTCntrType = False

        # pc_order = self._getParentDOsIfExistElseChildDOs([order])[0]
        # order = pc_order

        sql = f"""select * from lpn where tc_order_id='{order}'
                    and container_type not in (select code_id from sys_code where rec_type='C' and code_type='SCT')"""
        dbRows = DBService.fetch_rows(sql, self.schema)
        if dbRows is not None and len(dbRows) > 0 and dbRows[0].get('CONTAINER_TYPE') is not None:
            isOlpnHasNonSCTCntrType = True

        if isOlpnHasNonSCTCntrType:
            updQ = """update lpn set container_type='PLT' where tc_order_id='#ORDER#'"""
            updQ = updQ.replace('#ORDER#', order)
            DBService.update_db(updQ, self.schema)

        if isUpdateStgInd:
            updQ2 = """update lpn set stage_indicator=0 where tc_order_id='#ORDER#'"""
            updQ2 = updQ2.replace('#ORDER#', order)
            DBService.update_db(updQ2, self.schema)

    def _updateOLPNCntrTypeForDO(self, order: str, olpn: str):
        """Required during weigh & manifest"""
        updQ = f"""update lpn set container_type='BOX', container_size='LRG', length='21', width='15', height='12' 
                    where tc_order_id='{order}' and tc_lpn_id='{olpn}' """
        DBService.update_db(updQ, self.schema)

    def getManualActvLocnForCC(self, noOfLocn: int, minOnHand: int):
        """Get actv locn with cc pending = 'N' having only 1 item
        """
        RuntimeXL.createThreadLockFile()
        try:
            dbRows = []
            for i in range(noOfLocn):
                ignoreLocns = None if len(dbRows) == 0 else {r['LOCN_BRCD'] for r in dbRows}

                itemRows = self.getItemsForActvPick2(noOfItem=1, minOnHand=minOnHand, isCcPending=False, ignoreActvLocn=ignoreLocns)

                assert itemRows and len(itemRows) > 0 and itemRows[0]['LOCN_BRCD'] is not None, '<Data> Actv locn not found for CC'
                dbRows.append(itemRows[0])

                self._cancelTasks(itemId=itemRows[0]['ITEM_ID'])
                self._updateLocnCCPending(i_locnBrcd=itemRows[0]['LOCN_BRCD'], u_isCCPending=False)

            '''Update runtime thread data file'''
            locnAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getManualResvLocnForCC(self, noOfLpn: int = 1, lpnFacStat: int = None, minLpnQty: int = None,
                               isLocnWithNoCCTask: bool = None, isResvCCPending: bool = None):
        sql = f"""select lh.locn_brcd,ic.item_name,wi.tc_lpn_id,ld.size_value,wi.on_hand_qty 
                 from wm_inventory wi 
                 inner join locn_hdr lh on wi.location_id = lh.locn_id inner join lpn l on wi.tc_lpn_id = l.tc_lpn_id 
                 inner join lpn_detail ld on l.lpn_id = ld.lpn_id inner join item_cbo ic on ld.item_id = ic.item_id
                 --inner join task_hdr th on th.task_genrtn_ref_nbr=lh.locn_id 
                 where wi.location_id in (select location_id from wm_inventory where tc_lpn_id is not null 
                    group by location_id having count(location_id) = {noOfLpn})
                 and lh.cycle_cnt_pending = 'N' 
                 and wi.tc_lpn_id in (select tc_lpn_id from lpn where lpn_id in (select lpn_id from lpn_detail 
                    group by lpn_id having count(lpn_id) = '1')) 
                 and wi.tc_lpn_id not in (select tc_lpn_id from lpn_lock where tc_lpn_id is not null) 
                 #CONDITION# 
                 order by lh.locn_brcd
              """
        # sql = sql.replace('#NUMOFLPNS#', str(noOfLpn)).replace('#FACSTAT#',str(facStat))
        sqlCond = ''
        if lpnFacStat is not None:
            sqlCond += " \n and l.lpn_facility_status = '" + str(lpnFacStat) + "' "
        if minLpnQty is not None:
            sqlCond += f" \n and wi.on_hand_qty >= {minLpnQty}"

        final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType()
        if final_locnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
        elif final_avoidLocnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        if isLocnWithNoCCTask:
            sqlCond += """ \n and lh.locn_id not in (select task_genrtn_ref_nbr from task_hdr where INVN_NEED_TYPE in ('101','100') and stat_code in('10'))"""
            # sqlCond += " order by lh.locn_brcd"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', str(sqlCond))

        dbRows = DBService.fetch_rows(sql, self.schema)
        assert dbRows is not None and len(dbRows) >= noOfLpn and dbRows[0]['LOCN_BRCD'] is not None, f"<Data> Resv locn for CC not found " + sql

        if isResvCCPending is not None:
            for i in range(noOfLpn):
                temp_resvLocn = dbRows[i]['LOCN_BRCD']
                self._updateLocnCCPending(i_locnBrcd=temp_resvLocn, u_isCCPending=isResvCCPending)
        return dbRows

    def getInductLocn(self, zone: str):
        """Get 1 induct locn based on zone
        """
        sql = f"""select locn_brcd,locn_id from locn_hdr where zone='{zone}' and locn_class='S'
                    #CONDITION#"""
        sqlCond = ''

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and locn_brcd not in " + threadLocns

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        # inductLocn = [sub['LOCN_BRCD'] for sub in dbRows]
        inductLocn = dbRow.get('LOCN_BRCD')
        inductLocnId = dbRow.get('LOCN_ID')
        printit(inductLocn, inductLocnId)

        return inductLocn, inductLocnId

    def get1ShipmentNumFromDOs(self, orders: list[str]) -> str:
        """Get 1 shipment nbr from list of orders
        """
        # pc_orders = self._getParentDOsIfExistElseChildDOs(orders)
        # orders = pc_orders

        sql = f"""select distinct tc_shipment_id,tc_order_id from orders where tc_order_id in #ORDERS#"""
        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(orders))

        dbRow = DBService.fetch_row(sql, self.schema)
        shipmentNum = dbRow.get('TC_SHIPMENT_ID')

        return shipmentNum

    def assertShipConfirmXmlMsgExist(self, order: str):
        sql = f"""select * from tran_log_message tlm inner join tran_log tl on
                 tlm.tran_log_id=tl.tran_log_id where tl.msg_type='ShipmentConfirm' and tl.created_dttm >sysdate-2
                 and tlm.msg_line_text like '%TcOrderId>{order}</TcOrderId%' """
        # DBService.wait_for_value(sql, 'RESULT_CODE', '25', self.schema, maxWaitInSec=60)
        DBService.wait_for_records(sql, 1, self.schema, maxWaitInSec=60)

        dbRow = DBService.fetch_row(sql, self.schema)
        noShipmentXML = False if dbRow is None or len(dbRow) == 0 else True

        assert noShipmentXML, '<Shipment> No shipment XML is generated for order ' + order

    def getNextValOfSeq(self, seqName: str):
        seqOwner = ENV_CONFIG.get('wm_user').upper()
        # sql = f"""select last_number from all_sequences where sequence_name= '#SEQNAME#' and SEQUENCE_OWNER = '#SEQOWNER#'"""
        sql = "select '#SEQNAME#'.currval from dual"
        sql = sql.replace('#SEQNAME#', seqName)
        # sql = sql.replace('#SEQOWNER#', seqOwner)

        dbRow = DBService.fetch_row(sql, self.schema)
        maxSeqVal = dbRow.get('LAST_NUMBER')
        nextSeqVal = (int(maxSeqVal) + 1)

        return str(nextSeqVal)

    def getNextMsgIdSeqFromCLMessage(self):
        sql = "select max(msg_id) MAX_MSG_ID from cl_message"

        dbRow = DBService.fetch_row(sql, self.schema)
        maxSeqVal = dbRow.get('MAX_MSG_ID')
        nextSeqVal = (int(maxSeqVal) + 1)

        return str(nextSeqVal)

    def getOrderIdfromDO(self, order: str):
        sql = f"""select ORDER_ID from orders where tc_order_id='#ORDER#'"""
        sql = sql.replace('#ORDER#', order)

        dbRow = DBService.fetch_row(sql, self.schema)
        orderId = dbRow.get('ORDER_ID')

        return str(orderId)

    def getOrderTypeCountFromDO(self, orders: list[str]):
        sql = f"""select ORDER_TYPE, count(*) REQ_CONSOL_COUNT from orders where tc_order_id in #ORDERS# group by order_type"""
        sql = sql.replace('#ORDERS#', Commons.get_tuplestr(orders))

        dbRows = DBService.fetch_rows(sql, self.schema)

        return dbRows

    def getAllocInvnDtlIdfromWave(self, waveNum: str, item: str):
        sql = f"""select tc_order_id,alloc_invn_dtl_id,ic.item_id,ic.item_name,lh.dsp_locn,aid.invn_need_type,
                 aid.stat_code,aid.tc_order_id,aid.task_cmpl_ref_nbr,
                 aid.orig_reqmt,aid.qty_alloc,aid.qty_pulld from alloc_invn_dtl aid 
                 inner join locn_hdr lh on aid.pull_locn_id=lh.locn_id 
                 inner join item_cbo ic on aid.item_id=ic.item_id 
                 where aid.task_genrtn_ref_nbr='#WAVENUM#' and ic.item_name = '#ITEM_NAME#'"""
        sql = sql.replace('#WAVENUM#', waveNum).replace('#ITEM_NAME#', item)

        dbRow = DBService.fetch_row(sql, self.schema)
        allocInvDtlId = dbRow.get('ALLOC_INVN_DTL_ID')

        return allocInvDtlId

    def assertCLEndPointQueueStatus(self, msgId: str, o_status: int):
        sql = f"""select status from cl_endpoint_queue where msg_id='#MSGID#'"""
        sql = sql.replace('#MSGID#', str(msgId))

        DBService.wait_for_value(sql, 'STATUS', str(o_status), self.schema, maxWaitInSec=20)

        dbRow = DBService.fetch_row(sql, self.schema)
        dbVal = dbRow.get('STATUS')
        isStatusMatched = DBService.compareEqual(int(dbVal), o_status, '<CLEndPtQ> Endpoint status for msg_id ' + str(msgId))

        assert isStatusMatched, '<CLEndPtQ> Endpoint queue status validation failed ' + sql

    def _insertItemStatMsgFromAutoLocnVLM(self, waveNum: str, order: str, allocInvnDtlId: str, vlmLocn: str, oLpn: str,
                                          qty: str) -> str:
        """Insert VLM itemstatus record in cl message & endpoint queue
        """
        mheEventType = self._decide_mheEventType_itemStatus_forPack()
        # itemStatEventId = ENV_CONST.get('mhe_event_id', 'item_status')
        itemStatEventId = self._getMheEventId(mheEventType)
        endPointId = self._getEndPointId(mheEventType)

        nextMsgId = self.getNextMsgIdSeqFromCLMessage()
        msg = MHEUtil.buildItemSatusMsgFromVLM(waveNum, order, allocInvnDtlId, vlmLocn, oLpn, qty, nextMsgId)
        msgId = DBAdmin._insertRecordInCLMessage(self.schema, msg=msg, eventId=itemStatEventId, msgId=nextMsgId)
        msgId = DBAdmin._insertRecordInCLEndpointQueue(self.schema, msgId=msgId, endpointId=endPointId)

        return msgId

    def _insertInvnAdjMsgFromAutoLocnVLM(self, item: str, adjQty: int, adjOperator: str, vlmLocn: str, locnId: str):
        """Insert VLM inventory adjustment record in cl message & endpoint queue
        """
        # invnAdjustEventId = ENV_CONST.get('mhe_event_id', 'invn_adjust')
        invnAdjustEventId = self._getMheEventId(MheEventType.INVENTORYADJ)
        endPointId = self._getEndPointId(MheEventType.INVENTORYADJ)  # 285

        msg = MHEUtil.buildInventoryAdjMsgFromVLM(item, adjQty, adjOperator, vlmLocn, locnId)
        msgId = DBAdmin._insertRecordInCLMessage(self.schema, msg, eventId=invnAdjustEventId)
        msgId = DBAdmin._insertRecordInCLEndpointQueue(self.schema, msgId, endPointId)

        return msgId

    def pickPackFromAutoLocnVLM(self, waveNum: str, order: str, oLpn: str, item: str, qty: str, vlmLocn: str,
                                o_doStatus: DOStat = None):
        """Pack 1 olpn with 1 sku from 1 order
        """
        # pc_order = self._getParentDOsIfExistElseChildDOs([order])[0]
        # order = pc_order

        allocInvnDtlId = self.getAllocInvnDtlIdfromWave(waveNum=waveNum, item=item)
        msgId = self._insertItemStatMsgFromAutoLocnVLM(waveNum=waveNum, order=order, allocInvnDtlId=allocInvnDtlId,
                                                       vlmLocn=vlmLocn, oLpn=oLpn, qty=qty)

        '''Validation'''
        self.assertCLEndPointQueueStatus(msgId=msgId, o_status=5)
        self.assertLPNHdr(i_lpn=oLpn, o_facStatus=LPNFacStat.OLPN_PACKED)
        # final_DOstatus = 165 if isLastOLpnAvail else 150
        self.assertDOHdr(i_order=order, o_status=o_doStatus)

    def getCCTask(self, i_locnBrcd: str, i_intType: int, i_stat_code:int=10, i_currTaskPrty: int = None):
        sql = f"""select th.task_id,th.stat_code,lh.locn_brcd 
                from task_hdr th 
                inner join locn_hdr lh on th.task_genrtn_ref_nbr=lh.locn_id  
                where th.invn_need_type='#INTTYPE#' and lh.locn_brcd='#LOCN_BRCD#' and th.stat_code={i_stat_code}
                #CONDITION#
                and th.create_date_time > sysdate-1 order by th.task_id desc
                offset 0 rows fetch next 1 rows only
              """
        sql = sql.replace('#LOCN_BRCD#', i_locnBrcd).replace('#INTTYPE#', str(i_intType))

        sqlCond = ''
        if i_currTaskPrty is not None:
            sqlCond += f" \n and th.curr_task_prty = '{i_currTaskPrty}'"

        sql = sql.replace('#CONDITION#', str(sqlCond))

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and dbRow['TASK_ID'] is not None, f"<Data> CC task not found for locn {i_locnBrcd} " + sql

        return dbRow

    def assertCCTask(self, i_locnBrcd: str, i_intType: int, o_status:int):
        dbRow = self.getCCTask(i_locnBrcd=i_locnBrcd, i_intType=i_intType)
        taskId = dbRow.get('TASK_ID')
        statCode = dbRow.get('STAT_CODE')

        assert int(statCode) == o_status, "<CC> Validating status code is generated with 10"

    def assertCCVariance(self, i_locnBrcd: str, i_iLpn: str = None, isILpnAdded: bool = None,
                         isILpnOmitted: bool = None, o_qty: int = None):
        sql = f""" select ivh.* from invn_vari_hdr ivh 
                """
        if isILpnAdded:
            sql += f" \n where ivh.cnt_locn_id in (select locn_id from locn_hdr where locn_brcd = '{i_locnBrcd}') "
        if isILpnOmitted:
            sql += f" \n where ivh.frozn_locn_id in (select locn_id from locn_hdr where locn_brcd = '{i_locnBrcd}') "
        if i_iLpn is not None:
            sql += f" \n and ivh.case_nbr = '{i_iLpn}'"
        sql += " \n and ivh.create_date_time > sysdate -  interval '10' minute  "

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is not None and len(dbRow) > 0 and dbRow.get('CASE_NBR') is not None, '<CCVar> Cycle count variance record not found ' + sql

        assertlist = []
        if isILpnAdded:
            if o_qty is not None:
                dbVal = int(dbRow.get("CNT_INVN_QTY"))
                isMatched = DBService.compareEqual(dbVal, int(o_qty), '<CCVar> cnt invn qty for ' + i_iLpn)
                assertlist.append(isMatched)
        if isILpnOmitted:
            if o_qty is not None:
                dbVal = int(dbRow.get("FROZN_INVN_QTY"))
                isMatched = DBService.compareEqual(dbVal, int(o_qty), '<CCVar> frozen invn qty for ' + i_iLpn)
                assertlist.append(isMatched)

        assert not assertlist.count(False), f"<CCVar> {i_locnBrcd} cycle cnt variance validation failed " + sql

    def getManualResvLocnForAutoLock(self, lockCode: str):
        sql = f"""select distinct lh.locn_brcd,rlh.invn_lock_code from locn_hdr lh 
                    inner join resv_locn_hdr rlh on lh.locn_id=rlh.locn_id
                    left outer join wm_inventory wm on rlh.locn_id=wm.location_id
                    left outer join item_cbo ic on wm.item_id=ic.item_id 
                    where rlh.invn_lock_code ='{lockCode}'
                    #CONDITION#
                    offset 0 rows fetch next 1 rows only"""
        # noOfitemsBool = True if noOfItems is None else False
        # assert noOfitemsBool, "noOfItems is None"
        # sql = sql.replace('#noOfItems#', str(noOfItems))

        sqlCond = ''

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='R' 
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        locn = dbRow.get('LOCN_BRCD')

        return locn

    def assertNoPixPresent(self, i_itemBrcd: str):
        sql = f"""select pt.* from pix_tran pt where pt.item_name = '#ITEM_NAME#' 
                 and mod_date_time >= sysdate - interval '10' minute"""
        sql = sql.replace('#ITEM_NAME#', i_itemBrcd)

        dbRow = DBService.fetch_row(sql, self.schema)
        assert dbRow is None or len(dbRow) == 0 or dbRow.get('ITEM_NAME') is None, f'<Pix> Item {i_itemBrcd} with pix found ' + sql
        self.logger.info('<Pix> No pix found for item ' + str(i_itemBrcd))

    def getEmptyConsolLocn2(self, noOfLocns: int, consolAttr: str = None, isClearIfNotFound: bool = False):
        """Get/Clear empty consol locn
        """
        printit(f"Need {noOfLocns} no. of consol locns for attr {consolAttr}")

        consLocns, consLocnIds = [], []

        if consolAttr is not None:
            sql = sql2 = None
            if consolAttr == 'BIG':
                sql = OrdConsRuleData.GetSql.getEligLocnForBIG
            elif consolAttr == 'NXP':
                sql = OrdConsRuleData.GetSql.getEligLocnForNXP
            elif consolAttr == 'PLT':
                # sql = OrdConsRuleData.GetSql.getEligLocnForPLT
                printit(f"Common consol locn available attr {consolAttr}")
                pass
            elif consolAttr == 'SLP':
                sql = OrdConsRuleData.GetSql.getEligLocnForSLP
            elif consolAttr == 'WP':
                sql = OrdConsRuleData.GetSql.getEligLocnForWP
            elif consolAttr == 'PCK':
                sql = OrdConsRuleData.GetSql.getEligLocnForPCK
                sql2 = OrdConsRuleData.GetSql.getEligLocnForPCK2
            elif consolAttr == 'MarkFor':
                sql = OrdConsRuleData.GetSql.getEligLocnForMarkFor
                noOfLocns = 1
            else:
                assert False, '<Data> No valid order consol attr provided'

            if sql is not None:
                sqlTemp = sql.replace('#P_OR_W#', 'P')  # Get only P locns
                dbRows = DBService.fetch_only_rows(sqlTemp, noOfLocns, self.schema)
                # assert dbRows is not None and len(dbRows) == noOfLocns and dbRows[0]['DSP_LOCN'] is not None, \
                #     str(noOfLocns) + ' no. of consol locns not found'

                consLocnIds, consLocns, consLocns2 = [], [], []

                consLocnIds.extend(r.get('LOCN_ID') for r in dbRows)
                consLocns.extend(r.get('DSP_LOCN') for r in dbRows)
                consLocns2.extend(r.get('LOCN_BRCD') for r in dbRows)

                if sql2 is not None:
                    sqlTemp = sql2.replace('#P_OR_W#', 'P')  # Get only P locns
                    dbRows = DBService.fetch_only_rows(sqlTemp, noOfLocns, self.schema)
                    # assert dbRows is not None and len(dbRows) == noOfLocns and dbRows[0]['DSP_LOCN'] is not None, \
                    #     str(noOfLocns) + ' no. of consol locns not found'

                    # consLocnIds = []
                    consLocnIds.extend(r.get('LOCN_ID') for r in dbRows)
                    consLocns.extend(r.get('DSP_LOCN') for r in dbRows)
                    consLocns2.extend(r.get('LOCN_BRCD') for r in dbRows)

                assert len(consLocnIds) >= noOfLocns, f'<Data> {noOfLocns} no. of consol locns not found'
                printit(f"^^^ Found consol locns {consLocns} and ids {consLocnIds} for attr {consolAttr}")

                if isClearIfNotFound:
                    decide_isForceClear = True if consolAttr == 'MarkFor' else False
                    self._clearConsolLocn2(consLocnIds=consLocnIds, consLocns=consLocns2, isForceClear=decide_isForceClear)
        return consLocnIds, consLocns

    def _clearConsolLocn2(self, consLocnIds: list, consLocns:list, isForceClear: bool = None):
        """Remove W records from provided locn IDs
        """
        if 'true' in self.IS_ALLOW_CLEAR_CONS_LOCN:
            assert consLocnIds is not None, '<Data> consLocnIds not provided to clear the locn'

            final_consLocnIds, final_consLocns = [], []

            for i in range(len(consLocnIds)):
                sql = f"""select lock_pkt_consol_colm_1, lock_pkt_consol_colm_2, lock_pkt_consol_colm_3 
                            from pkt_consol_locn where rec_type in ('W') and locn_id='{consLocnIds[i]}' """

                dbRow = DBService.fetch_row(sql, self.schema)
                if dbRow is not None and (dbRow.get('LOCK_PKT_CONSOL_COLM_3') == 'Y' or isForceClear):
                    final_consLocnIds.append(consLocnIds[i])
                    final_consLocns.append(consLocns[i])

            '''Exclude runtime thread conslocns'''
            threadConsLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.CONSOL_LOCNS, replaceFrom=',', replaceWith="','")
            if threadConsLocns is not None:
                anyLocnToBeExcluded = [True if i in final_consLocns else False for i in threadConsLocns]
                assert True not in anyLocnToBeExcluded, f"<Data> To be excluded cons locn found to clear. Test manually"

            if len(final_consLocnIds) > 0:
                delQ = "delete from pkt_consol_locn where rec_type='W' and locn_id in #W_LOCN_IDS#"
                delQ = delQ.replace('#W_LOCN_IDS#', Commons.get_tuplestr(final_consLocnIds))

                DBService.update_db(delQ, self.schema)
            else:
                printit('>>> Didnt clear any order consol locns')
        else:
            assert False, f"Consol locn clear not allowed. Test manually"

    def _clearConsolLocn(self, orderType: str, consolLocn: list[str]):
        if 'true' in self.IS_ALLOW_CLEAR_CONS_LOCN:
            assert orderType is not None,  '<Data> orderType not provided to clear the locn'
            assert consolLocn is not None and len(consolLocn) > 0, '<Data> consolLocn not provided to clear the locn'

            delQ = f"""delete from pkt_consol_locn where pkt_consol_value_1='{orderType}' and rec_type='W' 
                      and locn_id in (select pcl.locn_id from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                      where pkt_consol_value_1='{orderType}' and pcl.rec_type='W'
                      #CONDITION#
                      and lh.locn_brcd in #LOCN_BRCDS#)"""
            delQ = delQ.replace('#LOCN_BRCDS#', Commons.get_tuplestr(consolLocn))

            sqlCond = ''

            '''Exclude runtime thread conslocns'''
            threadConsLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.CONSOL_LOCNS, replaceFrom=',', replaceWith="','")
            if threadConsLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadConsLocns
                printit(f"^^^ Not clearing to be excluded cons locn")

            delQ = delQ.replace('#CONDITION#', sqlCond)
            DBService.update_db(delQ, self.schema)
        else:
            assert False, f"Test consol locn clear not allowed. Test manually"

    def _printConsolLocnData(self, consolLocnList: list = None):
        """ Get current consol locn dtls
        """
        if consolLocnList is not None and len(consolLocnList) > 0 and consolLocnList[0] != '':
            sql = f"""select lh.locn_brcd,lh.locn_id,pcl.rec_type,pcl.pkt_consol_value_1,pcl.pkt_consol_value_2,pcl.pkt_consol_value_3, pcl.pkt_consol_attr,pcl.prty_seq_nbr 
                        from pkt_consol_locn pcl inner join locn_hdr lh on lh.locn_id=pcl.locn_id 
                        where lh.locn_brcd in #LOCN_BRCD#
                        order by pcl.rec_type,pcl.prty_seq_nbr
                  """
            sql = sql.replace('#LOCN_BRCD#', Commons.get_tuplestr(consolLocnList))

            dbRows = DBService.fetch_rows(sql, self.schema)
            printit(f"^^^ Curr consol locn details for {consolLocnList} {dbRows}")
        else:
            printit(f"^^^ Curr consol locn details for {consolLocnList} None")

    def assertTaskExist(self, taskRefNum: str):
        sql = f"""select * from task_hdr where task_genrtn_ref_nbr = '{taskRefNum}'"""
        dbRow = DBService.fetch_rows(sql, self.schema)

        dbRow = DBService.fetch_rows(sql, self.schema)

        assert dbRow and len(dbRow) >= 1, "<Task> Task not generated for taskRefNum " + taskRefNum

    def assertNoTaskPresent(self, taskRefNum: str):
        sql = f"""select * from task_hdr where task_genrtn_ref_nbr = '{taskRefNum}'"""
        dbRow = DBService.fetch_rows(sql, self.schema)

        assert len(dbRow) == 0, "<Task> Task generated for taskRefNum " + taskRefNum

    def assertNoTaskExist(self, i_cntrNbr: str, i_itemBrcd: str = None, i_intType: int = None, i_destLocn: str = None,
                           i_taskPrty: int = None):
        sql = f"""select ic.item_name, lhp.locn_brcd pull_locn_brcd, lhd.locn_brcd dest_locn_brcd, td.* 
                 from task_dtl td inner join task_hdr th on td.task_id = th.task_id
                 inner join item_cbo ic on td.item_id = ic.item_id
                 left outer join locn_hdr lhp on td.pull_locn_id = lhp.locn_id
                 left outer join locn_hdr lhd on td.dest_locn_id = lhd.locn_id
                 where 0=0 
                 #CONDITION#
              """
        sqlCond = ''
        if i_itemBrcd is not None:
            sqlCond += f" \n and ic.item_name = '{i_itemBrcd}'"
        if i_cntrNbr is not None:
            sqlCond += f" \n and td.cntr_nbr = '{i_cntrNbr}'"
        if i_intType is not None:
            sqlCond += f" \n and td.invn_need_type = '{i_intType}'"
        if i_destLocn is not None:
            sqlCond += f" \n and lhd.locn_brcd = '{i_destLocn}'"
        if i_taskPrty is not None:
            sqlCond += f" \n and td.task_prty = '{i_taskPrty}'"
        sqlCond += " \n and th.create_date_time >= sysdate-1 order by th.create_date_time desc"
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows is None or len(dbRows) == 0, f"<Task> Task found for item {i_itemBrcd}, sql {sql}"
        self.logger.info(f"<Task> No task found for item {i_itemBrcd}")

    def assertPickShortItemDtls(self, i_order: str, i_lpn: str, i_item: str, o_qty=None, o_statCode: int = None):
        sql = f"""select psi.short_qty,psi.stat_code, psi.short_type from lpn_detail ld 
                 inner join lpn l on ld.lpn_id=l.lpn_id
                 --inner join order_line_item oli on oli.line_item_id=ld.distribution_order_dtl_id
                 inner join order_line_item oli on #DOCONDITION#
                 --inner join picking_short_item psi on psi.line_item_id=oli.line_item_id
                 inner join picking_short_item psi on oli.wave_nbr=psi.wave_nbr
                 inner join orders ord on oli.order_id=ord.order_id 
                 inner join alloc_invn_dtl aid on l.tc_lpn_id=aid.carton_nbr
                 where ord.tc_order_id='#ORDER#' and l.tc_lpn_id='{i_lpn}'"""

        isParentDOExistForOrder = self._isParentDOExist(order=i_order)
        if isParentDOExistForOrder:
            sqlDOCond = " ld.tc_order_line_id = oli.reference_line_item_id "
        else:
            sqlDOCond = " ld.distribution_order_dtl_id = oli.line_item_id "

        sql = sql.replace('#DOCONDITION#', sqlDOCond)
        sql = sql.replace('#ORDER#', i_order)
        dbRow = DBService.fetch_row(sql, self.schema)

        assertlist = []
        if o_qty is not None:
            dbVal = dbRow.get("SHORT_QTY")
            isMatched = DBService.compareEqual(dbVal, o_qty, f"<PckShort> shortedQty for {i_item}")
            assertlist.append(isMatched)
        if o_statCode is not None:
            dbVal = dbRow.get("STAT_CODE")
            isMatched = DBService.compareEqual(dbVal, o_statCode, f"<PckShort> stat_code for {i_item}")
            assertlist.append(isMatched)

        assert not assertlist.count(False), f'<PckShort> picking_short_item validation failed for {i_item} ' + sql

    def getAllocatedLpnFromAllocInvDtl(self, waveNum: str, item: str):
        """Get 1 allocated ilpn from allocInvnDtl
        """
        # sql = f"""select ic.item_name,aid.cntr_nbr,aid.* from alloc_invn_dtl aid, item_cbo ic
        #             where aid.item_id = ic.item_id and aid.task_genrtn_ref_nbr = '#WAVENUM#' and  ic.item_name='#ITEM#'"""
        # sql = sql.replace('#WAVENUM#', waveNum).replace('#ITEM#', item)
        #
        # dbRows = DBService.fetch_rows(sql, self.schema)

        dbRows = self.getAllAllocatedLpnsFromAllocInvDtl(waveNum=waveNum, item=item)
        cntrNum = dbRows[0].get('CNTR_NBR')

        return cntrNum

    # def getItemsForReplen2_old(self, noOfItem: int, actvWA: str = None, currWG: str = 'RESV', currWA: str = None,
    #                        zone: str = None,
    #                        taskPath: TaskPath = None, isResvWAFromTPathCurrWA: bool = None, isActvWAFromTPathDestWA: bool = None,
    #                        isPCKItem: bool = None, isResvCCPending: bool = None,
    #                        isCreateInvnByDefault: bool = False,
    #                        actvQty: int = 1, maxInvQty: int = 15, noOfLpn: int = 1, lpnQty: list[int] = [10],
    #                        ignoreActvLocn: list[str] = None, ignoreResvLocn: list[str] = None,
    #                        isItemWithBundleQty: bool = None, pickDetrmZone: list[str] = None, actvQtyPercentRange: tuple = None,
    #                        isReplenLocnVLM:bool=None, isReplenLocnAS:bool=None, isASRSResv:bool=None):
    #     """(Generic method)
    #     """
    #     RuntimeXL.createThreadLockFile()
    #     try:
    #         isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = False
    #         noOfItem = 1
    #         dbRows = []
    #
    #         '''With unit check'''
    #         if not isCreateInvnByDefault:
    #             sql = self._buildQueryForGetItemsForReplenToManualActv(noOfItem=noOfItem, actvWA=actvWA, currWG=currWG, currWA=currWA, zone=zone,
    #                                                                    taskPath=taskPath, isResvWAFromTPathCurrWA=isResvWAFromTPathCurrWA, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
    #                                                                    isPCKItem=isPCKItem, isResvCCPending=isResvCCPending,
    #                                                                    isCheckActvQtyCond=True, isCheckResvQtyCond=True, lpnQty=lpnQty,
    #                                                                    ignoreActvLocn=ignoreActvLocn, ignoreResvLocn=ignoreResvLocn,
    #                                                                    isItemWithBundleQty=isItemWithBundleQty, pickDetrmZone=pickDetrmZone, actvQtyPercentRange=actvQtyPercentRange,
    #                                                                    isReplenLocnVLM=isReplenLocnVLM, isReplenLocnAS=isReplenLocnAS, isASRSResv=isASRSResv)
    #
    #             dbRows = DBService.fetch_rows(sql, self.schema)
    #             isItemFoundFromOrigQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False
    #
    #             '''Without unit check'''
    #             if not isItemFoundFromOrigQuery:
    #                 sql = self._buildQueryForGetItemsForReplenToManualActv(noOfItem=noOfItem, actvWA=actvWA, currWG=currWG, currWA=currWA, zone=zone,
    #                                                                        taskPath=taskPath, isResvWAFromTPathCurrWA=isResvWAFromTPathCurrWA, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
    #                                                                        isPCKItem=isPCKItem, isResvCCPending=isResvCCPending,
    #                                                                        ignoreActvLocn=ignoreActvLocn, ignoreResvLocn=ignoreResvLocn,
    #                                                                        isItemWithBundleQty=isItemWithBundleQty, pickDetrmZone=pickDetrmZone, actvQtyPercentRange=actvQtyPercentRange,
    #                                                                        isReplenLocnVLM=isReplenLocnVLM, isReplenLocnAS=isReplenLocnAS, isASRSResv=isASRSResv)
    #
    #                 dbRows = DBService.fetch_rows(sql, self.schema)
    #                 isItemFoundFromRevisedQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False
    #
    #             '''Update actv locn invn unit'''
    #             if (isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery) and actvQty is not None and actvQtyPercentRange is None:
    #                 final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn(isVLMLocn=isReplenLocnVLM, isASLocn=isReplenLocnAS, isASRSLocn=isASRSResv)
    #                 if final_isAllowUpdateInvn:
    #                     noOfItem = noOfItem
    #                     final_onHand = actvQty
    #                     for i in range(noOfItem):
    #                         itemBrcd = dbRows[i]['ITEM_NAME']
    #                         actvLocn = dbRows[i]['ACTIVE_LOCN']
    #                         resvLocn = dbRows[i]['RESERVE_LOCN']
    #                         lpn = dbRows[i]['TC_LPN_ID']
    #                         final_onHand = self._presetInvnForReplen(i_actvLocn=actvLocn, i_itemBrcd=itemBrcd, f_onHand=final_onHand,
    #                                                                  i_resvLocn=resvLocn, i_iLpn=lpn, f_lpnQty=lpnQty[i])
    #                         dbRows[i]['ACTIVE_ONHAND'] = final_onHand
    #                         dbRows[i]['RESERVE_ONHAND'] = lpnQty[i]
    #                 else:
    #                     assert False, 'Updating invn (updating invn in actv/resv for replen) is not allowed. Test manually'
    #             isCreateInvnByDefault = not (isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery)
    #
    #         '''Create invn'''
    #         if isCreateInvnByDefault:
    #             final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isVLMLocn=isReplenLocnVLM, isASLocn=isReplenLocnAS, isASRSLocn=isASRSResv)
    #             if final_isAllowCreateInvn:
    #                 dbRows = self._createInvnForReplenToManualActv(actvQty=actvQty, maxInvQty=maxInvQty, noOfLpn=noOfLpn, lpnQty=lpnQty,
    #                                                                ignoreResvLocn=ignoreResvLocn,
    #                                                                taskPath=taskPath, actvZone=zone, pickDetrmZone=pickDetrmZone,
    #                                                                isItemWithBundleQty=isItemWithBundleQty)
    #             else:
    #                 assert False, 'Creating invn (slotting/assigning item in actv/resv locn) is not allowed. Test manually'
    #
    #         assert len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None, f"{noOfItem} no. of items in resv & actv for repln not found"
    #
    #         '''Update other putaway-ilpns(stat 30) to in-transit(stat 0)'''
    #         if dbRows:
    #             fetchedResvLocnIds = [i['RESV_LOCN_ID'] for i in dbRows]
    #             fetchedItemIds = [i['ITEM_ID'] for i in dbRows]
    #             fetchedIlpns = [i['TC_LPN_ID'] for i in dbRows]
    #             self._updateOtherPutwyILpnsToInTranStat(currResvLocnIds=fetchedResvLocnIds, currItemIds=fetchedItemIds, ignoreIlpns=fetchedIlpns)
    #
    #         '''Update CC flag'''
    #         if isResvCCPending is not None:
    #             for i in range(noOfItem):
    #                 temp_resvLocn = dbRows[i]['RESERVE_LOCN']
    #                 self._updateLocnCCPending(i_locnBrcd=temp_resvLocn, u_isCCPending=isResvCCPending)
    #
    #         '''Print data'''
    #         for i in range(0, noOfItem):
    #             self._logDBResult(dbRows[i], ['ITEM_NAME', 'ACTIVE_LOCN', 'ACTIVE_ONHAND', 'MAX_INVN_QTY', 'RESERVE_LOCN', 'TC_LPN_ID', 'RESERVE_ONHAND'])
    #             self._printItemInvnData(dbRows[i]['ITEM_NAME'])
    #
    #         '''Update runtime thread data file'''
    #         itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
    #         RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
    #
    #         '''Update runtime thread data file'''
    #         locnsAsCsv = ','.join(i['ACTIVE_LOCN'] for i in dbRows)
    #         RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
    #         locnsAsCsv = ','.join(i['RESERVE_LOCN'] for i in dbRows)
    #         RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
    #     finally:
    #         RuntimeXL.removeThreadLockFile()
    #
    #     return dbRows

    def getItemsForReplen2(self, noOfItem: int, actvWA: str = None, currWG: str = 'RESV', currWA: str = None,
                           zone: str = None,
                           taskPath: TaskPath = None, isResvWAFromTPathCurrWA: bool = None, isActvWAFromTPathDestWA: bool = None,
                           # isPCKItem: bool = None, 
                           consolInvnType: ConsolInvnType = None, isResvCCPending: bool = None,
                           isCreateInvnByDefault: bool = False,
                           actvQty: int = 1, maxInvQty: int = 1000, noOfLpn: int = 1, lpnQty: list[int] = [10], isLpnQtyGTMaxQty:bool=None,
                           ignoreActvLocn: list[str] = None, ignoreResvLocn: list[str] = None,
                           isItemWithBundleQty: bool = None, pickDetrmZone: list[str] = None, actvQtyPercentRange: tuple = None,
                           isReplenLocnVLM:bool=None, isReplenLocnAS:bool=None, isASRSResv:bool=None):
        """(Generic method) This makes sure any actv + resv.
        """
        RuntimeXL.createThreadLockFile()
        try:
            noOfItem = 1
            dbRows = []

            if not isCreateInvnByDefault:
                isItemFoundFromOrigQuery = isItemFoundFromRevisedQuery = isItemFoundForAutoLocn = False

                '''Handle automated actv locn'''
                if isReplenLocnVLM or isReplenLocnAS:
                    dbRows = self._createInvnForReplenToAutoActv(isVLMItem=isReplenLocnVLM, isASItem=isReplenLocnAS, isASRSItem=isASRSResv,
                                                                 actvQty=actvQty, noOfLpn=noOfLpn, lpnQty=lpnQty, isLpnQtyGTMaxQty=isLpnQtyGTMaxQty,
                                                                 ignoreResvLocn=ignoreResvLocn, taskPath=taskPath)
                    isItemFoundForAutoLocn = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False
                else:
                    '''With unit check'''
                    sql = self._buildQueryForGetItemsForReplenToManualActv(noOfItem=noOfItem, actvWA=actvWA, currWG=currWG, currWA=currWA, zone=zone,
                                                                           taskPath=taskPath, isResvWAFromTPathCurrWA=isResvWAFromTPathCurrWA, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
                                                                           # isPCKItem=isPCKItem,
                                                                           consolInvnType=consolInvnType, isResvCCPending=isResvCCPending,
                                                                           isCheckActvQtyCond=True, isCheckResvQtyCond=True, lpnQty=lpnQty,
                                                                           ignoreActvLocn=ignoreActvLocn, ignoreResvLocn=ignoreResvLocn,
                                                                           isItemWithBundleQty=isItemWithBundleQty, pickDetrmZone=pickDetrmZone, actvQtyPercentRange=actvQtyPercentRange,
                                                                           isReplenLocnVLM=isReplenLocnVLM, isReplenLocnAS=isReplenLocnAS, isASRSResv=isASRSResv)
                    dbRows = DBService.fetch_rows(sql, self.schema)
                    isItemFoundFromOrigQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False

                    '''Without unit check'''
                    if not isItemFoundFromOrigQuery:
                        sql = self._buildQueryForGetItemsForReplenToManualActv(noOfItem=noOfItem, actvWA=actvWA, currWG=currWG, currWA=currWA, zone=zone,
                                                                               taskPath=taskPath, isResvWAFromTPathCurrWA=isResvWAFromTPathCurrWA, isActvWAFromTPathDestWA=isActvWAFromTPathDestWA,
                                                                               # isPCKItem=isPCKItem,
                                                                               consolInvnType=consolInvnType, isResvCCPending=isResvCCPending,
                                                                               ignoreActvLocn=ignoreActvLocn, ignoreResvLocn=ignoreResvLocn,
                                                                               isItemWithBundleQty=isItemWithBundleQty, pickDetrmZone=pickDetrmZone, actvQtyPercentRange=actvQtyPercentRange,
                                                                               isReplenLocnVLM=isReplenLocnVLM, isReplenLocnAS=isReplenLocnAS, isASRSResv=isASRSResv)
                        dbRows = DBService.fetch_rows(sql, self.schema)
                        isItemFoundFromRevisedQuery = True if len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None else False

                '''Update actv unit'''
                if (isItemFoundForAutoLocn or isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery) and actvQty is not None and actvQtyPercentRange is None:
                    final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn(isVLMLocn=isReplenLocnVLM, isASLocn=isReplenLocnAS, isASRSLocn=isASRSResv)
                    if final_isAllowUpdateInvn:
                        noOfItem = noOfItem
                        final_onHand = actvQty
                        for i in range(noOfItem):
                            itemBrcd = dbRows[i]['ITEM_NAME']
                            actvLocn = dbRows[i]['ACTIVE_LOCN']
                            resvLocn = dbRows[i]['RESERVE_LOCN']
                            lpn = dbRows[i]['TC_LPN_ID']

                            final_onHand = self._presetInvnForReplen(i_actvLocn=actvLocn, i_itemBrcd=itemBrcd, f_onHand=final_onHand,
                                                                     i_resvLocn=resvLocn, i_iLpn=lpn, f_lpnQty=lpnQty[i])

                            locnReqData = {'ACTIVE_ONHAND': final_onHand, 'RESERVE_ONHAND': lpnQty[i]}
                            dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=locnReqData)
                    else:
                        assert False, 'Updating invn (updating invn in actv/resv for replen) is not allowed. Test manually'
                isCreateInvnByDefault = not (isItemFoundForAutoLocn or isItemFoundFromOrigQuery or isItemFoundFromRevisedQuery)

            '''Create invn'''
            if isCreateInvnByDefault:
                final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isVLMLocn=isReplenLocnVLM, isASLocn=isReplenLocnAS, isASRSLocn=isASRSResv)
                if final_isAllowCreateInvn:
                    dbRows = self._createInvnForReplenToManualActv(actvQty=actvQty, maxInvQty=maxInvQty, noOfLpn=noOfLpn, lpnQty=lpnQty, isLpnQtyGTMaxQty=isLpnQtyGTMaxQty,
                                                                   ignoreResvLocn=ignoreResvLocn,
                                                                   taskPath=taskPath, actvZone=zone, pickDetrmZone=pickDetrmZone,
                                                                   isItemWithBundleQty=isItemWithBundleQty, isASRSResv=isASRSResv)
                else:
                    assert False, 'Creating invn (slotting/assigning item in actv/resv locn) is not allowed. Test manually'

            assert len(dbRows) >= noOfItem and dbRows[0]['ITEM_NAME'] is not None, f"<Data> {noOfItem} no. of items in resv & actv for repln not found"

            '''Update other putaway-ilpns(stat 30) to in-transit(stat 0)'''
            if dbRows:
                fetchedResvLocnIds = [i['RESV_LOCN_ID'] for i in dbRows]
                fetchedItemIds = [i['ITEM_ID'] for i in dbRows]
                fetchedIlpns = [i['TC_LPN_ID'] for i in dbRows]
                self._updateOtherPutwyILpnsToInTranStat(currResvLocnIds=fetchedResvLocnIds, currItemIds=fetchedItemIds, ignoreIlpns=fetchedIlpns)

            '''Update CC flag'''
            if isResvCCPending is not None:
                for i in range(noOfItem):
                    temp_resvLocn = dbRows[i]['RESERVE_LOCN']
                    self._updateLocnCCPending(i_locnBrcd=temp_resvLocn, u_isCCPending=isResvCCPending)

            '''Print data'''
            for i in range(0, noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'ACTIVE_LOCN', 'ACTIVE_ONHAND', 'MAX_INVN_QTY', 'RESERVE_LOCN', 'TC_LPN_ID', 'RESERVE_ONHAND'])
                self._printItemInvnData(dbRows[i]['ITEM_NAME'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['ACTIVE_LOCN'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
            locnsAsCsv = ','.join(i['RESERVE_LOCN'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItemsForReplenToManualActv(self, noOfItem: int, actvWA: str = None, currWG: str = 'RESV', currWA: str = None, zone: str = None,
                                                    taskPath: TaskPath = None,
                                                    isResvWAFromTPathCurrWA: bool = None, isActvWAFromTPathDestWA: bool = None,
                                                    # isPCKItem: bool = None,
                                                    consolInvnType: ConsolInvnType = None, isResvCCPending: bool = None,
                                                    actvQty: int = 1, maxInvQty: int = 15, noOfLpn: int = 1, lpnQty:list[int]=None,
                                                    isCheckActvQtyCond:bool=None, isCheckResvQtyCond:bool=None,
                                                    ignoreActvLocn: list[str] = None, ignoreResvLocn: list[str] = None, isItemWithBundleQty: bool = None,
                                                    pickDetrmZone: list[str] = None, actvQtyPercentRange:tuple=None,
                                                    isReplenLocnVLM:bool=None, isReplenLocnAS:bool=None, isASRSResv:bool=None):
        sql = f"""select a.std_bundl_qty,b.tc_lpn_id,a.item_id,a.item_name,a.max_invn_qty,a.min_invn_qty,a.locn_id,a.locn_brcd active_locn,a.zone
                 ,a.aisle,a.locn_pick_seq,a.work_area actv_work_area,a.on_hand_qty active_onhand,a.to_be_filled_qty,a.actv_qty_percent
                 ,b.locn_id resv_locn_id,b.locn_brcd reserve_locn,b.zone resv_zone,b.work_area as resv_work_area,b.on_hand_qty reserve_onhand 
                 from
                 (select ic.std_bundl_qty,ic.item_id,ic.item_name,ic.ref_field10,pld.max_invn_qty,pld.min_invn_qty,lh.locn_id,lh.locn_brcd,lh.zone,lh.aisle,lh.locn_pick_seq,lh.work_area,on_hand_qty,wm_allocated_qty,to_be_filled_qty
                    ,round(((nvl(wm.on_hand_qty,0)+nvl(wm.to_be_filled_qty,0))/case when pld.max_invn_qty<>0 then pld.max_invn_qty end*100)) actv_qty_percent,sku_dedctn_type
                    from wm_inventory wm inner join item_cbo ic on wm.item_id=ic.item_id inner join locn_hdr lh on wm.location_id=lh.locn_id
                    inner join pick_locn_dtl pld on wm.location_dtl_id=pld.pick_locn_dtl_id and wm.location_id=pld.locn_id and wm.item_id=pld.item_id 
                    and lh.work_grp in ('ACTV') #ACTV_WORK_AREA_COND#
                    inner join size_uom su on ic.base_storage_uom_id=su.size_uom_id
                    where wm.locn_class='A' and su.size_uom='EACH' 
                    and lh.sku_dedctn_type='P'
                    #ACTV_QTY_CONDITION#  
                    and ic.item_id in (select item_id from (select item_id,count(*) from wm_inventory where locn_class='A' group by item_id having count(*)=1)) order by lh.locn_pick_seq asc) a
                 inner join    
                 (select wm.tc_lpn_id,ic.item_id, ic.item_name,ic.ref_field10,lh.locn_brcd,lh.locn_id,lh.zone,lh.work_area
                    ,sum(on_hand_qty) as on_hand_qty,sum(wm_allocated_qty) as wm_allocated_qty,sum(to_be_filled_qty) as to_be_filled_qty
                    from wm_inventory wm inner join item_cbo ic on wm.item_id=ic.item_id
                    inner join locn_hdr lh on wm.location_id=lh.locn_id 
                    where wm.locn_class='R' and wm.wm_allocated_qty=0 and lh.work_grp in ('RESV') and lh.work_area in #CURR_WORK_AREA# 
                    #PULL_ZONE_COND# 
                    and ic.item_id in (select item_id from (select count(distinct location_id) cnt,item_id from wm_inventory where locn_class='R' group by item_id having count(location_id)=1))
                    --and wm.tc_lpn_id in (select tc_lpn_id from lpn where single_line_lpn='Y' and lpn_facility_status=30)
                    and wm.tc_lpn_id in (select tc_lpn_id from lpn where lpn_facility_status=30 and lpn_id in (select lpn_id from lpn_detail group by lpn_id having count(lpn_id)=1))
                    and wm.tc_lpn_id not in (select tc_lpn_id from lpn_lock where tc_lpn_id is not null)
                    group by wm.tc_lpn_id,ic.item_id,ic.item_name,ic.ref_field10,lh.locn_brcd,lh.locn_id,lh.zone,lh.work_area having sum(on_hand_qty)>0) b
                 on a.item_id=b.item_id
                 where 0=0 
                 #RESV_QTY_CONDITION# 
                 and a.item_id not in (select item_id from item_facility_mapping_wms where item_id is not null and mark_for_deletion='1')
                 and (b.work_area,a.work_area) in (select curr_work_area,dest_work_area from int_path_defn where invn_need_type='1')
                 --and b.locn_id in (select curr_sub_locn_id from lpn group by curr_sub_locn_id having(curr_sub_locn_id) > 1)                 
                 #CONDITION#
                 #ITEMS_EXCLUDE_COND#
                 #LOCNS_EXCLUDE_COND#
                 order by a.locn_pick_seq 
                 offset 0 rows fetch next {noOfItem} rows only
              """
        actvWACond = ''
        if actvWA is None:
            if isActvWAFromTPathDestWA:
                destWAList = self._getDestWAFromTaskPath2(taskPath)
                actvWA = destWAList
            elif isReplenLocnVLM:
                actvWA = self._decide_lh_workArea_forLocnType(locnType=LocnType.VLM_ACTV)
            elif isReplenLocnAS:
                actvWA = self._decide_lh_workArea_forLocnType(locnType=LocnType.AS_ACTV)
        actvWA = self.removeSpecialCharFromTaskPathVals(actvWA)
        if actvWA is not None:
            actvWACond = ' and lh.work_area in ' + Commons.get_tuplestr(actvWA)
        sql = sql.replace('#ACTV_WORK_AREA_COND#', actvWACond)

        if currWA is None:
            if isResvWAFromTPathCurrWA:
                currWAList = self._getCurrWAFromTaskPath2(taskPath)
                currWA = currWAList
            elif isASRSResv:
                currWA = self._decide_lh_workArea_forLocnType(locnType=LocnType.ASRS_RESV)
        currWA = self.removeSpecialCharFromTaskPathVals(currWA)
        if currWA is not None:
            sql = sql.replace('#CURR_WORK_AREA#', Commons.get_tuplestr(currWA))

        actvQtyCond = ''
        if isCheckActvQtyCond:
            actvQtyCond += " \n and wm.wm_allocated_qty=0 and wm.on_hand_qty>0"
        sql = sql.replace('#ACTV_QTY_CONDITION#', actvQtyCond)

        resvQtyCond = ''
        if isCheckResvQtyCond:
            if lpnQty is not None:
                resvQtyCond += f" \n and a.on_hand_qty + {lpnQty[0]} <= a.max_invn_qty"
            else:
                resvQtyCond += " \n and a.on_hand_qty + b.on_hand_qty <= a.max_invn_qty"
            resvQtyCond += " \n and b.wm_allocated_qty=0 and b.to_be_filled_qty=0"
        sql = sql.replace('#RESV_QTY_CONDITION#', resvQtyCond)

        final_itemAllocType, final_avoidItemAllocType = self._decide_iap_allocType_forItemType(defaultVal='STD')
        pullZoneCond = ''
        if final_itemAllocType is not None:
            pullZoneCond += f""" \n and lh.pull_zone in (select pull_zone from invn_alloc_prty where invn_need_type='1' and alloc_type in {final_itemAllocType})"""
        elif final_avoidItemAllocType is not None:
            pullZoneCond += f""" \n and lh.pull_zone not in (select pull_zone from invn_alloc_prty where invn_need_type='1' and alloc_type in {final_avoidItemAllocType})"""
        sql = sql.replace('#PULL_ZONE_COND#', pullZoneCond)

        sqlCond = ''
        if zone is not None:
            sqlCond += f" \n and a.zone = '{zone}'"
        if ignoreActvLocn is not None:
            sqlCond += " \n and a.locn_brcd not in " + Commons.get_tuplestr(ignoreActvLocn)
        if ignoreResvLocn is not None:
            sqlCond += " \n and b.locn_brcd not in " + Commons.get_tuplestr(ignoreResvLocn)
        if isItemWithBundleQty:
            sqlCond += " \n and a.std_bundl_qty > 1 "
        if actvQtyPercentRange is not None:
            startPercent, endPercent = actvQtyPercentRange
            sqlCond += f" \n and actv_qty_percent between {startPercent} and {endPercent}"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and a.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (a.ref_field10 is null or (a.ref_field10 is not null and a.ref_field10 not in ('ARC')))"

        sql = sql.replace('#CONDITION#', sqlCond)

        '''Exclude runtime thread locns'''
        locnsExcludeCond = ''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            locnsExcludeCond += " \n and a.locn_brcd not in " + threadLocns
            locnsExcludeCond += " \n and b.locn_brcd not in " + threadLocns
        sql = sql.replace('#LOCNS_EXCLUDE_COND#', locnsExcludeCond)

        '''Exclude runtime thread items'''
        itemsExcludeCond = ''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            itemsExcludeCond += f" \n and a.item_name not in " + threadItems
            itemsExcludeCond += f""" \n and a.locn_id not in (select location_id from wm_inventory where locn_class='A' 
                                                    and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""
        sql = sql.replace('#ITEMS_EXCLUDE_COND#', itemsExcludeCond)

        return sql

    def _presetInvnForReplen(self, i_actvLocn: str, i_itemBrcd: str, f_onHand: int = None, f_availCap: int = None,
                             i_resvLocn: str=None, i_iLpn:str=None, f_lpnQty: int = None):
        """(Generic method) This makes sure any actv + resv.
        """
        isUpdateActvOnHand = isUpdateResvOnHand = False
        final_onHandQty = final_lpnQty = None
        if i_actvLocn is not None:
            currAvailUnit = self.getAvailUnitFromWM(itemBrcd=i_itemBrcd, locnBrcd=i_actvLocn)
            currOnHand = self.getWMOnHandQty(itemBrcd=i_itemBrcd, locnBrcd=i_actvLocn)
            currWmAllocQty = self.getWMAllocQty(itemBrcd=i_itemBrcd, locnBrcd=i_actvLocn)
            currMaxInvnQty = self.getMaxInvnQty(i_locnBrcd=i_actvLocn, i_itemBrcd=i_itemBrcd)

            final_onHandQty = final_wmAllocQty = final_tbfQty = None
            final_lpnQty = None

            '''Approach 3'''
            if True:
                final_wmAllocQty = 0
                final_tbfQty = 0
                if f_onHand is not None:
                    final_onHandQty = f_onHand
                else:
                    final_onHandQty = currOnHand
                if f_availCap is not None:
                    final_onHandQty = currMaxInvnQty - f_availCap

                if f_lpnQty is not None:
                    final_lpnQty = f_lpnQty
                isUpdateOnHand = True

            assert final_onHandQty is not None, '<Data> OnHandQty is None for update, Check TC'

            '''Update actv wm invn'''
            if isUpdateOnHand:
                self._updateWMInvn(i_locnBrcd=i_actvLocn, i_itemBrcd=i_itemBrcd, u_onHandQty=final_onHandQty,
                                   u_wmAllocatedQty=final_wmAllocQty, u_toBeFilledQty=final_tbfQty)

            '''Update lpn qty'''
            if i_resvLocn and i_iLpn and final_lpnQty:
                self._updateWMInvnForLpn(i_locnBrcd=i_resvLocn, i_lpn=i_iLpn, i_itemBrcd=i_itemBrcd, u_onHandQty=final_lpnQty)
                self._updateLpnQty(i_iLpn=i_iLpn, i_item=i_itemBrcd, u_lpnQty=final_lpnQty)

        return final_onHandQty

    # def _createInvnForReplenToManualActv_old(self, itemBrcd:str=None, isItemWithBundleQty:bool = None,
    #                                      actvQty: int = None, maxInvQty: int = None, actvZone: str = None, pickDetrmZone: list[str] = None,
    #                                      noOfLpn: int = None, lpnQty: list[int] = None, ignoreResvLocn: list[str] = None,
    #                                      taskPath: TaskPath = None):
    #     """Create replen invn for resv and manual actv
    #     """
    #     dbRows = []
    #     dbRow = dict()
    #     noOfLpn = noOfLpn
    #
    #     '''Get both locns'''
    #     resvLocnRows = actvLocnRows = None
    #     if True:
    #         noOfResvLocn = 1
    #         noOfActvLocn = 1
    #
    #         tpdRows = self.getTaskPathDefs2(taskPath)
    #         for i in range(len(tpdRows)):
    #             currWG = tpdRows[i]['CURR_WORK_GRP']
    #             currWA = tpdRows[i]['CURR_WORK_AREA']
    #             resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=currWG, resvWA=currWA,
    #                                             isPullZoneInAllocPrty=True, taskPath=taskPath, ignoreLocn=ignoreResvLocn)
    #             if resvLocnRows:
    #                 destWA = tpdRows[i]['DEST_WORK_AREA']
    #                 actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=destWA,
    #                                                             zone=actvZone, pickDetrmZone=pickDetrmZone,
    #                                                             f_isClearLocnIfNotFound=False, f_isAssertResult=False)
    #             if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn:
    #                 break
    #         isBothLocnFoundFromTaskpath = True if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn else False
    #
    #         '''Clear actv locn'''
    #         if not isBothLocnFoundFromTaskpath:
    #             final_isAllowClearInvn = self._decide_isAllowClearInvn()
    #             if final_isAllowClearInvn:
    #                 for i in range(len(tpdRows)):
    #                     currWG = tpdRows[i]['CURR_WORK_GRP']
    #                     currWA = tpdRows[i]['CURR_WORK_AREA']
    #                     resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=currWG, resvWA=currWA,
    #                                                     isPullZoneInAllocPrty=True, taskPath=taskPath)
    #                     if resvLocnRows:
    #                         destWA = tpdRows[i]['DEST_WORK_AREA']
    #                         actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=destWA,
    #                                                                     zone=actvZone, pickDetrmZone=pickDetrmZone)
    #                     if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn:
    #                         break
    #             else:
    #                 assert False, 'Clearing invn (clearing invn in actv for replen) is not allowed. Test manually'
    #
    #     assert resvLocnRows and len(resvLocnRows) > 0 and actvLocnRows and len(actvLocnRows) > 0, 'New resv/actv locn not found to create invn'
    #
    #     '''Get item not in any locn'''
    #     if True:
    #         if itemBrcd is not None:
    #             itemId = self.getItemIdFromBrcd(itemBrcd=itemBrcd)
    #         else:
    #             itemRows = self.getItemsNotInAnyLocn(noOfItem=1, isItemWithBundleQty=isItemWithBundleQty)
    #             itemId = itemRows[0]['ITEM_ID']
    #             itemBrcd = itemRows[0]['ITEM_NAME']
    #         dbRow['ITEM_NAME'] = itemBrcd
    #         dbRow['ITEM_ID'] = itemId
    #
    #     '''Insert actv invn'''
    #     if True:
    #         actvQty = actvQty
    #         maxInvQty = maxInvQty
    #         actvQtyPercent = round((actvQty / maxInvQty) * 100)
    #         actvLocnId = actvLocnRows[0]['LOCN_ID']
    #         actvLocn = actvLocnRows[0]['LOCN_BRCD']
    #         actvZone = actvLocnRows[0]['ZONE']
    #         actvWorkArea = actvLocnRows[0]['WORK_AREA']
    #         DBAdmin._insertToActvInvnTables(self.schema, locnId=actvLocnId, maxInvQty=maxInvQty, itemId=itemId, qty=actvQty)
    #         dbRow['ACTIVE_LOCN'] = actvLocn
    #         dbRow['ZONE'] = actvZone
    #         dbRow['ACTV_WORK_AREA'] = actvWorkArea
    #         dbRow['MAX_INVN_QTY'] = maxInvQty
    #         dbRow['ACTIVE_ONHAND'] = actvQty
    #         dbRow['ACTV_QTY_PERCENT'] = actvQtyPercent
    #
    #         resvLocnId = resvLocnRows[0]['LOCN_ID']
    #         resvLocn = resvLocnRows[0]['LOCN_BRCD']
    #         resvWrkArea = resvLocnRows[0]['WORK_AREA']
    #         dbRow['RESERVE_LOCN'] = resvLocn
    #         dbRow['RESV_LOCN_ID'] = resvLocnId
    #         dbRow['RESV_WORK_AREA'] = resvWrkArea
    #         dbRows.extend([dbRow.copy() for i in range(noOfLpn)])
    #
    #     '''Insert resv invn'''
    #     if True:
    #         for i in range(noOfLpn):
    #             lpnBrcd = self.getNewILPNNum()
    #             lpnQtyTemp = lpnQty[i]
    #             DBAdmin._insertToResvInvnTables(self.schema, lpnBrcd=lpnBrcd, locnId=resvLocnId, lpnFacStat=30,
    #                                             itemId=itemId, itemBrcd=itemBrcd, qty=lpnQtyTemp)
    #             dbRows[i]['TC_LPN_ID'] = lpnBrcd
    #             dbRows[i]['RESERVE_ONHAND'] = lpnQtyTemp
    #
    #     return dbRows

    def _createInvnForReplenToManualActv(self, itemBrcd:str=None, isItemWithBundleQty:bool = None,
                                         actvQty: int = None, maxInvQty: int = None, actvZone: str = None, pickDetrmZone: list[str] = None,
                                         noOfLpn: int = None, lpnQty: list[int] = None, isLpnQtyGTMaxQty:bool=None, ignoreResvLocn: list[str] = None,
                                         taskPath: TaskPath = None, isASRSResv:bool=None):
        """Create replen invn for resv and manual actv
        """
        dbRows = []
        dbRow = dict()
        noOfLpn = noOfLpn

        '''Get both locns'''
        resvLocnRows = actvLocnRows = None
        if True:
            noOfResvLocn = 1
            noOfActvLocn = 1

            tpdRows = self.getTaskPathDefs2(taskPath)
            for i in range(len(tpdRows)):
                currWG = tpdRows[i]['CURR_WORK_GRP']
                currWA = tpdRows[i]['CURR_WORK_AREA']
                resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=currWG, resvWA=currWA,
                                                isPullZoneInAllocPrty=True, taskPath=taskPath, ignoreLocn=ignoreResvLocn,
                                                isASRSLocn=isASRSResv)
                if resvLocnRows:
                    destWA = tpdRows[i]['DEST_WORK_AREA']
                    actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=destWA,
                                                                zone=actvZone, pickDetrmZone=pickDetrmZone,
                                                                f_isClearLocnIfNotFound=False, f_isAssertResult=False)
                if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn:
                    break
            isBothLocnFoundFromTaskpath = True if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn else False

            '''Clear actv locn'''
            if not isBothLocnFoundFromTaskpath:
                final_isAllowClearInvn = self._decide_isAllowClearInvn()
                if final_isAllowClearInvn:
                    for i in range(len(tpdRows)):
                        currWG = tpdRows[i]['CURR_WORK_GRP']
                        currWA = tpdRows[i]['CURR_WORK_AREA']
                        resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=currWG, resvWA=currWA,
                                                        isPullZoneInAllocPrty=True, taskPath=taskPath, ignoreLocn=ignoreResvLocn,
                                                        isASRSLocn=isASRSResv)
                        if resvLocnRows:
                            destWA = tpdRows[i]['DEST_WORK_AREA']
                            actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=destWA,
                                                                        zone=actvZone, pickDetrmZone=pickDetrmZone)
                        if resvLocnRows and len(resvLocnRows) == noOfResvLocn and actvLocnRows and len(actvLocnRows) == noOfActvLocn:
                            break
                else:
                    assert False, 'Clearing invn (clearing invn in actv for replen) is not allowed. Test manually'

        assert resvLocnRows and len(resvLocnRows) > 0 and actvLocnRows and len(actvLocnRows) > 0, '<Data> New resv/actv locn not found to create invn'

        '''Get item not in any locn'''
        itemRows = None
        if resvLocnRows and actvLocnRows:
            if itemBrcd is not None:
                itemRow = self.getItemDtlsFrom(orItemBrcd=itemBrcd)
                itemRows = [itemRow]
            else:
                itemRows = self.getItemsNotInAnyLocn(noOfItem=1, isItemWithBundleQty=isItemWithBundleQty)

        '''Insert actv invn'''
        if itemRows is not None:
            actvLocnRow = actvLocnRows[0]  # Due to noOfActvLocn = 1
            resvLocnRow = resvLocnRows[0]  # Due to noOfResvLocn = 1
            itemRow = itemRows[0]  # Due to 1 item

            itemId = itemRow['ITEM_ID']
            itemBrcd = itemRow['ITEM_NAME']
            actvQtyPercent = round((actvQty / maxInvQty) * 100)

            DBAdmin._insertToActvInvnTables(self.schema, locnId=actvLocnRow['LOCN_ID'], maxInvQty=maxInvQty, itemId=itemId, qty=actvQty)

            locnReqData = {'ITEM_NAME': itemRow['ITEM_NAME'], 'ITEM_ID': itemRow['ITEM_ID'],
                           'ACTIVE_LOCN': actvLocnRow['LOCN_BRCD'], 'ZONE': actvLocnRow['ZONE'], 'ACTV_WORK_AREA': actvLocnRow['WORK_AREA'],
                           'MAX_INVN_QTY': maxInvQty, 'ACTIVE_ONHAND': actvQty, 'ACTV_QTY_PERCENT': actvQtyPercent,
                           'RESERVE_LOCN': resvLocnRow['LOCN_BRCD'], 'RESV_LOCN_ID': resvLocnRow['LOCN_ID'], 'RESV_WORK_AREA': resvLocnRow['WORK_AREA']}
            dbRow = Commons.update_dict(curr_dict=dbRow, new_dict=locnReqData)

            if dbRow is not None:
                dbRows.extend([dbRow.copy() for _ in range(noOfLpn)])

            '''Insert resv invn'''
            if dbRow is not None:
                resvLocnId = resvLocnRow['LOCN_ID']

                for i in range(noOfLpn):
                    lpnBrcd = self.getNewILPNNum()
                    lpnQtyTemp = maxInvQty + 1 if isLpnQtyGTMaxQty else lpnQty[i]

                    DBAdmin._insertToResvInvnTables(self.schema, lpnBrcd=lpnBrcd, locnId=resvLocnId, lpnFacStat=30,
                                                    itemId=itemId, itemBrcd=itemBrcd, qty=lpnQtyTemp)

                    lpnReqData = {'TC_LPN_ID': lpnBrcd, 'RESERVE_ONHAND': lpnQtyTemp}
                    dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=lpnReqData)

        return dbRows

    def _createInvnForReplenToAutoActv(self, itemBrcd: str = None, isVLMItem: bool = None, isASItem: bool = None,
                                       isASRSItem: bool = None, actvQty:int = None, noOfLpn: int = None, lpnQty: list[int] = None, isLpnQtyGTMaxQty:bool=None,
                                       ignoreResvLocn: list[str] = None, taskPath: TaskPath = None):
        """Create replen invn for resv.
        Get invn in automated actv
        """
        dbRows = []
        dbRow = dict()
        noOfLpn = noOfLpn

        resvLocnRows = None
        if True:
            noOfResvLocn = 1

            tpdRows = self.getTaskPathDefs2(taskPath)
            for i in range(len(tpdRows)):
                tpdResvWG = tpdRows[i].get('CURR_WORK_GRP')
                tpdResvWA = tpdRows[i].get('CURR_WORK_AREA')

                resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=tpdResvWG, resvWA=tpdResvWA,
                                                isPullZoneInAllocPrty=True, taskPath=taskPath,
                                                ignoreLocn=ignoreResvLocn, isASRSLocn=isASRSItem)
                if resvLocnRows is not None and len(resvLocnRows) == noOfResvLocn:
                    break

        assert resvLocnRows is not None, '<Data> Resv locn not found to create invn for replen to automated locn'

        '''Get item not in any locn'''
        itemRows = None
        if itemBrcd is not None:
            itemRow = self.getItemDtlsFrom(orItemBrcd=itemBrcd)
            itemRows = [itemRow]
        else:
            itemRows = self.getItemsInAutoActvLocn(noOfItem=1, isVLMItem=isVLMItem, isASItem=isASItem)

        '''Put actv locn data to dict'''
        if itemRows is not None:
            resvLocnRow = resvLocnRows[0]  # Due to noOfResvLocn = 1
            itemRow = itemRows[0]  # Due to 1 item

            maxInvQty = self.getMaxInvnQty(i_locnBrcd=itemRow['LOCN_BRCD'], i_itemBrcd=itemRow['ITEM_NAME'])

            locnReqData = {'ITEM_NAME': itemRow['ITEM_NAME'], 'ITEM_ID': itemRow['ITEM_ID'],
                           'ACTIVE_LOCN': itemRow['LOCN_BRCD'], 'ZONE': itemRow['ZONE'], 'ACTV_WORK_AREA': itemRow['WORK_AREA'],
                           'MAX_INVN_QTY': maxInvQty, 'ACTIVE_ONHAND': actvQty, 'ACTV_QTY_PERCENT': None,
                           'RESERVE_LOCN': resvLocnRow['LOCN_BRCD'], 'RESV_LOCN_ID': resvLocnRow['LOCN_ID'], 'RESV_WORK_AREA': resvLocnRow['WORK_AREA']}
            dbRow = Commons.update_dict(curr_dict=dbRow, new_dict=locnReqData)

            dbRows.extend([dbRow.copy() for i in range(noOfLpn)])

            '''Insert resv invn'''
            for i in range(noOfLpn):
                lpnBrcd = self.getNewILPNNum()
                itemId = itemRow['ITEM_ID']
                itemBrcd = itemRow['ITEM_NAME']
                lpnQtyTemp = maxInvQty + 1 if isLpnQtyGTMaxQty else lpnQty[i]
                resvLocnId = resvLocnRow['LOCN_ID']

                DBAdmin._insertToResvInvnTables(self.schema, lpnBrcd=lpnBrcd, locnId=resvLocnId, lpnFacStat=30,
                                                itemId=itemId, itemBrcd=itemBrcd, qty=lpnQtyTemp)

                lpnReqData = {'TC_LPN_ID': lpnBrcd, 'RESERVE_ONHAND': lpnQtyTemp}
                dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=lpnReqData)

            # '''Update actv unit'''
            # if actvQty is not None:
            #     final_isAllowUpdateInvn = self._decide_isAllowUpdateInvn(isVLMLocn=isVLMItem, isASLocn=isASItem)
            #     if final_isAllowUpdateInvn:
            #         noOfItem = 1
            #         final_onHand = actvQty
            #         for i in range(noOfItem):
            #             itemBrcd = dbRows[i].get('ITEM_NAME')
            #             locnBrcd = dbRows[i].get('ACTIVE_LOCN')
            #             final_onHand = self._setInvnInActvLocn(i_locnBrcd=locnBrcd, i_itemBrcd=itemBrcd, f_onHand=final_onHand)
            #
            #             locnReqData = {'ACTIVE_ONHAND': final_onHand}
            #             dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=locnReqData)
            #     else:
            #         assert False, 'Updating invn (updating invn in actv for replen) is not allowed. Test manually'

        return dbRows

    def getItemsForResvPick2(self, noOfItem: int, minLpnQty: int = 1, lpnQty: int = None,
                             # isPCKItem: bool = None,
                             consolInvnType: ConsolInvnType = None,
                             ignoreItems: list[str] = None, pullZone: str = None, itemAllocType:list[str]=['STD'],
                             isPickLocnASRS:bool=None):
        """(Generic method) This makes sure any resv.
        """
        dbRows = []

        RuntimeXL.createThreadLockFile()
        try:
            sql = f"""select distinct locn_class,item_name,item_id,location_id,locn_brcd,on_hand_qty,avail_unit
                     ,tc_lpn_id,locn_pick_seq,lpn_facility_status,putwy_type,zone,pull_zone
                     from 
                        (
                        select wi.locn_class,ic.item_name,rank()over(partition by ic.item_name order by wi.tc_lpn_id) rn
                        ,ic.item_id,location_id,locn_brcd,wi.on_hand_qty,wi.wm_allocated_qty,nvl(on_hand_qty,0)-nvl(wm_allocated_qty,0) as avail_unit
                        ,wi.tc_lpn_id,lh.locn_pick_seq ,lpn_facility_status,ifm.putwy_type,lh.zone,lh.pull_zone
                        from item_cbo ic,wm_inventory wi,locn_hdr lh,lpn,size_uom su,item_facility_mapping_wms ifm 
                        where ic.item_id=wi.item_id and lh.locn_id=wi.location_id and ic.item_id=ifm.item_id
                            and ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
                            and wi.tc_lpn_id=lpn.tc_lpn_id and lpn.inbound_outbound_indicator='I' 
                            and lpn.single_line_lpn='Y' and wi.locn_class='R' --and on_hand_qty>0 
                            and lpn_facility_status=30 and nvl(on_hand_qty,0)>0 and nvl(wm_allocated_qty,0)=0
                            and ic.item_id not in (select item_id from wm_inventory where locn_class='A')
                            #PULL_ZONE_COND#    
                        union all
                        select wi.locn_class,ic.item_name,rank()over(partition by ic.item_name order by wi.tc_lpn_id) rn
                        ,ic.item_id,location_id,locn_brcd,wi.on_hand_qty,wi.wm_allocated_qty,nvl(on_hand_qty,0)-nvl(wm_allocated_qty,0) as avail_unit
                        ,wi.tc_lpn_id,lh.locn_pick_seq,lpn_facility_status,ifm.putwy_type,lh.zone,lh.pull_zone
                        from item_cbo ic,wm_inventory wi,locn_hdr lh,lpn,size_uom su,item_facility_mapping_wms ifm 
                        where ic.item_id=wi.item_id and lh.locn_id=wi.location_id and ic.item_id=ifm.item_id
                            and ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
                            and wi.tc_lpn_id=lpn.tc_lpn_id and lpn.inbound_outbound_indicator='I' 
                            and lpn.single_line_lpn='Y' and wi.locn_class='R' --and on_hand_qty>0 
                            and lpn_facility_status=45 and nvl(on_hand_qty,0) - nvl(wm_allocated_qty,0)>0 
                            and ic.item_id not in (select item_id from wm_inventory where locn_class='A')
                            #PULL_ZONE_COND#    
                        ) 
                     where rn=1 
                        and item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion = '1')
                        #CONDITION#   
                        order by locn_pick_seq asc
                        offset 0 rows fetch next {noOfItem} rows only
                  """
            sqlCond = ''

            # if isPCKItem:
            #     sqlCond += """ \n and item_id in (select item_id from item_facility_mapping_wms where slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM'))"""
            #     sqlCond += """ \n and location_id in (select location_id from locn_hdr lh where lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66'))"""

            if consolInvnType is not None:
                if consolInvnType == ConsolInvnType.PCK:
                    sqlCond += """ \n and item_id in (select item_id from item_facility_mapping_wms where slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM'))""" + f" --{consolInvnType.value}"
                    sqlCond += """ \n and location_id in (select location_id from locn_hdr lh where lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66'))""" + f" --{consolInvnType.value}"

            if minLpnQty is not None:
                sqlCond += " \n and avail_unit >= " + str(minLpnQty) + ""
            if lpnQty is not None:
                sqlCond += " \n and avail_unit = " + str(lpnQty) + ""
            if ignoreItems is not None:
                sqlCond += " \n and item_name not in " + str(Commons.get_tuplestr(ignoreItems))

            final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType(isASRSLocn=isPickLocnASRS)
            if final_locnBrcd is not None:
                sqlCond += f" \n and locn_brcd in {final_locnBrcd}"
            elif final_avoidLocnBrcd is not None:
                sqlCond += f" \n and locn_brcd not in {final_avoidLocnBrcd}"

            final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isASRSLocn=isPickLocnASRS)
            if final_zone is not None:
                sqlCond += f" \n and zone in {final_zone}"
            elif final_avoidZone is not None:
                sqlCond += f" \n and zone not in {final_avoidZone}"

            pullZoneCond = ''
            final_pullZone, final_avoidPullZone = self._decide_lh_pullZone_forLocnType(isASRSLocn=isPickLocnASRS, providedVal=pullZone)
            if final_pullZone is not None:
                pullZoneCond = f" and pull_zone in {final_pullZone}"
            elif final_avoidPullZone is not None:
                pullZoneCond = f" and pull_zone not in {final_avoidPullZone}"
            sql = sql.replace('#PULL_ZONE_COND#', pullZoneCond)

            final_itemPutwyType, final_avoidItemPutwyType = self._decide_ifmw_putwyType_forItemType(isASRSItem=isPickLocnASRS, defaultVal='STD')
            if final_itemPutwyType is not None:
                sqlCond += f" \n and putwy_type in {final_itemPutwyType}"

            final_itemAllocType, final_avoidItemAllocType = \
                self._decide_ifmw_allocType_forItemType(isASRSItem=isPickLocnASRS, providedVal=itemAllocType, defaultVal='STD')
            if final_itemAllocType is not None:
                sqlCond += f" \n and item_id in (select item_id from item_facility_mapping_wms ifm where alloc_type in {final_itemAllocType})"

            final_refField10 = self._decide_ic_refField10_forItemType()
            if final_refField10 is not None:
                sqlCond += f" \n and item_id in (select item_id from item_cbo where ref_field10 in {final_refField10})"
            else:
                sqlCond += f" \n and item_id in (select item_id from item_cbo where ref_field10 is null or (ref_field10 is not null and ref_field10 not in ('ARC')))"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and locn_brcd not in " + threadLocns

            '''Exclude runtime thread items'''
            threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
            if threadItems is not None:
                sqlCond += f" \n and item_name not in " + threadItems
                sqlCond += f""" \n and location_id not in (select location_id from wm_inventory where locn_class='R'
                                                and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)
            isCreateInvnByDefault = True if len(dbRows) < noOfItem or dbRows[0]['TC_LPN_ID'] is None else False

            '''Create invn if required'''
            if isCreateInvnByDefault:
                final_isAllowCreateInvn = self._decide_isAllowCreateInvn(isASRSLocn=isPickLocnASRS)
                if final_isAllowCreateInvn:
                    dbRows = []

                    for i in range(noOfItem):
                        finalLpnQty = lpnQty if lpnQty is not None else minLpnQty if minLpnQty is not None else 10
                        taskPath = TaskPath(intType=2, currWG='RESV')
                        dbRowsNew = self._createInvnForResvPick(itemBrcd=None,
                                                                # isPCKItem=isPCKItem,
                                                                consolInvnType=consolInvnType,
                                                                noOfResvLocn=1, noOfLpn=1, lpnQty=[finalLpnQty], pullZone=pullZone,
                                                                itemAllocType=itemAllocType, taskPath=taskPath, isResvWAInTPDCurrWA=True,
                                                                isASRSItem=isPickLocnASRS)
                        dbRows.extend(dbRowsNew)
                else:
                    assert False, 'Creating invn (assigning item in resv locn) is not allowed. Test manually'

            assert len(dbRows) >= noOfItem, f'<Data> {noOfItem} no. of items present only in resv not found ' + sql

            '''Update cycle cnt flag to N'''
            for i in range(noOfItem):
                self._updateLocnCCPending(i_locnBrcd=dbRows[i]['LOCN_BRCD'], u_isCCPending=False)

            '''Print data'''
            for i in range(noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'LOCN_BRCD', 'ON_HAND_QTY', 'TC_LPN_ID'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _createInvnForResvPick(self, itemBrcd: str = None,
                               # isPCKItem: bool = None,
                               consolInvnType: ConsolInvnType = None, noOfResvLocn: int = 1,
                               noOfLpn: int = 1, lpnQty: list[int] = [10], pullZone: str = None, itemAllocType:list[str]=['STD'],
                               taskPath: TaskPath = None, isResvWAInTPDCurrWA: bool = None, resvLocns:list[str]=None,
                               isItemForCrossDock:bool=None, isASRSItem:bool=None):
        """(Generic method) This makes sure any resv.
        """
        dbRows = []

        RuntimeXL.createThreadLockFile()
        try:
            dbRow = dict()
            noOfLpn = noOfLpn
            # noOfRecords = noOfLpn

            '''If resvLocns provided'''
            resvLocnRows = None
            if resvLocns is not None:
                assert noOfResvLocn == len(resvLocns), '<Data> Number of resv locns didnt match'
                resvLocnRows = []
                resvLocnRow = dict()
                resvLocnIds = []
                for i in resvLocns:
                    locnId = DBLib().getLocnIdByLocnBrcd(i)
                    resvLocnIds.append(locnId)
                resvLocnRow['LOCN_BRCD'] = resvLocns[0]
                resvLocnRow['LOCN_ID'] = resvLocnIds[0]
                resvLocnRows.extend([resvLocnRow.copy() for i in range(len(resvLocns))])
                for i in range(len(resvLocns)):
                    resvLocnRows[i]['LOCN_BRCD'] = resvLocns[i]
                    resvLocnRows[i]['LOCN_ID'] = resvLocnIds[i]
            else:
                noOfResvLocn = noOfResvLocn

                # taskPathRows = self.getTaskPathDefs2(taskPath)
                # for i in range(len(taskPathRows)):
                #     tpdCurrWG = taskPathRows[i]['CURR_WORK_GRP']
                #     tpdCurrWA = taskPathRows[i]['CURR_WORK_AREA']
                tpdCurrWAs = None
                if isResvWAInTPDCurrWA:
                    tpdCurrWAs = self._getCurrWAFromTaskPath2(taskPath)

                    '''ASRS work area exclusion'''
                    if tpdCurrWAs and isASRSItem is not True:
                        asrsWAs = self._decide_lh_workArea_forLocnType(locnType=LocnType.ASRS_RESV)
                        tpdCurrWAs = Commons.remove_vals_from_list(tpdCurrWAs, asrsWAs)

                '''Consider only tpd int type'''
                if tpdCurrWAs is None or len(tpdCurrWAs) == 0:
                    taskPath = TaskPath(intType=taskPath.INT_TYPE) if taskPath is not None else TaskPath(intType=2)
                    tpdCurrWAs = self._getCurrWAFromTaskPath2(taskPath)

                    '''ASRS work area exclusion'''
                    if tpdCurrWAs and isASRSItem is not True:
                        asrsWAs = self._decide_lh_workArea_forLocnType(locnType=LocnType.ASRS_RESV)
                        tpdCurrWAs = Commons.remove_vals_from_list(tpdCurrWAs, asrsWAs)

                assert tpdCurrWAs and len(tpdCurrWAs) > 0, '<Data> Task path resv work areas not found to create invn'

                for i in range(len(tpdCurrWAs)):
                    resvWA = tpdCurrWAs[i]  # taskPathRows[i]['CURR_WORK_AREA']
                    # resvLocnRows = self.getResvLocnHavingNoItem(noOfLocn=noOfResvLocn, resvWG=tpdCurrWG, resvWA=None,
                    #                                             isPullZoneInAllocPrty=True, pullZone=pullZone, itemAllocType=itemAllocType,
                    #                                             taskPath=taskPath,
                    #                                             isResvWAFromTPathCurrWA=isResvWAFromTPathCurrWA)
                    resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG='RESV', resvWA=resvWA,
                                                    pullZone=pullZone, itemAllocType=itemAllocType,
                                                    taskPath=taskPath, isPullZoneInAllocPrty=True,  # isResvWAInTPDCurrWA=isResvWAInTPDCurrWA,
                                                    isASRSLocn=isASRSItem)
                    if resvLocnRows is not None and len(resvLocnRows) == noOfResvLocn:
                        break
            assert resvLocnRows is not None and len(resvLocnRows) > 0, '<Data> Resv locn not found to create invn for picking'

            if itemBrcd is not None:
                itemId = self.getItemIdFromBrcd(itemBrcd=itemBrcd)
            else:
                itemRows = self.getItemsNotInAnyLocn(noOfItem=1,
                                                     # isPCKItem=isPCKItem,
                                                     consolInvnType=consolInvnType, isItemForCrossDock=isItemForCrossDock,
                                                     isASRSItem=isASRSItem)
                itemBrcd = itemRows[0]['ITEM_NAME']
                itemId = itemRows[0]['ITEM_ID']
            dbRow['ITEM_NAME'] = itemBrcd

            resvLocnId = resvLocnRows[0]['LOCN_ID']
            resvLocn = resvLocnRows[0]['LOCN_BRCD']
            dbRow['LOCN_BRCD'] = resvLocn
            dbRows.extend([dbRow.copy() for i in range(noOfLpn)])

            for i in range(noOfLpn):
                lpnBrcd = self.getNewILPNNum()
                lpnQtyTemp = lpnQty[i]
                DBAdmin._insertToResvInvnTables(self.schema, lpnBrcd=lpnBrcd, locnId=resvLocnId, lpnFacStat=30,
                                                itemId=itemId, itemBrcd=itemBrcd, qty=lpnQtyTemp)
                dbRows[i]['TC_LPN_ID'] = lpnBrcd
                dbRows[i]['ON_HAND_QTY'] = lpnQtyTemp

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _createInvnForResvPutwy(self, itemBrcd: str = None,
                                # isPCKItem: bool = None,
                                consolInvnType: ConsolInvnType = None, noOfResvLocn: int = 1,
                                noOfLpn: int = 1, lpnQty: list[int] = [10], pullZone: str = None, itemAllocType:list[str]=['STD'],
                                taskPath: TaskPath = None, isResvWAInTPDCurrWA: bool = None, isResvWAInTPDDestWA:bool=None,
                                resvLocns:list[str]=None,
                                isItemForCrossDock:bool=None, isASRSItem:bool=None):
        """(Generic method) This makes sure any resv.
        """
        dbRows = []
        dbRow = dict()
        noOfLpn = noOfLpn

        '''If resvLocns provided'''
        resvLocnRows = None
        if resvLocns is not None:
            assert noOfResvLocn == len(resvLocns), '<Data> Number of resv locns didnt match'
            resvLocnRows = []
            resvLocnRow = dict()
            resvLocnIds = []
            for i in resvLocns:
                locnId = DBLib().getLocnIdByLocnBrcd(i)
                resvLocnIds.append(locnId)
            resvLocnRow['LOCN_BRCD'] = resvLocns[0]
            resvLocnRow['LOCN_ID'] = resvLocnIds[0]
            resvLocnRows.extend([resvLocnRow.copy() for i in range(len(resvLocns))])
            for i in range(len(resvLocns)):
                resvLocnRows[i]['LOCN_BRCD'] = resvLocns[i]
                resvLocnRows[i]['LOCN_ID'] = resvLocnIds[i]
        else:
            noOfResvLocn = noOfResvLocn
            tpdRows = self.getTaskPathDefs2(taskPath)
            for i in range(len(tpdRows)):
                tpdDestWG = tpdRows[i]['DEST_WORK_GRP']
                resvLocnRows = self.getResvLocn(noOfLocn=noOfResvLocn, resvWG=tpdDestWG, resvWA=None,
                                                isPullZoneInAllocPrty=False, pullZone=pullZone, itemAllocType=itemAllocType,
                                                taskPath=taskPath, isResvWAInTPDDestWA=isResvWAInTPDDestWA,
                                                isASRSLocn=isASRSItem)
                if resvLocnRows is not None and len(resvLocnRows) == noOfResvLocn:
                    break
        assert resvLocnRows is not None and len(resvLocnRows) > 0, '<Data> Resv locn not found to create invn for putaway'

        itemUnitVol = None
        if itemBrcd is not None:
            itemId = self.getItemIdFromBrcd(itemBrcd=itemBrcd)
        else:
            itemRows = self.getItemsNotInAnyLocn(noOfItem=1,
                                                 # isPCKItem=isPCKItem,
                                                 consolInvnType=consolInvnType, isItemForCrossDock=isItemForCrossDock,
                                                 isASRSItem=isASRSItem)
            itemBrcd = itemRows[0]['ITEM_NAME']
            itemId = itemRows[0]['ITEM_ID']
            itemUnitVol = itemRows[0]['UNIT_VOLUME']
        dbRow['ITEM_NAME'] = itemBrcd
        dbRow['UNIT_VOLUME'] = itemUnitVol

        resvLocnId = resvLocnRows[0]['LOCN_ID']
        resvLocn = resvLocnRows[0]['LOCN_BRCD']
        dbRow['LOCN_BRCD'] = resvLocn
        dbRows.extend([dbRow.copy() for i in range(noOfLpn)])

        for i in range(noOfLpn):
            lpnBrcd = self.getNewILPNNum()
            lpnQtyTemp = lpnQty[i]
            DBAdmin._insertToResvInvnTables(self.schema, lpnBrcd=lpnBrcd, locnId=resvLocnId, lpnFacStat=30,
                                            itemId=itemId, itemBrcd=itemBrcd, qty=lpnQtyTemp)
            dbRows[i]['TC_LPN_ID'] = lpnBrcd
            dbRows[i]['ON_HAND_QTY'] = lpnQtyTemp

        return dbRows

    def getTaskIdByItemName(self, itemName: str, intType: int):
        sql = f"""select ic.item_name ,th.invn_need_type, th.task_id, th.curr_task_prty, th.stat_code from task_hdr th 
                 inner join item_cbo ic on th.item_id = ic.item_id 
                 where ic.item_name = '#ITEMNAME#' and invn_need_type = '#INTTYPE#' 
                 and th.create_date_time > sysdate - interval '30' minute order by th.task_id desc"""
        sql = sql.replace('#ITEMNAME#', itemName).replace('#INTTYPE#', str(intType))

        dbRow = DBService.fetch_row(sql, self.schema)
        taskId = dbRow.get('TASK_ID')

        return taskId

    def _createInvnForActvPick(self, itemBrcd: str = None,
                               # isPCKItem: bool = None, isSLPItem:bool=None, isWPItem:bool=None,
                               consolInvnType: ConsolInvnType = None, isTHMItem:bool=None,
                               noOfActvLocn: int = 1, actvQty: int = 5, maxInvQty: int = 15,
                               zone: Union[str, list] = None, area: list[str] = None, aisle:list[str]=None,
                               isCheckTaskPath: bool = None, intType: int = None,
                               actvWG: str = 'ACTV', actvWA: str = None, itemAllocType: list[str] = None, pickDetrmZone: list[str] = None, ignoreItems:list[str]=None,
                               isLocnConveyable:bool=None, isHazmatItem:bool=None, isPromoLocn:bool=None, isCrossDockItem:bool=None,
                               isPickLocnVLM:bool=None, isPickLocnAS:bool=None):
        """(Generic method) This makes sure any actv.
        """
        RuntimeXL.createThreadLockFile()
        try:
            dbRows = []
            noOfActvLocn = noOfActvLocn

            actvLocnRows = None
            if isPickLocnVLM or isPickLocnAS:
                actvLocnRows = self.getAutoActvLocn(noOfLocn=noOfActvLocn, actvWG='ACTV', isVLMLocn=isPickLocnVLM, isASLocn=isPickLocnAS)
            elif isCheckTaskPath:
                taskPath = TaskPath(intType=50)
                actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=actvWA, taskPath=taskPath, isActvWAInTPDCurrWA=True,
                                                            zone=zone, area=area, aisle=aisle, pickDetrmZone=pickDetrmZone,
                                                            # isPCKLocn=isPCKItem, isSLPLocn=isSLPItem, isWPLocn=isWPItem,
                                                            consolInvnType=consolInvnType,
                                                            isLocnConveyable=isLocnConveyable, isPromoLocn=isPromoLocn)
            assert actvLocnRows is not None and len(actvLocnRows) == noOfActvLocn, '<Data> New actv locns not found to create invn'

            '''Get item not in any locn'''
            itemRows = None
            if actvLocnRows is not None:
                if itemBrcd is not None:
                    itemRow = self.getItemDtlsFrom(orItemBrcd=itemBrcd)
                    itemRows = [itemRow]
                else:
                    itemRows = self.getItemsNotInAnyLocn(noOfItem=1,
                                                         # isPCKItem=isPCKItem, isWPItem=isWPItem,
                                                         consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                                         itemAllocType=itemAllocType, isHazmatItem=isHazmatItem, ignoreItems=ignoreItems,
                                                         isItemForCrossDock=isCrossDockItem, isVLMItem=isPickLocnVLM, isASItem=isPickLocnAS)
                if itemRows is not None:
                    dbRows.extend([itemRows[0].copy() for i in range(noOfActvLocn)])

            '''Insert actv invn'''
            if itemRows is not None:
                for i in range(noOfActvLocn):
                    dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=actvLocnRows[i])
                    dbRows[i]['MAX_INVN_QTY'] = maxInvQty
                    dbRows[i]['ON_HAND_QTY'] = actvQty
                    dbRows[i]['AVAIL_UNIT'] = actvQty

                    actvLocnId = actvLocnRows[i]['LOCN_ID']
                    itemId = itemRows[0]['ITEM_ID']

                    DBAdmin._insertToActvInvnTables(self.schema, locnId=actvLocnId, maxInvQty=maxInvQty, itemId=itemId,
                                                    qty=actvQty)

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _createInvnForActvPutwy(self, itemBrcd: str = None,
                                # isPCKItem: bool = None, isSLPItem:bool=None, isWPItem:bool=None,
                                consolInvnType: ConsolInvnType = None, isTHMItem:bool=None,
                                noOfActvLocn: int = 1, actvQty: int = 5, maxInvQty: int = 15,
                                zone: Union[str, list] = None, area: list[str] = None, aisle:list[str]=None, locnGrpAttrs:list=None,
                                taskPath:TaskPath=None, isActvWAInTPDDestWA:bool=None, isCheckTaskPath: bool = None, intType: int = None,
                                actvWG: str = None, actvWA: str = None, itemAllocType: list[str] = None, pickDetrmZone: list[str] = None,
                                isLocnConveyable:bool=None, isHazmatItem:bool=None, isCrossDockItem:bool=None,
                                isPutwyLocnVLM:bool=None, isPutwyLocnAS:bool=None, isASRSItem:bool=None,
                                isForPutawayToActv:bool=True, isFetchByMaxVol:bool=None):
        """(Generic method) This makes sure any actv.
        """
        RuntimeXL.createThreadLockFile()
        try:
            dbRows = []
            noOfActvLocn = noOfActvLocn

            actvLocnRows = None
            if isPutwyLocnVLM or isPutwyLocnAS:
                actvLocnRows = self.getAutoActvLocn(noOfLocn=noOfActvLocn, actvWG='ACTV', isVLMLocn=isPutwyLocnVLM, isASLocn=isPutwyLocnAS)
            else:
                taskPath = taskPath if isActvWAInTPDDestWA else TaskPath(intType=intType) if isCheckTaskPath else TaskPath(intType=1, destWG='ACTV')
                actvLocnRows = self.getEmptyManualActvLocn3(noOfLocn=noOfActvLocn, actvWG='ACTV', actvWA=actvWA, taskPath=taskPath, isActvWAInTPDDestWA=True,
                                                            zone=zone, area=area, aisle=aisle, locnGrpAttrs=locnGrpAttrs,
                                                            # isPCKLocn=isPCKItem, isSLPLocn=isSLPItem,
                                                            consolInvnType=consolInvnType,
                                                            pickDetrmZone=pickDetrmZone, isLocnConveyable=isLocnConveyable,
                                                            isForPutawayToActv=isForPutawayToActv)
            assert actvLocnRows is not None and len(actvLocnRows) == noOfActvLocn, '<Data> New actv locns not found to create invn'

            '''Get item not in any locn'''
            itemRows = None
            if actvLocnRows is not None:
                if itemBrcd is not None:
                    itemRow = self.getItemDtlsFrom(orItemBrcd=itemBrcd)
                    itemRows = [itemRow]
                else:
                    itemRows = self.getItemsNotInAnyLocn(noOfItem=1,
                                                         # isPCKItem=isPCKItem, isWPItem=isWPItem,
                                                         consolInvnType=consolInvnType, isTHMItem=isTHMItem,
                                                         itemAllocType=itemAllocType, isHazmatItem=isHazmatItem, isItemForCrossDock=isCrossDockItem,
                                                         isVLMItem=isPutwyLocnVLM, isASItem=isPutwyLocnAS, isASRSItem=isASRSItem,
                                                         isFetchByMaxVol=isFetchByMaxVol)
                if itemRows is not None:
                    dbRows.extend([itemRows[0].copy() for i in range(noOfActvLocn)])

            '''insert actv invn'''
            if itemRows is not None:
                for i in range(noOfActvLocn):
                    dbRows[i] = Commons.update_dict(curr_dict=dbRows[i], new_dict=actvLocnRows[i])
                    dbRows[i]['MAX_INVN_QTY'] = maxInvQty
                    dbRows[i]['ON_HAND_QTY'] = actvQty
                    dbRows[i]['AVAIL_UNIT'] = actvQty

                    actvLocnId = actvLocnRows[i]['LOCN_ID']
                    itemId = itemRows[0]['ITEM_ID']

                    DBAdmin._insertToActvInvnTables(self.schema, locnId=actvLocnId, maxInvQty=maxInvQty, itemId=itemId,
                                                    qty=actvQty)

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def _buildQueryForGetItemsForActvPick(self, noOfItem: int,
                                          # isPCKItem: bool = None, isSLPItem:bool=None, isWPItem:bool=None,
                                          consolInvnType: ConsolInvnType = None, isTHMItem:bool=None,
                                          actvWG: str = 'ACTV', actvWA: Union[str, list] = None, zone: Union[str, list] = None, area: list[str] = None, aisle:list[str]=None,
                                          taskPath: TaskPath = None, isActvWAInTPDDestWA: bool = None,
                                          minAvailCap: int = None, minAvailUnit: int = None, availUnit: int = None,
                                          minOnHand: int = None, onHand: int = None,
                                          isCcPending: bool = None, isItemIn1Actv: bool = None, isItemNotInResv: bool = None,
                                          itemAllocType: list[str] = None, pickDetrmZone: list[str] = None, ignoreActvLocn: list[str] = None,
                                          isLocnConveyable:bool=None, isHazmatItem:bool=None, isCrossDockItem:bool=None,
                                          isPromoLocn:bool=None, isPickLocnVLM:bool=None, isPickLocnAS:bool=None):
        """(Generic method) This makes sure any actv.
        Build get item query by provided diff params
        """
        sql = f"""select /*+ PARALLEL(wi,8) */ lh.locn_brcd,lh.locn_id,lh.work_grp,lh.work_area,lh.zone,lh.area,ic.item_name,pld.max_invn_qty,
                 wi.on_hand_qty - wi.wm_allocated_qty + wi.to_be_filled_qty as avail_unit,su.size_uom,wi.* 
                 from wm_inventory wi 
                 inner join locn_hdr lh on wi.location_id=lh.locn_id
                 inner join item_cbo ic on wi.item_id=ic.item_id
                 inner join item_facility_mapping_wms ifm on wi.item_id=ifm.item_id 
                 left outer join pick_locn_dtl pld on wi.location_id=pld.locn_id and wi.item_id=pld.item_id
                 inner join size_uom su on ic.base_storage_uom_id=su.size_uom_id and su.size_uom='EACH'
                 where ic.item_id in (select item_id from wm_inventory where locn_class='A')
                 and ic.item_id in (select item_id from pick_locn_dtl)
                 and ic.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion='1')
                 and lh.work_grp='{actvWG}' and lh.sku_dedctn_type='P' 
                 #CONDITION#
                 order by lh.locn_pick_seq asc
                 offset 0 rows fetch next {noOfItem} rows only
              """
        sqlCond = ''
        # if isPCKItem:
        #     sqlCond += " \n and ifm.slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM')"
        #     sqlCond += " \n and lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66')"
        # if isSLPItem:
        #     sqlCond += " \n and lh.travel_aisle='Y' and lh.zone in ('63','64','65','66')"
        # if isWPItem:
        #     sqlCond += " \n and ifm.slot_misc_1 in ('NW','NTH','NSE','CHT','SER','THM')"
        #     sqlCond += " \n and lh.travel_aisle='Y'"
        #     sqlCond += " \n and ((lh.zone='40' and lh.aisle not in ('AA','BB','CC','DD','HH','II')) or lh.zone not in ('40','63','64','65','66'))"
        if isTHMItem:
            sqlCond += " \n and ifm.slot_misc_1 in ('THM')"
            sqlCond += " \n and lh.travel_aisle='Y'"

        if consolInvnType is not None:
            if consolInvnType == ConsolInvnType.PCK:
                sqlCond += " \n and ifm.slot_misc_1 not in ('NW','NTH','NSE','CHT','SER','THM')" + f" --{consolInvnType.value}"
                sqlCond += " \n and lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66')" + f" --{consolInvnType.value}"
            elif consolInvnType == ConsolInvnType.SLP:
                sqlCond += " \n and lh.travel_aisle='Y' and lh.zone in ('63','64','65','66')" + f" --{consolInvnType.value}"
            elif consolInvnType == ConsolInvnType.WP:
                sqlCond += " \n and ifm.slot_misc_1 in ('NW','NTH','NSE','CHT','SER','THM')" + f" --{consolInvnType.value}"
                sqlCond += " \n and lh.travel_aisle='Y'" + f" --{consolInvnType.value}"
                sqlCond += " \n and ((lh.zone='40' and lh.aisle not in ('AA','BB','CC','DD','HH','II')) or lh.zone not in ('40','63','64','65','66'))" + f" --{consolInvnType.value}"

        if isCrossDockItem:
            sqlCond += " \n and ifm.slot_misc_2 in ('Active-NS', 'Unreleased')"
        else:
            sqlCond += " \n and ifm.slot_misc_2 not in ('Active-NS', 'Unreleased')"

        if actvWA is None:
            if isActvWAInTPDDestWA:
                destWA = self._getDestWAFromTaskPath2(taskPath)
                actvWA = destWA
        actvWA = self.removeSpecialCharFromTaskPathVals(actvWA)
        if actvWA is not None:
            sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(actvWA)

        final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isVLMLocn=isPickLocnVLM, isASLocn=isPickLocnAS, providedVal=zone)
        if final_zone is not None:
            sqlCond += f" \n and lh.zone in {final_zone}"
        elif final_avoidZone is not None:
            sqlCond += f" \n and lh.zone not in {final_avoidZone}"

        if area is not None:
            sqlCond += " \n and lh.area in " + Commons.get_tuplestr(area)
        if aisle is not None:
            sqlCond += " \n and lh.aisle in " + Commons.get_tuplestr(aisle)

        if isItemIn1Actv:
            sqlCond += " \n and ic.item_id in (select item_id from wm_inventory where locn_class='A' group by item_id having count(item_id)=1)"
        if isItemNotInResv:
            sqlCond += " \n and ic.item_id not in (select item_id from wm_inventory where locn_class='R')"
        if isCcPending is not None:
            ccPendingFlag = 'Y' if isCcPending else 'N'
            sqlCond += " \n and lh.cycle_cnt_pending = '" + ccPendingFlag + "'"

        if minAvailCap is not None:
            sqlCond += f" \n and (wi.on_hand_qty + wi.to_be_filled_qty + {minAvailCap}) <= pld.max_invn_qty"
        if minAvailUnit is not None:
            sqlCond += f" \n and (wi.on_hand_qty - wi.wm_allocated_qty) >= {minAvailUnit}"
        if availUnit is not None:
            sqlCond += f" \n and (wi.on_hand_qty - wi.wm_allocated_qty) = {availUnit}"
        if minOnHand is not None:
            sqlCond += f" \n and wi.on_hand_qty >= {minOnHand}"
        if onHand is not None:
            sqlCond += f" \n and wi.on_hand_qty = {onHand}"
        if isHazmatItem:
            sqlCond += " \n and ic.un_number_id in (select un_number_id from un_number)"

        final_itemAllocType, final_avoidItemAllocType = self._decide_ifmw_allocType_forItemType(isVLMItem=isPickLocnVLM, isASItem=isPickLocnAS, isPromoItem=isPromoLocn,
                                                                                                providedVal=itemAllocType, defaultVal='STD')
        if final_itemAllocType is not None:
            sqlCond += f" \n and ifm.alloc_type in {final_itemAllocType}"

        final_pickDetrmZone, final_avoidPickDetrmZone = self._decide_lh_pickDetrmZone_forLocnType(isPromoLocn=isPromoLocn, providedVal=pickDetrmZone)
        if final_pickDetrmZone is not None:
            sqlCond += f" \n and lh.pick_detrm_zone in {final_pickDetrmZone}"

        if isLocnConveyable:
            sqlCond += " \n and lh.travel_aisle='Y'"
        if ignoreActvLocn is not None:
            sqlCond += " \n and lh.locn_brcd not in " + Commons.get_tuplestr(ignoreActvLocn)

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='A'
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', sqlCond)

        return sql

    def decideConsolAttrForOrder2(self, doConsolRuleData: OrdConsRuleData):
        """Check outbound rules for order consol and find the consol attr for 1 order
        """
        order = doConsolRuleData.ORDER
        items = doConsolRuleData.ITEMS
        locns = doConsolRuleData.LOCNS

        consolAttrList = []

        # For Mark-For order
        ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='M' and TC_ORDER_ID = '{order}' "
        dbRow = DBService.fetch_row(ruleQ1, self.schema)
        isMarkForOrderEligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

        if isMarkForOrderEligible:
            consolAttrList.append('MarkFor')

        '''Rule P1'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P1')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='O' and TC_ORDER_ID = '{order}' "
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP1Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP1Eligible:
                consolAttrList.append('BIG')

        '''Rule P2'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P2')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='U' and TC_ORDER_ID = '{order}' "
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP2Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP2Eligible:
                consolAttrList.append('NXP')

        '''Rule P3'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P3')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='N' and TC_ORDER_ID = '{order}'"
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP3Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP3Eligible:
                for locnsList in locns:
                    for locnBrcd in locnsList:
                        if locnBrcd is not None:
                            ruleQ2 = f"""select lh.locn_brcd from locn_hdr lh
                                        where lh.travel_aisle='Y' and lh.zone='40' and lh.aisle in ('AA','BB','CC','DD','HH','II')
                                        and lh.locn_brcd = '{locnBrcd}'"""
                            dbRow = DBService.fetch_row(ruleQ2, self.schema)
                            isP3Eligible = dbRow is not None and locnBrcd == dbRow.get('LOCN_BRCD')

                            if isP3Eligible:
                                consolAttrList.append('PLT')

        '''Rule P4'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P4')

            ruleQ1 = f"""SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='N' and TC_ORDER_ID = '{order}' """
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP4Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP4Eligible:
                for locnsList in locns:
                    for locnBrcd in locnsList:
                        if locnBrcd is not None:
                            ruleQ2 = f"""select lh.locn_brcd from locn_hdr lh
                                         where lh.travel_aisle='Y' and lh.zone in('63','64','65','66') 
                                         and lh.locn_brcd = '{locnBrcd}' """
                            dbRow = DBService.fetch_row(ruleQ2, self.schema)
                            isP4Eligible = dbRow is not None and locnBrcd == dbRow.get('LOCN_BRCD')

                            if isP4Eligible:
                                consolAttrList.append('SLP')

        '''Rule P5'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P5')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='N' and TC_ORDER_ID = '{order}' "
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP5Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP5Eligible:
                for i in range(len(items)):
                    ruleQ2 = f"""select ic.item_name from item_facility_mapping_wms ifm inner join item_cbo ic on
                                             ic.item_id=ifm.item_id where ifm.slot_misc_1 in ('NW','NTH','NSE','CHT','SER','THM')
                                             and ic.item_name = '{items[i]}' """
                    dbRow = DBService.fetch_row(ruleQ2, self.schema)
                    isP5Eligible = dbRow is not None and items[i] == dbRow.get('ITEM_NAME')

                    if isP5Eligible:
                        for locnBrcd in locns[i]:
                            if locnBrcd is not None:
                                ruleQ3 = f"""select lh.locn_brcd from locn_hdr lh
                                                        where lh.travel_aisle='Y' and lh.locn_brcd = '{locnBrcd}'  """
                                dbRow = DBService.fetch_row(ruleQ3, self.schema)
                                isP5Eligible = dbRow is not None and locnBrcd == dbRow.get('LOCN_BRCD')

                                if isP5Eligible:
                                    consolAttrList.append('WP')

        '''Rule P6'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P6')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE='N' and TC_ORDER_ID = '{order}' "
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP6Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP6Eligible:
                for locnsList in locns:
                    for locnBrcd in locnsList:
                        if locnBrcd is not None:
                            ruleQ2 = f"""select lh.locn_brcd from locn_hdr lh where lh.work_grp ='ACTV' and lh.work_area like 'VM%'
                                        and lh.locn_brcd = '{locnBrcd}' """
                            dbRow = DBService.fetch_row(ruleQ2, self.schema)
                            isP6Eligible = dbRow is not None and locnBrcd == dbRow.get('LOCN_BRCD')

                            if isP6Eligible:
                                consolAttrList.append('WP')

        '''Rule P7'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P7')

            isP7Eligible = True

            for locnsList in locns:
                for locnBrcd in locnsList:
                    if locnBrcd is not None:
                        ruleQ1 = f"""select lh.locn_brcd from locn_hdr lh where (lh.travel_aisle is null or lh.travel_aisle='N') 
                                    and lh.locn_brcd = '{locnBrcd}' """
                        dbRow = DBService.fetch_row(ruleQ1, self.schema)
                        isP7Eligible = dbRow is not None and locnBrcd == dbRow.get('LOCN_BRCD')

                        if isP7Eligible:
                            consolAttrList.append('PLT')

        '''Rule P8'''
        if len(consolAttrList) < len(items):
            printit('Order consol OB rule check', 'P8')

            ruleQ1 = f"SELECT TC_ORDER_ID FROM ORDERS WHERE ORDER_TYPE is NOT NULL and TC_ORDER_ID = '{order}' "
            dbRow = DBService.fetch_row(ruleQ1, self.schema)
            isP8Eligible = dbRow is not None and order == dbRow.get('TC_ORDER_ID')

            if isP8Eligible:
                consolAttrList.append('PCK')

        printit(f">>> Order {order}, consol attrs {consolAttrList}")
        return consolAttrList

    def getEmptyConsLocnByRuleData(self, ordConsRuleDataList: list[OrdConsRuleData]):
        """Get consol attr for all orders.
        Get/Clear empty consol locn for all orders.
        """
        ordConsAttrDict = {}  # {Ord: ConsolAttr}
        ordConsLocnDict = {}  # {Ord: ConsolLocn}
        consLocnList = []

        for ordConsRuleData in ordConsRuleDataList:
            '''Get consol attr per order'''
            consolAttr = self.decideConsolAttrForOrder2(ordConsRuleData)
            ordConsAttrDict[ordConsRuleData.ORDER] = consolAttr

        printit(f">>> Order & consol attrs dict {ordConsAttrDict}")

        '''Get total count per distinct consol attr'''
        from collections import Counter
        # countr = Counter(ordConsAttrDict.values())
        # unqConsAttrs = list(countr.keys())  # Get all unique consol attrs in a list
        ordConsvalue = ordConsAttrDict.values()
        consolLocnList = [consolLocn for sublist in ordConsvalue for consolLocn in sublist]
        countr = Counter(consolLocnList)
        unqConsAttrs = list(countr.keys())  # Get all unique consol attrs in a list
        for i in unqConsAttrs:
            consLocnIds, consLocns = self.getEmptyConsolLocn2(noOfLocns=countr.get(i), consolAttr=i, isClearIfNotFound=True)
            consLocnList.extend(consLocns)

        '''Get consol locn by consol attr (clear consol locn)'''
        # consolLocn = self.getEmptyConsolLocn()
        # ordConsLocnDict[ordConsRuleData.ORDER] = consolLocn

        return consLocnList

    def _buildQueryForGetActvLocn(self, noOfLocn:int, actvWG:str= 'ACTV', actvWA:Union[str, list]=None, taskPath:TaskPath=None, isActvWAInTPDCurrWA:bool=None, isActvWAInTPDDestWA:bool=None,
                                  zone:Union[str, list]=None, area:list[str]=None, aisle:list[str]=None, locnGrpAttrs:list=None,
                                  pickDetrmZone:list[str]=None, locnType:str=None, isReplenElig:bool=None,
                                  # isPCKLocn:bool=None, isSLPLocn:bool=None, isWPLocn:bool=None,
                                  consolInvnType: ConsolInvnType = None, isTHMLocn:bool=None,
                                  isLocnWithItem:bool=None, isLocnHas1item:bool=None, isLocnWithNoItem:bool=None,
                                  isLocnConveyable:bool=None, isForPutawayToActv:bool=None, isCreatedByAutom:bool=None,
                                  isPromoLocn:bool=None, isVLMLocn:bool=None, isASLocn:bool=None):
        """(Generic method) This makes sure any actv.
        """
        sql = f"""select /*+ PARALLEL(lh,8) */ ic.item_name,lh.* from locn_hdr lh 
                    left outer join wm_inventory wi on lh.locn_id=wi.location_id
                    left outer join item_cbo ic on wi.item_id=ic.item_id 
                    where lh.locn_class='A' and lh.work_grp='{actvWG}' 
                    #CONDITION#
                    offset 0 rows fetch next {noOfLocn} rows only"""
        sqlCond = ''

        if actvWA is None:
            if isActvWAInTPDCurrWA:
                actvWA = currWAs = self._getCurrWAFromTaskPath2(taskPath)
            elif isActvWAInTPDDestWA:
                actvWA = destWAs = self._getDestWAFromTaskPath2(taskPath)
        actvWA = self.removeSpecialCharFromTaskPathVals(actvWA)
        if actvWA is not None:
            sqlCond += " \n and lh.work_area in " + Commons.get_tuplestr(actvWA)

        if isForPutawayToActv:
            sqlCond += """ \n and lh.locn_id in (select plh.locn_id from pick_locn_hdr plh inner join putwy_method_prty pmp on plh.putwy_type=pmp.putwy_type
                                        inner join sys_code sc1 on pmp.putwy_type=sc1.code_id inner join sys_code sc2 on sc2.code_id=pmp.putwy_method and sc2.code_desc='Direct to active')"""

        if isLocnWithItem:
            sqlCond += " \n and lh.locn_id in (select location_id from wm_inventory where location_id is not null)"
        if isLocnWithNoItem:
            sqlCond += """ \n and lh.locn_id not in (select location_id from wm_inventory where location_id is not null)
                                            and lh.locn_id not in (select locn_id from pick_locn_dtl where locn_id is not null)"""
        if isCreatedByAutom:
            sqlCond += " \n and lh.locn_id in (select locn_id from pick_locn_dtl where user_id='AUTOMATION')"
        if isLocnHas1item:
            sqlCond += " \n and lh.locn_id in (select location_id from wm_inventory where location_id is not null group by location_id having count(location_id)=1)"

        if locnGrpAttrs:
            sqlCond += f" \n and lh.locn_id in (select lg.locn_id from locn_grp lg where lg.grp_attr in {Commons.get_tuplestr(locnGrpAttrs)})"

        final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType(isVLMLocn=isVLMLocn, isASLocn=isASLocn)
        if final_locnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
        elif final_avoidLocnBrcd is not None:
            sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

        final_zone, final_avoidZone = self._decide_lh_zone_forLocnType(isVLMLocn=isVLMLocn, isASLocn=isASLocn, providedVal=zone)
        if final_zone is not None:
            sqlCond += f" \n and lh.zone in {final_zone}"
        elif final_avoidZone is not None:
            sqlCond += f" \n and lh.zone not in {final_avoidZone}"

        if area is not None:
            sqlCond += " \n and lh.area in " + Commons.get_tuplestr(area)
        if aisle is not None:
            sqlCond += " \n and lh.area in " + Commons.get_tuplestr(aisle)

        # if isPCKLocn:
        #     sqlCond += " \n and lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66')"
        # if isSLPLocn:
        #     sqlCond += " \n and lh.travel_aisle='Y' and lh.zone in ('63','64','65','66')"
        # if isWPLocn:
        #     sqlCond += " \n and lh.travel_aisle='Y'"
        #     sqlCond += " \n and ((lh.zone = '40' and lh.aisle not in ('AA','BB','CC','DD','HH','II')) or lh.zone not in ('40','63','64','65','66')) "
        if isTHMLocn:
            sqlCond += " \n and lh.travel_aisle='Y'"

        if consolInvnType is not None:
            if consolInvnType == ConsolInvnType.PCK:
                sqlCond += " \n and lh.travel_aisle='Y' and lh.zone not in ('40','VM','63','64','65','66')" + f" --{consolInvnType.value}"
            elif consolInvnType == ConsolInvnType.SLP:
                sqlCond += " \n and lh.travel_aisle='Y' and lh.zone in ('63','64','65','66')" + f" --{consolInvnType.value}"
            elif consolInvnType == ConsolInvnType.WP:
                sqlCond += " \n and lh.travel_aisle='Y'" + f" --{consolInvnType.value}"
                sqlCond += " \n and ((lh.zone = '40' and lh.aisle not in ('AA','BB','CC','DD','HH','II')) or lh.zone not in ('40','63','64','65','66')) " + f" --{consolInvnType.value}"

        final_pickDetrmZone, final_avoidPickDetrmZone = self._decide_lh_pickDetrmZone_forLocnType(isPromoLocn=isPromoLocn, providedVal=pickDetrmZone)
        if final_pickDetrmZone is not None:
            sqlCond += f" \n and lh.pick_detrm_zone in {final_pickDetrmZone}"

        if isLocnConveyable:
            sqlCond += " \n and lh.travel_aisle='Y'"
        if locnType is not None:
            sqlCond += f" \n and lh.sku_dedctn_type='{locnType}'"
        else:
            sqlCond += f" \n and lh.sku_dedctn_type='P'"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='A'
                                            and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

        sql = sql.replace('#CONDITION#', sqlCond)

        return sql

    def getSlotAndOlpnByPickSeqFromCartNum(self, cartId: str):
        """Returns list of slots and list of olpns
        """
        sql = f"""select td.* from task_dtl td inner join locn_hdr lh on lh.locn_id = td.pull_locn_id
                 where task_cmpl_ref_nbr='#CARTID#' order by locn_pick_seq asc"""
        sql = sql.replace('#CARTID#', cartId)

        dbRows = DBService.fetch_rows(sql, self.schema)

        slots = []
        olpns = []
        for i in range(len(dbRows)):
            slots.append(str(dbRows[i]['SLOT_NBR']))
            olpns.append(str(dbRows[i]['CARTON_NBR']))

        return slots, olpns

    def getItemInvnInManualResvLocn(self, noOfItem: int):
        """Get ilpn with no lock in resv
        """
        RuntimeXL.createThreadLockFile()
        try:
            dbRows = []

            sql = f"""select distinct ic.item_name, lh.locn_brcd, wi.* from wm_inventory wi
                     inner join lpn on wi.tc_lpn_id = lpn.tc_lpn_id and lpn.inbound_outbound_indicator='I'
                     inner join locn_hdr lh on wi.location_id = lh.locn_id 
                     inner join item_cbo ic on wi.item_id = ic.item_id
                     inner join size_uom su on ic.base_storage_uom_id = su.size_uom_id and su.size_uom = 'EACH'
                     where wi.locn_class='R' and lpn.single_line_lpn='Y' and lpn_facility_status=30 
                     and wi.tc_lpn_id not in (select tc_lpn_id from lpn_lock where tc_lpn_id is not null)
                     and on_hand_qty>0 and wm_allocated_qty=0 and to_be_filled_qty=0
                     and wi.item_id in (select item_id from item_facility_mapping_wms ifm where alloc_type='STD')
                     and wi.item_id not in (select item_id from item_facility_mapping_wms where mark_for_deletion = '1')
                     #CONDITION#
                     offset 0 rows fetch next {noOfItem} rows only"""

            sqlCond = ''

            final_locnBrcd, final_avoidLocnBrcd = self._decide_lh_locnBrcd_forLocnType()
            if final_locnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd in {final_locnBrcd}"
            elif final_avoidLocnBrcd is not None:
                sqlCond += f" \n and lh.locn_brcd not in {final_avoidLocnBrcd}"

            final_refField10 = self._decide_ic_refField10_forItemType()
            if final_refField10 is not None:
                sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
            else:
                sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

            '''Exclude runtime thread locns'''
            threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
            if threadLocns is not None:
                sqlCond += " \n and lh.locn_brcd not in " + threadLocns

            '''Exclude runtime thread items'''
            threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
            if threadItems is not None:
                sqlCond += f" \n and ic.item_name not in " + threadItems
                sqlCond += f""" \n and lh.locn_id not in (select location_id from wm_inventory where locn_class='R'
                                                and item_id in (select item_id from item_cbo where item_name in {threadItems}))"""

            sql = sql.replace('#CONDITION#', sqlCond)

            dbRows = DBService.fetch_rows(sql, self.schema)
            assert len(dbRows) >= noOfItem and dbRows[0]['TC_LPN_ID'] is not None, f'<Data> {noOfItem} no. of items present in resv not found ' + sql

            '''Print data'''
            for i in range(noOfItem):
                self._logDBResult(dbRows[i], ['ITEM_NAME', 'LOCN_BRCD', 'ON_HAND_QTY', 'TC_LPN_ID'])

            '''Update runtime thread data file'''
            itemsAsCsv = ','.join(i['ITEM_NAME'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.ITEMS, itemsAsCsv)

            '''Update runtime thread data file'''
            locnsAsCsv = ','.join(i['LOCN_BRCD'] for i in dbRows)
            RuntimeXL.updateThisAttrForThread(RuntimeAttr.LOCNS, locnsAsCsv)
        finally:
            RuntimeXL.removeThreadLockFile()

        return dbRows

    def getOLPNs(self, noOfOlpn: int, lpnFacStat: int = None, noOfItem: int = None, minLpnQty:int=1, qaFlag: int = None):
        """Get olpns.
        Note: Don't use lpn.* in query, this returns ITEM_NAME None
        """
        sql = f"""select distinct l.qa_flag, l.tc_lpn_id, ic.item_name, ld.size_value,l.tc_order_id
                 --, l.created_source, l.last_updated_source, ld.created_source, ld.last_updated_source
                 from lpn l
                 inner join lpn_detail ld on l.lpn_id = ld.lpn_id
                 inner join item_cbo ic on ld.item_id = ic.item_id
                 where l.inbound_outbound_indicator = 'O' and ld.size_value >= {minLpnQty}
                 #CONDITION# 
                 order by l.tc_lpn_id"""
        sqlCond = ""
        if lpnFacStat is not None:
            sqlCond += " \n and l.lpn_facility_status = " + str(lpnFacStat)
        if noOfItem is not None:
            sqlCond += f" \n and l.lpn_id in (select lpn_id from lpn_detail where size_value >= {minLpnQty} group by lpn_id having count(lpn_id)={noOfItem})"

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and l.lpn_id not in (select lpn_id from lpn_detail where item_name not in {threadItems})"

        sql = sql.replace('#CONDITION#', sqlCond)
        noOfRowsToFetch = noOfOlpn * noOfItem if noOfItem is not None else noOfOlpn
        dbRows = DBService.fetch_only_rows(sql, noOfRowsToFetch, self.schema)

        assert dbRows and len(dbRows) >= noOfItem and dbRows[0]['TC_LPN_ID'] is not None, f"<Data> {noOfItem} no. of olpn not found " + sql

        '''Update qa_flag'''
        if 'true' in self.IS_ALLOW_UPDATE_OLPN:
            if qaFlag is not None:
                olpnSet = set()
                for i in dbRows:
                    olpnSet.add(i['TC_LPN_ID'])

                sql = f"""update lpn set qa_flag = '#QAFLAG#' where tc_lpn_id in #LPNS#"""
                sql = sql.replace('#QAFLAG#', str(qaFlag)).replace('#LPNS#', Commons.get_tuplestr(olpnSet))
                DBService.update_db(sql, self.schema)
        else:
            assert False, f"Olpn update not allowed. Test manually"

        for i in range(len(dbRows)):
            self._logDBResult(dbRows[i], ['TC_LPN_ID', 'ITEM_NAME', 'SIZE_VALUE'])

        return dbRows

    def getAllDtlsFromOlpnForPackSeq(self, oLpn:str):
        """Get item, locnbrcd in a seq for packing the olpn
        """
        sql = f"""select ic.item_name,lh.locn_brcd from alloc_invn_dtl aid 
                 inner join locn_hdr lh on lh.locn_id = aid.pull_locn_id
                 inner join item_cbo ic on ic.item_id = aid.item_id
                 where aid.task_cmpl_ref_nbr = '#OLPNNUM#' order by aid.carton_seq_nbr 
              """
        sql = sql.replace('#OLPNNUM#', str(oLpn))

        dbRows = DBService.fetch_rows(sql,self.schema)

        return dbRows

    def _printWaveTemplateConfig(self, template: str):
        """Get wave configs: alloc type and processng type
        """
        sql = f"""select swp.wave_desc,wp.wave_desc as dtl_wave_desc,wp.alloc_type,wpt.wave_proc_type_desc,wp.force_wpt 
                 from wave_parm wp inner join wave_proc_type wpt on wp.wave_proc_type = wpt.wave_proc_type
                 inner join ship_wave_parm swp on swp.wave_parm_id = wp.wave_parm_id
                 where wp.rec_type='T' and swp.wave_desc = '#WAVETEMPLATE#'
              """
        sql = sql.replace('#WAVETEMPLATE#', str(template))

        dbRow = DBService.fetch_row(sql, self.schema)

        printit(f"^^^ Curr wave config for {template} {dbRow}")
        # return dbRow

    def _presetLTRRuleWithLocnRange(self, fromLocn: str, toLocn: str, cutOffPercent:int=50, ruleDesc='AUTOMATION'):
        fromLocnId = self.getLocnIdByLocnBrcd(fromLocn)
        toLocnId = self.getLocnIdByLocnBrcd(toLocn)

        sql = f"""update lean_time_repl_parm set from_locn='{fromLocnId}', to_locn='{toLocnId}'
                ,repl_type='0', repl_locn_class='A', cut_off_prcnt='{cutOffPercent}', item_id=null, item_name=null 
                where parm_desc='{ruleDesc}' """
        # sql = sql.replace('#FROMLOCN#', fromLocnId).replace('#TOLOCN#', toLocnId)

        DBService.update_db(sql, self.schema)

    def _updateLTRRuleWithItem(self, itemBrcd: str, cutOffPercent: int = 50, ruleDesc='AUTOMATION'):
        itemId = self.getItemIdFromBrcd(itemBrcd)

        sql = f"""update lean_time_repl_parm set repl_type='1', repl_locn_class=null, cut_off_prcnt='{cutOffPercent}',
                from_locn=null, to_locn=null, item_id='{itemId}', item_name='{itemBrcd}' 
                where parm_desc='{ruleDesc}' """
        DBService.update_db(sql, self.schema)
        self.logger.info(f"Cut off percent for item {itemBrcd} is set to {cutOffPercent}%")

    def getPriorLocn(self, currLocn: str):
        """"""
        sql = f"""select * from locn_hdr where locn_brcd < '{currLocn}' and locn_class = 'A' order by locn_brcd desc
                 offset 0 rows fetch next 1 rows only
              """

        dbRow = DBService.fetch_row(sql, self.schema)

        priorLocn = dbRow.get('LOCN_BRCD')

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            assert priorLocn not in threadLocns, f"<Data> To be excluded prior locn {priorLocn} found for {currLocn}. Test manually"

        return priorLocn

    def getAllAllocatedLpnsFromAllocInvDtl(self, waveNum: str, item: str, intType: int = None):
        """Get all allocated ilpns from allocInvnDtl
        """
        sql = f"""select aid.cntr_nbr from alloc_invn_dtl aid, item_cbo ic 
                 where aid.item_id = ic.item_id and aid.task_genrtn_ref_nbr='{waveNum}' and ic.item_name='{item}' 
                 --and aid.invn_need_type='1'
                 #CONDITIONS#
                 order by aid.alloc_invn_dtl_id asc 
              """
        sqlCond = ''
        if intType is not None:
            sqlCond += f" \n and aid.invn_need_type='{intType}'"

        sql = sql.replace('#CONDITIONS#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)

        allCntrNums = [i['CNTR_NBR'] for i in dbRows]
        self.logger.info(f"All allocated ilpns {allCntrNums}")

        return dbRows

    def rearrangeLocnsByOrder(self, locnBrcd: list[str]):
        """"""
        sql = f"""select locn_brcd from locn_hdr where  locn_brcd in #LOCN_BRCD# order by work_area, locn_pick_seq"""
        sql = sql.replace('#LOCN_BRCD#', Commons.get_tuplestr(locnBrcd))

        dbRows = DBService.fetch_rows(sql, self.schema)

        return dbRows

    def rearrangeLocnsByLocnOrder(self, locnBrcd: list[str]):
        """"""
        sql = f"""select locn_brcd from locn_hdr where locn_brcd in #LOCN_BRCD# order by locn_brcd asc"""
        sql = sql.replace('#LOCN_BRCD#', Commons.get_tuplestr(locnBrcd))

        dbRows = DBService.fetch_rows(sql, self.schema)
        orderedLocns = [i['LOCN_BRCD'] for i in dbRows]
        self.logger.info(f"Orders locns {orderedLocns}")

        return orderedLocns

    def _presetWMRuleStatus(self, ruleId: str, statCode: int = None):
        """Update the rule check box for respective rules.
        statcode 0 means enabled and statcode 90 means disabled
        """
        sql = f"""update rule_hdr set stat_code='#STATCODE#' where rule_id='#RULEID#'"""
        sql = sql.replace('#STATCODE#', str(statCode)).replace('#RULEID#', str(ruleId))

        DBService.update_db(sql, self.schema)

    def getPrintRequestor(self, codeDesc: str):
        sql = f"""select lpr.name from lrf_prt_requestor lpr inner join lrf_prt_queue_service lpqs on lpqs.prt_q_id = lpr.prt_reqstr_id 
                 inner join sys_code sc on sc.rec_type = 'S' and sc.code_type = '507'
                 and sc.code_desc = '#CODEDESC#' and sc.code_id = lpqs.prt_serv_type 
                 order by lpr.created_dttm desc 
              """
        sql = sql.replace('#CODEDESC#', str(codeDesc))

        dbRows = DBService.fetch_rows(sql, self.schema)

        printReqstr = str(dbRows[0].get('NAME'))
        return printReqstr

    def assertLRFReport(self, printReqstr: str):
        sql = f"""select lr.report_id, lr.report_name, lr.user_id, lr.modified_dttm,lr.rpt_outpt_path, lr.prt_reqstr
                 ,lrd.description, lrd.type, lrd.subcategory, lps.description as print_status,lps.stat_code 
                 from lrf_report lr inner join lrf_report_def lrd on lrd.report_def_id = lr.report_def_id 
                 inner join lrf_print_status lps on lr.print_status = lps.stat_code  
                 --where lr.RPT_OUTPT_PATH like '%XXX%'
                 where lr.prt_reqstr = '#PRINTER#'
                 and lr.created_dttm > sysdate - interval '5' minute 
                 order by lr.report_id desc 
              """
        sql = sql.replace('#PRINTER#', str(printReqstr))

        dbRow = DBService.fetch_row(sql, self.schema)

        assert dbRow is not None, '<LRF> No LRF record is found for the printer' + str(printReqstr)

    def invnAdjustInAutoLocnVLM(self, i_item: str, i_qty: int, i_adjOperator: str, i_vlmLocn: str, i_locnId: str,
                                o_newQty: int):
        """Increase/decrease the VLM inventory for an item
        """
        msgId = self._insertInvnAdjMsgFromAutoLocnVLM(item=i_item, adjQty=i_qty, adjOperator=i_adjOperator,
                                                      vlmLocn=i_vlmLocn, locnId=i_locnId)
        '''Validation'''
        self.assertCLEndPointQueueStatus(msgId=msgId, o_status=5)
        self.assertWMInvnDtls(i_itemBrcd=i_item, i_locn=i_vlmLocn, o_onHandQty=o_newQty)

    def _cancelTasks(self, itemId: str):
        """"""
        if 'true' in self.IS_ALLOW_CANCEL_TASK:
            '''Task dtl'''
            sql = f"""select * from task_dtl where item_id='{itemId}' and stat_code not in ('90','99')"""

            taskDtlRows = DBService.fetch_rows(sql, self.schema)
            foundTaskDtlToCancel = True if len(taskDtlRows) > 0 else False

            if foundTaskDtlToCancel:
                taskDtlSql = f"""update task_dtl set stat_code='99',user_id='AUTOMATION' where item_id='{itemId}' and stat_code not in ('90','99')"""
                DBService.update_db(taskDtlSql, self.schema)

            '''Task hdr'''
            sql = f"""select * from task_hdr where item_id='{itemId}' and stat_code not in ('90','99')"""

            taskHdrRows = DBService.fetch_rows(sql, self.schema)
            foundTaskHdrToCancel = True if len(taskHdrRows) > 0 else False

            if foundTaskHdrToCancel:
                taskHdrSql = f"""update task_hdr set stat_code='99',user_id='AUTOMATION' where item_id='{itemId}' and stat_code not in ('90','99')"""
                DBService.update_db(taskHdrSql, self.schema)
        else:
            assert False, "Task cancel not allowed. Test manually"

    # def _cancelTasks_new(self, itemId: str):
    #     """"""
    #     '''Task dtl'''
    #     sql = f"""select td.task_dtl_id,td.pull_locn_id,lh.locn_brcd pull_locn,td.dest_locn_id,lh2.locn_brcd dest_locn from task_dtl td
    #                 left outer join locn_hdr lh on td.pull_locn_id=lh.locn_id
    #                 left outer join locn_hdr lh2 on td.dest_locn_id=lh2.locn_id
    #                 where item_id='{itemId}' and stat_code not in ('90','99')"""
    #
    #     taskDtlRows = DBService.fetch_rows(sql, self.schema)
    #
    #     foundTaskDtlToCancel = True if len(taskDtlRows) > 0 else False
    #     if foundTaskDtlToCancel:
    #         taskDtlIDs = [r['TASK_DTL_ID'] for r in taskDtlRows]
    #         tdUpdQ = f"""update task_dtl set stat_code='99',user_id='AUTOMATION' where task_dtl_id in {Commons.get_tuplestr(taskDtlIDs)}"""
    #         DBService.update_db(tdUpdQ, self.schema)
    #
    #     '''Task hdr'''
    #     sql = f"""select * from task_hdr where item_id='{itemId}' and stat_code not in ('90','99')"""
    #
    #     taskHdrRows = DBService.fetch_rows(sql, self.schema)
    #     foundTaskHdrToCancel = True if len(taskHdrRows) > 0 else False
    #
    #     if foundTaskHdrToCancel:
    #         taskHdrSql = f"""update task_hdr set stat_code='99',user_id='AUTOMATION' where item_id='{itemId}' and stat_code not in ('90','99')"""
    #         DBService.update_db(taskHdrSql, self.schema)

    def _cancelAllocs(self, itemId: str):
        if 'true' in self.IS_ALLOW_CANCEL_ALLOC:
            sql = f"""update alloc_invn_dtl set stat_code='99',user_id='AUTOMATION' where item_id='{itemId}' and stat_code not in ('90','99')"""
            DBService.update_db(sql, self.schema)
        else:
            assert False, "Alloc cancel not allowed. Test manually"

    def _updateLpnsToConsumed(self, itemId: str):
        if 'true' in self.IS_ALLOW_UPDATE_ILPN:
            sql = f"""update lpn set lpn_facility_status='95',lpn_status='70',curr_sub_locn_id=null,dest_sub_locn_id=null,last_updated_source='AUTOMATION'
                        where item_id='{itemId}' and inbound_outbound_indicator='I' and lpn_facility_status not in ('95','96','99')
                  """
            DBService.update_db(sql, self.schema)
        else:
            assert False, f"Ilpn update not allowed. Test manually"

    def _clearManualActvAndResvInvnForReplen(self, item: str):
        """Clear the actv & resv locn records, allocations, tasks for an item, lpns
        """
        actvLocns, resvLocns = self._getManualActvAndResvLocnForItemToClear(item=item)
        itemId = str(self.getItemIdFromBrcd(itemBrcd=item))

        self._cancelTasks(itemId=itemId)
        self._cancelAllocs(itemId=itemId)

        for i in range(len(actvLocns)):
            DBAdmin._deleteFromActvInvnTables(self.schema, locnBrcd=actvLocns[i], item=item)

        self._updateLpnsToConsumed(itemId=itemId)

        for i in range(len(resvLocns)):
            DBAdmin._deleteRecordFromWMInv(self.schema, locnBrcd=resvLocns[i], item=item)

    def _getManualActvAndResvLocnForItemToClear(self, item: str):
        locnSql = f"""select lh.locn_brcd from wm_inventory wi inner join locn_hdr lh on wi.location_id=lh.locn_id
                         inner join item_cbo ic on wi.item_id=ic.item_id
                         where ic.item_name='{item}' and wi.location_id is not null and wi.locn_class='#LOCN_CLASS#'
                         #CONDITION#
                    """
        sqlCond = ''

        # '''Exclude runtime thread locns'''
        # threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        # if threadLocns is not None:
        #     sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Actv locn'''
        actvLocnSql = locnSql.replace('#LOCN_CLASS#', 'A')
        actvLocnSql = actvLocnSql.replace('#CONDITION#', sqlCond)

        actvRows = DBService.fetch_rows(actvLocnSql, self.schema)
        actvLocns = [i['LOCN_BRCD'] for i in actvRows]

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            # sqlCond += " \n and lh.locn_brcd not in " + threadLocns
            anyLocnToBeExcluded = [True if i in actvLocns else False for i in threadLocns]
            assert True not in anyLocnToBeExcluded, f"<Data> To be excluded actv locn found to clear. Test manually"

        '''Resv locn'''
        resvLocnSql = locnSql.replace('#LOCN_CLASS#', 'R')
        resvLocnSql = resvLocnSql.replace('#CONDITION#', sqlCond)

        resvRows = DBService.fetch_rows(resvLocnSql, self.schema)
        resvLocns = [i['LOCN_BRCD'] for i in resvRows]

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            # sqlCond += " \n and lh.locn_brcd not in " + threadLocns
            anyLocnToBeExcluded = [True if i in resvLocns else False for i in threadLocns]
            assert True not in anyLocnToBeExcluded, f"<Data> To be excluded resv locn found to clear. Test manually"

        return actvLocns, resvLocns

    def getDestLocnForLpn(self, lpn: str):
        """"""
        sql = f"""select lh.locn_brcd from lpn l left outer join locn_hdr lh on lh.locn_id=l.dest_sub_locn_id
                 where l.tc_lpn_id='{lpn}' 
              """
        dbRow = DBService.fetch_row(sql, self.schema)
        destLocn = str(dbRow.get('LOCN_BRCD'))

        return destLocn

    def getILPNs(self, noOfLPN: int, lpnFacStatus: int = None, noOfItem: int = None, isResvWithTBF: bool = False,
                 isLocnWithNoCCTask: bool = None):
        sql = f"""select l.tc_lpn_id,ic.item_name,ld.size_value,lh.locn_brcd,l.lpn_facility_status,lh.*
                    from lpn l inner join lpn_detail ld on l.lpn_id=ld.lpn_id
                    inner join item_cbo ic on ld.item_id=ic.item_id
                    left outer join locn_hdr lh on l.curr_sub_locn_id=lh.locn_id
                    left outer join locn_hdr lh2 on l.dest_sub_locn_id=lh2.locn_id
                    where l.inbound_outbound_indicator='I'  
                    #CONDITION# 
                    offset 0 rows fetch next {noOfLPN} rows only
              """
        sqlCond = ''
        if lpnFacStatus is not None:
            sqlCond += " \n and l.lpn_facility_status = " + str(lpnFacStatus)
        if isResvWithTBF:
            sqlCond += " \n and lh.locn_id in (select location_id from wm_inventory where locn_class='R' and TO_BE_FILLED_QTY > 0)"
        if noOfItem is not None:
            sqlCond += f" \n and l.lpn_id in (select lpn_id from lpn_detail group by lpn_id having count(lpn_id) = {noOfItem})"
        if isLocnWithNoCCTask:
            sqlCond += """ \n and lh.locn_id not in (select task_genrtn_ref_nbr from task_hdr where INVN_NEED_TYPE in ('101','100') and stat_code in('10')) """
        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns
            sqlCond += " \n and lh2.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            # sqlCond += " \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and l.lpn_id not in (select lpn_id from lpn_detail where item_name in {threadItems})"""
            
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)
        assert len(dbRows) >= noOfLPN and dbRows[0]['TC_LPN_ID'] is not None, f"<Data> {noOfLPN} no. of ilpn not found " + sql

        for i in range(len(dbRows)):
            self._logDBResult(dbRows[i], ['TC_LPN_ID', 'ITEM_NAME', 'SIZE_VALUE', 'LOCN_BRCD'])

        return dbRows

    # def getILpnsFromResvLocn(self, noOfLpn: int = 1, lpnFacStat: int = None, minLpnQty:int=None, isLocnWithNoCCTask: bool = None,
    #                          isResvCCPending: bool = None):
    #     # TODO Method not used yet
    #     sql = f"""select lh.locn_brcd,ic.item_name,wi.tc_lpn_id,ld.size_value,wi.on_hand_qty
    #              from wm_inventory wi
    #              inner join locn_hdr lh on wi.location_id=lh.locn_id inner join lpn l on wi.tc_lpn_id=l.tc_lpn_id
    #              inner join lpn_detail ld on l.lpn_id=ld.lpn_id inner join item_cbo ic on ld.item_id=ic.item_id
    #              inner join task_hdr th on th.task_genrtn_ref_nbr=lh.locn_id
    #              where wi.location_id in (select location_id from wm_inventory
    #                                     where tc_lpn_id is not null group by location_id having count(location_id)={noOfLpn})
    #              and lh.cycle_cnt_pending='N'
    #              and wi.tc_lpn_id in (select tc_lpn_id from lpn
    #                                     where lpn_id in (select lpn_id from lpn_detail group by lpn_id having count(lpn_id)='1'))
    #              and wi.tc_lpn_id not in (select tc_lpn_id from lpn_lock where tc_lpn_id is not null)
    #              #CONDITION#
    #           """
    #     # sql = sql.replace('#NUMOFLPNS#', str(noOfLpn)).replace('#FACSTAT#',str(facStat))
    #     sqlCond = ''
    #     if lpnFacStat is not None:
    #         sqlCond += f" \n and l.lpn_facility_status = '{lpnFacStat}'"
    #     if minLpnQty is not None:
    #         sqlCond += f" \n and wi.on_hand_qty >= {minLpnQty}"
    #     if isLocnWithNoCCTask:
    #         sqlCond += """ \n and lh.locn_id not in(select task_genrtn_ref_nbr from task_hdr where INVN_NEED_TYPE in ('101','100') and stat_code in('10')) order by lh.locn_brcd"""
    #
    #     '''Exclude runtime thread locns'''
    #     threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
    #     if threadLocns is not None:
    #         sqlCond += " \n and lh.locn_brcd not in " + threadLocns
    #
    #     '''Exclude runtime thread items'''
    #     threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
    #     if threadItems is not None:
    #         # sqlCond += " \n and ic.item_name not in " + threadItems
    #         sqlCond += f""" \n and l.lpn_id not in (select lpn_id from lpn_detail where item_name in {threadItems})"""
    #
    #     sql = sql.replace('#CONDITION#', str(sqlCond))
    #
    #     dbRows = DBService.fetch_rows(sql, self.schema)
    #
    #     if isResvCCPending is not None:
    #         for i in range(noOfLpn):
    #             temp_resvLocn = dbRows[i]['LOCN_BRCD']
    #             self._updateLocnCCPending(i_locnBrcd=temp_resvLocn, u_isCCPending=isResvCCPending)
    #
    #     for i in range(len(dbRows)):
    #         self._logDBResult(dbRows[i], ['TC_LPN_ID', 'ITEM_NAME', 'SIZE_VALUE', 'LOCN_BRCD'])
    #
    #     return dbRows
    
    def getILpnsNotInResv(self, noOfLPN: int, lpnFacStatus: int = None, noOfItem: int = None):
        sql = f"""select l.tc_lpn_id, ic.item_name, ld.size_value,l.lpn_facility_status
                    from lpn l inner join lpn_detail ld on l.lpn_id = ld.lpn_id
                    inner join item_cbo ic on ld.item_id = ic.item_id
                    where l.inbound_outbound_indicator='I' and l.curr_sub_locn_id is null and l.dest_sub_locn_id is null  
                    #CONDITION# 
                    offset 0 rows fetch next {noOfLPN} rows only
                """
        sqlCond = ''
        if lpnFacStatus is not None:
            sqlCond += " \n and l.lpn_facility_status = " + str(lpnFacStatus)
        if noOfItem is not None:
            sqlCond += " \n and l.lpn_id in (select lpn_id from lpn_detail group by lpn_id having count(lpn_id) = #NO_OF_ITEMS#)"
            sqlCond = sqlCond.replace('#NO_OF_ITEMS#', str(noOfItem))

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        # '''Exclude runtime thread locns'''
        # threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        # if threadLocns is not None:
        #     sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            # sqlCond += " \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and l.lpn_id not in (select lpn_id from lpn_detail where item_name in {threadItems})"""
            
        sql = sql.replace('#CONDITION#', sqlCond)

        dbRows = DBService.fetch_rows(sql, self.schema)
        assert len(dbRows) >= noOfLPN and dbRows[0]['TC_LPN_ID'] is not None, f"<Data> {noOfLPN} no. of ilpn not found " + sql

        for i in range(len(dbRows)):
            self._logDBResult(dbRows[i], ['TC_LPN_ID', 'ITEM_NAME', 'SIZE_VALUE'])

        return dbRows

    def getILpnsWithLock(self,noOfLPN:int, lpnFacStat:int):
        sql = f"""select l.tc_lpn_id, lh.locn_brcd,ll.inventory_lock_code,ic.item_name from lpn l
                    inner join locn_hdr lh on l.curr_sub_locn_id=lh.locn_id and lh.work_grp='RESV'
                    inner join resv_locn_hdr rlh on lh.locn_id=rlh.locn_id and rlh.invn_lock_code is not null
                    inner join lpn_lock ll on l.tc_lpn_id=ll.tc_lpn_id
                    inner join item_cbo ic on ic.item_id=l.item_id 
                    where l.inbound_outbound_indicator='I' and l.lpn_facility_status='{lpnFacStat}' and l.dest_sub_locn_id is null
                    #CONDITION# 
                    offset 0 rows fetch next {noOfLPN} rows only
                """
        sqlCond = ''

        final_refField10 = self._decide_ic_refField10_forItemType()
        if final_refField10 is not None:
            sqlCond += f" \n and ic.ref_field10 in {final_refField10}"
        else:
            sqlCond += f" \n and (ic.ref_field10 is null or (ic.ref_field10 is not null and ic.ref_field10 not in ('ARC')))"

        '''Exclude runtime thread locns'''
        threadLocns = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.LOCNS, replaceFrom=',', replaceWith="','")
        if threadLocns is not None:
            sqlCond += " \n and lh.locn_brcd not in " + threadLocns

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            # sqlCond += " \n and ic.item_name not in " + threadItems
            sqlCond += f""" \n and l.lpn_id not in (select lpn_id from lpn_detail where item_name in {threadItems})"""

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRows = DBService.fetch_rows(sql, self.schema)

        assert dbRows and len(dbRows) >= noOfLPN, f"<Data> Lpn with lock not found"

        for i in range(len(dbRows)):
            self._logDBResult(dbRows[i], ['TC_LPN_ID', 'LOCN_BRCD'])

        return dbRows

    # def _setASNTolerancePercent(self, warningPercent: int = None, overidePercent: int = None):
    #     sql = f"""select error_rcpt_pcnt_shmt_po_sku, ovrd_rcpt_pcnt_shmt_po_sku,warn_rcpt_pcnt_shmt_po_sku
    #              from whse_parameters where whse_master_id = '1'
    #           """
    #     dbRows = DBService.fetch_rows(sql, self.schema)
    #     assert len(dbRows) > 0, "Record not found for tolerance percent"
    #
    #     sqlCond = ""
    #     isValAlreadyMatched = True
    #     if warningPercent is not None:
    #         tolerancePercent = dbRows[0].get('WARN_RCPT_PCNT_SHMT_PO_SKU')
    #         isValAlreadyMatched = True if tolerancePercent == warningPercent else False
    #         if isValAlreadyMatched:
    #             sqlCond += "warn_rcpt_pcnt_shmt_po_sku = " + str(warningPercent)
    #     elif overidePercent is not None:
    #         tolerancePercent = dbRows[0].get('OVRD_RCPT_PCNT_SHMT_PO_SKU')
    #         isValAlreadyMatched = True if tolerancePercent == overidePercent else False
    #         if isValAlreadyMatched:
    #             sqlCond += "ovrd_rcpt_pcnt_shmt_po_sku = " + str(overidePercent)
    #
    #     if not isValAlreadyMatched and sqlCond != '':
    #         sql = "update whse_parameters set #CONDITION# where whse_master_id = '1'"
    #         sql = sql.replace('#CONDITION#', sqlCond)
    #
    #         DBService.update_db(sql, self.schema)
    #
    #     return dbRows

    def getASN(self, asnStat: int, noOfPO: int, noOfItemPerASN: int):
        """Get asn and asn details
        """
        sql = f"""select a.tc_asn_id, ad.*, a.* from asn_detail ad inner join asn a on ad.asn_id = a.asn_id 
               where a.asn_status = '{asnStat}'
               #CONDITION# 
               and a.tc_asn_id in (select tc_asn_id from asn_detail ad inner join asn a on ad.asn_id = a.asn_id 
                                group by tc_asn_id having count(distinct tc_purchase_orders_id)='{noOfPO}' and count(distinct SKU_ID)='{noOfItemPerASN}')
               offset 1 rows fetch next 1 row only 
              """
        sqlCond = ''

        '''Exclude runtime thread items'''
        threadItems = RuntimeXL.getThisAttrFromAllThreads(RuntimeAttr.ITEMS, replaceFrom=',', replaceWith="','")
        if threadItems is not None:
            sqlCond += f" \n and a.asn_id not in (select asn_id from asn_detail where sku_name in {threadItems})"

        sql = sql.replace('#CONDITION#', sqlCond)
        dbRow = DBService.fetch_row(sql, self.schema)

        self._logDBResult(dbRow, ['TC_ASN_ID'])

        return dbRow

    def getASNDtls(self, i_asn: str):
        """Get asn details
        """
        sql = f"""select a.tc_asn_id, ad.*, a.* from asn_detail ad inner join asn a on ad.asn_id = a.asn_id 
                    where a.tc_asn_id = '{i_asn}'
              """
        dbRows = DBService.fetch_rows(sql, self.schema)
        assert dbRows is not None and len(dbRows) > 0, '<Data> Asn dtl not found ' + sql

        return dbRows

    def getProNumberLevelFromShipVia(self, shipVia: str):
        sql = f"""select carrier_code_name,pro_number_level from CARRIER_CODE where carrier_code = '#SHIP_VIA#' 
                 order by pro_number_level desc
              """
        sql = sql.replace('#SHIP_VIA#', shipVia)

        dbRow = DBService.fetch_rows(sql, self.schema)
        assert len(dbRow) > 0, "<Data> Carrier code ship via record not found " + sql

        proNumberLevel = dbRow[0].get('PRO_NUMBER_LEVEL')
        return proNumberLevel

    def assertProNumberForShipment(self, shipmentId: str, proNumberLevel: int):
        """Validate pro_number from shipmen_id
        1 for stop-level
        2, 3 for shipment-level
        """
        stopLevelSql = f"""select pro_nbr from stop where shipment_id in (select shipment_id from shipment where tc_shipment_id = '#SHIP_ID#') 
                          order by stop_seq asc
                       """
        shipmentLevelSql = f"""select pro_number,shipment_id from shipment where shipment_id = '#SHIP_ID#'"""

        if proNumberLevel == 1:  # stop-level
            sqlForStopLevel = stopLevelSql.replace('#SHIP_ID#', shipmentId)
            dbRow = DBService.fetch_rows(sqlForStopLevel, self.schema)
            proNumber = dbRow[1].get('PRO_NBR')
            assert proNumber is not None and proNumber != '', "<Shipment> Stop-level pro-number validation failed " + sqlForStopLevel
        elif proNumberLevel in (2, 3):  # shipment-level
            sqlForShipmentLevel = shipmentLevelSql.replace('#SHIP_ID#', shipmentId)
            dbRows = DBService.fetch_rows(sqlForShipmentLevel, self.schema)
            for i in range(1, len(dbRows)):
                proNumber = dbRows[i].get('PRO_NBR')
                assert proNumber is not None and proNumber != '', "<Shipment> Shipment-level pro-number validation failed " + shipmentLevelSql

    def _logDBResult(self, dbResult: Union[Dict, list[Dict]], columns: list[str]):
        """ Iterate through DB records and log data for provided columns
        """
        dbResults = dbResult if type(dbResult) == list else [dbResult] if type(dbResult) == dict else None

        if dbResults:
            for r in dbResults:
                line = ''
                for c in columns:
                    line += f"{c.lower()} {r[c.upper()]} "
                self.logger.info(f"Data {line}")

    def _printItemInvnData(self, itemName: str):
        """ Get current item invn dtls
        """
        if itemName is not None and itemName != '':
            sql = f"""select ic.item_name,ic.item_id,lh.locn_class,lh.locn_brcd,lh.locn_id,lh.work_grp,lh.work_area,lh.zone,lh.area
                     ,pld.max_invn_qty
                     ,wm.allocatable,wm.tc_lpn_id,wm.on_hand_qty,wm.wm_allocated_qty,wm.to_be_filled_qty
                     from wm_inventory wm inner join item_cbo ic on wm.item_id=ic.item_id inner join locn_hdr lh on wm.location_id=lh.locn_id
                     left outer join pick_locn_dtl pld on wm.location_id=pld.locn_id and wm.item_id=pld.item_id
                     where ic.item_name='{itemName}'
                     offset 0 rows fetch next 50 rows only
                    """
            dbRows = DBService.fetch_rows(sql, self.schema)
            printit(f"^^^ Curr invn details for {itemName} {dbRows}")

    def _updateOtherPutwyILpnsToInTranStat(self, currResvLocnIds: list[str], currItemIds: list[str], ignoreIlpns: list[str]):
        """Update lpns stat to 10 having same items present in same resv locns
        """
        if 'true' in self.IS_ALLOW_UPDATE_ILPN:
            restIlpnSql = f"""select distinct wm.tc_lpn_id from wm_inventory wm inner join lpn l on wm.location_id=l.curr_sub_locn_id 
                                 where wm.location_id in {Commons.get_tuplestr(currResvLocnIds)} 
                                 and wm.item_id in {Commons.get_tuplestr(currItemIds)} 
                                 and wm.tc_lpn_id not in {Commons.get_tuplestr(ignoreIlpns)}
                                 and l.inbound_outbound_indicator='I' and l.lpn_facility_status=30
                              """
            dbRows = DBService.fetch_rows(restIlpnSql, self.schema)
            if dbRows is not None and len(dbRows) > 0:
                otherIlpns = [i['TC_LPN_ID'] for i in dbRows]
                printit(f"Updating other ilpns to 0 stat {otherIlpns}")

                if len(otherIlpns) > 0:
                    updRestIlpnSql = f"""update lpn set lpn_facility_status=0, last_updated_source='AUTOMATION' 
                                            where tc_lpn_id in {Commons.get_tuplestr(otherIlpns)}"""
                    DBService.update_db(updRestIlpnSql, self.schema)
            else:
                printit(f"Not updating any ilpns to 0 stat")
        else:
            assert False, f"Other ilpn update not allowed. Test manually"

    def _presetCCTaskRuleForLocn(self, taskCriteria: str, ruleName: str = None, locnBrcd: str = None):
        sql = f"""select tp.task_parm_id, rh.rule_id, tp.crit_nbr, rh.rule_name, rh.stat_code from task_parm tp 
                 inner join task_rule_parm trp on tp.task_parm_id = trp.task_parm_id 
                 inner join rule_hdr rh on trp.rule_id = rh.rule_id
                 where rh.rule_type='CC' and tp.crit_nbr = '#TASKCRIT#' 
              """
        sql = sql.replace('#TASKCRIT#', taskCriteria)
        dbRows = DBService.fetch_rows(sql, self.schema)
        assert dbRows[0].get('RULE_NAME') is not None, f"<Config> CC task criteria {taskCriteria} has no rules " + sql

        rules = []
        ruleStatDict = dict()
        ruleStatDict['RULE_ID'] = dbRows[0].get('RULE_ID')
        ruleStatDict['RULE_NAME'] = dbRows[0].get('RULE_NAME')
        ruleStatDict['STAT_CODE'] = dbRows[0].get('STAT_CODE')
        rules.extend([ruleStatDict.copy() for i in range(len(dbRows))])
        for i in range(len(dbRows)):
            rules[i]['RULE_ID'] = dbRows[i].get('RULE_ID')
            rules[i]['RULE_NAME'] = dbRows[i].get('RULE_NAME')
            rules[i]['STAT_CODE'] = dbRows[i].get('STAT_CODE')

        isRuleExist = True if any(key['RULE_NAME'] == ruleName for key in rules) else False
        assert isRuleExist, f"<Config> CC task rule {ruleName} not found for task criteria {taskCriteria}"

        '''get ruleid from rulename'''
        ruleId = None
        for i in rules:
            if i.get('RULE_NAME') == ruleName:
                ruleId = i.get('RULE_ID')
                break

        # '''updating rule sel dtl for locnClass and locnBrcd'''
        # sql = f""" update rule_sel_dtl set tbl_name='LOCN_HDR', colm_name='LOCN_CLASS', oper_code='=', rule_cmpar_value='R', and_or_or='A'
        #           where rule_id='#RULEID#' and sel_seq_nbr=1 """
        # sql = sql.replace('#RULEID#', str(ruleId))
        # DBService.update_db(sql,self.schema)

        updSql = f"""update rule_sel_dtl set tbl_name='LOCN_HDR', colm_name='LOCN_BRCD', oper_code='=', rule_cmpar_value='#LOCNBRCD#'
                    where rule_id='#RULEID#' --and sel_seq_nbr=2 
                 """
        updSql = updSql.replace('#RULEID#', str(ruleId)).replace('#LOCNBRCD#', str(locnBrcd))
        DBService.update_db(updSql, self.schema)

        '''Update rule checkbox: Enable for given rulename, Disable for others'''
        ruleIdTobeEnabled = ruleId
        ruleIdToBeDisbaled = None
        for i in rules:
            if ruleIdTobeEnabled == i.get('RULE_ID'):
                self._presetWMRuleStatus(ruleId=ruleIdTobeEnabled, statCode=0)
            else:
                ruleIdToBeDisbaled = i.get('RULE_ID')
                self._presetWMRuleStatus(ruleId=ruleIdToBeDisbaled, statCode=90)

        return rules

    def assertConsolLocnList(self, actualConsolList, expConsolList):
        """Validate actual consol Locn list with expected list"""
        final_actualConsolList = sorted(actualConsolList)
        final_expConsolList = sorted(expConsolList)

        isMatched = final_actualConsolList == final_expConsolList
        
        if not isMatched:
            self._printConsolLocnData(consolLocnList=expConsolList)

        assert isMatched, f"<Consol> Consol locns didnt match, actual = {actualConsolList}, exp = {expConsolList}"

    def getASNTolerancePercents(self):
        """Get tolerance percentage config"""
        sql = f"""select error_rcpt_pcnt_shmt_po_sku, ovrd_rcpt_pcnt_shmt_po_sku,warn_rcpt_pcnt_shmt_po_sku
                 from whse_parameters where whse_master_id = '1'
                 offset 0 rows fetch next 1 rows only
              """
        dbRows = DBService.fetch_row(sql, self.schema)
        assert len(dbRows) != 1, "<Config> Record not found for tolerance percent"

        warnPercent = dbRows.get('WARN_RCPT_PCNT_SHMT_PO_SKU')
        ovrdPercent = dbRows.get('OVRD_RCPT_PCNT_SHMT_PO_SKU')
        errorPercent = dbRows.get('ERROR_RCPT_PCNT_SHMT_PO_SKU')

        self.logger.info(f"Recv asn tolerance percent config: error {errorPercent}, override {ovrdPercent}, warn {warnPercent}")
        return warnPercent, ovrdPercent, errorPercent

    def assertAsnRcvTolConfigSetForWarnMsg(self):
        warnPercent, ovrdPercent, errorPercent = self.getASNTolerancePercents()
        isOK = RecvASNTolConfig.isWarnMsgConfigSet(warnPercent, ovrdPercent, errorPercent)
        assert isOK, f"<Config> ASN rcv tolerance percent config is not right for warn msg, " \
                     f"warn% {warnPercent} ovrd% {ovrdPercent} err% {errorPercent}"
        return warnPercent, ovrdPercent, errorPercent

    def assertAsnRcvTolConfigSetForOvrdMsg(self):
        warnPercent, ovrdPercent, errorPercent = self.getASNTolerancePercents()
        isOK = RecvASNTolConfig.isOverrideWarnMsgConfigSet(warnPercent, ovrdPercent, errorPercent)
        assert isOK, f"<Config> ASN rcv tolerance percent config is not right for ovrd msg, " \
                     f"warn% {warnPercent} ovrd% {ovrdPercent} err% {errorPercent}"
        return warnPercent, ovrdPercent, errorPercent

    def decideRecvAsnTolMsgOrRegularRecv(self, refRecvQty: int, isWarn: bool = None, isOverride: bool = None, isError: bool = None):
        """"""
        warnPercent, ovrdPercent, errorPercent = self.getASNTolerancePercents()
        warnPercent, ovrdPercent, errorPercent = map(lambda x: int(x) if x is not None else None, (warnPercent, ovrdPercent, errorPercent))
        # print(errorPercent, ovrdPercent, warnPercent)

        givenRecvType = RecvASNTolConfig.ERROR if isError else RecvASNTolConfig.OVERRIDE if isOverride else RecvASNTolConfig.WARNING

        recvAsnTolMode = RecvASNTolConfig.REGULAR_RECV
        finalRecvQty = refRecvQty + 1

        listOfPairs = [(RecvASNTolConfig.ERROR, errorPercent), (RecvASNTolConfig.OVERRIDE, ovrdPercent), (RecvASNTolConfig.WARNING, warnPercent)]
        orderedDict = OrderedDict(listOfPairs)
        # print('orderedDict', orderedDict)

        orderedDict = OrderedDict((k, v) for k, v in orderedDict.items() if v is not None)
        sortedDictAsc = Commons.remove_duplicate_val_from_ordered_dict(Commons.sort_ordered_dict_by_val(orderedDict))
        # print('sortedDictAsc', sortedDictAsc)
        # sortedDictDesc = OrderedDict(reversed(list(sortedDictAsc.items())))
        # print('sortedDictDesc', sortedDictDesc)
        # eligbleRcvTypes = sortedDictAsc.keys()
        # print('eligbleRcvTypes', eligbleRcvTypes)

        if sortedDictAsc and givenRecvType in sortedDictAsc.keys():
            recvAsnTolMode = givenRecvType
            finalRecvQty = finalRecvQty + sortedDictAsc[recvAsnTolMode]
        elif sortedDictAsc and givenRecvType not in sortedDictAsc.keys():
            recvAsnTolMode = next(iter(sortedDictAsc))
            finalRecvQty = finalRecvQty + sortedDictAsc[recvAsnTolMode]
        else:
            pass

        return finalRecvQty, recvAsnTolMode
    
    def assertWaitForDOInShipment(self, shipmentNum: str, order: str):
        # pc_order = self._getParentDOsIfExistElseChildDOs([order])[0]
        # order = pc_order

        sql = f"select tc_order_id from orders where tc_shipment_id='{shipmentNum}' and tc_order_id='{order}'"
        DBService.wait_for_value(sql, 'TC_ORDER_ID', str(order), self.schema, maxWaitInSec=30)


