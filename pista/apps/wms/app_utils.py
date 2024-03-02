import os
from typing import Union

from apps.wms.app_db_lib import DBLib
from core.config_service import ENV_CONFIG
from core.log_service import Logging, printit
from resources.data.template import tags_doxml, tags_lpnlevel_asnxml, tags_skulevel_asnxml, tags_poxml, tags_lpnxml
from core.file_service import DataGeneric, XMLUtil

from root import VARIABLE_FILE


class XMLBuilder:
    logger = Logging.get(__qualname__)

    @classmethod
    def _decide_doXml_refField10(cls):
        _ENV = os.environ['env']
        _ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')
        _IS_FOR_ARC = os.environ['isForARC']

        refField10 = ''
        if _ENV_TYPE in ['PLN']:
            refField10 = ''
        else:
            if _ENV_TYPE in ['EXP']:
                if 'true' in _IS_FOR_ARC:
                    refField10 = 'ARC'
                else:
                    refField10 = 'BKP'
        return refField10

    @classmethod
    def _decide_doXml_majorOrderGrpAttr(cls, doType, dcCntrNbr, refField10, providedVal):
        _ENV = os.environ['env']
        _ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')
        _TC_MARK = os.environ['tcMark']
        _IS_FOR_ARC = os.environ['isForARC']

        majorOrdGrpAttr = ''
        if _ENV_TYPE in ['PLN']:
            majorOrdGrpAttr = str(providedVal) if providedVal is not None else ''
        else:
            if _ENV_TYPE in ['EXP']:
                if 'true' in _IS_FOR_ARC and doType in ['P', 'N']:
                    majorOrdGrpAttr = f"{dcCntrNbr} {refField10}"
                elif 'true' not in _IS_FOR_ARC and doType in ['N'] and 'tcPLN' not in _TC_MARK:
                    majorOrdGrpAttr = f"{dcCntrNbr} {refField10}"
                else:
                    majorOrdGrpAttr = str(providedVal) if providedVal is not None else ''
        return majorOrdGrpAttr

    @classmethod
    def buildDOXml(cls, doType, items: Union[str, list], qtys: Union[int, list], shipVia=None, doNum=None,
                   varColumn=None, majorOrdGrpAttr=None) -> (str, str):
        """Returns: doNum(str), DO xml(str)
            majorOrdGrpAttr can be Transfer"""
        varFile, varSheet = VARIABLE_FILE, 'DO'
        varColumn = varColumn if varColumn is not None else 'DC-NAPA-89'
        if doNum is None:
            doNum = DBLib().getNewDONum()
        # majorOrdGrpAttr = str(majorOrdGrpAttr) if majorOrdGrpAttr is not None else ''
        final_xml_lines = str()
        shipVia = '' if shipVia is None else shipVia
        # dcNum = '' if dcNum is None else dcNum
        MSG_TYPE = ENV_CONFIG.get('data', 'do_xml_message_type')
        origFacilityAliasId = ENV_CONFIG.get('facility', 'facility_alias_id')

        items_list = items if type(items) == list else [items]
        qtys_list = qtys if type(qtys) == list else [qtys]

        if len(items_list) != len(qtys_list):
            assert False, 'Insufficient items provided against qtys'
        elif varFile is None or varSheet is None or varColumn is None:
            assert False, 'Variable file not provided'
        else:
            for i in range(0, len(items_list)):
                final_xml_lines = final_xml_lines + tags_doxml.LINE.replace('#DO_LINE#', str(i + 1)) \
                    .replace('#ITEM_NAME#', items_list[i]).replace('#ITEM_QTY#', str(qtys_list[i]))

        if final_xml_lines == '':
            assert False, 'Xml for lines didnt create'
        else:
            final_xml = tags_doxml.XML_START + '\n' \
                        + tags_doxml.HEADER.replace('#MSG_TYPE#', MSG_TYPE).replace('#REF_NUM#', doNum) + '\n' \
                        + tags_doxml.MSG_DO_START.replace('#DO_NUM#', doNum).replace('#ORDER_TYPE#', doType) \
                        .replace('#ORIG_FACILITY_ALIAS_ID#', origFacilityAliasId).replace('#SHIP_VIA#', shipVia) \
                        + '\n' + final_xml_lines + '\n' + tags_doxml.XML_END
            final_xml = DataGeneric.replace_dyn(final_xml)
        final_xml = XMLUtil.format_xml(final_xml)

        '''Replace data from var file'''
        final_xml, varFileData = DataGeneric.replace_from_varfile(varFile, varSheet, varColumn, data=final_xml)

        dcCntrNbr = varFileData['DEST_FACILITY_ID']
        refField10 = cls._decide_doXml_refField10()
        majorOrdGrpAttr = cls._decide_doXml_majorOrderGrpAttr(doType=doType, dcCntrNbr=dcCntrNbr, refField10=refField10, providedVal=majorOrdGrpAttr)
        final_xml = final_xml.replace('#MAJOR_ORD_GRP_ATTR#', majorOrdGrpAttr).replace('#REF_FIELD_10#', refField10)

        '''Export xml to file'''
        # threadId = str(threading.current_thread().native_id)
        # xmlFilePath = os.path.join(RUNTIME_DIR, threadId + '_doxml_' + Commons.build_date_forfilename() + '.xml')
        # FileUtil.write_file(xmlFilePath, final_xml)
        # printit('DO xml path:' + xmlFilePath)
        printit('DO xml:', final_xml)

        return doNum, final_xml

    @classmethod
    def buildLpnLevelASNXml(cls, poNum: list[str], items: list[list[str]], qtys: list[list[int]],
                            poLineNums: list[list[int]], lpns: list[str] = None, asnNum=None, varColumn=None) -> (str, str):
        """Returns: asnNum(str), ASN xml(str)"""
        varFile, varSheet = VARIABLE_FILE, 'PO'
        if varColumn is None:
            varColumn = 'DUMMY'
        if asnNum is None:
            asnNum = DBLib().getNewASNNum()
        MSG_TYPE = ENV_CONFIG.get('data', 'asn_xml_message_type')
        destFacilityAliasId = ENV_CONFIG.get('facility', 'facility_alias_id')

        lpns_list = lpns
        items_list = items
        qtys_list = qtys
        poLineNums_list = poLineNums

        final_all_lpns = str()
        if len(qtys_list) != len(items_list):
            assert False, 'Insufficient items provided for qty'
        elif varFile is None or varSheet is None or varColumn is None:
            assert False, 'Variable file not provided'
        else:
            for i in range(0, len(lpns_list)):  # for each lpn in list
                final_xml_lpn = str()
                final_lines_per_lpn = str()

                # if type(items_list[i]) == str:  # 1 item per lpn
                #     final_lines_per_lpn = final_lines_per_lpn + tags_lpnlevel_asnxml.XML_LPN_DETAIL \
                #         .replace('#ITEM_NAME#', items_list[i]).replace('#ITEM_QTY#', str(qtys_list[i])) \
                #         .replace('#PO_NUM#', poNum[i]).replace('#LINE_NUM#', '1') \
                #         .replace('#PO_LINE_NUM#', str(poLineNums_list[i]))
                # else:
                for j in range(0, len(items_list[i])):  # 1 item per lpn or list of items per lpn
                    final_lines_per_lpn = final_lines_per_lpn + '\n' + tags_lpnlevel_asnxml.XML_LPN_DETAIL \
                        .replace('#ITEM_NAME#', items_list[i][j]).replace('#ITEM_QTY#', str(qtys_list[i][j])) \
                        .replace('#PO_NUM#', poNum[i]).replace('#LINE_NUM#', str(j + 1)) \
                        .replace('#PO_LINE_NUM#', str(poLineNums_list[i][j]))
                final_xml_lpn = final_xml_lpn + tags_lpnlevel_asnxml.XML_LPN.replace('#LPN_ID#', lpns_list[i]) \
                    .replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId) \
                    .replace('#CURR_FACILITY_ALIAS_ID#', destFacilityAliasId) \
                    .replace('#PO_NUM#',
                             poNum[i]) + '\n' + final_lines_per_lpn + '\n' + tags_lpnlevel_asnxml.XML_LPN_END
                final_all_lpns = final_all_lpns + '\n' + final_xml_lpn

        if final_all_lpns == "":
            assert False, 'Xml for lpn didnt create'
        else:
            final_xml = tags_lpnlevel_asnxml.XML_START + '\n' \
                        + tags_lpnlevel_asnxml.XML_HEADER.replace('#MSG_TYPE#', MSG_TYPE) + '\n' \
                        + tags_lpnlevel_asnxml.XML_MESSAGE.replace('#ASN_ID#', asnNum) \
                            .replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId) + '\n' \
                        + final_all_lpns + '\n' + tags_lpnlevel_asnxml.XML_END
            final_xml = DataGeneric.replace_dyn(final_xml)
        final_xml = XMLUtil.format_xml(final_xml)

        '''Replace data from var file'''
        final_xml, varFileData = DataGeneric.replace_from_varfile(varFile, varSheet, varColumn, data=final_xml)

        '''Export xml to file'''
        # threadId = str(threading.current_thread().native_id)
        # xmlFilePath = os.path.join(RUNTIME_DIR,
        #                            threadId + '_lpnlvl_asnxml_' + Commons.build_date_forfilename() + '.xml')
        # FileUtil.write_file(xmlFilePath, final_xml)
        # printit('Lpn level ASN xml path:' + xmlFilePath)
        printit('Lpn level ASN xml:', final_xml)

        return asnNum, final_xml

    @classmethod
    def buildSkuLevelASNXml(cls, poNums: Union[str, list], items: Union[str, list], qtys: Union[int, list],
                            poLineNums: Union[int, list], asnNum=None, varColumn=None) -> (str, str):
        """Returns: asnNum(str), ASN xml (str)"""
        varFile, varSheet = VARIABLE_FILE, 'PO'
        if varColumn is None:
            varColumn = 'DUMMY'
        if asnNum is None:
            asnNum = DBLib().getNewASNNum()
        final_xml_lines = str()
        MSG_TYPE = ENV_CONFIG.get('data', 'asn_xml_message_type')
        destFacilityAliasId = ENV_CONFIG.get('facility', 'facility_alias_id')

        assert type(items) == type(qtys), 'Datatype didnt match for items and qtys'

        items_list = items if type(items) == list else [items]
        qtys_list = qtys if type(qtys) == list else [qtys]
        poLineNums_list = poLineNums if type(poLineNums) == list else [poLineNums]
        pos_list = []
        if type(poNums) == str:
            pos_list.extend([poNums for i in range(len(items_list))])
        else:
            pos_list = poNums
        assert len(items_list) == len(pos_list), 'Datatype didnt match for items and poNums'

        if len(qtys_list) != len(items_list):
            assert False, 'Insufficient qtys provided for items'
        elif varFile is None or varSheet is None or varColumn is None:
            assert False, 'Variable file not provided'
        else:
            for i in range(0, len(items_list)):
                final_xml_lines = final_xml_lines + tags_skulevel_asnxml.XML_ASN_DETAIL \
                    .replace('#ITEM_NAME#', items_list[i]).replace('#ITEM_QTY#', str(qtys_list[i])) \
                    .replace('#SEQ_NUM#', str(i + 1)).replace('#PO_NUM#', pos_list[i]) \
                    .replace('#PO_LINE_NUM#', str(poLineNums_list[i]))

        if final_xml_lines == "":
            assert False, 'Xml for lines didnt create'
        else:
            final_xml = tags_skulevel_asnxml.XML_START + '\n' \
                        + tags_skulevel_asnxml.XML_HEADER.replace('#MSG_TYPE#', MSG_TYPE) + '\n' \
                        + tags_skulevel_asnxml.XML_MESSAGE.replace('#ASN_ID#', asnNum) \
                            .replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId) + '\n' \
                        + final_xml_lines + '\n' + tags_skulevel_asnxml.XML_END
            final_xml = DataGeneric.replace_dyn(final_xml)
        final_xml = XMLUtil.format_xml(final_xml)

        '''Replace data from var file'''
        final_xml, varFileData = DataGeneric.replace_from_varfile(varFile, varSheet, varColumn, data=final_xml)

        '''Export xml to file'''
        # threadId = str(threading.current_thread().native_id)
        # xmlFilePath = os.path.join(RUNTIME_DIR,
        #                            threadId + '_skulvl_asnxml_' + Commons.build_date_forfilename() + '.xml')
        # FileUtil.write_file(xmlFilePath, final_xml)
        # printit('Sku level ASN xml path:' + xmlFilePath)
        printit('Sku level ASN xml:', final_xml)

        return asnNum, final_xml

    @classmethod
    def buildPOXml(cls, items: Union[str, list], qtys: Union[int, list], poNum=None, varColumn=None) -> (str, dict):
        """Returns: poNum(str), PO xml (str)"""
        varFile, varSheet = VARIABLE_FILE, 'PO'
        if varColumn is None:
            varColumn = 'DUMMY'
        if poNum is None:
            poNum = DBLib().getNewPONum()
        final_xml_lines = str()
        MSG_TYPE = ENV_CONFIG.get('data', 'po_xml_message_type')
        destFacilityAliasId = ENV_CONFIG.get('facility', 'facility_alias_id')

        items_list = items if type(items) == list else [items]
        qtys_list = qtys if type(qtys) == list else [qtys]

        if len(items_list) != len(qtys_list):
            assert False, 'Insufficient items provided against qtys'
        elif varFile is None or varSheet is None or varColumn is None:
            assert False, 'Variable file not provided'
        else:
            for i in range(0, len(items_list)):
                final_xml_lines = final_xml_lines + tags_poxml.LINE.replace('#PO_LINE#', str(i + 1)) \
                    .replace('#ITEM_NAME#', items_list[i]).replace('#ITEM_QTY#', str(qtys_list[i]))

        if final_xml_lines == '':
            assert False, 'Xml for lines didnt create'
        else:
            final_xml = tags_poxml.XML_START + '\n' \
                        + tags_poxml.HEADER.replace('#MSG_TYPE#', MSG_TYPE) + '\n' \
                        + tags_poxml.MSG_PO_START.replace('#PO_NUM#', poNum) \
                            .replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId) + '\n' + final_xml_lines \
                        + '\n' + tags_poxml.XML_END
            final_xml = DataGeneric.replace_dyn(final_xml)
        final_xml = XMLUtil.format_xml(final_xml)

        '''Replace data from var file'''
        final_xml, varFileData = DataGeneric.replace_from_varfile(varFile, varSheet, varColumn, data=final_xml)

        '''Export xml to file'''
        # threadId = str(threading.current_thread().native_id)
        # xmlFilePath = os.path.join(RUNTIME_DIR, threadId + '_poxml_' + Commons.build_date_forfilename() + '.xml')
        # FileUtil.write_file(xmlFilePath, final_xml)
        # printit('PO xml path:' + xmlFilePath)
        printit('PO xml:', final_xml)

        return poNum, final_xml

    @classmethod
    def buildLPNXml(cls, items: Union[str, list], qtys: Union[int, list], isLpnLock: bool = None,
                    lpnId=None, varColumn=None) -> (str, dict):
        """Returns: """
        varFile, varSheet = VARIABLE_FILE, 'LPN'
        if varColumn is None:
            varColumn = 'DUMMY'
        if lpnId is None:
            lpnId = DBLib().getNewILPNNum()
        final_xml_lines = str()
        MSG_TYPE = ENV_CONFIG.get('data', 'lpn_xml_message_type')
        destFacilityAliasId = ENV_CONFIG.get('facility', 'facility_alias_id')

        items_list = items if type(items) == list else [items]
        qtys_list = qtys if type(qtys) == list else [qtys]

        if len(items_list) != len(qtys_list):
            assert False, 'Insufficient items provided against qtys'
        elif varFile is None or varSheet is None or varColumn is None:
            assert False, 'Variable file not provided'
        else:
            if len(items_list) == 1:
                final_xml_lines = final_xml_lines + tags_lpnxml.MSG_LPN_START.replace('#LPN_ID#', lpnId) \
                    .replace('#IS_SINGLE_SKU#', 'Y').replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId) \
                                  + tags_lpnxml.LPN_DETAIL.replace('#ITEM_NAME#', items_list[0]) \
                                      .replace('#ITEM_QTY#', str(qtys_list[0]))
            else:
                final_xml_lines = final_xml_lines + tags_lpnxml.MSG_LPN_START.replace('#LPN_ID#', lpnId) \
                    .replace('#IS_SINGLE_SKU#', 'N').replace('#DEST_FACILITY_ALIAS_ID#', destFacilityAliasId)
                for i in range(0, len(items_list)):
                    final_xml_lines = final_xml_lines + tags_lpnxml.LPN_DETAIL \
                        .replace('#ITEM_NAME#', items_list[i]).replace('#ITEM_QTY#', str(qtys_list[i]))
        if isLpnLock:
            final_xml_lines = final_xml_lines + tags_lpnxml.LPN_LOCK

        if final_xml_lines == '':
            assert False, 'Xml for lines didnt create'
        else:
            final_xml = tags_lpnxml.XML_START + '\n' \
                        + tags_lpnxml.HEADER.replace('#MSG_TYPE#', MSG_TYPE)\
                        + '\n' + final_xml_lines \
                        + '\n' + tags_lpnxml.XML_END
            final_xml = DataGeneric.replace_dyn(final_xml)
        final_xml = XMLUtil.format_xml(final_xml)

        '''Replace data from var file'''
        final_xml, varFileData = DataGeneric.replace_from_varfile(varFile, varSheet, varColumn, data=final_xml)

        '''Export xml to file'''
        # threadId = str(threading.current_thread().native_id)
        # xmlFilePath = os.path.join(RUNTIME_DIR, threadId + '_lpnxml_' + Commons.build_date_forfilename() + '.xml')
        # FileUtil.write_file(xmlFilePath, final_xml)
        # printit('LPN xml path:' + xmlFilePath)
        printit('LPN xml:', final_xml)

        return lpnId, final_xml
