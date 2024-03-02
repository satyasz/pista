from core.config_service import ENV_CONFIG
from core.db_service import DBService
from core.log_service import Logging, printit


class DBAdmin:
    logger = Logging.get(__qualname__)

    @staticmethod
    def _insertRecordInCLMessage(schema, msg, eventId, msgId: str = None) -> str:
        """"""
        # eventId = '4020000' if isForContainerStatus else '3660000' if isForItemStatus else None
        assert eventId is not None, 'Event id not available'

        finalMsgId = msgId if msgId is not None else 'CL_MESSAGE_ID_SEQ.NEXTVAL'

        insertQ = f"""insert into cl_message(version_id,msg_id,event_id,prty,encoding,when_created,source_id,source_uri
                        ,data,created_dttm,last_updated_dttm)
                    values (0,{finalMsgId},'{eventId}',9999,1,SYSDATE,null,null
                        ,'{msg}',SYSDATE,SYSDATE)"""
        DBService.update_db(insertQ, schema)

        '''Get msg_id from cl_message'''
        sqlClMsg = """select * from cl_message where msg_id is not null #CONSTRAINTS# order by when_created desc"""
        constraints = " and data like '%" + msg[1:-1] + "%'"
        sqlClMsg = sqlClMsg.replace('#CONSTRAINTS#', constraints)

        dbResult = DBService.fetch_row(sqlClMsg, schema)
        msgId = dbResult.get('MSG_ID')
        
        return msgId

    @staticmethod
    def _insertRecordInCLEndpointQueue(schema, msgId, endpointId) -> str:
        """"""
        insertQ = f"""insert into cl_endpoint_queue (version_id,endpoint_queue_id,endpoint_id,msg_id,when_queued,status
                         ,prty,hold_until,error_count,error_cost,disposition,when_status_changed,log_id
                         ,target_id,target_uri,error_details)
                     values (1,CL_ENDPOINT_QUEUE_SEQ.NEXTVAL,{endpointId},'{msgId}',SYSDATE,2
                         ,0,null,0,0,0,SYSDATE,null
                         ,null,null,null)"""
        isExecuted = DBService.update_db(insertQ, schema)
        
        assert isExecuted, 'Insert to cl_endpoint_queue didnt work'
        
        return msgId

    @staticmethod
    def _insertRecordInPLD(schema, locnId: str, maxInvQty: int, itemId: str) -> str:
        """Insert 1 PICK_LOCN_DTL record
        """
        assert schema is not None, 'schema missing'
        assert locnId is not None, 'locnId missing'
        assert maxInvQty is not None, 'maxInvQty missing'
        assert itemId is not None, 'itemId missing'

        sqlLocnHdr = f"""select lh.locn_id, lh.locn_brcd, plh.pick_locn_hdr_id from locn_hdr lh 
                        inner join pick_locn_hdr plh on lh.locn_id = plh.locn_id
                        where lh.locn_id = '{locnId}'"""
        locnHdrRow = DBService.fetch_row(sqlLocnHdr, schema)
        pickLocnHdrId = locnHdrRow.get('PICK_LOCN_HDR_ID')

        assert pickLocnHdrId is not None, 'PickLocnHdrId not available ' + sqlLocnHdr

        sqlPldMaxSeq = f"""SELECT MAX(LOCN_SEQ_NBR) MAX_LOCN_SEQ FROM PICK_LOCN_DTL WHERE LOCN_ID = '{locnId}'"""
        pldMaxSeqRow = DBService.fetch_row(sqlPldMaxSeq, schema)
        
        maxLocnSeqNbr = pldMaxSeqRow.get('MAX_LOCN_SEQ')
        locnSeqNbr = 1 if maxLocnSeqNbr is None else int(maxLocnSeqNbr) + 1

        insertQ = f"""insert into pick_locn_dtl(locn_id,locn_seq_nbr
                        ,sku_attr_1,sku_attr_2,sku_attr_3,sku_attr_4,sku_attr_5,prod_stat,batch_nbr,cntry_of_orgn
                        ,max_invn_qty,min_invn_qty,min_invn_cases,max_invn_cases,trig_repl_for_sku
                        ,ltst_sku_assign,create_date_time,mod_date_time,user_id,pick_locn_dtl_id,pick_locn_hdr_id
                        ,item_master_id,item_id,created_dttm)
                    values ('{locnId}',{locnSeqNbr}
                        ,'*','*','*','*','*','*','*','*'
                        ,{maxInvQty},1,0,0,'Y'
                        ,'Y',SYSDATE,SYSDATE,'AUTOMATION',PICK_LOCN_DTL_ID_SEQ.NEXTVAL,{pickLocnHdrId}
                        ,{itemId},{itemId},SYSDATE)"""
        DBService.update_db(insertQ, schema)

        sqlPldDtlId = f"""SELECT PICK_LOCN_DTL_ID FROM PICK_LOCN_DTL WHERE LOCN_ID = '{locnId}'"""
        pldDtlIdRow = DBService.fetch_row(sqlPldDtlId, schema)
        
        pickLocnDtlId = pldDtlIdRow.get('PICK_LOCN_DTL_ID')

        return pickLocnDtlId

    @staticmethod
    def _insertRecordInLPN(schema, lpnBrcd: str, isSingleLnLpn: bool, currLocnId: str, lpnFacStat: int,
                           itemId: str = None, itemBrcd: str = None) -> str:
        """Insert 1 LPN record
        """
        assert schema is not None, 'schema missing'
        assert lpnBrcd is not None, 'lpnBrcd missing'
        # assert itemId is not None or itemBrcd is not None, 'itemId and itemBrcd missing'

        isSingleSkuLpn = 'Y' if isSingleLnLpn else 'N'
        itemId = '' if itemId is None else itemId
        itemBrcd = '' if itemBrcd is None else itemBrcd

        companyId = int(ENV_CONFIG.get('facility', 'company_id'))
        facilityAliasId = str(ENV_CONFIG.get('facility', 'facility_alias_id'))
        lpnStatus = 45

        insertQ = f"""insert into lpn (lpn_id,tc_lpn_id,tc_company_id,lpn_type,c_facility_id,o_facility_id
                        ,lpn_status,lpn_facility_status,hibernate_version
                        ,estimated_volume,weight,actual_volume,qty_uom_id_base,weight_uom_id_base,volume_uom_id_base
                        ,created_source,created_dttm,c_facility_alias_id,o_facility_alias_id,curr_sub_locn_id
                        ,inbound_outbound_indicator,billing_method,frt_forwarder_acct_nbr, shipment_print_sed
                        ,physical_entity_code,single_line_lpn,item_id,estimated_weight,item_name)
                    values (LPN_ID_SEQ.NEXTVAL,'{lpnBrcd}',{companyId},1,{companyId},{companyId}
                        ,{lpnStatus},{lpnFacStat},3
                        ,1,1,1,54,23,22
                        ,'AUTOMATION',SYSDATE,'{facilityAliasId}','{facilityAliasId}','{currLocnId}'
                        ,'I',1,'DUMYY',1
                        ,'P','{isSingleSkuLpn}','{itemId}',1,'{itemBrcd}')"""
        DBService.update_db(insertQ, schema)

        sqlLpnHdr = f"""select * from lpn l where tc_lpn_id = '{lpnBrcd}'"""
        lpnHdrRow = DBService.fetch_row(sqlLpnHdr, schema)
        
        lpnId = lpnHdrRow.get('LPN_ID')

        return lpnId

    @staticmethod
    def _insertRecordInLPNDtl(schema, lpnId: str, itemId: str, itemBrcd: str, qty: int) -> str:
        """Insert 1 LPN_DETAIL record
        """
        assert schema is not None, 'schema missing'
        assert lpnId is not None, 'lpnId missing'
        assert itemId is not None, 'itemId missing'
        assert itemBrcd is not None, 'itemBrcd missing'
        assert qty is not None, 'qty missing'

        sqlItemDtls = f"""select ic.*,iw.*,ifmw.*,im.* from item_cbo ic inner join item_wms iw on ic.item_id=iw.item_id 
                        inner join item_facility_mapping_wms ifmw on ic.item_id = ifmw.item_id
                        inner join item_master im on ic.item_id = im.item_id
                        where ic.item_name = '{itemBrcd}'"""
        itemDtlsRow = DBService.fetch_row(sqlItemDtls, schema)

        itemStdPckQty = itemDtlsRow.get('STD_PACK_QTY')
        itemBaseStrgUomId = itemDtlsRow.get('BASE_STORAGE_UOM_ID')

        insertQ = f"""insert into lpn_detail(tc_company_id,lpn_id,lpn_detail_id,lpn_detail_status,received_qty
                        ,hibernate_version,item_id,std_pack_qty,size_value,qty_uom_id,created_source,created_dttm
                        ,shipped_qty,initial_qty,qty_uom_id_base,item_name)
                    values (1,{lpnId},LPN_DETAIL_ID_SEQ.NEXTVAL,null,0
                        ,1,{itemId},{itemStdPckQty},{qty},{itemBaseStrgUomId},'AUTOMATION',SYSDATE
                        ,0,{qty},{itemBaseStrgUomId},'{itemBrcd}')"""
        DBService.update_db(insertQ, schema)

        sqMaxLpnDtlId = f"""SELECT MAX(LPN_DETAIL_ID) MAX_LPN_DTL_ID FROM LPN_DETAIL WHERE LPN_ID = '{lpnId}'"""
        maxLpnDtlIdRow = DBService.fetch_row(sqMaxLpnDtlId, schema)
        
        lpnDtlId = maxLpnDtlIdRow.get('MAX_LPN_DTL_ID')

        return lpnDtlId

    @staticmethod
    def _insertRecordInWMInvForResv(schema, locnId: str, lpnId, lpnBrcd, lpnDtlId, itemId, onHandQty):
        """Insert 1 WM_INVENTORY record for resv locn
        """
        assert schema is not None, 'schema missing'
        assert locnId is not None, 'locnId missing'
        assert lpnId is not None, 'lpnId missing'
        assert lpnBrcd is not None, 'lpnBrcd missing'
        assert lpnDtlId is not None, 'lpnDtlId missing'
        assert itemId is not None, 'itemId missing'
        assert onHandQty is not None, 'onHandQty missing'

        companyId = int(ENV_CONFIG.get('facility', 'company_id'))
        facilityAliasId = str(ENV_CONFIG.get('facility', 'facility_alias_id'))
        locnClass = 'R'

        insertQ = f"""insert into wm_inventory(tc_company_id,location_id,tc_lpn_id,locn_class,allocatable
                        ,created_source,created_dttm,inbound_outbound_indicator,lpn_detail_id,c_facility_id,lpn_id
                        ,wm_inventory_id,on_hand_qty,wm_allocated_qty,to_be_filled_qty,item_id) 
                    values ({companyId},'{locnId}','{lpnBrcd}','{locnClass}','Y'
                        ,'AUTOMATION',SYSDATE,'I',{lpnDtlId},{companyId},{lpnId}
                        ,WM_INVENTORY_ID_SEQ.nextval,{onHandQty},0,0,{itemId})"""
        DBService.update_db(insertQ, schema)

    @staticmethod
    def _insertRecordInWMInvForActv(schema, locnId: str, itemId: str, onHandQty: int, pickLocnDtlId: str):
        """Insert 1 WM_INVENTORY record for actv locn
        """
        assert schema is not None, 'schema missing'
        assert locnId is not None, 'locnId missing'
        assert itemId is not None, 'itemId missing'
        assert onHandQty is not None, 'onHandQty missing'
        assert pickLocnDtlId is not None, 'pickLocnDtlId missing'

        companyId = int(ENV_CONFIG.get('facility', 'company_id'))
        facilityAliasId = str(ENV_CONFIG.get('facility', 'facility_alias_id'))
        locnClass = 'A'

        insertQ = f"""insert into wm_inventory(tc_company_id,location_id,locn_class,allocatable,created_source
                        ,product_status,cntry_of_orgn,item_attr_1,item_attr_2,item_attr_3,item_attr_4,item_attr_5,batch_nbr
                        ,created_dttm,c_facility_id,wm_inventory_id,on_hand_qty,wm_allocated_qty,to_be_filled_qty
                        ,location_dtl_id,item_id)
                    values ({companyId},'{locnId}','{locnClass}','Y','AUTOMATION'
                        ,'*','*','*','*','*','*','*','*'
                        ,SYSDATE,{companyId},WM_INVENTORY_ID_SEQ.nextval,{onHandQty},0,0
                        ,{pickLocnDtlId},{itemId})"""
        DBService.update_db(insertQ, schema)

    @staticmethod
    def _insertToResvInvnTables(schema, lpnBrcd, locnId: str, lpnFacStat: int, itemId: str, itemBrcd: str, qty: int):
        """Insert record into lpn, lpn_detail, wm_inventory
        lpnFacStat: 10 or 30
        """
        # lpnBrcd = self.getNewILPNNum()
        printit(f"Inserting resv wm/lpn invn: itemId {itemId} item {itemBrcd} locnId {locnId} lpnFacStat {lpnFacStat} lpn {lpnBrcd} qty {qty}")

        '''Insert into lpn'''
        lpnId = DBAdmin._insertRecordInLPN(schema, lpnBrcd=lpnBrcd, isSingleLnLpn=True, currLocnId=locnId,
                                           lpnFacStat=lpnFacStat, itemId=itemId, itemBrcd=itemBrcd)
        for i in range(1):
            '''Insert into lpn dtl'''
            lpnDtlId = DBAdmin._insertRecordInLPNDtl(schema, lpnId=lpnId, itemId=itemId, itemBrcd=itemBrcd, qty=qty)

            '''Insert into wm'''
            DBAdmin._insertRecordInWMInvForResv(schema, locnId=locnId, lpnId=lpnId, lpnBrcd=lpnBrcd, lpnDtlId=lpnDtlId,
                                                itemId=itemId, onHandQty=qty)

        return lpnBrcd

    @staticmethod
    def _insertToActvInvnTables(schema, locnId: str, maxInvQty: int, itemId: str, qty: int):
        """Insert record into pick_locn_dtl, wm_inventory
        """
        printit(f"Inserting actv wm/pld invn: itemId {itemId} locnId {locnId} maxInvQty {maxInvQty} qty {qty}")

        assert schema is not None, 'schema missing'
        assert locnId is not None, 'locnId missing'
        assert itemId is not None, 'itemId missing'

        for i in range(1):
            '''Insert into pld'''
            pickLocnDtlId = DBAdmin._insertRecordInPLD(schema, locnId=locnId, maxInvQty=maxInvQty, itemId=itemId)

            '''Insert into wm'''
            DBAdmin._insertRecordInWMInvForActv(schema, locnId=locnId, itemId=itemId, onHandQty=qty, pickLocnDtlId=pickLocnDtlId)

    @staticmethod
    def _deleteRecordFromWMInv(schema, locnBrcd: str, item: str):
        """Delete 1 WM_INVENTORY record
        """
        printit(f"Deleting wm invn: item {item} locn {locnBrcd}")

        assert schema is not None, 'schema missing'
        assert locnBrcd is not None, 'locnBrcd missing'
        assert item is not None, 'item missing'

        deleteQ = f"""delete from wm_inventory 
                        where location_id in (select locn_id from locn_hdr where locn_brcd = '{locnBrcd}')
                        and item_id in (select item_id from item_cbo where item_name = '{item}')"""
        DBService.update_db(deleteQ, schema)

    @staticmethod
    def _deleteRecordFromPLD(schema, locnBrcd: str, item: str):
        """Delete 1 PICK_LOCN_DTL record
        """
        printit(f"Deleting pld: item {item} locn {locnBrcd}")

        assert schema is not None, 'schema missing'
        assert locnBrcd is not None, 'locnBrcd missing'
        assert item is not None, 'item missing'

        deleteQ = f"""delete from pick_locn_dtl 
                        where locn_id in (select locn_id from locn_hdr where locn_brcd = '{locnBrcd}') 
                        and item_id in (select item_id from item_cbo where item_name = '{item}')"""
        DBService.update_db(deleteQ, schema)

    @staticmethod
    def _deleteFromActvInvnTables(schema, locnBrcd: str, item: str):
        """Delete record from wm_inventory & pick_locn_dtl
        """
        printit(f"Deleting actv wm/pld invn: item {item} locn {locnBrcd}")

        '''Delete from wm'''
        DBAdmin._deleteRecordFromWMInv(schema, locnBrcd=locnBrcd, item=item)

        '''Delete from pld'''
        DBAdmin._deleteRecordFromPLD(schema, locnBrcd=locnBrcd, item=item)
        