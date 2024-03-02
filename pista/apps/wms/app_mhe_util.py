import os

from core.config_service import ENV_CONFIG
from core.file_service import TCPUtil
from core.log_service import printit


class MHEUtil(TCPUtil):

    def __init__(self, host, port):
        super().__init__(host, port)

    @staticmethod
    def buildContainerSatusMsgFromVLM(ilpn: str, vlmLocn: str):
        msg = None

        _ENV = os.environ['env']
        _ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')
        _FLAG = ENV_CONFIG.get('flag', 'cntrStatMsgFormatFromVlm_flag')

        printit(f">>> Using flag {_FLAG}")

        if _FLAG == '1':
            msg = f"00001^CONTAINERSTATUS^{ilpn}^REP^REP^{vlmLocn}^4363^20230315060000"
        elif _FLAG == '2':
            msg = f"00099|CONTAINERSTATUS|{ilpn}|REP|REP|{vlmLocn}|4363|2023 10 19 17 00 00|2023 10 19 17 00 00"

        assert msg is not None, 'Building cntrstat msg from vlm didnt succeed'
        return msg

    @staticmethod
    def buildContainerSatusMsgFromAutoStore(pallet: str, autoStoreLocn: str):
        msg = f"00007|CONTAINERSTATUS|{pallet}|REP|REP|{autoStoreLocn}|4363|2023 03 15 06 00 00|2023 03 15 06 00 00"
        return msg

    @staticmethod
    def buildContainerSatusMsgFromASRS(ilpn: str, asrsLocn: str):
        msg = f"00020|CONTAINERSTATUS|{ilpn}|REP|REP|{asrsLocn}|4363|2023 03 15 06 00 00|2023 03 15 06 00 00"
        return msg

    @staticmethod
    def buildItemSatusMsgFromVLM(waveNum: str, order: str, allocInvnDtlId: str, vlmLocn: str, oLpn: str, qty: str, msgId):
        msg = f"^^^^ITEMSTATUS^{msgId}^ITEMSTATUS^{waveNum}^1^{order}^ {allocInvnDtlId}^COMPLETED^{vlmLocn}^^^^" \
              f"{oLpn}^{qty}^^{qty}^^^^23/02/2023 02:20:28^4363^^^^^23/02/2023 02:20:28"
        return msg

    @staticmethod
    def buildInventoryAdjMsgFromVLM(item: str, adjQty: int, adjOperator: str, vlmLocn: str, locnId: str):
        msg = f"1082499|INVENTORYADJ|{item}|{adjQty}|{adjOperator}|01|{vlmLocn}|4360|2/21/2022 11:22:39 AM|{locnId}|1|0"
        return msg

    @staticmethod
    def buildPutawayReplenMsgToAutoLocnForAssert(ilpn: str, item: str, qty: int, locn:str=None, locnId:str=None):
        msg = None

        _ENV = os.environ['env']
        _ENV_TYPE = ENV_CONFIG.get('framework', 'env_type')
        _FLAG = ENV_CONFIG.get('flag', 'putawayReplenMsgFormatToAutoLocn_flag')

        printit(f">>> Using flag {_FLAG}")
        
        if _FLAG == '1':
            msg = f"^REPLENISHMENT^{ilpn}^{item}^{qty}^"
        elif _FLAG == '2':
            if locn is None:
                msg = f"|3||{item}|{ilpn}||EACH|{qty}|"
            else:
                msg = f"|3||{item}|{ilpn}||EACH|{qty}|{locn}|{locnId}"

        assert msg is not None, 'Building putaway/replen msg to auto locn didnt succeed'
        return msg
