"""
事件解码器 - 解码 OrderFilled 事件 (legacy CTF/NegRisk + new 0xe111180… exchange)

The new exchange contract (deployed early May 2026) emits OrderFilled with a
DIFFERENT ABI: 7 uint256 chunks (side, asset_id, amt_a, amt_b, fee, 0, 0)
instead of the legacy 7-uint shape (makerAssetId, takerAssetId, makerAmt,
takerAmt, makerFee, takerFee, protocolFee). The new event is also emitted
TWICE per fill (once per matched order side). See memory:
polymarket_new_exchange_abi.md for verified layout.

This decoder dispatches by topic[0] and reconstructs the legacy field names
(makerAssetId/takerAssetId/makerAmountFilled/takerAmountFilled) for the new
event, so downstream processors (trades.py) work unchanged.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from web3 import Web3
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)


# Topic constants (mirror config.py — kept here so this module is self-contained)
TOPIC_OLD = 'd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6'
TOPIC_NEW = 'd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee'


class EventDecoder:
    """OrderFilled 事件解码器 — handles both legacy and new exchange ABIs."""

    # Legacy OrderFilled ABI (CTF + NegRisk exchanges)
    ORDER_FILLED_ABI = [
        ("orderHash", "bytes32", True),
        ("maker", "address", True),
        ("taker", "address", True),
        ("makerAssetId", "uint256", False),
        ("takerAssetId", "uint256", False),
        ("makerAmountFilled", "uint256", False),
        ("takerAmountFilled", "uint256", False),
        ("makerFee", "uint256", False),
        ("takerFee", "uint256", False),
        ("protocolFee", "uint256", False),
    ]

    # New-exchange OrderFilled ABI — verified 2026-05-09
    # topics: [event_sig, orderHash, maker (indexed), taker (indexed)]
    # data 7 uints: [side_flag, asset_id, amt_a, amt_b, fee, 0, 0]
    #   side=0 (maker BUY):  amt_a = USDC paid, amt_b = tokens received
    #   side=1 (maker SELL): amt_a = tokens given, amt_b = USDC received
    NEW_ORDER_FILLED_ABI = [
        ("orderHash", "bytes32", True),
        ("maker", "address", True),
        ("taker", "address", True),
        ("sideFlag",  "uint256", False),
        ("assetId",   "uint256", False),
        ("amountA",   "uint256", False),
        ("amountB",   "uint256", False),
        ("fee",       "uint256", False),
        ("_reserved1", "uint256", False),
        ("_reserved2", "uint256", False),
    ]

    def __init__(self):
        self.w3 = Web3()

    def decode(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch by topic[0] to the legacy or new decoder."""
        topics = record.get('topics', [])
        topic0 = (topics[0] if topics else '').replace('0x', '').lower()

        if topic0 == TOPIC_NEW:
            return self._decode_new(record)
        # Default to legacy decode (also handles unknown topics if any leak through)
        return self._decode_legacy(record)

    def _decode_legacy(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Decode the legacy OrderFilled ABI (CTF + NegRisk)."""
        topics = record.get('topics', [])
        data = record.get('data', '')

        record['event_name'] = 'OrderFilled'

        indexed = [(n, t) for n, t, i in self.ORDER_FILLED_ABI if i]
        non_indexed = [(n, t) for n, t, i in self.ORDER_FILLED_ABI if not i]

        params = {}
        for i, (name, ptype) in enumerate(indexed):
            if i + 1 < len(topics):
                params[name] = self._decode_topic(ptype, topics[i + 1])
        if non_indexed and data:
            types = [t for _, t in non_indexed]
            values = self._decode_data(types, data)
            for (name, _), val in zip(non_indexed, values):
                params[name] = val

        record['decoded_params'] = params
        return record

    def _decode_new(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Decode new-exchange OrderFilled and reconstruct legacy field names
        (makerAssetId / takerAssetId / makerAmountFilled / takerAmountFilled)
        so downstream processors don't need to change.

        USDC's "asset_id" in the legacy convention is 0; the conditional
        token's id is the non-zero side. We map according to side_flag:
          side=0 (maker BUY):  maker gives USDC,    taker gives token
                               → makerAssetId = 0,  takerAssetId = assetId
          side=1 (maker SELL): maker gives token,   taker gives USDC
                               → makerAssetId = assetId, takerAssetId = 0
        """
        topics = record.get('topics', [])
        data = record.get('data', '')

        record['event_name'] = 'OrderFilled'

        # Indexed params (orderHash + maker + taker)
        params = {}
        indexed = [(n, t) for n, t, i in self.NEW_ORDER_FILLED_ABI if i]
        for i, (name, ptype) in enumerate(indexed):
            if i + 1 < len(topics):
                params[name] = self._decode_topic(ptype, topics[i + 1])

        # Non-indexed: 7 uint256 chunks
        non_indexed_types = [t for n, t, i in self.NEW_ORDER_FILLED_ABI if not i]
        values = self._decode_data(non_indexed_types, data) if data else [0] * 7

        side_flag = values[0] if len(values) > 0 else 0
        asset_id  = values[1] if len(values) > 1 else 0
        amount_a  = values[2] if len(values) > 2 else 0
        amount_b  = values[3] if len(values) > 3 else 0
        fee       = values[4] if len(values) > 4 else 0

        # Reconstruct legacy field names so trades.py works unchanged.
        # Per-side fees not separately reported in the new event — put the
        # whole `fee` on protocolFee, leave makerFee/takerFee as 0.
        if side_flag == 0:
            # maker BUY: maker gives USDC, taker gives token
            params['makerAssetId']      = 0
            params['takerAssetId']      = asset_id
            params['makerAmountFilled'] = amount_a   # USDC paid
            params['takerAmountFilled'] = amount_b   # tokens received
        else:
            # maker SELL (side=1): maker gives token, taker gives USDC
            params['makerAssetId']      = asset_id
            params['takerAssetId']      = 0
            params['makerAmountFilled'] = amount_a   # tokens given
            params['takerAmountFilled'] = amount_b   # USDC received

        params['makerFee']    = 0
        params['takerFee']    = 0
        params['protocolFee'] = fee
        # Stash the new-event-only fields under non-conflicting names for
        # callers that want them (e.g., to dedupe the 2 logs per fill by
        # picking the side_flag=0 view).
        params['_new_side_flag'] = side_flag
        params['_new_asset_id']  = asset_id

        record['decoded_params'] = params
        return record

    def decode_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量解码"""
        return [self.decode(r) for r in records]

    def format_event(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """格式化 OrderFilled 事件为输出格式"""
        params = record.get('decoded_params', {})

        # 基础字段
        result = {
            'transaction_hash': record.get('transaction_hash', ''),
            'block_number': record.get('block_number', 0),
            'log_index': record.get('log_index', 0),
            'timestamp': record.get('timestamp', 0),
            'contract': record.get('contract', ''),
            'event_name': 'OrderFilled',
        }

        # 格式化时间
        ts = result['timestamp']
        if isinstance(ts, (int, float)) and 0 < ts < 4102444800:
            result['datetime'] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # OrderFilled 参数 (asset_id 是超大整数，必须转成字符串)
        result.update({
            'order_hash': params.get('orderHash', ''),
            'maker': params.get('maker', ''),
            'taker': params.get('taker', ''),
            'maker_asset_id': str(params.get('makerAssetId', 0)),
            'taker_asset_id': str(params.get('takerAssetId', 0)),
            'maker_amount_filled': params.get('makerAmountFilled', 0),
            'taker_amount_filled': params.get('takerAmountFilled', 0),
            'maker_fee': params.get('makerFee', 0),
            'taker_fee': params.get('takerFee', 0),
            'protocol_fee': params.get('protocolFee', 0),
        })

        return result

    def format_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量格式化"""
        return [self.format_event(r) for r in records]

    def _decode_topic(self, ptype: str, value: str) -> Any:
        """解码 topic"""
        try:
            val = value.replace('0x', '').zfill(64)
            if ptype == 'address':
                return to_checksum_address('0x' + val[24:])
            elif ptype == 'uint256':
                return int(val, 16)
            elif ptype == 'bytes32':
                return value
            return value
        except (ValueError, TypeError, AttributeError):
            return value

    def _decode_data(self, types: List[str], data: Any) -> List[Any]:
        """解码 data 字段"""
        try:
            if isinstance(data, bytes):
                data = data.hex()
            clean = data.replace('0x', '')
            if len(clean) % 64 != 0:
                clean = clean.ljust(((len(clean) // 64) + 1) * 64, '0')

            results = []
            offset = 0

            for ptype in types:
                if offset + 64 > len(clean):
                    results.append(0 if ptype.startswith('uint') else None)
                    continue

                chunk = clean[offset:offset + 64]

                if ptype.startswith('uint') and not ptype.endswith('[]'):
                    results.append(int(chunk, 16))
                elif ptype == 'address':
                    results.append(to_checksum_address('0x' + chunk[24:]))
                elif ptype.endswith('[]'):
                    # 简化处理数组
                    results.append([])
                else:
                    results.append('0x' + chunk)

                offset += 64

            return results
        except Exception as e:
            logger.warning(f"解码失败: {e}")
            return [0 if t.startswith('uint') else None for t in types]
