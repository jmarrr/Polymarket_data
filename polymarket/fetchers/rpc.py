"""
Polygon RPC 客户端和日志获取
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from ..config import (
    POLYGON_RPC_URL, get_rpc_url, BLOCKS_PER_BATCH, REQUEST_DELAY,
    POLYMARKET_CONTRACTS, EVENT_SIGNATURES, ORDER_FILLED_TOPIC, ORDER_FILLED_TOPICS
)

logger = logging.getLogger(__name__)


class PolygonRpcClient:
    """Polygon RPC 客户端"""

    # Polygon 出块时间约 2 秒
    BLOCK_TIME = 2

    def __init__(self, use_alchemy: bool = False):
        rpc_url = get_rpc_url(use_alchemy)
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        # Polygon 是 POA 链，需要添加中间件处理 extraData 字段
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        # web3.py 要求 checksum 地址格式
        self.contract_addresses = [Web3.to_checksum_address(addr) for addr in POLYMARKET_CONTRACTS.values()]
        self._timestamp_cache: Dict[int, int] = {}
        logger.info(f"RPC 连接: {rpc_url.split('/v2/')[0] if '/v2/' in rpc_url else rpc_url}")

    def get_latest_block(self) -> int:
        return self.w3.eth.block_number

    def get_logs(self, start_block: int, end_block: int) -> Optional[List[Dict[str, Any]]]:
        """获取区块范围内的 OrderFilled 日志

        返回:
            List[Dict]: 成功时返回日志列表（可能为空）
            None: RPC 请求失败时返回 None
        """
        try:
            logs = self.w3.eth.get_logs({
                'fromBlock': start_block,
                'toBlock': end_block,
                'address': self.contract_addresses,
                'topics': [ORDER_FILLED_TOPICS]  # OLD + NEW OrderFilled 事件 (OR'd)
            })
            return [dict(log) for log in logs]
        except Exception as e:
            logger.error(f"获取日志失败: {e}")
            return None  # 返回 None 表示请求失败，区分于空列表（无数据）

    def get_block_timestamp(self, block_number: int) -> int:
        """获取区块时间戳"""
        if block_number in self._timestamp_cache:
            return self._timestamp_cache[block_number]
        try:
            block = self.w3.eth.get_block(block_number)
            ts = block['timestamp']
            self._timestamp_cache[block_number] = ts
            return ts
        except Exception as e:
            # 不要返回错误的时间戳，而是抛出异常让调用方处理
            raise RuntimeError(f"无法获取区块 {block_number} 的时间戳: {e}")

    def batch_get_timestamps(self, block_numbers: List[int]) -> Dict[int, int]:
        """批量获取时间戳"""
        result = {}
        for bn in block_numbers:
            result[bn] = self.get_block_timestamp(bn)
        return result

    def estimate_timestamps(self, block_numbers: List[int]) -> Dict[int, int]:
        """估算时间戳（减少 RPC 调用）"""
        if not block_numbers:
            return {}

        sorted_blocks = sorted(block_numbers)
        first_ts = self.get_block_timestamp(sorted_blocks[0])

        result = {}
        for bn in sorted_blocks:
            offset = (bn - sorted_blocks[0]) * self.BLOCK_TIME
            result[bn] = first_ts + offset
        return result

    def test_connection(self) -> bool:
        try:
            self.w3.eth.block_number
            return True
        except Exception:
            return False


class LogFetcher:
    """链上日志获取器"""

    def __init__(self, use_alchemy: bool = False):
        self.client = PolygonRpcClient(use_alchemy=use_alchemy)
        self.address_to_name = {
            addr.lower(): name for name, addr in POLYMARKET_CONTRACTS.items()
        }

    def fetch_block_range(self, start_block: int, end_block: int) -> Optional[List[Dict[str, Any]]]:
        """获取指定区块范围的日志

        返回:
            List[Dict]: 成功时返回记录列表（可能为空）
            None: RPC 请求失败时返回 None
        """
        logger.info(f"获取区块 {start_block} - {end_block}")

        logs = self.client.get_logs(start_block, end_block)
        if logs is None:
            return None  # RPC 失败
        if not logs:
            return []  # 成功但无数据

        # 获取时间戳 - 优先使用 RPC 返回的 blockTimestamp
        block_timestamps = {}
        unique_blocks_without_ts = set()

        for log in logs:
            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)

            # 检查 RPC 是否返回了 blockTimestamp
            block_ts = log.get('blockTimestamp')
            if block_ts:
                if isinstance(block_ts, str):
                    ts = int(block_ts, 16) if block_ts.startswith('0x') else int(block_ts)
                else:
                    ts = int(block_ts)
                block_timestamps[bn] = ts
            else:
                unique_blocks_without_ts.add(bn)

        # 只对没有时间戳的区块进行查询
        if unique_blocks_without_ts:
            missing_timestamps = (
                self.client.batch_get_timestamps(sorted(unique_blocks_without_ts))
                if len(unique_blocks_without_ts) <= 3
                else self.client.estimate_timestamps(sorted(unique_blocks_without_ts))
            )
            block_timestamps.update(missing_timestamps)

        # 处理日志
        records = []
        for log in logs:
            record = self._process_log(log, start_block, end_block, block_timestamps)
            if record:
                records.append(record)

        logger.info(f"获取到 {len(records)} 条记录")
        return records

    def _process_log(self, log: Dict, start_block: int, end_block: int,
                     block_timestamps: Dict[int, int]) -> Optional[Dict[str, Any]]:
        """处理单个日志"""
        try:
            log_address = log.get('address', '').lower()
            contract_name = self.address_to_name.get(log_address, 'Unknown')

            bn = log['blockNumber']
            if isinstance(bn, str):
                bn = int(bn, 16) if bn.startswith('0x') else int(bn)

            # 获取区块时间戳，如果缺失则记录警告并尝试单独获取
            timestamp = block_timestamps.get(bn)
            if timestamp is None:
                logger.warning(f"区块 {bn} 缺失时间戳，尝试单独获取...")
                try:
                    timestamp = self._get_block_timestamp(bn)
                    block_timestamps[bn] = timestamp
                except Exception as e:
                    logger.error(f"无法获取区块 {bn} 的时间戳: {e}")
                    # 跳过这条记录而不是使用错误的时间戳
                    return None

            tx_hash = log['transactionHash']
            if hasattr(tx_hash, 'hex'):
                tx_hash = tx_hash.hex()

            topics = [t.hex() if hasattr(t, 'hex') else t for t in log['topics']]

            # 识别事件名
            event_name = 'Unknown'
            event_sig = ''
            if topics:
                event_sig = topics[0].replace('0x', '').lower()
                for name, sig in EVENT_SIGNATURES.items():
                    if sig.lower() == event_sig:
                        event_name = name
                        break

            return {
                'contract': contract_name,
                'address': log['address'],
                'block_number': bn,
                'transaction_hash': tx_hash,
                'log_index': log['logIndex'],
                'timestamp': timestamp,
                'block_range': f"{start_block}-{end_block}",
                'topics': topics,
                'data': log['data'],
                'event_name': event_name,
                'event_signature': event_sig
            }
        except Exception as e:
            logger.warning(f"处理日志失败: {e}")
            return None

    def fetch_range_in_batches(self, start_block: int, end_block: int,
                                batch_size: int = BLOCKS_PER_BATCH) -> Optional[List[Dict[str, Any]]]:
        """分批获取

        返回:
            List[Dict]: 成功时返回记录列表（可能为空）
            None: RPC 请求失败时返回 None
        """
        all_records = []
        current = start_block

        while current <= end_block:
            batch_end = min(current + batch_size - 1, end_block)
            records = self.fetch_block_range(current, batch_end)
            if records is None:
                # RPC 失败，返回 None 让上层处理
                return None
            all_records.extend(records)
            current = batch_end + 1
            if current <= end_block:
                time.sleep(REQUEST_DELAY)

        logger.info(f"总共获取 {len(all_records)} 条记录")
        return all_records

    def get_latest_block(self) -> int:
        return self.client.get_latest_block()

    def test_connection(self) -> bool:
        return self.client.test_connection()
