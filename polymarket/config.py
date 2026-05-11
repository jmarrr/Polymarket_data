"""
poly_onchain 配置
"""

import os
from pathlib import Path
from typing import Optional


# ============== 路径 ==============

# PROJECT_ROOT 指向 poly_onchain 目录本身
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
LOG_DIR = PROJECT_ROOT / 'logs'

# 数据集目录 (parquet 完整数据)
DATASET_DIR = DATA_DIR / 'dataset'
# 最新结果目录 (csv 预览数据)
LATEST_RESULT_DIR = DATA_DIR / 'latest_result'

# Parquet 完整数据文件 (存放于 data/dataset/)
DECODED_EVENTS_FILE = DATASET_DIR / 'orderfilled.parquet'
MARKETS_FILE = DATASET_DIR / 'markets.parquet'
MISSING_MARKETS_FILE = DATASET_DIR / 'missing_markets.parquet'
TRADES_OUTPUT_FILE = DATASET_DIR / 'trades.parquet'

# CSV 预览文件 (存放于 data/latest_result/)
MARKETS_PREVIEW_FILE = LATEST_RESULT_DIR / 'markets.csv'
ORDERFILLED_PREVIEW_FILE = LATEST_RESULT_DIR / 'orderfilled.csv'
TRADES_PREVIEW_FILE = LATEST_RESULT_DIR / 'trades.csv'

# 清洗后数据目录 (存放于 data/data_clean/)
DATA_CLEAN_DIR = DATA_DIR / 'data_clean'
USERS_CLEAN_FILE = DATA_CLEAN_DIR / 'users.parquet'
QUANT_CLEAN_FILE = DATA_CLEAN_DIR / 'quant.parquet'

# 清洗后数据的 CSV 预览 (存放于 data/latest_result/)
USERS_PREVIEW_FILE = LATEST_RESULT_DIR / 'users.csv'
QUANT_PREVIEW_FILE = LATEST_RESULT_DIR / 'quant.csv'

# 状态文件
STATE_FILE = DATA_DIR / 'state.json'
TEMP_DIR = DATA_DIR / 'temp'


# ============== 区块链 ==============

POLYGON_CHAIN_ID = 137
POLYGON_RPC_URL = 'https://polygon.drpc.org'


def get_rpc_url(use_alchemy: bool = False) -> str:
    """获取 RPC URL"""
    if use_alchemy:
        api_key = os.getenv('ALCHEMY_API_KEY', '')
        if api_key:
            return f'https://polygon-mainnet.g.alchemy.com/v2/{api_key}'
    return POLYGON_RPC_URL


# ============== API ==============

GAMMA_API_URL = "https://gamma-api.polymarket.com"


# ============== 处理参数 ==============

BLOCKS_PER_BATCH = 100
REQUEST_DELAY = 0.2
USDC_ASSET_ID = '0'

OUTPUT_COLUMNS = [
    'timestamp', 'block_number', 'transactionHash', 'market_id',
    'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction',
    'price', 'usd_amount', 'token_amount',
    'maker_fee', 'taker_fee', 'protocol_fee', 'order_hash'
]


# ============== 合约地址 ==============

# 默认交易所合约地址
_DEFAULT_CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'
_DEFAULT_NEGRISK_CTF_EXCHANGE = '0xC5d563A36AE78145C45a50134d48A1215220f80a'
# Polymarket migrated CLOB v2 settlement to a new exchange contract.
# Verified 2026-05-11: 100% of recent OrderFilled events come from this
# contract; the legacy CTF + NegRisk contracts above are dormant.
_DEFAULT_NEW_EXCHANGE = '0xe111180000d2663c0091e4f400237545b87b996b'

# 从环境变量读取，支持自定义
_CTF_EXCHANGE = os.getenv('POLYMARKET_CTF_EXCHANGE', _DEFAULT_CTF_EXCHANGE)
_NEGRISK_CTF_EXCHANGE = os.getenv('POLYMARKET_NEGRISK_CTF_EXCHANGE', _DEFAULT_NEGRISK_CTF_EXCHANGE)
_NEW_EXCHANGE = os.getenv('POLYMARKET_NEW_EXCHANGE', _DEFAULT_NEW_EXCHANGE)

# 监听三个交易所合约（OrderFilled 事件来源）
POLYMARKET_CONTRACTS = {
    'CTF_EXCHANGE': _CTF_EXCHANGE,
    'NEGRISK_CTF_EXCHANGE': _NEGRISK_CTF_EXCHANGE,
    'NEW_EXCHANGE': _NEW_EXCHANGE,
}

# 交易所地址集合（小写，用于筛选）
EXCHANGE_ADDRESSES = {
    _CTF_EXCHANGE.lower(),
    _NEGRISK_CTF_EXCHANGE.lower(),
    _NEW_EXCHANGE.lower(),
}


# ============== 事件签名 ==============

# OrderFilled 事件签名:
#   OLD: legacy CTFExchange / NegRisk — 10 uint256 fields, decoded by EventDecoder
#   NEW: 0xe111180… exchange — 7 uint256 fields, decoded by EventDecoderNew
#        (reverse-engineered 2026-05-09; see polymarket_new_exchange_abi memory)
EVENT_SIGNATURES = {
    'OrderFilled':     'd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6',
    'OrderFilled_NEW': 'd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee',
}

ORDER_FILLED_TOPIC     = '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6'
ORDER_FILLED_TOPIC_NEW = '0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee'

# Both topics OR'd — pass as topics[0] in eth_getLogs / eth_subscribe to
# match either OLD or NEW contract emissions in a single query.
ORDER_FILLED_TOPICS = [ORDER_FILLED_TOPIC, ORDER_FILLED_TOPIC_NEW]


def get_event_name(signature: str) -> str:
    """根据签名获取事件名"""
    sig = signature.replace('0x', '').lower()
    for name, s in EVENT_SIGNATURES.items():
        if s.lower() == sig:
            return name
    return 'Unknown'
