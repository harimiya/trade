"""
company_mapper.py
落札企業名 → 親会社 / 自社 → 東証ティッカー のマッピングモジュール

- parent_ticker : 親会社のティッカー（子会社の場合は親）
- self_ticker   : 落札企業自身が上場している場合のティッカー
"""

import csv
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
MAPPING_FILE = DATA_DIR / "company_ticker_map.csv"


class CompanyMapper:
    def __init__(self, mapping_file: str = str(MAPPING_FILE)):
        self.mapping: list[dict] = []
        self._load(mapping_file)

    def _load(self, path: str) -> None:
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("parent_ticker") or row.get("self_ticker"):
                    self.mapping.append(row)
        logger.info(f"Loaded {len(self.mapping)} company mappings")

    def _normalize(self, name: str) -> str:
        name = name.strip()
        for suffix in ["株式会社", "(株)", "（株）", "合同会社", "有限会社",
                       "ＪＶ", "JV", "共同企業体"]:
            name = name.replace(suffix, "")
        name = name.replace("　", "").replace(" ", "")
        # 全角→半角英数字
        name = name.translate(str.maketrans(
            "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
            "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
            "０１２３４５６７８９",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789"
        ))
        return name

    def resolve(self, company_name: str) -> Optional[dict]:
        """
        会社名から マッピング情報を返す。
        Returns dict or None
        """
        normalized = self._normalize(company_name)

        # 1. 完全一致
        for row in self.mapping:
            if self._normalize(row["subsidiary_name"]) == normalized:
                return row

        # 2. 部分一致
        for row in self.mapping:
            key = self._normalize(row["subsidiary_name"])
            if key and (key in normalized or normalized in key):
                return row

        logger.debug(f"No mapping found for: {company_name}")
        return None

    def get_tickers(self, company_name: str) -> list[dict]:
        """
        落札企業名から対象ティッカーリストを返す。
        - 自社上場あり  → [{"ticker": "XXXX.T", "role": "self"}, {"ticker": "YYYY.T", "role": "parent"}]
        - 親会社のみ    → [{"ticker": "YYYY.T", "role": "parent"}]
        - 非上場・未登録 → []
        """
        result = self.resolve(company_name)
        if not result:
            return []

        tickers = []
        seen = set()

        # 自社上場
        st = result.get("self_ticker", "").strip()
        if st:
            t = f"{st}.T"
            tickers.append({
                "ticker": t,
                "role": "self",
                "company": result["subsidiary_name"],
                "parent": result["parent_company"],
            })
            seen.add(t)

        # 親会社
        pt = result.get("parent_ticker", "").strip()
        if pt:
            t = f"{pt}.T"
            if t not in seen:
                tickers.append({
                    "ticker": t,
                    "role": "parent",
                    "company": result["parent_company"],
                    "parent": result["parent_company"],
                })

        return tickers


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mapper = CompanyMapper()
    tests = ["矢沢フェロマイト", "オルガノ", "五洋建設", "富士通", "JTB", "鹿島建設", "存在しない会社"]
    for t in tests:
        tickers = mapper.get_tickers(t)
        print(f"{t} → {tickers}")
