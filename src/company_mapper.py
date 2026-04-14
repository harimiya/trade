"""
company_mapper.py
落札企業名 → 上場ティッカー（自社 / 親会社）マッパー
"""

import csv
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DATA_DIR    = Path(__file__).parent.parent / "data"
MAPPING_CSV = DATA_DIR / "company_ticker_map.csv"


class CompanyMapper:
    def __init__(self, csv_path: str = str(MAPPING_CSV)):
        self._rows: list[dict] = []
        self._load(csv_path)

    def _load(self, path: str) -> None:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # コメント行をスキップ
                if row.get("subsidiary_name", "").startswith("#"):
                    continue
                if row.get("parent_ticker") or row.get("self_ticker"):
                    self._rows.append(row)
        logger.info(f"Loaded {len(self._rows)} company mappings")

    @staticmethod
    def _normalize(name: str) -> str:
        name = name.strip()
        # 全角→半角
        name = name.translate(str.maketrans(
            "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
            "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
            "０１２３４５６７８９",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789"
        ))
        # 会社形態・JV表記を除去
        for suffix in ["株式会社", "（株）", "(株)", "合同会社", "有限会社",
                        "ＪＶ", "JV", "共同企業体", "グループ"]:
            name = name.replace(suffix, "")
        return name.replace("　", "").replace(" ", "")

    def _find(self, name: str) -> Optional[dict]:
        norm = self._normalize(name)
        # 完全一致
        for row in self._rows:
            if self._normalize(row["subsidiary_name"]) == norm:
                return row
        # 部分一致
        for row in self._rows:
            key = self._normalize(row["subsidiary_name"])
            if key and (key in norm or norm in key):
                return row
        return None

    def get_tickers(self, winner: str) -> list[dict]:
        """
        Returns list of:
          {"ticker": "XXXX.T", "role": "self"|"parent", "company": str}
        """
        row = self._find(winner)
        if not row:
            return []

        result = []
        seen: set = set()

        # 自社上場
        st = row.get("self_ticker", "").strip()
        if st:
            t = f"{st}.T"
            result.append({"ticker": t, "role": "self",
                            "company": row["subsidiary_name"],
                            "parent":  row["parent_company"]})
            seen.add(t)

        # 親会社
        pt = row.get("parent_ticker", "").strip()
        if pt:
            t = f"{pt}.T"
            if t not in seen:
                result.append({"ticker": t, "role": "parent",
                                "company": row["parent_company"],
                                "parent":  row["parent_company"]})

        return result
