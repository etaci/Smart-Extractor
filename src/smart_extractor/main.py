"""
Smart Data Extractor 主入口。
"""

import sys

# 修复 Windows 环境下 rich 输出 emoji 时的 UnicodeEncodeError
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

from smart_extractor.cli import app

def main():
    app()


if __name__ == "__main__":
    main()
