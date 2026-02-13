import os  # ✅ 必须先导入，否则 os 未定义

# --- Output path (works on local + GitHub Actions) ---
# 默认：写到仓库下 outputs/jijinlist.xlsx
# 如果你想本地写到 OneDrive，运行前在环境变量里设置 OUT_PATH 即可覆盖
# Windows PowerShell 示例：
#   $env:OUT_PATH="C:\Users\134971\OneDrive - Arrow Electronics, Inc\Desktop\KEVIN\Share\jijinlist.xlsx"
#   python jijinalllist.py
#
# GitHub Actions 不要设置 OUT_PATH，就会自动写到 outputs/

DEFAULT_OUT = os.path.join("outputs", "jijinlist.xlsx")
OUT_PATH = os.getenv("OUT_PATH", DEFAULT_OUT)

TOP_N = 1000
SLEEP_SEC = 0.03  # 每次请求间隔，防止过快被限流


def ensure_dir(path: str) -> None:
    """确保输出目录存在（兼容 path 直接是文件名、或没有目录的情况）"""
    dir_ = os.path.dirname(path)
    if dir_:
        os.makedirs(dir_, exist_ok=True)


def get_fund_list() -> pd.DataFrame:
    """
    获取基金列表（全量），兼容不同 akshare 版本可能的函数名。
    """
    candidates = [
        "fund_name_em",
        "fund_em_fund_name",
        "fund_open_fund_name_em",
    ]
    func = None
    for name in candidates:
        if hasattr(ak, name):
            func = getattr(ak, name)
            break
    if func is None:
        raise RuntimeError(
            "你的 akshare 版本找不到基金列表接口。\n"
            "请先升级：pip install -U akshare\n"
            "并确认存在以下任一函数：\n" + "\n".join(candidates)
        )

    df = func()

    # 统一列名
    rename_map = {}
    for c in df.columns:
        if c in ("基金代码", "代码"):
            rename_map[c] = "基金代码"
        elif c in ("基金简称", "简称", "基金名称", "名称"):
            rename_map[c] = "基金名称"
        elif c in ("基金类型", "类型"):
            rename_map[c] = "基金类型"
        elif c in ("基金全称", "全称"):
            rename_map[c] = "基金全称"
        elif c in ("基金公司", "公司"):
            rename_map[c] = "基金公司"
        elif c in ("成立日期", "成立日"):
            rename_map[c] = "成立日期"

    df = df.rename(columns=rename_map)

    if "基金代码" not in df.columns:
        # 尝试猜一个包含“代码”的列
        code_col = next((c for c in df.columns if "代码" in c), None)
        if not code_col:
            raise RuntimeError(f"基金列表缺少代码列，实际列为：{df.columns.tolist()}")
        df = df.rename(columns={code_col: "基金代码"})

    if "基金名称" not in df.columns:
        name_col = next((c for c in df.columns if "简称" in c or "名称" in c), None)
        df["基金名称"] = df[name_col] if name_col else ""

    keep = [c for c in ["基金代码", "基金名称", "基金类型", "基金全称", "基金公司", "成立日期"] if c in df.columns]
    df = df[keep].copy()

    df["基金代码"] = df["基金代码"].astype(str).str.zfill(6)
    df = df.drop_duplicates(subset=["基金代码"]).reset_index(drop=True)
    return df


def fetch_latest_nav(symbol: str) -> Tuple[Optional[str], Optional[float]]:
    """
    取最新单位净值和日期：用“单位净值走势”最后一行。
    """
    try:
        dfn = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        if dfn is None or len(dfn) == 0:
            return None, None

        # 常见列
        date_col = "净值日期" if "净值日期" in dfn.columns else dfn.columns[0]
        nav_col = "单位净值" if "单位净值" in dfn.columns else dfn.columns[1]

        dfn = dfn.copy()
        dfn[date_col] = pd.to_datetime(dfn[date_col], errors="coerce")
        dfn[nav_col] = pd.to_numeric(dfn[nav_col], errors="coerce")
        dfn = dfn.dropna(subset=[date_col, nav_col]).sort_values(date_col)

        if len(dfn) == 0:
            return None, None

        last = dfn.iloc[-1]
        return last[date_col].strftime("%Y-%m-%d"), float(last[nav_col])
    except Exception:
        return None, None


def main():
    print("\n" + "=" * 90)
    print("📥 Step1) 获取基金列表（AkShare）...")
    df = get_fund_list()
    print(f"✅ 获取成功：{len(df)} 条基金")

    # 只取前 TOP_N
    df = df.head(TOP_N).copy()
    print(f"🎯 Step2) 截取 TOP {TOP_N}：{len(df)} 条")

    print("📌 Step3) 开始补充最新净值（会花一点时间）...")
    nav_dates, nav_vals = [], []
    total = len(df)

    for i, code in enumerate(df["基金代码"].tolist(), start=1):
        d, v = fetch_latest_nav(code)
        nav_dates.append(d)
        nav_vals.append(v)

        if i % 50 == 0 or i == total:
            print(f"  ...进度 {i}/{total}")

        time.sleep(SLEEP_SEC)

    df["最新净值日期"] = nav_dates
    df["最新单位净值"] = nav_vals
    df["导出时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("📤 Step4) 写入 Excel ...")
    ensure_dir(OUT_PATH)
    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=f"Top{TOP_N}")

    print("✅ 完成！文件已生成：")
    print(OUT_PATH)
    print("=" * 90)


if __name__ == "__main__":
    main()


