import os
import sys
import pandas as pd
import numpy as np
from dbfread import DBF
from datetime import datetime, timedelta

from test import file_paths
from py_paths import g_current_path


# -------------------- Utils --------------------
def hhmm(minutes: int) -> str:
    h = minutes // 60
    m = minutes % 60
    return f"{h}:{m:02d}"


def hhmm_from_float(v):
    """Converts VFP-style time float like 21.30 -> '21:30'"""
    if pd.isna(v):
        return None
    v = float(v)
    h = int(v)
    m = int(round((v - h) * 100))
    return f"{h:02d}:{m:02d}"


def float_hhmm_to_minutes(x) -> int:
    """
    Converts HH.MM float (e.g., 0.15 meaning 15 minutes) to minutes.
    Works for values like 1.30 -> 90, 0.45 -> 45, 2.00 -> 120
    """
    if pd.isna(x):
        return 0
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x):
        return 0
    hrs = int(x)
    mins = int(round((x - hrs) * 100))
    return hrs * 60 + mins


def safe_drop(df: pd.DataFrame, cols) -> pd.DataFrame:
    """Drop columns only if they exist (prevents KeyError)."""
    return df.drop(columns=[c for c in cols if c in df.columns], errors="ignore")


# -------------------- OUTPASS OVERRIDE --------------------
def apply_outpass_override(out: pd.DataFrame, outpass_csv_path: str) -> pd.DataFrame:
    """
    OUTPASS rule:
      - Read OUTPASS.csv columns: token, passdate, totpasshrs
      - Match out.TOKEN == outpass.token AND out.PDATE == passdate (date match)
      - If matched: REMARKS='@'
      - If matched AND PUNCH_STATUS == 'HD': set to 'PR'
      - Keep TOTALTIME as-is
      - Add TOTPASSHRS column (from totpasshrs) at the end
    """
    out = out.copy()

    # Ensure TOTPASSHRS exists
    if "TOTPASSHRS" not in out.columns:
        out["TOTPASSHRS"] = np.nan

    # If file missing → no change
    if not outpass_csv_path or not os.path.exists(outpass_csv_path):
        cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
        return out[cols]

    # Read CSV (old pandas safe)
    pass_df = pd.read_csv(
        outpass_csv_path,
        dtype=str,
        skipinitialspace=True,
        engine="python",
        error_bad_lines=False,
        warn_bad_lines=False
    )
    pass_df.columns = [c.strip().lower() for c in pass_df.columns]

    required = {"token", "passdate", "totpasshrs"}
    if not required.issubset(pass_df.columns):
        cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
        return out[cols]

    # Clean fields
    pass_df["token"] = pass_df["token"].astype(str).str.strip().str.replace('"', "")
    pass_df["totpasshrs"] = pass_df["totpasshrs"].astype(str).str.strip().str.replace('"', "")

    # Parse date (DD/MM/YYYY)
    pass_df["passdate"] = pd.to_datetime(
        pass_df["passdate"], dayfirst=True, errors="coerce"
    ).dt.date
    pass_df = pass_df.dropna(subset=["passdate"])

    # Keep last entry per token+date
    pass_df = pass_df.drop_duplicates(subset=["token", "passdate"], keep="last")

    # Build lookup: (token, date) → totpasshrs
    key_to_passhrs = {
        (r.token, r.passdate): r.totpasshrs
        for r in pass_df[["token", "passdate", "totpasshrs"]].itertuples(index=False)
    }

    # Prepare OUT keys
    out_token = out["TOKEN"].astype(str).str.strip()
    out_pdate = pd.to_datetime(out["PDATE"], errors="coerce").dt.date

    keys = list(zip(out_token.tolist(), out_pdate.tolist()))
    matched_mask = pd.Series([k in key_to_passhrs for k in keys], index=out.index)

    # Apply TOTPASSHRS
    out.loc[matched_mask, "TOTPASSHRS"] = [
        key_to_passhrs[keys[i]]
        for i in range(len(keys))
        if matched_mask.iat[i]
    ]

    # REMARKS
    if "REMARKS" not in out.columns:
        out["REMARKS"] = ""
    out["REMARKS"] = out["REMARKS"].fillna("").astype(str)
    out.loc[matched_mask, "REMARKS"] = "@"

    # HD → PR
    out.loc[
        matched_mask
        & out["PUNCH_STATUS"].astype(str).str.strip().str.upper().eq("HD"),
        "PUNCH_STATUS"
    ] = "PR"

    # Keep TOTPASSHRS at end
    cols = [c for c in out.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
    return out[cols]


# -------------------- Mode-1 last day helper --------------------
def mode_1_last_day_shift(gseldate, start_date, end_date, start_date_str, end_date_str, table_paths):
    gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    if is_last_day:
        punches_table_new_month = DBF(table_paths["punches_dbf_path"], load=True)
        punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
        punches_df_new_month["DEL"] = False

        end_date_dt = pd.to_datetime(end_date)
        end_date_date = end_date_dt.date()
        end_date_plus1_date = (end_date_dt + timedelta(days=1)).date()

        punches_df_new_month["PDATE"] = pd.to_datetime(punches_df_new_month["PDATE"]).dt.date

        filtered_df = punches_df_new_month.loc[
            punches_df_new_month["PDATE"].between(end_date_date, end_date_plus1_date)
        ].copy()

        filtered_df = filtered_df.sort_values(by=["TOKEN", "PDATE", "PDTIME"])
        matching = []
        for token, group in filtered_df.groupby("TOKEN"):
            group = group.reset_index(drop=True)
            for i in range(len(group) - 1):
                if (
                    group.loc[i, "PDATE"] == end_date_date
                    and group.loc[i, "MODE"] == 0
                    and group.loc[i + 1, "PDATE"] == end_date_plus1_date
                    and group.loc[i + 1, "MODE"] == 1
                ):
                    matching.append(group.loc[i])
                    matching.append(group.loc[i + 1])

        df = pd.DataFrame(matching)
        if not df.empty:
            mode_1_only = df.loc[df["MODE"] == 1].copy()
            mode_1_only["DEL"] = False
        else:
            mode_1_only = pd.DataFrame(
                columns=["TOKEN", "COMCODE", "PDATE", "HOURS", "MINUTES", "MODE", "PDTIME", "MCIP", "DEL"]
            )

        mode_1_only.to_csv("mode_1_only_df_check_punches.csv", index=False)

    else:
        mode_1_only = pd.DataFrame(
            columns=["TOKEN", "COMCODE", "PDATE", "HOURS", "MINUTES", "MODE", "PDTIME", "MCIP", "DEL"]
        )

    date_range = pd.date_range(start=start_date_str, end=end_date_str, freq="D")
    return mode_1_only, date_range


# -------------------- Main punch generator --------------------
def generate_punch_shift(punches_df, muster_df, g_current_path):
    table_paths = file_paths(g_current_path)

    # -------------------- Read global settings --------------------
    with open(table_paths["gsel_date_path"]) as file:
        f = [x.strip() for x in file.readlines()]
    gseldate = f[0]
    ghalf_day = int(f[1])
    gfull_day = int(f[2])

    # -------------------- Shift master --------------------
    shinfo = pd.read_csv(table_paths["shiftmast_csv_path"])
    shinfo["shcode"] = shinfo["shcode"].astype(str).str.strip().str.upper()
    shinfo["shift_st_time"] = shinfo["shift_st"].apply(hhmm_from_float)
    shinfo["shift_ed_time"] = shinfo["shift_ed"].apply(hhmm_from_float)
    shinfo["workhrs_minutes"] = (shinfo["workhrs"] * 60).astype(int)
    shinfo["halfday_minutes"] = ((shinfo["workhrs"] / 2) * 60).astype(int)

    # inc_grt -> minutes
    shinfo["inc_grt"] = pd.to_numeric(shinfo["inc_grt"], errors="coerce").fillna(0)
    hrs = shinfo["inc_grt"].astype(int)
    mins = ((shinfo["inc_grt"] - hrs) * 100).round().astype(int)
    shinfo["inc_grt_minutes"] = hrs * 60 + mins

    # gratime -> minutes
    if "gratime" in shinfo.columns:
        shinfo["gratime_minutes"] = shinfo["gratime"].apply(float_hhmm_to_minutes)
    else:
        shinfo["gratime_minutes"] = 0

    shinfo_unique = shinfo.drop_duplicates(subset=["shcode"])
    shift_minutes = shinfo_unique.set_index("shcode")[["workhrs_minutes", "halfday_minutes"]].to_dict("index")

    shift_merge_info = shinfo_unique[
        [
            "shcode",
            "inc_grt_minutes",
            "gratime_minutes",
            "workhrs_minutes",
            "halfday_minutes",
            "workhrs",
            "shift_st_time",
            "shift_ed_time",
        ]
    ].copy()

    # -------------------- Muster --------------------
    muster_df = muster_df.copy()
    muster_df["TOKEN"] = muster_df["TOKEN"].astype(str).str.strip()
    muster_df["PDATE"] = pd.to_datetime(muster_df["PDATE"]).dt.date
    muster_df["SHIFT_STATUS"] = muster_df["SHIFT_STATUS"].astype(str).str.strip().str.upper()
    if "STATUS" in muster_df.columns:
        muster_df["STATUS"] = muster_df["STATUS"].astype(str).str.upper()

    day_shift_map = muster_df.set_index(["TOKEN", "PDATE"])["SHIFT_STATUS"].to_dict()

    def thresholds(token, pdate):
        sc = day_shift_map.get((token, pdate))
        if sc in shift_minutes:
            return shift_minutes[sc]["workhrs_minutes"], shift_minutes[sc]["halfday_minutes"]
        return gfull_day, ghalf_day

    # -------------------- Date range --------------------
    dated = DBF(table_paths["dated_dbf_path"], load=True)
    start_date = dated.records[0]["MUFRDATE"]
    end_date = dated.records[0]["MUTODATE"]

    mode1, date_range = mode_1_last_day_shift(
        gseldate,
        start_date,
        end_date,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        table_paths,
    )

    # -------------------- Robust PDTIME parser --------------------
    def parse_pdtime(series):
        s = series.astype(str).str.strip()
        dt = pd.to_datetime(s, format="%d/%m/%Y %H:%M:%S", errors="coerce")
        dt2 = pd.to_datetime(s, format="%d/%m/%Y %H:%M", errors="coerce")
        dt = dt.fillna(dt2)
        dt = dt.fillna(pd.to_datetime(s, dayfirst=True, errors="coerce"))
        return dt

    # -------------------- Punches input --------------------
    punches_df = punches_df.copy()
    punches_df["TOKEN"] = punches_df["TOKEN"].astype(str).str.strip()
    if "COMCODE" in punches_df.columns:
        punches_df["COMCODE"] = punches_df["COMCODE"].fillna("").astype(str).str.strip()
    else:
        punches_df["COMCODE"] = ""

    punches_df["PDTIME"] = parse_pdtime(punches_df["PDTIME"])
    punches_df["PDATE"] = pd.to_datetime(punches_df["PDATE"], dayfirst=True, errors="coerce").dt.date
    punches_df["MODE"] = pd.to_numeric(punches_df["MODE"], errors="coerce").fillna(0).astype(int)

    if not mode1.empty:
        mode1 = mode1.copy()
        mode1["TOKEN"] = mode1["TOKEN"].astype(str).str.strip()
        if "COMCODE" in mode1.columns:
            mode1["COMCODE"] = mode1["COMCODE"].fillna("").astype(str).str.strip()
        else:
            mode1["COMCODE"] = ""
        mode1["PDTIME"] = parse_pdtime(mode1["PDTIME"])
        mode1["PDATE"] = pd.to_datetime(mode1["PDATE"], dayfirst=True, errors="coerce").dt.date
        mode1["MODE"] = pd.to_numeric(mode1["MODE"], errors="coerce").fillna(0).astype(int)
        punches_df = pd.concat([punches_df, mode1], ignore_index=True)

    punches_df = (
        punches_df.dropna(subset=["PDTIME"])
        .sort_values(by=["TOKEN", "PDTIME"])
        .reset_index(drop=True)
    )

    # -------------------- Output punch_df skeleton --------------------
    punch_df = pd.DataFrame(
        columns=[
            "TOKEN", "COMCODE", "PDATE",
            "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
            "INTIME", "OUTTIME", "TOTALTIME", "PUNCH_STATUS", "REMARKS", "OT",
        ]
    )

    # =================================================================
    # Helpers
    # =================================================================
    def pick_row_pdate(token, in_dt, out_dt):
        in_date_str = in_dt.date().strftime("%Y-%m-%d")
        already = ((punch_df["TOKEN"] == token) & (punch_df["PDATE"] == in_date_str)).any()
        if already:
            return out_dt.date().strftime("%Y-%m-%d")
        return in_date_str

    def add_or_append_pair(token, comcode, pdate_str, in_time, out_time):
        nonlocal punch_df

        diff = out_time - in_time
        if diff.total_seconds() <= 0:
            return

        minutes = int(diff.total_seconds() // 60)

        full_m, half_m = thresholds(token, pd.to_datetime(pdate_str).date())
        if minutes >= full_m:
            st = "PR"
        elif minutes <= half_m:
            st = "AB"
        else:
            st = "HD"

        otm = max(0, minutes - full_m)
        ot_str = hhmm(otm)
        remarks = "#" if in_time.date() != out_time.date() else ""

        exists = punch_df[(punch_df["TOKEN"] == token) & (punch_df["PDATE"] == pdate_str)]
        if exists.empty:
            punch_df = pd.concat(
                [
                    punch_df,
                    pd.DataFrame(
                        {
                            "TOKEN": [token],
                            "COMCODE": [str(comcode) if comcode is not None else ""],
                            "PDATE": [pdate_str],
                            "INTIME1": [in_time.strftime("%Y-%m-%d %H:%M")],
                            "OUTTIME1": [out_time.strftime("%Y-%m-%d %H:%M")],
                            "INTIME2": [np.nan], "OUTTIME2": [np.nan],
                            "INTIME3": [np.nan], "OUTTIME3": [np.nan],
                            "INTIME4": [np.nan], "OUTTIME4": [np.nan],
                            "INTIME": [in_time.strftime("%Y-%m-%d %H:%M")],
                            "OUTTIME": [out_time.strftime("%Y-%m-%d %H:%M")],
                            "TOTALTIME": [hhmm(minutes)],
                            "PUNCH_STATUS": [st],
                            "REMARKS": [remarks],
                            "OT": [ot_str],
                        }
                    ),
                ],
                ignore_index=True,
            )
        else:
            idx = exists.index[-1]
            for col_in, col_out in [("INTIME2", "OUTTIME2"), ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                if pd.isna(punch_df.loc[idx, col_in]):
                    punch_df.loc[idx, col_in] = in_time.strftime("%Y-%m-%d %H:%M")
                    punch_df.loc[idx, col_out] = out_time.strftime("%Y-%m-%d %H:%M")
                    break

            punch_df.loc[idx, "OUTTIME"] = out_time.strftime("%Y-%m-%d %H:%M")

            total = pd.to_timedelta(0)
            for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                              ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                if not pd.isna(punch_df.loc[idx, cin]) and not pd.isna(punch_df.loc[idx, cout]):
                    total += pd.to_datetime(punch_df.loc[idx, cout]) - pd.to_datetime(punch_df.loc[idx, cin])

            tm = int(total.total_seconds() // 60)
            full_m2, half_m2 = thresholds(token, pd.to_datetime(pdate_str).date())
            if tm >= full_m2:
                st2 = "PR"
            elif tm <= half_m2:
                st2 = "AB"
            else:
                st2 = "HD"

            ot2 = max(0, tm - full_m2)
            punch_df.loc[idx, "TOTALTIME"] = hhmm(tm)
            punch_df.loc[idx, "PUNCH_STATUS"] = st2
            punch_df.loc[idx, "OT"] = hhmm(ot2)

            old_r = "" if pd.isna(punch_df.loc[idx, "REMARKS"]) else str(punch_df.loc[idx, "REMARKS"])
            if old_r.strip() == "":
                punch_df.loc[idx, "REMARKS"] = remarks

    # orphan OUT (MODE=1 with no pending IN) -> create row and mark MM
    def mark_orphan_out_as_mm(token, comcode, out_dt):
        nonlocal punch_df
        pdate_str = out_dt.date().strftime("%Y-%m-%d")
        out_str = out_dt.strftime("%Y-%m-%d %H:%M")

        exists = punch_df[(punch_df["TOKEN"] == token) & (punch_df["PDATE"] == pdate_str)]
        if exists.empty:
            punch_df = pd.concat(
                [
                    punch_df,
                    pd.DataFrame(
                        {
                            "TOKEN": [token],
                            "COMCODE": [str(comcode) if comcode is not None else ""],
                            "PDATE": [pdate_str],
                            "INTIME1": [np.nan],
                            "OUTTIME1": [out_str],
                            "INTIME2": [np.nan], "OUTTIME2": [np.nan],
                            "INTIME3": [np.nan], "OUTTIME3": [np.nan],
                            "INTIME4": [np.nan], "OUTTIME4": [np.nan],
                            "INTIME": [np.nan],
                            "OUTTIME": [out_str],
                            "TOTALTIME": [""],
                            "PUNCH_STATUS": ["MM"],
                            "REMARKS": [""],        # <-- IMPORTANT: MM only in PUNCH_STATUS
                            "OT": [""],
                        }
                    ),
                ],
                ignore_index=True,
            )
        else:
            idx = exists.index[-1]
            for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                              ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                if pd.isna(punch_df.loc[idx, cout]) or str(punch_df.loc[idx, cout]).strip() == "":
                    punch_df.loc[idx, cout] = out_str
                    break
            punch_df.loc[idx, "OUTTIME"] = out_str
            punch_df.loc[idx, "PUNCH_STATUS"] = "MM"
            punch_df.loc[idx, "TOTALTIME"] = ""
            punch_df.loc[idx, "OT"] = ""
            punch_df.loc[idx, "REMARKS"] = ""  # <-- IMPORTANT

    # orphan IN (MODE=0 without OUT) -> create row and mark MM
    def mark_orphan_in_as_mm(token, comcode, in_dt):
        nonlocal punch_df
        pdate_str = in_dt.date().strftime("%Y-%m-%d")
        in_str = in_dt.strftime("%Y-%m-%d %H:%M")

        exists = punch_df[(punch_df["TOKEN"] == token) & (punch_df["PDATE"] == pdate_str)]
        if exists.empty:
            punch_df = pd.concat(
                [
                    punch_df,
                    pd.DataFrame(
                        {
                            "TOKEN": [token],
                            "COMCODE": [str(comcode) if comcode is not None else ""],
                            "PDATE": [pdate_str],
                            "INTIME1": [in_str],
                            "OUTTIME1": [np.nan],
                            "INTIME2": [np.nan], "OUTTIME2": [np.nan],
                            "INTIME3": [np.nan], "OUTTIME3": [np.nan],
                            "INTIME4": [np.nan], "OUTTIME4": [np.nan],
                            "INTIME": [in_str],
                            "OUTTIME": [np.nan],
                            "TOTALTIME": [""],
                            "PUNCH_STATUS": ["MM"],
                            "REMARKS": [""],        # <-- IMPORTANT: MM only in PUNCH_STATUS
                            "OT": [""],
                        }
                    ),
                ],
                ignore_index=True,
            )
        else:
            idx = exists.index[-1]
            for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                              ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                if pd.isna(punch_df.loc[idx, cin]) or str(punch_df.loc[idx, cin]).strip() == "":
                    punch_df.loc[idx, cin] = in_str
                    break
            punch_df.loc[idx, "INTIME"] = in_str
            punch_df.loc[idx, "PUNCH_STATUS"] = "MM"
            punch_df.loc[idx, "TOTALTIME"] = ""
            punch_df.loc[idx, "OT"] = ""
            punch_df.loc[idx, "REMARKS"] = ""  # <-- IMPORTANT

    # =================================================================
    # Pairing loop (FIXED: do not overwrite pending IN silently)
    # =================================================================
    pending_in = {}  # token -> (in_time_dt, comcode)

    for _, row in punches_df.iterrows():
        token = str(row["TOKEN"]).strip()
        comcode = str(row.get("COMCODE", "")).strip()
        dt = pd.to_datetime(row["PDTIME"]).replace(second=0)
        mode = int(row["MODE"])

        if mode == 0:
            # FIX: if an IN already pending and we get another IN, the old one is orphan -> MM
            if token in pending_in:
                old_in_dt, old_cc = pending_in[token]
                mark_orphan_in_as_mm(token, old_cc, old_in_dt.to_pydatetime())
            pending_in[token] = (dt, comcode)

        else:
            if token not in pending_in:
                mark_orphan_out_as_mm(token, comcode, dt.to_pydatetime())
                continue

            in_time, in_cc = pending_in[token]
            out_time = dt

            pdate_str = pick_row_pdate(token, in_time.to_pydatetime(), out_time.to_pydatetime())
            add_or_append_pair(token, in_cc, pdate_str, in_time.to_pydatetime(), out_time.to_pydatetime())

            pending_in.pop(token, None)

    # Any leftover IN without OUT -> MM
    for token, (in_time, in_cc) in list(pending_in.items()):
        mark_orphan_in_as_mm(token, in_cc, in_time.to_pydatetime())
    pending_in.clear()

    # -------------------- Fill AB for missing days --------------------
    for token in muster_df["TOKEN"].unique():
        token = str(token).strip()
        token_df = punch_df[punch_df["TOKEN"] == token]
        for d in date_range:
            ds = d.strftime("%Y-%m-%d")
            if not ((token_df["PDATE"] == ds) & (token_df["TOKEN"] == token)).any():
                punch_df = pd.concat(
                    [
                        punch_df,
                        pd.DataFrame(
                            {
                                "TOKEN": [token],
                                "COMCODE": [""],
                                "PDATE": [ds],
                                "INTIME1": [np.nan], "OUTTIME1": [np.nan],
                                "INTIME2": [np.nan], "OUTTIME2": [np.nan],
                                "INTIME3": [np.nan], "OUTTIME3": [np.nan],
                                "INTIME4": [np.nan], "OUTTIME4": [np.nan],
                                "INTIME": [np.nan],
                                "OUTTIME": [np.nan],
                                "TOTALTIME": [np.nan],
                                "PUNCH_STATUS": ["AB"],
                                "REMARKS": [np.nan],
                                "OT": [""],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

    # -------------------- Fill COMCODE from muster if blank --------------------
    comcode_map = {}
    if "COMCODE" in muster_df.columns:
        tmp = muster_df[["TOKEN", "COMCODE"]].copy()
        tmp["TOKEN"] = tmp["TOKEN"].astype(str).str.strip()
        tmp["COMCODE"] = tmp["COMCODE"].astype(str).str.strip()
        tmp = tmp[tmp["COMCODE"].ne("") & tmp["COMCODE"].ne("nan")]
        comcode_map = tmp.drop_duplicates("TOKEN", keep="last").set_index("TOKEN")["COMCODE"].to_dict()

    if not comcode_map:
        tmp = punch_df[["TOKEN", "COMCODE"]].copy()
        tmp["TOKEN"] = tmp["TOKEN"].astype(str).str.strip()
        tmp["COMCODE"] = tmp["COMCODE"].fillna("").astype(str).str.strip()
        tmp = tmp[tmp["COMCODE"].ne("") & tmp["COMCODE"].ne("nan")]
        comcode_map = tmp.drop_duplicates("TOKEN", keep="last").set_index("TOKEN")["COMCODE"].to_dict()

    punch_df["COMCODE"] = punch_df["COMCODE"].fillna("").astype(str).str.strip()
    blank_cc = punch_df["COMCODE"].eq("") | punch_df["COMCODE"].str.lower().eq("nan")
    punch_df.loc[blank_cc, "COMCODE"] = (
        punch_df.loc[blank_cc, "TOKEN"].astype(str).str.strip().map(comcode_map).fillna("")
    )

    # -------------------- Save punches.csv (intermediate) --------------------
    punch_df.to_csv(table_paths["punch_csv_path"], index=False)

    # -------------------- Generate ppunches.csv --------------------
    ppunches_path = table_paths["ppunches_csv_path"]
    try:
        pp = punches_df.copy()
        pp["TOKEN"] = pp["TOKEN"].astype(str).str.strip()
        pp["COMCODE"] = pp.get("COMCODE", "").fillna("").astype(str).str.strip()

        pp["PDTIME"] = parse_pdtime(pp["PDTIME"])
        pp = pp.dropna(subset=["TOKEN", "PDTIME"])
        pp["PDATE_DAY"] = pp["PDTIME"].dt.normalize()
        pp["MODE"] = pd.to_numeric(pp["MODE"], errors="coerce").fillna(0).astype(int)
        pp = pp.sort_values(["TOKEN", "PDATE_DAY", "PDTIME"])

        base = pd.read_csv(table_paths["punch_csv_path"], dtype={"COMCODE": str, "TOKEN": str})
        base["TOKEN"] = base["TOKEN"].astype(str).str.strip()
        base["COMCODE"] = base.get("COMCODE", "").fillna("").astype(str).str.strip()
        base["PDATE"] = pd.to_datetime(base["PDATE"], errors="coerce").dt.normalize()
        base["EMPCODE"] = base.get("EMPCODE", base["TOKEN"]).fillna("").astype(str).str.strip()
        base = base.dropna(subset=["TOKEN", "PDATE"])[["TOKEN", "COMCODE", "EMPCODE", "PDATE"]].drop_duplicates()

        rows = []
        max_punches = int(pp.groupby(["TOKEN", "PDATE_DAY"]).size().max() or 0)

        for (token, day), g in pp.groupby(["TOKEN", "PDATE_DAY"]):
            g = g.sort_values("PDTIME")
            row = {"TOKEN": token, "PDATE": day}

            in_count = int((g["MODE"] == 0).sum())
            out_count = int((g["MODE"] == 1).sum())
            row["PP_STATUS"] = "MM" if (in_count != out_count) else ""

            i = 1
            for _, r in g.iterrows():
                row[f"PUNCH_{i}"] = r["PDTIME"].strftime("%Y-%m-%d %H:%M")
                row[f"MODE{i}"] = int(r["MODE"])
                i += 1

            row["PUNCH_COUNT"] = len(g)
            rows.append(row)

        punch_wide = pd.DataFrame(rows)
        ppunches = base.merge(punch_wide, on=["TOKEN", "PDATE"], how="left")

        ppunches["PUNCH_COUNT"] = ppunches["PUNCH_COUNT"].fillna(0).astype(int)
        ppunches["PP_STATUS"] = ppunches.get("PP_STATUS", "").fillna("")

        for i in range(1, max_punches + 1):
            pcol = f"PUNCH_{i}"
            mcol = f"MODE{i}"

            if pcol not in ppunches.columns:
                ppunches[pcol] = ""
            if mcol not in ppunches.columns:
                ppunches[mcol] = pd.Series([pd.NA] * len(ppunches), dtype="Int64")

            ppunches[pcol] = ppunches[pcol].fillna("")
            ppunches[mcol] = pd.to_numeric(ppunches[mcol], errors="coerce").astype("Int64")

        ppunches["PDATE"] = pd.to_datetime(ppunches["PDATE"], errors="coerce").dt.strftime("%Y-%m-%d 00:00:00")

        # ---- CHANGE #1: add BLANK_STATUS after PUNCH_COUNT, then PP_STATUS after BLANK_STATUS ----
        if "BLANK_STATUS" not in ppunches.columns:
            ppunches["BLANK_STATUS"] = ""

        ordered_cols = ["TOKEN", "COMCODE", "PDATE", "EMPCODE"]
        for i in range(1, max_punches + 1):
            ordered_cols += [f"PUNCH_{i}", f"MODE{i}"]
        ordered_cols += ["PUNCH_COUNT", "BLANK_STATUS", "PP_STATUS"]

        # Ensure missing cols exist
        for c in ordered_cols:
            if c not in ppunches.columns:
                ppunches[c] = "" if c not in ["PUNCH_COUNT"] else 0

        ppunches = ppunches[ordered_cols].sort_values(["TOKEN", "PDATE"])
        ppunches.to_csv(ppunches_path, index=False)

    except Exception as e:
        print("ppunches.csv generation failed:", e)

    # -----------------------------------------------------------------
    # Force MM into punches.csv for any day that ppunches says MM
    # (MM only in PUNCH_STATUS; DO NOT write MM into REMARKS)
    # -----------------------------------------------------------------
    try:
        if os.path.exists(ppunches_path):
            ppm = pd.read_csv(ppunches_path, dtype={"TOKEN": str, "COMCODE": str})
            ppm["TOKEN"] = ppm["TOKEN"].astype(str).str.strip()
            ppm["PDATE_N"] = pd.to_datetime(ppm["PDATE"], errors="coerce").dt.normalize()
            mm_days = ppm[ppm["PP_STATUS"].fillna("").astype(str).str.strip().str.upper().eq("MM")].copy()

            punch_df["TOKEN"] = punch_df["TOKEN"].astype(str).str.strip()
            punch_df["PDATE_N"] = pd.to_datetime(punch_df["PDATE"], errors="coerce").dt.normalize()

            mm_key = set(zip(mm_days["TOKEN"], mm_days["PDATE_N"]))

            if mm_key:
                mask = punch_df.apply(lambda r: (r["TOKEN"], r["PDATE_N"]) in mm_key, axis=1)
                punch_df.loc[mask, "PUNCH_STATUS"] = "MM"
                punch_df.loc[mask, "TOTALTIME"] = ""
                punch_df.loc[mask, "OT"] = ""
                punch_df.loc[mask, "REMARKS"] = ""  # <-- CHANGE #2

                # Optional: copy first punch into INTIME1 or OUTTIME1 for visibility (no synthetic)
                mm_days = mm_days.set_index(["TOKEN", "PDATE_N"])
                for idx in punch_df[mask].index:
                    t = punch_df.at[idx, "TOKEN"]
                    d = punch_df.at[idx, "PDATE_N"]
                    if (t, d) not in mm_days.index:
                        continue

                    p1 = str(mm_days.at[(t, d), "PUNCH_1"]) if "PUNCH_1" in mm_days.columns else ""
                    m1 = mm_days.at[(t, d), "MODE1"] if "MODE1" in mm_days.columns else pd.NA
                    p1 = "" if p1.lower() == "nan" else p1

                    if p1:
                        # Only fill if BOTH empty (so we don't overwrite your real paired data)
                        if pd.isna(punch_df.at[idx, "INTIME1"]) and pd.isna(punch_df.at[idx, "OUTTIME1"]):
                            if pd.notna(m1) and int(m1) == 0:
                                punch_df.at[idx, "INTIME1"] = p1
                                punch_df.at[idx, "INTIME"] = p1
                            else:
                                punch_df.at[idx, "OUTTIME1"] = p1
                                punch_df.at[idx, "OUTTIME"] = p1

            punch_df = punch_df.drop(columns=["PDATE_N"], errors="ignore")

    except Exception as e:
        print("MM forcing from ppunches failed:", e)

    # -------------------- Merge muster shift info --------------------
    shift_cols = ["TOKEN", "PDATE", "SHIFT_STATUS", "STATUS"] if "STATUS" in muster_df.columns else ["TOKEN", "PDATE", "SHIFT_STATUS"]
    muster_merge = muster_df[shift_cols].copy()
    muster_merge["TOKEN"] = muster_merge["TOKEN"].astype(str).str.strip()
    muster_merge["PDATE"] = pd.to_datetime(muster_merge["PDATE"], errors="coerce").dt.normalize()

    punch_df["TOKEN"] = punch_df["TOKEN"].astype(str).str.strip()
    punch_df["PDATE"] = pd.to_datetime(punch_df["PDATE"], errors="coerce").dt.normalize()

    out = punch_df.merge(muster_merge, on=["TOKEN", "PDATE"], how="left")
    out = out.merge(shift_merge_info, left_on="SHIFT_STATUS", right_on="shcode", how="left")
    out = safe_drop(out, ["shcode"])

    if "COMCODE" not in out.columns:
        out["COMCODE"] = np.nan

    out["COMCODE"] = out["COMCODE"].fillna("").astype(str).str.strip()
    blank_cc2 = out["COMCODE"].eq("") | out["COMCODE"].str.lower().eq("nan")
    if blank_cc2.any():
        out.loc[blank_cc2, "COMCODE"] = out.loc[blank_cc2, "TOKEN"].astype(str).str.strip().map(comcode_map).fillna("")

    out["INTIME"] = pd.to_datetime(out["INTIME"], errors="coerce")
    out["OUTTIME"] = pd.to_datetime(out["OUTTIME"], errors="coerce")

    # -------------------- Compute ACTUAL mins as sum of all COMPLETE IN/OUT pairs --------------------
    total_secs = pd.Series(0.0, index=out.index)
    pair_cols = [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                 ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]
    any_pair = pd.Series(False, index=out.index)

    for cin, cout in pair_cols:
        if cin in out.columns and cout in out.columns:
            tin = pd.to_datetime(out[cin], errors="coerce")
            tout = pd.to_datetime(out[cout], errors="coerce")
            m = tin.notna() & tout.notna()
            any_pair |= m
            total_secs.loc[m] += (tout.loc[m] - tin.loc[m]).dt.total_seconds().clip(lower=0)

    valid = any_pair
    mins_actual = (total_secs.loc[valid] // 60).astype(int)

    fullm = out.loc[valid, "workhrs_minutes"].fillna(gfull_day).astype(int)
    halfm = out.loc[valid, "halfday_minutes"].fillna(ghalf_day).astype(int)

    base_status = np.where(mins_actual >= fullm, "PR", np.where(mins_actual <= halfm, "AB", "HD"))
    out.loc[valid, "PUNCH_STATUS"] = base_status
    out.loc[valid, "TOTAL_HRS"] = [hhmm(m) for m in mins_actual]
    out.loc[valid, "TOTALTIME"] = out.loc[valid, "TOTAL_HRS"]

    otm = np.maximum(mins_actual - fullm, 0)
    incm = out.loc[valid, "inc_grt_minutes"].fillna(0).astype(int)
    otm = np.where(otm < incm, 0, otm)
    out.loc[valid, "OT"] = [hhmm(m) for m in otm]

    # ---------------- GRATIME PROMOTION (AB->HD and HD->PR) ----------------
    grace_m = out.loc[valid, "gratime_minutes"].fillna(0).astype(int)
    mins_with_grace = (mins_actual + grace_m).astype(int)

    if "REMARKS" not in out.columns:
        out["REMARKS"] = ""
    out["REMARKS"] = out["REMARKS"].fillna("").astype(str)
    out.loc[out["REMARKS"].str.strip().eq("&"), "REMARKS"] = ""

    cur_status_s = out.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()

    ab_to_hd_mask = cur_status_s.eq("AB") & (mins_actual < halfm) & (mins_with_grace >= halfm)
    ab_to_hd_idx = out.loc[valid].index[ab_to_hd_mask]
    out.loc[ab_to_hd_idx, "PUNCH_STATUS"] = "HD"
    out.loc[ab_to_hd_idx, "REMARKS"] = "&"

    cur_status_s = out.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()
    hd_to_pr_mask = cur_status_s.eq("HD") & (mins_actual < fullm) & (mins_with_grace >= fullm)
    promoted_idx = out.loc[valid].index[hd_to_pr_mask]
    out.loc[promoted_idx, "PUNCH_STATUS"] = "PR"
    out.loc[promoted_idx, "REMARKS"] = "&"

    # OUTPASS override
    outpass_csv_path = os.path.join(g_current_path, "OUTPASS.csv")
    out = apply_outpass_override(out, outpass_csv_path)

    # keep your final MM override (final)  -> MM only clears hours, does NOT write MM in remarks
    mask_mm = (
        out.get("STATUS", pd.Series(index=out.index, dtype="object"))
        .astype(str)
        .str.strip()
        .str.upper()
        .eq("MM")
    )
    out.loc[mask_mm, "TOTALTIME"] = ""
    out.loc[mask_mm, "OT"] = ""
    out.loc[mask_mm, "REMARKS"] = ""   # <-- IMPORTANT
    if "TOTAL_HRS" in out.columns:
        out.loc[mask_mm, "TOTAL_HRS"] = ""
    if "TOTPASSHRS" in out.columns:
        out.loc[mask_mm, "TOTPASSHRS"] = ""

    out = out.sort_values(by=["TOKEN", "PDATE"])
    out.to_csv(table_paths["punch_csv_path"], index=False)
    return out


#####################################################  end of punch shift logic ##############################################

#####################################################  start of punch flexi logic ##############################################


def mode_1_last_day_flexi(gseldate,start_date,end_date,start_date_str,end_date_str,table_paths):

    gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    # if is_last_day == True:
    #     end_date_dt = pd.to_datetime(end_date)
    #     end_date_plus1 = end_date_dt + pd.Timedelta(days=1)
    #     end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
    #     date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')
    
    # else:
    #     date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    if is_last_day == True:
        # Load the DBF file into a DataFrame
        punches_dbf_new_month = table_paths['punches_dbf_path']  
        punches_table_new_month = DBF(punches_dbf_new_month, load=True)
        print("punches dbf length: ", len(punches_table_new_month))
        punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
        punches_df_new_month['DEL'] = False
        print("punches df new month columns: ",punches_df_new_month.columns)

        # Convert 'end_date' to datetime and add one day
        end_date_dt = pd.to_datetime(end_date)
        end_date_plus1 = end_date_dt + pd.Timedelta(days=1)

        # Convert the dates to date format for filtering
        end_date_date = end_date_dt.date()
        end_date_plus1_date = end_date_plus1.date()

        end_date_plus1_str = end_date_plus1.strftime('%Y-%m-%d')
        # date_range = pd.date_range(start=start_date_str, end=end_date_plus1_str, freq='D')

        # Ensure 'PDATE' column is in date format
        punches_df_new_month['PDATE'] = pd.to_datetime(punches_df_new_month['PDATE']).dt.date

        # Filter the DataFrame by 'PDATE'
        filtered_df = punches_df_new_month[
            punches_df_new_month['PDATE'].between(end_date_date, end_date_plus1_date)
        ]

        # Sort by TOKEN, PDATE, and PDTIME to ensure the order of events is maintained
        filtered_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)

        # Initialize an empty list to store matching records
        matching_records = []

        # Iterate through each TOKEN group to find consecutive MODE=0 (end_date) and MODE=1 (end_date_plus1_date)
        for token, group in filtered_df.groupby('TOKEN'):
            group = group.reset_index(drop=True)  # Reset index for easier iteration
            for i in range(len(group) - 1):
                if (
                    group.loc[i, 'PDATE'] == end_date_date
                    and group.loc[i, 'MODE'] == 0
                    and group.loc[i + 1, 'PDATE'] == end_date_plus1_date
                    and group.loc[i + 1, 'MODE'] == 1
                ):
                    # Add the matching rows to the list
                    matching_records.append(group.loc[i])
                    matching_records.append(group.loc[i + 1])

        # Convert the matching records to a DataFrame
        consecutive_matching_df = pd.DataFrame(matching_records)

        print("Filtered consecutive matching records saved to 'consecutive_matching_records.csv'")

        # === Additional Step: Filter Out Only MODE=1 Records ===
        if not consecutive_matching_df.empty:  # Check if DataFrame has rows
            mode_1_only_df = consecutive_matching_df[consecutive_matching_df['MODE'] == 1]
            mode_1_only_df['DEL'] = "False"
        else:
            # Create an empty DataFrame with the desired columns
            mode_1_only_df = pd.DataFrame(columns=[
                'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
                'MODE', 'PDTIME', 'MCIP', 'DEL'
            ])

        print("Filtered MODE=1 records saved to 'mode_1_only_records.csv'")
    
        mode_1_only_df.to_csv('mode_1_only_df_check_punches.csv',index=False)
    else:
        mode_1_only_df = pd.DataFrame(columns=[
                'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
                'MODE', 'PDTIME', 'MCIP', 'DEL'
        ])
    date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    return mode_1_only_df,date_range 




# def generate_punch_flexi(punches_df, muster_df, g_current_path):
#     table_paths = file_paths(g_current_path)

#     # -------- read gsel params (now has 4 lines) --------
#     with open(table_paths["gsel_date_path"]) as file:
#         file_contents = [line.strip() for line in file.readlines()]

#     gseldate = file_contents[0]
#     gsel_datetime = pd.to_datetime(gseldate)

#     ghalf_day = int(file_contents[1])
#     gfull_day = int(file_contents[2])
#     ggrace_time = int(file_contents[3])  # <-- new line (e.g. 10)

#     # -------- date range from DATED.DBF --------
#     dated_table = DBF(table_paths["dated_dbf_path"], load=True)
#     start_date = dated_table.records[0]["MUFRDATE"]
#     end_date = dated_table.records[0]["MUTODATE"]

#     start_date_str = start_date.strftime("%Y-%m-%d")
#     end_date_str = end_date.strftime("%Y-%m-%d")

#     # -------- last day mode-1 logic (UNCHANGED reference func) --------
#     mode_1_only_df, date_range = mode_1_last_day_flexi(
#         gseldate, start_date, end_date, start_date_str, end_date_str, table_paths
#     )

#     punches_df = pd.concat([punches_df, mode_1_only_df], ignore_index=True)
#     punches_df.sort_values(by=["TOKEN", "PDATE", "PDTIME"], inplace=True)
#     punches_df.to_csv("concated_punches_and_mode_1.csv", index=False)

#     # -------- build punch_df (keep your existing behavior) --------
#     punch_df = pd.DataFrame(
#         columns=[
#             "TOKEN", "COMCODE", "PDATE",
#             "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
#             "INTIME", "OUTTIME", "TOTALTIME",
#             "PUNCH_STATUS", "REMARKS", "OT",
#         ]
#     )

#     in_punch_time = None
#     out_punch_time = None

#     for _, row in punches_df.iterrows():
#         if row["MODE"] == 0:
#             in_punch_time = pd.to_datetime(row["PDTIME"]).replace(second=0)

#         elif row["MODE"] == 1:
#             out_punch_time = pd.to_datetime(row["PDTIME"]).replace(second=0)

#             if in_punch_time is not None:
#                 time_difference = out_punch_time - in_punch_time

#                 if time_difference.total_seconds() > 0:
#                     days = time_difference.days
#                     hours, remainder = divmod(time_difference.seconds, 3600)
#                     minutes, _ = divmod(remainder, 60)

#                     minutes_status = int(time_difference.total_seconds() / 60)
#                     attn_status = "PR" if minutes_status >= gfull_day else ("AB" if minutes_status <= ghalf_day else "HD")

#                     overtime_hours, overtime_minutes = (
#                         divmod(minutes_status - gfull_day, 60) if minutes_status > gfull_day else (0, 0)
#                     )
#                     overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)

#                     pdate_str = in_punch_time.strftime("%Y-%m-%d")

#                     duplicates = punch_df[
#                         (punch_df["PDATE"] == pdate_str) & (punch_df["TOKEN"] == row["TOKEN"])
#                     ]

#                     comcode_val = row.get("COMCODE", np.nan)

#                     if duplicates.empty:
#                         punch_df = pd.concat(
#                             [
#                                 punch_df,
#                                 pd.DataFrame(
#                                     {
#                                         "TOKEN": [row["TOKEN"]],
#                                         "COMCODE": [comcode_val],
#                                         "PDATE": [pdate_str],
#                                         "INTIME1": [in_punch_time.strftime("%Y-%m-%d %H:%M")],
#                                         "OUTTIME1": [out_punch_time.strftime("%Y-%m-%d %H:%M")],
#                                         "INTIME2": [np.nan],
#                                         "OUTTIME2": [np.nan],
#                                         "INTIME3": [np.nan],
#                                         "OUTTIME3": [np.nan],
#                                         "INTIME4": [np.nan],
#                                         "OUTTIME4": [np.nan],
#                                         "INTIME": [in_punch_time.strftime("%Y-%m-%d %H:%M")],
#                                         "OUTTIME": [out_punch_time.strftime("%Y-%m-%d %H:%M")],
#                                         "TOTALTIME": [f"{hours:02}:{minutes:02}"],
#                                         "PUNCH_STATUS": [attn_status],
#                                         "REMARKS": [("#" if days > 0 else "")],
#                                         "OT": [overtime_formatted],
#                                     }
#                                 ),
#                             ],
#                             ignore_index=True,
#                         )
#                     else:
#                         idx = duplicates.index[-1]

#                         # fill next available IN/OUT slot
#                         for col_in, col_out in [("INTIME2", "OUTTIME2"), ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
#                             if pd.isna(punch_df.loc[idx, col_in]):
#                                 punch_df.loc[idx, col_in] = in_punch_time.strftime("%Y-%m-%d %H:%M")
#                                 punch_df.loc[idx, col_out] = out_punch_time.strftime("%Y-%m-%d %H:%M")
#                                 break

#                         punch_df.loc[idx, "OUTTIME"] = out_punch_time.strftime("%Y-%m-%d %H:%M") if not pd.isna(out_punch_time) else np.nan

#                         # recompute total time across all pairs (your existing approach)
#                         total = pd.to_timedelta(0)

#                         for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
#                                           ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
#                             if (cin in punch_df.columns) and (cout in punch_df.columns):
#                                 if not pd.isna(punch_df.loc[idx, cin]) and not pd.isna(punch_df.loc[idx, cout]):
#                                     total += pd.to_datetime(punch_df.loc[idx, cout]) - pd.to_datetime(punch_df.loc[idx, cin])

#                         totaldays = total.days

#                         punch_df.loc[idx, "REMARKS"] = "*"  # default
#                         if totaldays > 0:
#                             punch_df.loc[idx, "REMARKS"] = "#"

#                         total_hours, total_remainder = divmod(total.seconds, 3600)
#                         total_minutes, _ = divmod(total_remainder, 60)

#                         punch_df.loc[idx, "TOTALTIME"] = f"{total_hours:02}:{total_minutes:02}"

#                         total_minutes_status = int(total.total_seconds() / 60)
#                         attn_status2 = "PR" if total_minutes_status >= gfull_day else ("AB" if total_minutes_status <= ghalf_day else "HD")

#                         overtime_hours2, overtime_minutes2 = (
#                             divmod(total_minutes_status - gfull_day, 60) if total_minutes_status > gfull_day else (0, 0)
#                         )
#                         overtime_formatted2 = "{:02d}:{:02d}".format(overtime_hours2, overtime_minutes2)

#                         punch_df.loc[idx, "OT"] = overtime_formatted2
#                         punch_df.loc[idx, "PUNCH_STATUS"] = attn_status2

#                         # keep COMCODE if we have one
#                         if pd.isna(punch_df.loc[idx, "COMCODE"]) and not pd.isna(comcode_val):
#                             punch_df.loc[idx, "COMCODE"] = comcode_val

#     # -------- Ensure AB rows for missing days --------
#     for token in muster_df["TOKEN"].unique():
#         token_punch_df = punch_df[punch_df["TOKEN"] == token]

#         for d in date_range:
#             ds = d.strftime("%Y-%m-%d")
#             if not ((token_punch_df["PDATE"] == ds) & (token_punch_df["TOKEN"] == token)).any():
#                 punch_df = pd.concat(
#                     [
#                         punch_df,
#                         pd.DataFrame(
#                             {
#                                 "TOKEN": [token],
#                                 "COMCODE": [np.nan],
#                                 "PDATE": [ds],
#                                 "INTIME1": [np.nan],
#                                 "OUTTIME1": [np.nan],
#                                 "INTIME2": [np.nan],
#                                 "OUTTIME2": [np.nan],
#                                 "INTIME3": [np.nan],
#                                 "OUTTIME3": [np.nan],
#                                 "INTIME4": [np.nan],
#                                 "OUTTIME4": [np.nan],
#                                 "INTIME": [np.nan],
#                                 "OUTTIME": [np.nan],
#                                 "TOTALTIME": [np.nan],
#                                 "PUNCH_STATUS": ["AB"],
#                                 "REMARKS": [np.nan],
#                                 "OT": [""],
#                             }
#                         ),
#                     ],
#                     ignore_index=True,
#                 )

#     punch_df = punch_df.sort_values(by=["TOKEN", "PDATE"])

#     # ----------------------------
#     # NEW: Base status from ACTUAL mins (sum of all IN/OUT pairs)
#     #      OT based on ACTUAL mins (unchanged)
#     #      GRATIME promotion (AB->HD and HD->PR) + CLEAN OLD "&"
#     # ----------------------------

#     # Ensure REMARKS exists & normalize
#     if "REMARKS" not in punch_df.columns:
#         punch_df["REMARKS"] = ""
#     punch_df["REMARKS"] = punch_df["REMARKS"].fillna("").astype(str)

#     # Clear any existing "&" first (old marker)
#     punch_df.loc[punch_df["REMARKS"].str.strip().eq("&"), "REMARKS"] = ""

#     # Compute ACTUAL seconds as sum of all pairs
#     total_secs = pd.Series(0.0, index=punch_df.index)
#     any_pair = pd.Series(False, index=punch_df.index)

#     pair_cols = [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
#                  ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]

#     for cin, cout in pair_cols:
#         tin = pd.to_datetime(punch_df[cin], errors="coerce")
#         tout = pd.to_datetime(punch_df[cout], errors="coerce")
#         m = tin.notna() & tout.notna()
#         any_pair |= m
#         total_secs.loc[m] += (tout.loc[m] - tin.loc[m]).dt.total_seconds().clip(lower=0)

#     valid = any_pair
#     mins_actual = (total_secs.loc[valid] // 60).astype(int)

#     fullm = pd.Series(gfull_day, index=mins_actual.index).astype(int)
#     halfm = pd.Series(ghalf_day, index=mins_actual.index).astype(int)

#     # Base status from ACTUAL mins
#     base_status = np.where(mins_actual >= fullm, "PR", np.where(mins_actual <= halfm, "AB", "HD"))
#     punch_df.loc[valid, "PUNCH_STATUS"] = base_status

#     punch_df.loc[valid, "TOTAL_HRS"] = [hhmm(int(m)) for m in mins_actual]
#     punch_df.loc[valid, "TOTALTIME"] = punch_df.loc[valid, "TOTAL_HRS"]

#     # OT based on ACTUAL mins (unchanged)
#     otm = np.maximum(mins_actual - fullm, 0)
#     punch_df.loc[valid, "OT"] = [hhmm(int(m)) for m in otm]

#     # GRATIME promotion (use ggrace_time minutes)
#     grace_m = pd.Series(int(ggrace_time), index=mins_actual.index).astype(int)
#     mins_with_grace = (mins_actual + grace_m).astype(int)

#     cur_status_s = punch_df.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()

#     # 1) Promote AB -> HD
#     ab_to_hd_mask = (
#         cur_status_s.eq("AB")
#         & (mins_actual < halfm)
#         & (mins_with_grace >= halfm)
#     )
#     ab_to_hd_idx = punch_df.loc[valid].index[ab_to_hd_mask]
#     punch_df.loc[ab_to_hd_idx, "PUNCH_STATUS"] = "HD"
#     punch_df.loc[ab_to_hd_idx, "REMARKS"] = "&"

#     # Refresh status
#     cur_status_s = punch_df.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()

#     # 2) Promote HD -> PR
#     hd_to_pr_mask = (
#         cur_status_s.eq("HD")
#         & (mins_actual < fullm)
#         & (mins_with_grace >= fullm)
#     )
#     promoted_idx = punch_df.loc[valid].index[hd_to_pr_mask]
#     punch_df.loc[promoted_idx, "PUNCH_STATUS"] = "PR"
#     punch_df.loc[promoted_idx, "REMARKS"] = "&"

#     # -------------------- Fill COMCODE from muster_df if possible --------------------
#     if "COMCODE" in muster_df.columns:
#         tmp = muster_df[["TOKEN", "COMCODE"]].copy()
#         tmp["TOKEN"] = tmp["TOKEN"].astype(str).str.strip()
#         tmp["COMCODE"] = tmp["COMCODE"].astype(str).str.strip()
#         tmp = tmp[tmp["COMCODE"].ne("") & tmp["COMCODE"].ne("nan")]
#         comcode_map = tmp.drop_duplicates("TOKEN", keep="last").set_index("TOKEN")["COMCODE"].to_dict()

#         punch_df["COMCODE"] = punch_df["COMCODE"].fillna("").astype(str).str.strip()
#         blank_cc = punch_df["COMCODE"].eq("") | punch_df["COMCODE"].str.lower().eq("nan")
#         punch_df.loc[blank_cc, "COMCODE"] = (
#             punch_df.loc[blank_cc, "TOKEN"].astype(str).str.strip().map(comcode_map).fillna("")
#         )

#     # -------------------- Force same output schema + order as generate_punch_shift --------------------
#     required_cols = [
#         "TOKEN", "COMCODE", "PDATE",
#         "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
#         "INTIME", "OUTTIME", "TOTALTIME",
#         "PUNCH_STATUS", "REMARKS", "OT",
#         "SHIFT_STATUS",
#         "inc_grt_minutes", "gratime_minutes", "workhrs_minutes", "halfday_minutes", "workhrs",
#         "shift_st_time", "shift_ed_time",
#         "TOTAL_HRS", "TOTPASSHRS",
#     ]

#     # Create missing columns as blank (NaN) — no defaults, per your request
#     for c in required_cols:
#         if c not in punch_df.columns:
#             punch_df[c] = np.nan

#     # Drop anything extra (like MODE if it leaked in)
#     punch_df = punch_df[required_cols]

#     punch_df = punch_df.sort_values(by=["TOKEN", "PDATE"])
#     punch_df.to_csv(table_paths["punch_csv_path"], index=False)
#     return punch_df


def generate_punch_flexi(punches_df, muster_df, g_current_path):
    table_paths = file_paths(g_current_path)

    # -------- read gsel params (now has 4 lines) --------
    with open(table_paths["gsel_date_path"]) as file:
        file_contents = [line.strip() for line in file.readlines()]

    gseldate = file_contents[0]
    gsel_datetime = pd.to_datetime(gseldate)

    ghalf_day = int(file_contents[1])
    gfull_day = int(file_contents[2])
    ggrace_time = int(file_contents[3])  # e.g. 10

    # -------- date range from DATED.DBF --------
    dated_table = DBF(table_paths["dated_dbf_path"], load=True)
    start_date = dated_table.records[0]["MUFRDATE"]
    end_date = dated_table.records[0]["MUTODATE"]

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # -------- last day mode-1 logic (UNCHANGED reference func) --------
    mode_1_only_df, date_range = mode_1_last_day_flexi(
        gseldate, start_date, end_date, start_date_str, end_date_str, table_paths
    )

    punches_df = pd.concat([punches_df, mode_1_only_df], ignore_index=True)
    punches_df.sort_values(by=["TOKEN", "PDATE", "PDTIME"], inplace=True)
    punches_df.to_csv("concated_punches_and_mode_1.csv", index=False)

    # -------- build punch_df --------
    punch_df = pd.DataFrame(
        columns=[
            "TOKEN", "COMCODE", "PDATE",
            "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
            "INTIME", "OUTTIME", "TOTALTIME",
            "PUNCH_STATUS", "REMARKS", "OT",
        ]
    )

    in_punch_time = None
    out_punch_time = None

    # Track "extra OUT without IN" -> mark MM for that TOKEN+date later
    mm_pairs = set()

    for _, row in punches_df.iterrows():
        if row["MODE"] == 0:
            in_punch_time = pd.to_datetime(row["PDTIME"]).replace(second=0)

        elif row["MODE"] == 1:
            out_punch_time = pd.to_datetime(row["PDTIME"]).replace(second=0)

            # OUT without a pending IN => mark MM for that OUT date
            if in_punch_time is None:
                try:
                    mm_pairs.add((row["TOKEN"], out_punch_time.strftime("%Y-%m-%d")))
                except Exception:
                    pass
                continue

            time_difference = out_punch_time - in_punch_time

            if time_difference.total_seconds() > 0:
                days = time_difference.days
                hours, remainder = divmod(time_difference.seconds, 3600)
                minutes, _ = divmod(remainder, 60)

                minutes_status = int(time_difference.total_seconds() / 60)
                attn_status = "PR" if minutes_status >= gfull_day else ("AB" if minutes_status <= ghalf_day else "HD")

                overtime_hours, overtime_minutes = (
                    divmod(minutes_status - gfull_day, 60) if minutes_status > gfull_day else (0, 0)
                )
                overtime_formatted = "{:02d}:{:02d}".format(overtime_hours, overtime_minutes)

                pdate_str = in_punch_time.strftime("%Y-%m-%d")

                duplicates = punch_df[
                    (punch_df["PDATE"] == pdate_str) & (punch_df["TOKEN"] == row["TOKEN"])
                ]

                comcode_val = row.get("COMCODE", np.nan)

                if duplicates.empty:
                    punch_df = pd.concat(
                        [
                            punch_df,
                            pd.DataFrame(
                                {
                                    "TOKEN": [row["TOKEN"]],
                                    "COMCODE": [comcode_val],
                                    "PDATE": [pdate_str],
                                    "INTIME1": [in_punch_time.strftime("%Y-%m-%d %H:%M")],
                                    "OUTTIME1": [out_punch_time.strftime("%Y-%m-%d %H:%M")],
                                    "INTIME2": [np.nan],
                                    "OUTTIME2": [np.nan],
                                    "INTIME3": [np.nan],
                                    "OUTTIME3": [np.nan],
                                    "INTIME4": [np.nan],
                                    "OUTTIME4": [np.nan],
                                    "INTIME": [in_punch_time.strftime("%Y-%m-%d %H:%M")],
                                    "OUTTIME": [out_punch_time.strftime("%Y-%m-%d %H:%M")],
                                    "TOTALTIME": [f"{hours:02}:{minutes:02}"],
                                    "PUNCH_STATUS": [attn_status],
                                    "REMARKS": [("#" if days > 0 else "")],
                                    "OT": [overtime_formatted],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )
                else:
                    idx = duplicates.index[-1]

                    # fill next available IN/OUT slot
                    for col_in, col_out in [("INTIME2", "OUTTIME2"), ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                        if pd.isna(punch_df.loc[idx, col_in]):
                            punch_df.loc[idx, col_in] = in_punch_time.strftime("%Y-%m-%d %H:%M")
                            punch_df.loc[idx, col_out] = out_punch_time.strftime("%Y-%m-%d %H:%M")
                            break

                    punch_df.loc[idx, "OUTTIME"] = out_punch_time.strftime("%Y-%m-%d %H:%M") if not pd.isna(out_punch_time) else np.nan

                    # recompute total time across all pairs
                    total = pd.to_timedelta(0)
                    for cin, cout in [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                                      ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]:
                        if (cin in punch_df.columns) and (cout in punch_df.columns):
                            if not pd.isna(punch_df.loc[idx, cin]) and not pd.isna(punch_df.loc[idx, cout]):
                                total += pd.to_datetime(punch_df.loc[idx, cout]) - pd.to_datetime(punch_df.loc[idx, cin])

                    totaldays = total.days
                    punch_df.loc[idx, "REMARKS"] = "*"  # default
                    if totaldays > 0:
                        punch_df.loc[idx, "REMARKS"] = "#"

                    total_hours, total_remainder = divmod(total.seconds, 3600)
                    total_minutes, _ = divmod(total_remainder, 60)
                    punch_df.loc[idx, "TOTALTIME"] = f"{total_hours:02}:{total_minutes:02}"

                    total_minutes_status = int(total.total_seconds() / 60)
                    attn_status2 = "PR" if total_minutes_status >= gfull_day else ("AB" if total_minutes_status <= ghalf_day else "HD")

                    overtime_hours2, overtime_minutes2 = (
                        divmod(total_minutes_status - gfull_day, 60) if total_minutes_status > gfull_day else (0, 0)
                    )
                    overtime_formatted2 = "{:02d}:{:02d}".format(overtime_hours2, overtime_minutes2)

                    punch_df.loc[idx, "OT"] = overtime_formatted2
                    punch_df.loc[idx, "PUNCH_STATUS"] = attn_status2

                    # keep COMCODE if we have one
                    if pd.isna(punch_df.loc[idx, "COMCODE"]) and not pd.isna(comcode_val):
                        punch_df.loc[idx, "COMCODE"] = comcode_val

            # IMPORTANT: consume IN after using it once so extra OUTs get treated as MM
            in_punch_time = None

    # -------- Ensure AB rows for missing days --------
    for token in muster_df["TOKEN"].unique():
        token_punch_df = punch_df[punch_df["TOKEN"] == token]
        for d in date_range:
            ds = d.strftime("%Y-%m-%d")
            if not ((token_punch_df["PDATE"] == ds) & (token_punch_df["TOKEN"] == token)).any():
                punch_df = pd.concat(
                    [
                        punch_df,
                        pd.DataFrame(
                            {
                                "TOKEN": [token],
                                "COMCODE": [np.nan],
                                "PDATE": [ds],
                                "INTIME1": [np.nan],
                                "OUTTIME1": [np.nan],
                                "INTIME2": [np.nan],
                                "OUTTIME2": [np.nan],
                                "INTIME3": [np.nan],
                                "OUTTIME3": [np.nan],
                                "INTIME4": [np.nan],
                                "OUTTIME4": [np.nan],
                                "INTIME": [np.nan],
                                "OUTTIME": [np.nan],
                                "TOTALTIME": [np.nan],
                                "PUNCH_STATUS": ["AB"],
                                "REMARKS": [np.nan],
                                "OT": [""],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

    punch_df = punch_df.sort_values(by=["TOKEN", "PDATE"])

    # ----------------------------
    # Base status from ACTUAL mins (sum of all IN/OUT pairs)
    # OT based on ACTUAL mins (unchanged)
    # GRATIME PROMOTION (AB->HD and HD->PR) + CLEAN OLD "&"
    # ----------------------------
    if "REMARKS" not in punch_df.columns:
        punch_df["REMARKS"] = ""
    punch_df["REMARKS"] = punch_df["REMARKS"].fillna("").astype(str)

    punch_df.loc[punch_df["REMARKS"].str.strip().eq("&"), "REMARKS"] = ""

    total_secs = pd.Series(0.0, index=punch_df.index)
    any_pair = pd.Series(False, index=punch_df.index)
    pair_cols = [("INTIME1", "OUTTIME1"), ("INTIME2", "OUTTIME2"),
                 ("INTIME3", "OUTTIME3"), ("INTIME4", "OUTTIME4")]

    for cin, cout in pair_cols:
        tin = pd.to_datetime(punch_df[cin], errors="coerce")
        tout = pd.to_datetime(punch_df[cout], errors="coerce")
        m = tin.notna() & tout.notna()
        any_pair |= m
        total_secs.loc[m] += (tout.loc[m] - tin.loc[m]).dt.total_seconds().clip(lower=0)

    valid = any_pair
    mins_actual = (total_secs.loc[valid] // 60).astype(int)

    fullm = pd.Series(gfull_day, index=mins_actual.index).astype(int)
    halfm = pd.Series(ghalf_day, index=mins_actual.index).astype(int)

    base_status = np.where(mins_actual >= fullm, "PR", np.where(mins_actual <= halfm, "AB", "HD"))
    punch_df.loc[valid, "PUNCH_STATUS"] = base_status
    punch_df.loc[valid, "TOTAL_HRS"] = [hhmm(int(m)) for m in mins_actual]
    punch_df.loc[valid, "TOTALTIME"] = punch_df.loc[valid, "TOTAL_HRS"]

    otm = np.maximum(mins_actual - fullm, 0)
    punch_df.loc[valid, "OT"] = [hhmm(int(m)) for m in otm]

    grace_m = pd.Series(int(ggrace_time), index=mins_actual.index).astype(int)
    mins_with_grace = (mins_actual + grace_m).astype(int)

    cur_status_s = punch_df.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()

    ab_to_hd_mask = (
        cur_status_s.eq("AB")
        & (mins_actual < halfm)
        & (mins_with_grace >= halfm)
    )
    ab_to_hd_idx = punch_df.loc[valid].index[ab_to_hd_mask]
    punch_df.loc[ab_to_hd_idx, "PUNCH_STATUS"] = "HD"
    punch_df.loc[ab_to_hd_idx, "REMARKS"] = "&"

    cur_status_s = punch_df.loc[valid, "PUNCH_STATUS"].astype(str).str.strip().str.upper()

    hd_to_pr_mask = (
        cur_status_s.eq("HD")
        & (mins_actual < fullm)
        & (mins_with_grace >= fullm)
    )
    promoted_idx = punch_df.loc[valid].index[hd_to_pr_mask]
    punch_df.loc[promoted_idx, "PUNCH_STATUS"] = "PR"
    punch_df.loc[promoted_idx, "REMARKS"] = "&"

    # -------------------- Fill COMCODE from muster_df if possible (do NOT change TOKEN dtype) --------------------
    comcode_map = {}
    if "COMCODE" in muster_df.columns:
        tmp = muster_df[["TOKEN", "COMCODE"]].copy()
        tmp["COMCODE"] = tmp["COMCODE"].astype(str).str.strip()
        tmp = tmp[tmp["COMCODE"].ne("") & tmp["COMCODE"].ne("nan")]
        comcode_map = tmp.drop_duplicates("TOKEN", keep="last").set_index("TOKEN")["COMCODE"].to_dict()

        punch_df["COMCODE"] = punch_df["COMCODE"].fillna("").astype(str).str.strip()
        blank_cc = punch_df["COMCODE"].eq("") | punch_df["COMCODE"].str.lower().eq("nan")
        punch_df.loc[blank_cc, "COMCODE"] = punch_df.loc[blank_cc, "TOKEN"].map(comcode_map).fillna("")

    # -------------------- Apply MM for "extra OUT" dates (WITHOUT changing TOKEN dtype) --------------------
    if mm_pairs:
        mm_df = pd.DataFrame(list(mm_pairs), columns=["TOKEN", "PDATE"])
        mm_df["PDATE"] = pd.to_datetime(mm_df["PDATE"], errors="coerce")
        punch_df["PDATE"] = pd.to_datetime(punch_df["PDATE"], errors="coerce")

        punch_df = punch_df.merge(mm_df.assign(_MMFLAG=1), on=["TOKEN", "PDATE"], how="left")
        mm_mask = punch_df["_MMFLAG"].fillna(0).astype(int).eq(1)

        punch_df.loc[mm_mask, "PUNCH_STATUS"] = "MM"
        punch_df.loc[mm_mask, "TOTALTIME"] = ""
        punch_df.loc[mm_mask, "OT"] = ""
        punch_df.loc[mm_mask, "REMARKS"] = ""
        if "TOTAL_HRS" in punch_df.columns:
            punch_df.loc[mm_mask, "TOTAL_HRS"] = ""

        punch_df = punch_df.drop(columns=["_MMFLAG"], errors="ignore")

    # -------------------- Force schema + order like shift punch output --------------------
    required_cols = [
        "TOKEN", "COMCODE", "PDATE",
        "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2", "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4",
        "INTIME", "OUTTIME", "TOTALTIME",
        "PUNCH_STATUS", "REMARKS", "OT",
        "SHIFT_STATUS",
        "inc_grt_minutes", "gratime_minutes", "workhrs_minutes", "halfday_minutes", "workhrs",
        "shift_st_time", "shift_ed_time",
        "TOTAL_HRS", "TOTPASSHRS",
    ]
    for c in required_cols:
        if c not in punch_df.columns:
            punch_df[c] = np.nan
    punch_df = punch_df[required_cols]

    # -------------------- OUTPASS override (same as shift) --------------------
    outpass_csv_path = os.path.join(g_current_path, "OUTPASS.csv")
    punch_df = apply_outpass_override(punch_df, outpass_csv_path)

    # If any columns got reordered by override, force order again
    for c in required_cols:
        if c not in punch_df.columns:
            punch_df[c] = np.nan
    punch_df = punch_df[required_cols]

    punch_df = punch_df.sort_values(by=["TOKEN", "PDATE"])
    punch_df.to_csv(table_paths["punch_csv_path"], index=False)
    return punch_df


