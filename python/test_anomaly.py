import pandas as pd
import numpy as np
import os
from pytz import timezone, UTC
import anomaly
import alert
import yaml
import re
import cPickle as pickle
EASTERN = timezone('US/Eastern')

def load_csv(filename, data_type, start_time=None, end_time=None):
    """Load and clean a single CSV file.
    Parameters
    ----------
    filename : str
        The path to the CSV file to be loaded.
    data_type : str
        The type of dataset to load. One of {'SCADA', 'EDNA', 'AMI', 'TICKETS'}.
    start_time : datetime.datetime or None, optional (default=None)
        The time to crop the file to. If None, the entire file will be used.
    end_time : datetime.datetime or None, optional (default=None)
        The time to crop the file to. If None, the entire file will be used.
    Returns
    -------
    feeder : str, or None
        The feeder ID number. Returns None for ticket datasets.
    df : Pandas DataFrame
        The time-series data.
    """
    if data_type == 'SCADA':
        feeder = filename.split('_')[-1]
        df = pd.read_csv(filename, parse_dates=[2])
        scada_cols = ['feederNumber', 'OBSERV_DATA', 'localTime']
        df = df[scada_cols]
        df.feederNumber = df.feederNumber.astype(str)
        df = df.loc[df.feederNumber == feeder]
        df.localTime = df.localTime.dt.tz_localize(EASTERN)
        df.localTime = df.localTime.dt.tz_convert(UTC)
        time_col = 'localTime'
    elif data_type == 'AMI':
        feeder = filename.split('_')[-1]
        real_cols = ["substn_name", "fdr_num", "prem_num", "phas_type",
                     "cis_dvc_coor", "ami_dvc_name", "mtr_evnt_id",
                     "mtr_evnt_tmstmp", "mtr_evnt_insrt_tmstmp", "evnt_argus",
                     "evnt_txt"]
        df = pd.read_csv(filename, warn_bad_lines=False, error_bad_lines=False,
                         skiprows=1, names=real_cols)
        ami_cols = ['fdr_num', 'ami_dvc_name', 'mtr_evnt_tmstmp', 'mtr_evnt_id']
        df = df[ami_cols]
        df = df[df.ami_dvc_name != 'ami_dvc_name']
        df.fdr_num = df.fdr_num.astype(str)
        df.mtr_evnt_id = df.mtr_evnt_id.astype(str)
        df = df[df.mtr_evnt_id.isin({'12007', '12024'})]
        # meter starts with G=GE, with L=L&G
        df = df[df.ami_dvc_name.str.startswith('G')]
        df.mtr_evnt_tmstmp = pd.to_datetime(df.mtr_evnt_tmstmp)
        df.mtr_evnt_tmstmp = df.mtr_evnt_tmstmp.dt.tz_localize(EASTERN)
        df.mtr_evnt_tmstmp = df.mtr_evnt_tmstmp.dt.tz_convert(UTC)
        time_col = 'mtr_evnt_tmstmp'
    elif data_type == 'TICKETS':
        feeder = None
        real_cols = ["DW_TCKT_KEY", "FDR_NUM", "TRBL_TCKT_NUM", "GRN_TCKT_FLAG",
                     "IRPT_TYPE_CODE", "TCKT_TYPE_CODE", "SUPT_CODE",
                     "IRPT_CAUS_CODE", "CMI", "POWEROFF", "POWERRESTORE",
                     "RPR_ACTN_TYPE", "RPR_ACTN_SUB_TYPE", "RPR_ACTN_DS",
                     "A_PHAS_INVOLVED", "B_PHAS_INVOLVED", "C_PHAS_INVOLVED",
                     "TCKT_DVC_COOR", "REPAIRACTIONCREATETIME",
                     "REPAIREDACTIONSTATEPLANEX", "REPAIREDACTIONSTATEPLANEY",
                     "CRNT_ROW_FLAG", "A_TIME"]
        df = pd.read_csv(filename, warn_bad_lines=False, error_bad_lines=False,
                         skiprows=1, names=real_cols, parse_dates=[9, 10])
        tick_cols = ['DW_TCKT_KEY', 'FDR_NUM', 'TRBL_TCKT_NUM',
                     'IRPT_TYPE_CODE', 'SUPT_CODE', 'IRPT_CAUS_CODE', 'CMI',
                     'POWEROFF', 'POWERRESTORE', 'RPR_ACTN_TYPE', 'RPR_ACTN_DS']
        df = df[tick_cols]
        df = df[pd.notnull(df['FDR_NUM'])]
        df.FDR_NUM = df.FDR_NUM.astype(int).astype(str)
        df.POWEROFF = df.POWEROFF.dt.tz_localize(EASTERN)
        df.POWERRESTORE = df.POWERRESTORE.dt.tz_localize(EASTERN)
        df.POWEROFF = df.POWEROFF.dt.tz_convert(UTC)
        df.POWERRESTORE = df.POWERRESTORE.dt.tz_convert(UTC)
        time_col = 'POWEROFF'
    elif data_type == 'EDNA':
        feeder = filename.split('.')[-2].split('/')[-1]
        df = pd.read_csv(filename, dtype={'ValueString': str})
        df.columns = [col.replace(' ', '') for col in df.columns]
        edna_codes = df.ExtendedId.unique()
        edna_codes = pd.Series(edna_codes)
        edna_codes = edna_codes[-edna_codes.str.contains(r'Bad point')]
        edna_codes = edna_codes[((edna_codes.str.contains(r'\.PF\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.THD_')) &
                                 (edna_codes.str.contains(r'urrent'))) |
                                (((edna_codes.str.contains(r'\.MVAR')) |
                                  (edna_codes.str.contains(r'\.MVR\.'))) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.V\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.I\.')) &
                                 (edna_codes.str.contains(r'_PH')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 (-edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.MW')) &
                                 (edna_codes.str.contains(r'\.FDR\.')) &
                                 -(edna_codes.str.contains(r'BKR\.'))) |
                                ((edna_codes.str.contains(r'\.FCI\.')) &
                                 ((edna_codes.str.contains(r'\.FAULT')) |
                                  (edna_codes.str.contains(r'\.I_FAULT'))))
                                ((edna_codes.str.contains(r'\.AFS\.')) &
                                 ((edna_codes.str.contains(r'\.ALARM')) |
                                  (edna_codes.str.contains(r'\.GROUND')) |
                                  (edna_codes.str.contains(r'\.I_FAULT'))))]
        edna_codes = set(edna_codes.values)
        df = df[df.ExtendedId.isin(edna_codes)]
        df.Time = pd.to_datetime(df.Time)
        df.Time = df.Time.dt.tz_localize(EASTERN)
        df.Time = df.Time.dt.tz_convert(UTC)
        time_col = 'Time'
    else:
        raise ValueError("Unknown data_type: %s." % data_type)

    if start_time:
        df = df.loc[df[time_col] >= start_time]
    if end_time:
        df = df.loc[df[time_col] < end_time]

    return feeder, df

def get_fdr(id_str):
    """Get the feeder ID from the eDNA point name."""
    fdr_pattern = re.compile(r'[\._][0-9]{6}[\._]')
    try:
        fdr = fdr_pattern.findall(id_str)[0][1:7]
    except IndexError:
        fdr = 'NULL'
    return fdr
        
def _get_window(df, hours):
    resolution = df.Time.max() - df.Time.min()
    resolution = resolution.total_seconds() / (60. * 60.)
    if resolution < 1:
        return -1
    if df.shape[0] < 24:
        return -1
    resolution = df.shape[0] / resolution
    return int(np.ceil(resolution * float(hours)))

file_num = "401636"
print "Reading file " + file_num + ".csv ..."
DATA_PATH = '/Volumes/auto-grid-pam/DISK1/bulk_data/'
edna_zero = pd.read_csv(DATA_PATH + 'edna/response/' + file_num + '.csv')
df = edna_zero
print "Read file " + file_num + ".csv"
points = pd.DataFrame({'Point': df['Extended Id'].unique()})
points = points[-points.Point.str.contains('Bad point')]
points['Feeder'] = points.Point.map(get_fdr)
points = points[points.Feeder == file_num].Point

DAY = pd.Timedelta(days=1)

points_sub = points.loc[(points.str.contains(r'\.PF\.')) &
                        (points.str.contains(r'_PH')) &
                        (points.str.contains(r'\.FDR\.')) &
                        (-points.str.contains(r'BKR\.'))]
df_sub = df.loc[df['Extended Id'].isin(points_sub)]
print "Shape = " + str(df_sub.shape[0])
df_sub['Time'] = pd.to_datetime(df_sub['Time'])
print "Finished to_datetime ..."

for point_id in df_sub['Extended Id'].unique():

    df_sub2 = df_sub.loc[df_sub['Extended Id'] == point_id]
    df_sub2 = df_sub2.sort_values(by='Time')
    print "Shape2 = " + str(df_sub2.shape[0])
    window = _get_window(df_sub2, 24)
    if window == -1:
        continue
    anoms = df_sub2[(df_sub2.Value.abs() < 0.75) &
                    (pd.rolling_quantile(df_sub2.Value.abs(),
                                        window,
                                        0.01) > 0.8)].Time.tolist()
    anoms = np.array(anoms)
    anoms = [e for e in anoms if
    df_sub2[(df_sub2.Time <= e) &
            (df_sub2.Time > (e - DAY))].shape[0] > 24]
    df_sub2 = df_sub2.loc[df_sub2.Time.isin(anoms)]
    print "\n\nNUM ANOMS = " + str(len(anoms)) + "\n\n"

