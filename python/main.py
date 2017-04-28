import sys
import time
import re
from os import listdir
from os.path import isfile, join
from dateutil.parser import parse
from time import mktime

def process_anoms_in_all_anoms(file_path):
    count = 0
    file        = open(file_path)
    anoms_count = {}
    anoms_feeder_count = {}
    for line in iter(file):
        count  = count + 1
        line_components = line.split(',')
        anom_type = line_components[1]
        feeder_id = line_components[5]
        if anom_type in anoms_count:
            anoms_count[anom_type] += 1
            # anoms_dict[anom_type].append(line)
            if feeder_id in anoms_feeder_count[anom_type]:
                anoms_feeder_count[anom_type][feeder_id] += 1
            else:
                anoms_feeder_count[anom_type][feeder_id]  = 1
        else:
            anoms_count[anom_type] = 1
            # anoms_dict[anom_type]  = [line]
            anoms_feeder_count[anom_type] = {}
            anoms_feeder_count[anom_type][feeder_id] = 1
        if count % 1000000 == 0:
            sys.stdout.write("anom_type:" + anom_type + ", count:" + str(count) + "  " + line)
    file.close()
    for anom_type in anoms_feeder_count:
        for feeder_id in anoms_feeder_count[anom_type]:
            if anoms_feeder_count[anom_type][feeder_id] >= 1000:
                print anom_type, feeder_id, anoms_feeder_count[anom_type][feeder_id]
    anoms_count_sorted = [(k,v) for v,k in sorted([(v,k) for k,v in anoms_count.items()], reverse=True)]
    print ""
    for k, v in anoms_count_sorted:
        print k, v                
    # return anoms_dict

def filter_all_anoms_by_date(file_path, ofile1_path, ofile2_path):
    count = 0
    bulk_count = 0
    file   = open(file_path)
    ofile1 = open(ofile1_path, 'w')
    ofile2 = open(ofile2_path, 'w')
    for line in iter(file):
        count  = count + 1
        line_components = line.split(',')
        # ts     = parse(line_components[7], fuzzy=True)
        date   = line_components[7].split(' ')[0]
        date_components = date.split('-')
        if len(date_components) >= 3:
            year  = int(date_components[0])
            month = int(date_components[1])
            day   = int(date_components[2])
            if year <= 2014 or (year == 2015 and month <= 2):
                bulk_count = bulk_count + 1
                ofile1.write(line)
            else:
                ofile2.write(line)
        if count % 1000000 == 0:
            sys.stdout.write("count:" + str(count) + ", bulk_count: " + str(bulk_count) + "  " + line)
    file.close()
    ofile1.close()

def compare_old_with_new_anoms(old_file_path, new_file_path):
    count = 0
    old_file    = open(old_file_path)
    new_file    = open(new_file_path)
    anoms_feeder_count = {}
    for line in iter(old_file):
        count  = count + 1
        line_components = line.split(',')
        anom_type = line_components[1]
        feeder_id = line_components[5]
        ts        = time.strptime(line_components[7].strip(), "%Y-%m-%d %H:%M:%S+00:00")
        epoch_ts  = int(mktime(ts))
        if anom_type in anoms_feeder_count:
            if feeder_id in anoms_feeder_count[anom_type]:
                anoms_feeder_count[anom_type][feeder_id] += 1
            else:
                anoms_feeder_count[anom_type][feeder_id]  = 1
        else:
            anoms_feeder_count[anom_type] = {}
            anoms_feeder_count[anom_type][feeder_id] = 1
        if count % 1000000 == 0:
            sys.stdout.write("anom_type:" + anom_type + ", count:" + str(count) + ", ts: " + str(epoch_ts) + "  " + line)
    old_file.close()
    new_file.close()
    
def process_edna_file(file_name, file_path, file_count, anomaly_count):
    count = 0
    file  = open(file_path)
    for line in iter(file):
        count = count + 1
        line_components = line.split(',')
        if len(line_components) < 5 or count <= 1:
            continue
        # ts         = time.strptime(line_components[1].replace('"', ''), "%m/%d/%Y %I:%M:%S %p")
        extendedId = line_components[0]
        if ".AFS." in extendedId:
            # handle potential AFS anomalies
            if ".ALARM" in extendedId and "ALARM" in line_components[3]:
                anomaly_count["AFS_ALARM_ALARM"] += 1
            elif ".GROUND" in extendedId and "ALARM" in line_components[3]:
                anomaly_count["AFS_GROUND_ALARM"] += 1
            elif ".I_FAULT" in extendedId:
                value = int(line_components[2].replace('"', ''))
                if value >= 600:
                    if value >= 900:
                        anomaly_count["AFS_I_FAULT_FULL"] += 1
                        # writer.WriteString(fmt.Sprintf("{type:\"AFS_I_FAULT_FULL\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
                    else:
                        anomaly_count["AFS_I_FAULT_TEMP"] += 1
                        # writer.WriteString(fmt.Sprintf("{type:\"AFS_I_FAULT_TEMP\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
        if count % 1000000 == 0:
            # sys.stdout.write("{file_num:" + str(file_count) + ", file_name:" + file_name + ", count:" + str(count) + ", ts:" + time.asctime(ts) + "}\n")
            sys.stdout.write("{file_num:" + str(file_count) + ", file_name:" + file_name + ", count:" + str(count) + "}\n")
    file.close()

    
"""
dir        = '/Volumes/auto-grid-pam/DISK1/bulk_data/edna/response'
csv_files  = [f for f in listdir(dir) if ".csv" in f and isfile(join(dir, f))]
file_count = 0

for file_name in csv_files:
    file_count = file_count + 1
    file_path  = join(dir, file_name)
    anomaly_count = {
        "AFS_ALARM_ALARM": 0, "AFS_GROUND_ALARM": 0, "AFS_I_FAULT_FULL": 0, "AFS_I_FAULT_TEMP": 0,
        "FCI_FAULT_ALARM": 0, "FCI_I_FAULT_FULL": 0, "FCI_I_FAULT_TEMP": 0,
        "ZERO_CURRENT_V3": 0, "ZERO_CURRENT_V4":  0,
        "ZERO_POWER_V3":   0, "ZERO_POWER_V4":    0,
        "ZERO_VOLTAGE_V3": 0, "ZERO_VOLTAGE_V4":  0,
        "PF_SPIKES_V3":    0, "THD_SPIKES_V3":    0,
    }
    process_edna_file(file_name, file_path, file_count, anomaly_count)
"""

# process_anoms_in_all_anoms("/Users/sanjaynoronha/Desktop/all_anoms.csv")
# process_anoms_in_all_anoms("/Users/sanjaynoronha/Desktop/edna_out_0_950.csv")
filter_all_anoms_by_date("/Users/sanjaynoronha/Desktop/all_anoms.csv", "/Users/sanjaynoronha/Desktop/all_anoms_feb2015.csv", "/Users/sanjaynoronha/Desktop/all_anoms_after_feb2015.csv")
# compare_old_with_new_anoms("/Users/sanjaynoronha/Desktop/all_anoms_feb2015.csv", "/Users/sanjaynoronha/Desktop/edna_out.txt")
