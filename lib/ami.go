package lib

type AMI struct {
    // substn_name,fdr_num,prem_num,phas_type,cis_dvc_coor,ami_dvc_name,mtr_evnt_id,mtr_evnt_tmstmp,mtr_evnt_insrt_tmstmp,evnt_argus,evnt_txt
    SubstnName    string
    FdrNum        string
    PremNum       string
    PhasType      string
    CisDvcCoor    string
    AmiDvcName    string
    MtrEvntId     string
    MtrEvntTmstmp string
    EvntTxt       string

    MtrEvntEpoch  int64
}
