package lib

import (
    "bufio"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "time"
)

type Ticket struct {
    // "DW_TCKT_KEY","FDR_NUM","TRBL_TCKT_NUM","GRN_TCKT_FLAG","IRPT_TYPE_CODE","TCKT_TYPE_CODE","SUPT_CODE","IRPT_CAUS_CODE","EQP_CODE","CMI","POWEROFF","POWERRESTORE","RPR_ACTN_TYPE","RPR_ACTN_SUB_TYPE","RPR_ACTN_DS","A_PHAS_INVOLVED","B_PHAS_INVOLVED","C_PHAS_INVOLVED","TCKT_DVC_COOR","REPAIRACTIONCREATETIME","REPAIREDACTIONSTATEPLANEX","REPAIREDACTIONSTATEPLANEY","CRNT_ROW_FLAG"
    // "10761437","505963","4","N","LAT","LAT","66","20","104","1381","06-01-2016 00:10:00","06-01-2016 03:09:00","TMRepairActionRefuseLateral","General","Refuse C Lateral","","","","56349314408","06-01-2016 03:47:52","397338","998285","Y","6/1/2016 12:14:00 AM"
    TicketKey               string
    FeederNumber            string
    TroubleTicketNumber     string
    GrnTicketFlag           string
    IrptTypeCode            string
    TicketTypeCode          string
    SuptCode                string
    IrptCauseCode           string
    EquipmentCode           string
    CMI                     string
    PowerOff                time.Time
    PowerRestore            time.Time
    RprActionType           string
    RprActionSubtype        string
    RprActionDs             string
    APhaseInvolved          string
    BPhaseInvolved          string
    CPhaseInvolved          string
    TicketDvcCoor           string
    RepairActionCreateTime  time.Time
    RepairActionStatePlaneX string
    RepairActionStatePlaneY string
    CurrentRowFlag          string
}

func (t *Ticket) Create(ticketLine string) {
    longForm        := "2006-01-02 15:04:05"
    lineComponents  := strings.Split(ticketLine, ",")
    t.TicketKey                 = strings.Replace(lineComponents[0], "\"", "", -1)
    t.FeederNumber              = strings.Replace(lineComponents[1], "\"", "", -1)
    t.TroubleTicketNumber       = strings.Replace(lineComponents[2], "\"", "", -1)
    t.GrnTicketFlag             = strings.Replace(lineComponents[3], "\"", "", -1)
    t.IrptTypeCode              = strings.Replace(lineComponents[4], "\"", "", -1)
    t.TicketTypeCode            = strings.Replace(lineComponents[5], "\"", "", -1)
    t.SuptCode                  = strings.Replace(lineComponents[6], "\"", "", -1)
    t.IrptCauseCode             = strings.Replace(lineComponents[7], "\"", "", -1)
    t.EquipmentCode             = strings.Replace(lineComponents[8], "\"", "", -1)
    t.CMI                       = strings.Replace(lineComponents[9], "\"", "", -1)
    t.PowerOff, _               = time.Parse(longForm, strings.Replace(lineComponents[10], "\"", "", -1))
    t.PowerRestore, _           = time.Parse(longForm, strings.Replace(lineComponents[11], "\"", "", -1))
    t.RprActionType             = strings.Replace(lineComponents[12], "\"", "", -1)
    t.RprActionSubtype          = strings.Replace(lineComponents[13], "\"", "", -1)
    t.RprActionDs               = strings.Replace(lineComponents[14], "\"", "", -1)
    t.APhaseInvolved            = strings.Replace(lineComponents[15], "\"", "", -1)
    t.BPhaseInvolved            = strings.Replace(lineComponents[16], "\"", "", -1)
    t.CPhaseInvolved            = strings.Replace(lineComponents[17], "\"", "", -1)
    t.TicketDvcCoor             = strings.Replace(lineComponents[18], "\"", "", -1)
    t.RepairActionCreateTime, _ = time.Parse(longForm, strings.Replace(lineComponents[19], "\"", "", -1))
    t.RepairActionStatePlaneX   = strings.Replace(lineComponents[20], "\"", "", -1)
    t.RepairActionStatePlaneY   = strings.Replace(lineComponents[21], "\"", "", -1)
    t.CurrentRowFlag            = strings.Replace(lineComponents[22], "\"", "", -1)
}

func GetTicketMap(dirName string) map[string][]Ticket {
    var ticketMap map[string][]Ticket = make(map[string][]Ticket)
    files, _  := ioutil.ReadDir(dirName)
    for _, f  := range files {
        filePath := dirName + "/" + f.Name()
        if strings.Contains(f.Name(), "TICKETS") && strings.Contains(f.Name(), ".csv") {
            if file, err := os.Open(filePath); err == nil {
                defer file.Close()
                scanner   := bufio.NewScanner(file)
                lineCount := 0
                for scanner.Scan() {
                    line := scanner.Text()
                    if lineCount > 0 {
                        if len(strings.Split(line, ",")) >= 5 {
                            ticket := new(Ticket)
                            ticket.Create(line)
                            ticketMap[ticket.FeederNumber] = append(ticketMap[ticket.FeederNumber], *ticket)
                        }
                    }
                    lineCount++
                }
                if err = scanner.Err(); err != nil {
                    log.Fatal(err)
                }
            } else {
                log.Fatal(err)
            }
        }
    }
    
    return ticketMap
}
