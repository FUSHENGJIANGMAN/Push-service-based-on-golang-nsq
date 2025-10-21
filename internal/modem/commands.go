package modem

const (
	CMD_RESET             = "AT&F\r"
	CMD_ECHO_OFF          = "ATE0\r"
	CMD_SAVE              = "AT&W\r"
	CMD_GET_IMSI          = "AT+CIMI\r"
	CMD_SMS_TEXT_MODE     = "AT+CMGF=1\r"
	CMD_SMS_PDU_MODE      = "AT+CMGF=0\r"
	CMD_LIST_SMS          = "AT+CMGL=\"ALL\"\r"
	CMD_SET_CHARSET       = "AT+CSCS=\"UCS2\"\r"
	CMD_GET_SMS_CENTER    = "AT+CSCA?\r"
	CMD_SET_SMS_STORAGE   = "AT+CPMS=\"MT\",\"MT\",\"MT\"\r"
	CMD_ENABLE_SMS_NOTIFY = "AT+CNMI=2,1\r"
	CMD_REBOOT            = "AT+CFUN=1,1\r"
	CMD_ENABLE_TTS        = "AT+CDTAM=1\r"
)
