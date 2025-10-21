package modem

type OperatorType int

const (
	OperatorUnknown OperatorType = iota
	OperatorChinaMobile
	OperatorChinaUnicom
	OperatorChinaTelecom
	OperatorChinaTietong
)

func (o OperatorType) String() string {
	switch o {
	case OperatorChinaMobile:
		return "中国移动"
	case OperatorChinaUnicom:
		return "中国联通"
	case OperatorChinaTelecom:
		return "中国电信"
	case OperatorChinaTietong:
		return "中国铁通"
	default:
		return "未知运营商"
	}
}

type SMS struct {
	Index     int    `json:"index"`
	Status    string `json:"status"`
	Sender    string `json:"sender"`    // 新增
	Timestamp string `json:"timestamp"` // 新增
	Text      string `json:"text"`
	RawHex    string `json:"-"` // ✅ 新增：存储原始 HEX 内容，用于后期统一解码
}

type ModemConfig struct {
	PortName           string
	BaudRate           int
	VoiceQueueSize     int
	SMSQueueSize       int
	ReadTimeout        int    // ms 或秒值(由调用方解释) - 占位
	CommandTimeout     int    // 同上
	DialTimeout        int    // 同上
	CallDuration       int    // 通话最长秒数
	SMSEncoding        string // 短信编码方式 ("UCS2"/"GBK"等)
	MaxConcurrentVoice int    // 最大并发语音（当前未使用，可预留）
	MaxConcurrentSMS   int    // 最大并发短信（当前未使用）
}
