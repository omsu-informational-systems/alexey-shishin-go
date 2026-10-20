package events

type RegistrationStatus string

const (
	RegistrationStatusRegistered RegistrationStatus = "REGISTERED"
	RegistrationStatusRejected   RegistrationStatus = "REJECTED"
)

type RegistrationRequestPayload struct {
	SecretKey    string  `json:"secretKey"`
	AccountID    string  `json:"accountId"`
	AccountName  string  `json:"accountName"`
	GroupID      string  `json:"groupId"`
	StrategyID   string  `json:"strategyId"`
	StrategyName string  `json:"strategyName"`
	ServiceName  string  `json:"serviceName"`
	Description  *string `json:"description,omitempty"`
}

type RegistrationResultPayload struct {
	SecretKey   string             `json:"secretKey"`
	AccountID   string             `json:"accountId"`
	GroupID     string             `json:"groupId"`
	StrategyID  string             `json:"strategyId"`
	ServiceName string             `json:"serviceName"`
	Status      RegistrationStatus `json:"status"`
	Reason      string             `json:"reason,omitempty"`
}
