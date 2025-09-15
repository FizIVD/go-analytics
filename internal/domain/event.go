package domain

// Event represents the incoming data from the generator.
type Event struct {
	DeviceID  string                 `json:"device_id"`
	ProfileID int64                  `json:"profile_id"`
	Action    string                 `json:"action"`
	Extras    map[string]interface{} `json:"extras"`
}

// EnrichedEvent is the event structure that is written to Kafka.
type EnrichedEvent struct {
	EventID   string                 `json:"event_id"`
	EventTime int64                  `json:"event_time"`
	DeviceID  string                 `json:"device_id"`
	ProfileID int64                  `json:"profile_id"`
	Action    string                 `json:"action"`
	Extras    map[string]interface{} `json:"extras"`
}
