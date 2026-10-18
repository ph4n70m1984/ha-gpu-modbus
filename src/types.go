package main

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/simonvetter/modbus"
)

type CommandConfig struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Address uint16 `json:"address"`
	Type    string `json:"type"`
	Value   uint16 `json:"value"`
}

type ControllerModel struct {
	ModelID      string           `json:"model_id"`
	Name         string           `json:"name"`
	Manufacturer string           `json:"manufacturer"`
	Registers    []RegisterConfig `json:"registers"`
	Commands     []CommandConfig  `json:"commands,omitempty"`
}

type RegisterConfig struct {
	Name        string  `json:"name"`
	ID          string  `json:"id"`
	Address     uint16  `json:"address"`
	Type        string  `json:"type"`
	Bitrate     int     `json:"bitrate"`
	Signed      bool    `json:"signed,omitempty"`
	Unit        string  `json:"unit"`
	DeviceClass string  `json:"device_class"`
	Scale       float64 `json:"scale"`
}

type UserDevice struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Address            string  `json:"address"`
	ModelID            string  `json:"model_id"`
	TimeoutSec         int     `json:"timeout_sec"`
	AnomalyWindow      int     `json:"anomaly_window,omitempty"`
	AnomalySensitivity float64 `json:"anomaly_sensitivity,omitempty"`
}

type HADiscoveryPayload struct {
	Name              string         `json:"name"`
	StateTopic        string         `json:"state_topic"`
	UnitOfMeasurement string         `json:"unit_of_measurement,omitempty"`
	DeviceClass       string         `json:"device_class,omitempty"`
	UniqueID          string         `json:"unique_id"`
	Device            map[string]any `json:"device"`
}

type HAButtonDiscovery struct {
	Name         string         `json:"name"`
	CommandTopic string         `json:"command_topic"`
	PayloadPress string         `json:"payload_press"`
	UniqueID     string         `json:"unique_id"`
	Device       map[string]any `json:"device"`
}

type MetricRole uint8

const (
	RoleNone MetricRole = iota
	RoleCurrentA
	RoleCurrentB
	RoleCurrentC
	RoleRPM
	RoleCoolantTemp
)

type PreparedRegister struct {
	Config       RegisterConfig
	ModbusType   modbus.RegType
	IsSigned     bool
	IsProcess    bool
	Role         MetricRole
	StateTopic   string
	AnomalyTopic string
	MetricKey    string
	LabelsScore  []prompb.Label
	LabelsYhat   []prompb.Label
	LabelsUpper  []prompb.Label
	LabelsLower  []prompb.Label
}
