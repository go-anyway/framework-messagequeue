// Copyright 2025 zampo.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// @contact  zampo3380@gmail.com

package messagequeue

import (
	"fmt"
	"time"
)

// Options 统一的消息队列配置选项
type Options struct {
	// 通用选项
	Type        Type
	EnableTrace bool

	// Kafka 选项
	KafkaBrokers         []string
	KafkaClientID        string
	KafkaVersion         string
	KafkaEnableSASL      bool
	KafkaSASLMechanism   string
	KafkaSASLUsername    string
	KafkaSASLPassword    string
	KafkaEnableTLS       bool
	KafkaMaxOpenRequests int
	KafkaDialTimeout     time.Duration
	KafkaReadTimeout     time.Duration
	KafkaWriteTimeout    time.Duration

	// RabbitMQ 选项
	RabbitMQURL            string
	RabbitMQHost           string
	RabbitMQPort           int
	RabbitMQUsername       string
	RabbitMQPassword       string
	RabbitMQVHost          string
	RabbitMQClientID       string
	RabbitMQEnableTLS      bool
	RabbitMQDialTimeout    time.Duration
	RabbitMQReadTimeout    time.Duration
	RabbitMQWriteTimeout   time.Duration
	RabbitMQPrefetchCount  int
	RabbitMQPrefetchSize   int
	RabbitMQReconnectDelay time.Duration
}

// Validate 验证配置选项
func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("options cannot be nil")
	}

	switch o.Type {
	case TypeKafka:
		if len(o.KafkaBrokers) == 0 {
			return fmt.Errorf("kafka brokers cannot be empty")
		}
	case TypeRabbitMQ:
		if o.RabbitMQURL == "" && o.RabbitMQHost == "" {
			return fmt.Errorf("rabbitmq url or host is required")
		}
	default:
		return fmt.Errorf("unsupported message queue type: %s", o.Type)
	}

	return nil
}

// Config 配置结构体（用于从配置文件创建）
// 对应 message_queue.yaml 文件结构
type Config struct {
	Type     string          `yaml:"type" env:"MESSAGE_QUEUE_TYPE" default:"none"`
	Kafka    *KafkaConfig    `yaml:"kafka,omitempty"`
	RabbitMQ *RabbitMQConfig `yaml:"rabbitmq,omitempty"`
}

// KafkaConfig Kafka 配置
type KafkaConfig struct {
	Enabled         bool     `yaml:"enabled" env:"KAFKA_ENABLED" required:"true"`
	Brokers         []string `yaml:"brokers" env:"KAFKA_BROKERS" required:"true"`
	ClientID        string   `yaml:"client_id" env:"KAFKA_CLIENT_ID" default:"ai-api-market"`
	Version         string   `yaml:"version" env:"KAFKA_VERSION" default:"2.8.0"`
	EnableSASL      bool     `yaml:"enable_sasl" env:"KAFKA_ENABLE_SASL" default:"false"`
	SASLMechanism   string   `yaml:"sasl_mechanism" env:"KAFKA_SASL_MECHANISM" default:"PLAIN"`
	SASLUsername    string   `yaml:"sasl_username" env:"KAFKA_SASL_USERNAME"`
	SASLPassword    string   `yaml:"sasl_password" env:"KAFKA_SASL_PASSWORD"`
	EnableTLS       bool     `yaml:"enable_tls" env:"KAFKA_ENABLE_TLS" default:"false"`
	MaxOpenRequests int      `yaml:"max_open_requests" env:"KAFKA_MAX_OPEN_REQUESTS" default:"10"`
	DialTimeout     string   `yaml:"dial_timeout" env:"KAFKA_DIAL_TIMEOUT" default:"30s"`
	ReadTimeout     string   `yaml:"read_timeout" env:"KAFKA_READ_TIMEOUT" default:"30s"`
	WriteTimeout    string   `yaml:"write_timeout" env:"KAFKA_WRITE_TIMEOUT" default:"30s"`
	EnableTrace     bool     `yaml:"enable_trace" env:"KAFKA_ENABLE_TRACE" default:"true"`
}

// RabbitMQConfig RabbitMQ 配置
type RabbitMQConfig struct {
	Enabled        bool   `yaml:"enabled" env:"RABBITMQ_ENABLED" required:"true"`
	URL            string `yaml:"url" env:"RABBITMQ_URL"`
	Host           string `yaml:"host" env:"RABBITMQ_HOST" default:"localhost"`
	Port           int    `yaml:"port" env:"RABBITMQ_PORT" default:"5672"`
	Username       string `yaml:"username" env:"RABBITMQ_USERNAME" default:"guest"`
	Password       string `yaml:"password" env:"RABBITMQ_PASSWORD" default:"guest"`
	VHost          string `yaml:"vhost" env:"RABBITMQ_VHOST" default:"/"`
	ClientID       string `yaml:"client_id" env:"RABBITMQ_CLIENT_ID" default:"ai-api-market"`
	EnableTLS      bool   `yaml:"enable_tls" env:"RABBITMQ_ENABLE_TLS" default:"false"`
	DialTimeout    string `yaml:"dial_timeout" env:"RABBITMQ_DIAL_TIMEOUT" default:"30s"`
	ReadTimeout    string `yaml:"read_timeout" env:"RABBITMQ_READ_TIMEOUT" default:"30s"`
	WriteTimeout   string `yaml:"write_timeout" env:"RABBITMQ_WRITE_TIMEOUT" default:"30s"`
	PrefetchCount  int    `yaml:"prefetch_count" env:"RABBITMQ_PREFETCH_COUNT" default:"10"`
	PrefetchSize   int    `yaml:"prefetch_size" env:"RABBITMQ_PREFETCH_SIZE" default:"0"`
	ReconnectDelay string `yaml:"reconnect_delay" env:"RABBITMQ_RECONNECT_DELAY" default:"5s"`
	EnableTrace    bool   `yaml:"enable_trace" env:"RABBITMQ_ENABLE_TRACE" default:"true"`
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// 如果类型为 none 或空，不需要验证
	if c.Type == "" || c.Type == "none" {
		return nil
	}

	switch Type(c.Type) {
	case TypeKafka:
		if c.Kafka == nil {
			return fmt.Errorf("kafka config is required when type is kafka")
		}
		if !c.Kafka.Enabled {
			return fmt.Errorf("kafka config is not enabled")
		}
		if len(c.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka brokers cannot be empty")
		}
		for i, broker := range c.Kafka.Brokers {
			if broker == "" {
				return fmt.Errorf("kafka brokers[%d] cannot be empty", i)
			}
		}
		if c.Kafka.ClientID == "" {
			return fmt.Errorf("kafka client_id is required")
		}
		if c.Kafka.EnableSASL {
			if c.Kafka.SASLUsername == "" {
				return fmt.Errorf("kafka sasl_username is required when enable_sasl is true")
			}
			if c.Kafka.SASLPassword == "" {
				return fmt.Errorf("kafka sasl_password is required when enable_sasl is true")
			}
		}

	case TypeRabbitMQ:
		if c.RabbitMQ == nil {
			return fmt.Errorf("rabbitmq config is required when type is rabbitmq")
		}
		if !c.RabbitMQ.Enabled {
			return fmt.Errorf("rabbitmq config is not enabled")
		}
		if c.RabbitMQ.URL == "" {
			if c.RabbitMQ.Host == "" {
				return fmt.Errorf("rabbitmq url or host is required")
			}
			if c.RabbitMQ.Port < 1 || c.RabbitMQ.Port > 65535 {
				return fmt.Errorf("rabbitmq port must be between 1 and 65535, got %d", c.RabbitMQ.Port)
			}
		}
		if c.RabbitMQ.Username == "" {
			return fmt.Errorf("rabbitmq username is required")
		}
		if c.RabbitMQ.Password == "" {
			return fmt.Errorf("rabbitmq password is required")
		}

	default:
		return fmt.Errorf("unsupported message queue type: %s", c.Type)
	}

	return nil
}

// ToOptions 将 Config 转换为 Options
func (c *Config) ToOptions() (*Options, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts := &Options{
		Type:        Type(c.Type),
		EnableTrace: true, // 默认启用
	}

	switch Type(c.Type) {
	case TypeKafka:
		if c.Kafka == nil || !c.Kafka.Enabled {
			return nil, fmt.Errorf("kafka config is not enabled")
		}
		opts.KafkaBrokers = c.Kafka.Brokers
		opts.KafkaClientID = c.Kafka.ClientID
		opts.KafkaVersion = c.Kafka.Version
		opts.KafkaEnableSASL = c.Kafka.EnableSASL
		opts.KafkaSASLMechanism = c.Kafka.SASLMechanism
		opts.KafkaSASLUsername = c.Kafka.SASLUsername
		opts.KafkaSASLPassword = c.Kafka.SASLPassword
		opts.KafkaEnableTLS = c.Kafka.EnableTLS
		opts.KafkaMaxOpenRequests = c.Kafka.MaxOpenRequests
		opts.EnableTrace = c.Kafka.EnableTrace

		// 解析时间字段
		if c.Kafka.DialTimeout != "" {
			if dur, err := time.ParseDuration(c.Kafka.DialTimeout); err == nil {
				opts.KafkaDialTimeout = dur
			}
		}
		if c.Kafka.ReadTimeout != "" {
			if dur, err := time.ParseDuration(c.Kafka.ReadTimeout); err == nil {
				opts.KafkaReadTimeout = dur
			}
		}
		if c.Kafka.WriteTimeout != "" {
			if dur, err := time.ParseDuration(c.Kafka.WriteTimeout); err == nil {
				opts.KafkaWriteTimeout = dur
			}
		}

	case TypeRabbitMQ:
		if c.RabbitMQ == nil || !c.RabbitMQ.Enabled {
			return nil, fmt.Errorf("rabbitmq config is not enabled")
		}
		rabbitMQCfg := c.RabbitMQ
		opts.RabbitMQURL = rabbitMQCfg.URL
		opts.RabbitMQHost = rabbitMQCfg.Host
		opts.RabbitMQPort = rabbitMQCfg.Port
		opts.RabbitMQUsername = rabbitMQCfg.Username
		opts.RabbitMQPassword = rabbitMQCfg.Password
		opts.RabbitMQVHost = rabbitMQCfg.VHost
		opts.RabbitMQClientID = rabbitMQCfg.ClientID
		opts.RabbitMQEnableTLS = rabbitMQCfg.EnableTLS
		opts.RabbitMQPrefetchCount = rabbitMQCfg.PrefetchCount
		opts.RabbitMQPrefetchSize = rabbitMQCfg.PrefetchSize
		opts.EnableTrace = rabbitMQCfg.EnableTrace

		// 解析时间字段
		if rabbitMQCfg.DialTimeout != "" {
			if dur, err := time.ParseDuration(rabbitMQCfg.DialTimeout); err == nil {
				opts.RabbitMQDialTimeout = dur
			}
		}
		if rabbitMQCfg.ReadTimeout != "" {
			if dur, err := time.ParseDuration(rabbitMQCfg.ReadTimeout); err == nil {
				opts.RabbitMQReadTimeout = dur
			}
		}
		if rabbitMQCfg.WriteTimeout != "" {
			if dur, err := time.ParseDuration(rabbitMQCfg.WriteTimeout); err == nil {
				opts.RabbitMQWriteTimeout = dur
			}
		}
		if rabbitMQCfg.ReconnectDelay != "" {
			if dur, err := time.ParseDuration(rabbitMQCfg.ReconnectDelay); err == nil {
				opts.RabbitMQReconnectDelay = dur
			}
		}

	default:
		return nil, fmt.Errorf("unsupported message queue type: %s", c.Type)
	}

	// 设置默认值
	if opts.KafkaDialTimeout == 0 {
		opts.KafkaDialTimeout = 30 * time.Second
	}
	if opts.KafkaReadTimeout == 0 {
		opts.KafkaReadTimeout = 30 * time.Second
	}
	if opts.KafkaWriteTimeout == 0 {
		opts.KafkaWriteTimeout = 30 * time.Second
	}
	if opts.RabbitMQDialTimeout == 0 {
		opts.RabbitMQDialTimeout = 30 * time.Second
	}
	if opts.RabbitMQReadTimeout == 0 {
		opts.RabbitMQReadTimeout = 30 * time.Second
	}
	if opts.RabbitMQWriteTimeout == 0 {
		opts.RabbitMQWriteTimeout = 30 * time.Second
	}
	if opts.RabbitMQReconnectDelay == 0 {
		opts.RabbitMQReconnectDelay = 5 * time.Second
	}

	return opts, nil
}
