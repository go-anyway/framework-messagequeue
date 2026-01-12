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

	"github.com/go-anyway/framework-log"

	"go.uber.org/zap"
)

// NewClient 根据配置创建统一的消息队列客户端
func NewClient(opts *Options) (*Client, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	var producer Producer
	var consumer Consumer
	var err error

	switch opts.Type {
	case TypeKafka:
		producer, err = newKafkaProducer(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka producer: %w", err)
		}
		consumer, err = newKafkaConsumer(opts)
		if err != nil {
			_ = producer.Close()
			return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
		}

	case TypeRabbitMQ:
		producer, err = newRabbitMQProducer(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create rabbitmq producer: %w", err)
		}
		consumer, err = newRabbitMQConsumer(opts)
		if err != nil {
			_ = producer.Close()
			return nil, fmt.Errorf("failed to create rabbitmq consumer: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported message queue type: %s", opts.Type)
	}

	log.Info("Message queue client created",
		zap.String("type", string(opts.Type)),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	return &Client{
		Producer: producer,
		Consumer: consumer,
		Type:     opts.Type,
	}, nil
}

// NewFromConfig 从配置创建客户端
func NewFromConfig(cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	opts, err := cfg.ToOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to options: %w", err)
	}

	client, err := NewClient(opts)
	if err != nil {
		return nil, err
	}

	log.Info("Message queue client created from config",
		zap.String("type", cfg.Type),
	)

	return client, nil
}

// HealthCheck 检查消息队列服务端连接是否正常
func (c *Client) HealthCheck() error {
	if c.Producer == nil && c.Consumer == nil {
		return fmt.Errorf("message queue client is not initialized")
	}

	var err error
	switch c.Type {
	case TypeKafka:
		err = c.healthCheckKafka()
	case TypeRabbitMQ:
		err = c.healthCheckRabbitMQ()
	default:
		err = fmt.Errorf("unsupported message queue type: %s", c.Type)
	}

	if err != nil {
		log.Error("Message queue health check failed",
			zap.String("type", string(c.Type)),
			zap.Error(err),
		)
	} else {
		log.Debug("Message queue health check passed",
			zap.String("type", string(c.Type)),
		)
	}

	return err
}

// healthCheckKafka 检查 Kafka 连接
func (c *Client) healthCheckKafka() error {
	// Kafka 的 producer 和 consumer 在创建时已经测试了连接
	// 如果创建成功，说明连接正常
	if c.Producer == nil {
		return fmt.Errorf("kafka producer is not initialized")
	}
	if c.Consumer == nil {
		return fmt.Errorf("kafka consumer is not initialized")
	}
	return nil
}

// healthCheckRabbitMQ 检查 RabbitMQ 连接
func (c *Client) healthCheckRabbitMQ() error {
	// RabbitMQ 的 producer 和 consumer 在创建时已经测试了连接
	// 如果创建成功，说明连接正常
	if c.Producer == nil {
		return fmt.Errorf("rabbitmq producer is not initialized")
	}
	if c.Consumer == nil {
		return fmt.Errorf("rabbitmq consumer is not initialized")
	}
	return nil
}

// Close 关闭客户端（关闭生产者和消费者）
func (c *Client) Close() error {
	var errs []error

	if c.Producer != nil {
		if err := c.Producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
		}
	}

	if c.Consumer != nil {
		if err := c.Consumer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close consumer: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing message queue client: %v", errs)
	}

	return nil
}
