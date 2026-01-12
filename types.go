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
	"context"
)

// Type 消息队列类型
type Type string

const (
	// TypeKafka Kafka 消息队列
	TypeKafka Type = "kafka"
	// TypeRabbitMQ RabbitMQ 消息队列
	TypeRabbitMQ Type = "rabbitmq"
)

// Client 统一的消息队列客户端
type Client struct {
	Producer Producer
	Consumer Consumer
	Type     Type
}

// Producer 统一的生产者接口
type Producer interface {
	// Send 发送消息（自动处理 trace）
	Send(ctx context.Context, topic string, body []byte, opts ...SendOption) error
	// SendString 发送字符串消息
	SendString(ctx context.Context, topic string, body string, opts ...SendOption) error
	// Close 关闭生产者
	Close() error
}

// Consumer 统一的消费者接口
type Consumer interface {
	// Subscribe 订阅主题并处理消息（自动处理 trace）
	// ctx 用于控制订阅的生命周期
	// topic 是主题名称
	// handler 是消息处理函数，ctx 已包含 trace context，body 是消息体
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
	// Close 关闭消费者
	Close() error
}

// MessageHandler 消息处理函数类型
// ctx 已包含从消息中提取的 trace context，可以直接使用
// body 是消息体
type MessageHandler func(ctx context.Context, body []byte) error

// SendOption 发送选项函数类型
type SendOption func(*SendOptions)

// SendOptions 发送选项配置
type SendOptions struct {
	Key     string
	Headers map[string]string
	// 队列特定选项通过扩展接口提供
}

// WithKey 设置消息键
func WithKey(key string) SendOption {
	return func(opts *SendOptions) {
		opts.Key = key
	}
}

// WithHeader 设置消息头
func WithHeader(key, value string) SendOption {
	return func(opts *SendOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		opts.Headers[key] = value
	}
}

// WithHeaders 批量设置消息头
func WithHeaders(headers map[string]string) SendOption {
	return func(opts *SendOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		for k, v := range headers {
			opts.Headers[k] = v
		}
	}
}
