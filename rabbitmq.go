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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// rabbitMQProducer RabbitMQ 生产者实现
type rabbitMQProducer struct {
	conn        *amqp091.Connection
	channel     *amqp091.Channel
	opts        *Options
	mu          sync.RWMutex
	enableTrace bool
}

// rabbitMQConsumer RabbitMQ 消费者实现
type rabbitMQConsumer struct {
	conn        *amqp091.Connection
	channel     *amqp091.Channel
	opts        *Options
	mu          sync.RWMutex
	enableTrace bool
}

// newRabbitMQProducer 创建 RabbitMQ 生产者
func newRabbitMQProducer(opts *Options) (Producer, error) {
	log.Info("Creating RabbitMQ producer",
		zap.String("host", opts.RabbitMQHost),
		zap.Int("port", opts.RabbitMQPort),
		zap.String("vhost", opts.RabbitMQVHost),
		zap.String("client_id", opts.RabbitMQClientID),
		zap.Bool("enable_tls", opts.RabbitMQEnableTLS),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	// 构建连接 URL
	var url string
	if opts.RabbitMQURL != "" {
		url = opts.RabbitMQURL
		log.Debug("Using RabbitMQ URL from config")
	} else {
		if opts.RabbitMQHost == "" {
			return nil, fmt.Errorf("rabbitmq host cannot be empty")
		}
		if opts.RabbitMQPort == 0 {
			opts.RabbitMQPort = 5672
		}
		if opts.RabbitMQVHost == "" {
			opts.RabbitMQVHost = "/"
		}

		scheme := "amqp"
		if opts.RabbitMQEnableTLS {
			scheme = "amqps"
		}

		if opts.RabbitMQUsername != "" && opts.RabbitMQPassword != "" {
			url = fmt.Sprintf("%s://%s:%s@%s:%d%s", scheme, opts.RabbitMQUsername, opts.RabbitMQPassword, opts.RabbitMQHost, opts.RabbitMQPort, opts.RabbitMQVHost)
		} else {
			url = fmt.Sprintf("%s://%s:%d%s", scheme, opts.RabbitMQHost, opts.RabbitMQPort, opts.RabbitMQVHost)
		}
	}

	// 配置连接参数
	config := amqp091.Config{
		Properties: amqp091.Table{
			"connection_name": opts.RabbitMQClientID,
		},
	}

	// 创建连接
	conn, err := amqp091.DialConfig(url, config)
	if err != nil {
		log.Error("Failed to connect to RabbitMQ",
			zap.String("host", opts.RabbitMQHost),
			zap.Int("port", opts.RabbitMQPort),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	// 创建通道
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		log.Error("Failed to open RabbitMQ channel",
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	log.Info("RabbitMQ producer created successfully",
		zap.String("host", opts.RabbitMQHost),
		zap.Int("port", opts.RabbitMQPort),
		zap.String("vhost", opts.RabbitMQVHost),
		zap.String("client_id", opts.RabbitMQClientID),
	)

	return &rabbitMQProducer{
		conn:        conn,
		channel:     channel,
		opts:        opts,
		enableTrace: opts.EnableTrace,
	}, nil
}

// Send 发送消息（自动处理 trace）
// topic 作为 exchange，key 作为 routing key
func (p *rabbitMQProducer) Send(ctx context.Context, topic string, body []byte, opts ...SendOption) error {
	// 解析选项
	sendOpts := &SendOptions{}
	for _, opt := range opts {
		opt(sendOpts)
	}

	// topic 作为 exchange，key 作为 routing key
	exchange := topic
	routingKey := sendOpts.Key
	if routingKey == "" {
		routingKey = topic // 如果没有 key，使用 topic 作为 routing key
	}

	// 获取通道（检查连接状态）
	p.mu.RLock()
	channel := p.channel
	connected := p.isConnected()
	p.mu.RUnlock()

	if !connected {
		p.mu.Lock()
		if !p.isConnected() {
			if err := p.reconnect(); err != nil {
				p.mu.Unlock()
				return fmt.Errorf("rabbitmq client is not connected and reconnect failed: %w", err)
			}
		}
		channel = p.channel
		p.mu.Unlock()
	}

	// 准备消息头
	headers := make(map[string]interface{})
	if len(sendOpts.Headers) > 0 {
		for k, v := range sendOpts.Headers {
			headers[k] = v
		}
	}

	// 注入 trace context 并创建 span
	if p.enableTrace {
		// 先创建 span（这样 context 包含 span）
		var span trace.Span
		ctx, span = createSpan(ctx, "rabbitmq.send", "rabbitmq",
			attribute.String("messaging.destination", exchange),
			attribute.String("messaging.rabbitmq.routing_key", routingKey),
			attribute.Int("messaging.message_size", len(body)),
		)
		// 从包含 span 的 context 中注入 trace context
		injectTraceContext(ctx, headers, "rabbitmq")
		defer func() {
			finishSpan(span, nil)
		}()

		msg := amqp091.Publishing{
			ContentType:  "application/octet-stream",
			Body:         body,
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Headers:      amqp091.Table(headers),
		}

		startTime := time.Now()
		err := channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
		duration := time.Since(startTime)

		if span != nil {
			span.SetAttributes(
				attribute.Float64("messaging.duration_ms", float64(duration.Milliseconds())),
			)
		}

		if err != nil {
			log.FromContext(ctx).Error("RabbitMQ send message failed",
				zap.String("exchange", exchange),
				zap.String("routing_key", routingKey),
				zap.Int("message_size", len(body)),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
			// 检查是否是连接错误，尝试重连
			if p.isConnectionError(err) {
				log.FromContext(ctx).Warn("RabbitMQ connection error detected, attempting reconnect",
					zap.String("exchange", exchange),
					zap.String("routing_key", routingKey),
				)
				p.mu.Lock()
				if reconnectErr := p.reconnect(); reconnectErr == nil {
					channel = p.channel
					p.mu.Unlock()
					log.FromContext(ctx).Info("RabbitMQ reconnected successfully, retrying send",
						zap.String("exchange", exchange),
						zap.String("routing_key", routingKey),
					)
					// 重连成功后再次尝试
					err = channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
					if err == nil {
						log.FromContext(ctx).Debug("RabbitMQ send message succeeded after reconnect",
							zap.String("exchange", exchange),
							zap.String("routing_key", routingKey),
							zap.Int("message_size", len(body)),
						)
					}
				} else {
					p.mu.Unlock()
					log.FromContext(ctx).Error("RabbitMQ reconnect failed",
						zap.String("exchange", exchange),
						zap.String("routing_key", routingKey),
						zap.Error(reconnectErr),
					)
				}
			}
		} else {
			log.FromContext(ctx).Debug("RabbitMQ send message succeeded",
				zap.String("exchange", exchange),
				zap.String("routing_key", routingKey),
				zap.Int("message_size", len(body)),
				zap.Duration("duration", duration),
			)
		}

		return err
	}

	msg := amqp091.Publishing{
		ContentType:  "application/octet-stream",
		Body:         body,
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now(),
		Headers:      amqp091.Table(headers),
	}

	startTime := time.Now()
	err := channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
	duration := time.Since(startTime)

	if err != nil {
		log.FromContext(ctx).Error("RabbitMQ send message failed",
			zap.String("exchange", exchange),
			zap.String("routing_key", routingKey),
			zap.Int("message_size", len(body)),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
		if p.isConnectionError(err) {
			log.FromContext(ctx).Warn("RabbitMQ connection error detected, attempting reconnect",
				zap.String("exchange", exchange),
				zap.String("routing_key", routingKey),
			)
			p.mu.Lock()
			if reconnectErr := p.reconnect(); reconnectErr == nil {
				channel = p.channel
				p.mu.Unlock()
				log.FromContext(ctx).Info("RabbitMQ reconnected successfully, retrying send",
					zap.String("exchange", exchange),
					zap.String("routing_key", routingKey),
				)
				err = channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
				if err == nil {
					log.FromContext(ctx).Debug("RabbitMQ send message succeeded after reconnect",
						zap.String("exchange", exchange),
						zap.String("routing_key", routingKey),
						zap.Int("message_size", len(body)),
					)
				}
			} else {
				p.mu.Unlock()
				log.FromContext(ctx).Error("RabbitMQ reconnect failed",
					zap.String("exchange", exchange),
					zap.String("routing_key", routingKey),
					zap.Error(reconnectErr),
				)
			}
		}
	} else {
		log.FromContext(ctx).Debug("RabbitMQ send message succeeded",
			zap.String("exchange", exchange),
			zap.String("routing_key", routingKey),
			zap.Int("message_size", len(body)),
			zap.Duration("duration", duration),
		)
	}

	return err
}

// SendString 发送字符串消息
func (p *rabbitMQProducer) SendString(ctx context.Context, topic string, body string, opts ...SendOption) error {
	return p.Send(ctx, topic, []byte(body), opts...)
}

// Close 关闭生产者
func (p *rabbitMQProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			errs = append(errs, err)
		}
		p.channel = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing rabbitmq producer: %v", errs)
	}
	return nil
}

// isConnected 检查连接是否有效
func (p *rabbitMQProducer) isConnected() bool {
	if p.conn == nil || p.channel == nil {
		return false
	}
	return !p.conn.IsClosed()
}

// isConnectionError 检查是否是连接错误
func (p *rabbitMQProducer) isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "channel/connection is not open") ||
		strings.Contains(errStr, "channel is closed") ||
		strings.Contains(errStr, "connection is closed") ||
		strings.Contains(errStr, "Exception (504)")
}

// reconnect 重新连接
func (p *rabbitMQProducer) reconnect() error {
	if p.isConnected() {
		return nil
	}

	// 关闭旧的连接和通道
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}

	// 构建连接 URL
	var url string
	if p.opts.RabbitMQURL != "" {
		url = p.opts.RabbitMQURL
	} else {
		if p.opts.RabbitMQHost == "" {
			return fmt.Errorf("rabbitmq host cannot be empty")
		}
		if p.opts.RabbitMQPort == 0 {
			p.opts.RabbitMQPort = 5672
		}
		if p.opts.RabbitMQVHost == "" {
			p.opts.RabbitMQVHost = "/"
		}

		scheme := "amqp"
		if p.opts.RabbitMQEnableTLS {
			scheme = "amqps"
		}

		if p.opts.RabbitMQUsername != "" && p.opts.RabbitMQPassword != "" {
			url = fmt.Sprintf("%s://%s:%s@%s:%d%s", scheme, p.opts.RabbitMQUsername, p.opts.RabbitMQPassword, p.opts.RabbitMQHost, p.opts.RabbitMQPort, p.opts.RabbitMQVHost)
		} else {
			url = fmt.Sprintf("%s://%s:%d%s", scheme, p.opts.RabbitMQHost, p.opts.RabbitMQPort, p.opts.RabbitMQVHost)
		}
	}

	// 配置连接参数
	config := amqp091.Config{
		Properties: amqp091.Table{
			"connection_name": p.opts.RabbitMQClientID,
		},
	}

	// 创建新连接
	conn, err := amqp091.DialConfig(url, config)
	if err != nil {
		return fmt.Errorf("failed to reconnect to rabbitmq: %w", err)
	}

	// 创建新通道
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel after reconnect: %w", err)
	}

	// 设置 QoS
	if p.opts.RabbitMQPrefetchCount > 0 {
		if err := channel.Qos(p.opts.RabbitMQPrefetchCount, p.opts.RabbitMQPrefetchSize, false); err != nil {
			_ = channel.Close()
			_ = conn.Close()
			return fmt.Errorf("failed to set QoS after reconnect: %w", err)
		}
	}

	p.conn = conn
	p.channel = channel
	return nil
}

// newRabbitMQConsumer 创建 RabbitMQ 消费者
func newRabbitMQConsumer(opts *Options) (Consumer, error) {
	log.Info("Creating RabbitMQ consumer",
		zap.String("host", opts.RabbitMQHost),
		zap.Int("port", opts.RabbitMQPort),
		zap.String("vhost", opts.RabbitMQVHost),
		zap.String("client_id", opts.RabbitMQClientID),
		zap.Bool("enable_tls", opts.RabbitMQEnableTLS),
		zap.Int("prefetch_count", opts.RabbitMQPrefetchCount),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	// 构建连接 URL
	var url string
	if opts.RabbitMQURL != "" {
		url = opts.RabbitMQURL
		log.Debug("Using RabbitMQ URL from config")
	} else {
		if opts.RabbitMQHost == "" {
			return nil, fmt.Errorf("rabbitmq host cannot be empty")
		}
		if opts.RabbitMQPort == 0 {
			opts.RabbitMQPort = 5672
		}
		if opts.RabbitMQVHost == "" {
			opts.RabbitMQVHost = "/"
		}

		scheme := "amqp"
		if opts.RabbitMQEnableTLS {
			scheme = "amqps"
		}

		if opts.RabbitMQUsername != "" && opts.RabbitMQPassword != "" {
			url = fmt.Sprintf("%s://%s:%s@%s:%d%s", scheme, opts.RabbitMQUsername, opts.RabbitMQPassword, opts.RabbitMQHost, opts.RabbitMQPort, opts.RabbitMQVHost)
		} else {
			url = fmt.Sprintf("%s://%s:%d%s", scheme, opts.RabbitMQHost, opts.RabbitMQPort, opts.RabbitMQVHost)
		}
	}

	// 配置连接参数
	config := amqp091.Config{
		Properties: amqp091.Table{
			"connection_name": opts.RabbitMQClientID,
		},
	}

	// 创建连接
	conn, err := amqp091.DialConfig(url, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	// 创建通道
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// 设置 QoS
	if opts.RabbitMQPrefetchCount > 0 {
		if err := channel.Qos(opts.RabbitMQPrefetchCount, opts.RabbitMQPrefetchSize, false); err != nil {
			_ = channel.Close()
			_ = conn.Close()
			log.Error("Failed to set RabbitMQ QoS",
				zap.Int("prefetch_count", opts.RabbitMQPrefetchCount),
				zap.Int("prefetch_size", opts.RabbitMQPrefetchSize),
				zap.Error(err),
			)
			return nil, fmt.Errorf("failed to set QoS: %w", err)
		}
		log.Debug("RabbitMQ QoS set",
			zap.Int("prefetch_count", opts.RabbitMQPrefetchCount),
			zap.Int("prefetch_size", opts.RabbitMQPrefetchSize),
		)
	}

	log.Info("RabbitMQ consumer created successfully",
		zap.String("host", opts.RabbitMQHost),
		zap.Int("port", opts.RabbitMQPort),
		zap.String("vhost", opts.RabbitMQVHost),
		zap.String("client_id", opts.RabbitMQClientID),
	)

	return &rabbitMQConsumer{
		conn:        conn,
		channel:     channel,
		opts:        opts,
		enableTrace: opts.EnableTrace,
	}, nil
}

// Subscribe 订阅主题并处理消息（自动处理 trace）
// topic 作为 exchange 和队列名称
func (c *rabbitMQConsumer) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	log.FromContext(ctx).Info("Subscribing to RabbitMQ topic",
		zap.String("topic", topic),
	)

	exchange := topic
	queueName := topic

	// 先尝试使用 passive 模式检查 exchange 是否存在
	// 如果已存在，就不需要再创建
	err := c.channel.ExchangeDeclarePassive(
		exchange, // exchange 名称
		"topic",  // 尝试 topic 类型（最常见）
		true,     // 持久化
		false,    // 自动删除
		false,    // 内部
		false,    // 无等待
		nil,      // 参数
	)

	// 如果 passive 模式失败（exchange 不存在），则创建新的 exchange
	if err != nil {
		// 尝试创建 topic 类型的 exchange（与现有 exchange 类型匹配）
		err = c.channel.ExchangeDeclare(
			exchange, // exchange 名称
			"topic",  // exchange 类型（使用 topic 以匹配现有 exchange）
			true,     // 持久化
			false,    // 自动删除
			false,    // 内部
			false,    // 无等待
			nil,      // 参数
		)
		if err != nil {
			return fmt.Errorf("failed to declare exchange: %w (note: if exchange already exists with different type, delete it first)", err)
		}
	}

	// 声明队列（持久化）
	_, err = c.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 将队列绑定到 exchange
	// 对于 topic 类型的 exchange，使用 "#" 通配符匹配所有 routing key
	// 这样无论生产者使用什么 routing key（包括 taskID），消息都能路由到队列
	bindingKey := "#" // 使用通配符匹配所有 routing key
	err = c.channel.QueueBind(
		queueName,  // 队列名称
		bindingKey, // routing key（使用通配符匹配所有）
		exchange,   // exchange 名称
		false,      // 无等待
		nil,        // 参数
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	// 消费消息（手动确认模式）
	deliveries, err := c.channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.FromContext(ctx).Error("Failed to start RabbitMQ consuming",
			zap.String("queue", queueName),
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.FromContext(ctx).Info("RabbitMQ consumer subscribed successfully",
		zap.String("topic", topic),
		zap.String("exchange", exchange),
		zap.String("queue", queueName),
	)

	// 处理消息
	go func() {
		for delivery := range deliveries {
			// 提取 trace context 并创建 span
			msgCtx := extractTraceContext(delivery, "rabbitmq")
			var span trace.Span
			if c.enableTrace {
				// 如果 Exchange 为空，使用队列名称作为 destination
				destination := delivery.Exchange
				if destination == "" {
					destination = queueName
				}
				msgCtx, span = createSpan(msgCtx, "rabbitmq.consume", "rabbitmq",
					attribute.String("messaging.destination", destination),
					attribute.String("messaging.rabbitmq.routing_key", delivery.RoutingKey),
					attribute.Int64("messaging.rabbitmq.delivery_tag", int64(delivery.DeliveryTag)),
					attribute.Int("messaging.message_size", len(delivery.Body)),
				)
			}

			// 处理消息（msgCtx 已包含从消息中提取的 trace context 和创建的 span）
			startTime := time.Now()
			err := handler(msgCtx, delivery.Body)
			duration := time.Since(startTime)

			if err != nil {
				log.FromContext(msgCtx).Error("RabbitMQ message handler failed",
					zap.String("exchange", delivery.Exchange),
					zap.String("routing_key", delivery.RoutingKey),
					zap.Uint64("delivery_tag", delivery.DeliveryTag),
					zap.Int("message_size", len(delivery.Body)),
					zap.Duration("duration", duration),
					zap.Error(err),
				)
				// 拒绝消息（不重新入队）
				_ = c.channel.Nack(delivery.DeliveryTag, false, false)
				if c.enableTrace && span != nil {
					finishSpan(span, err)
				}
			} else {
				log.FromContext(msgCtx).Debug("RabbitMQ message processed",
					zap.String("exchange", delivery.Exchange),
					zap.String("routing_key", delivery.RoutingKey),
					zap.Uint64("delivery_tag", delivery.DeliveryTag),
					zap.Int("message_size", len(delivery.Body)),
					zap.Duration("duration", duration),
				)
				// 确认消息
				_ = c.channel.Ack(delivery.DeliveryTag, false)
				if c.enableTrace && span != nil {
					finishSpan(span, nil)
				}
			}
		}
	}()

	return nil
}

// Close 关闭消费者
func (c *rabbitMQConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			errs = append(errs, err)
		}
		c.channel = nil
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		c.conn = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing rabbitmq consumer: %v", errs)
	}
	return nil
}
