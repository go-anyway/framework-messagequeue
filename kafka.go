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
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// kafkaProducer Kafka 生产者实现
type kafkaProducer struct {
	producer    sarama.SyncProducer
	enableTrace bool
}

// kafkaConsumer Kafka 消费者实现
type kafkaConsumer struct {
	consumer    sarama.Consumer
	client      sarama.Client
	enableTrace bool
}

// newKafkaProducer 创建 Kafka 生产者
func newKafkaProducer(opts *Options) (Producer, error) {
	if len(opts.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}

	log.Info("Creating Kafka producer",
		zap.Strings("brokers", opts.KafkaBrokers),
		zap.String("client_id", opts.KafkaClientID),
		zap.String("version", opts.KafkaVersion),
		zap.Bool("enable_sasl", opts.KafkaEnableSASL),
		zap.Bool("enable_tls", opts.KafkaEnableTLS),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	// 解析 Kafka 版本
	var version sarama.KafkaVersion
	if opts.KafkaVersion != "" {
		var err error
		version, err = sarama.ParseKafkaVersion(opts.KafkaVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid kafka version %s: %w", opts.KafkaVersion, err)
		}
	} else {
		version = sarama.V2_8_0_0
	}

	// 配置 Kafka 客户端
	config := sarama.NewConfig()
	config.Version = version
	config.ClientID = opts.KafkaClientID
	if config.ClientID == "" {
		config.ClientID = "ai-api-market"
	}

	// 设置超时
	if opts.KafkaDialTimeout > 0 {
		config.Net.DialTimeout = opts.KafkaDialTimeout
	}
	if opts.KafkaReadTimeout > 0 {
		config.Net.ReadTimeout = opts.KafkaReadTimeout
	}
	if opts.KafkaWriteTimeout > 0 {
		config.Net.WriteTimeout = opts.KafkaWriteTimeout
	}

	// 设置最大打开请求数
	if opts.KafkaMaxOpenRequests > 0 {
		config.Net.MaxOpenRequests = opts.KafkaMaxOpenRequests
	}

	// 配置 SASL 认证
	if opts.KafkaEnableSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLMechanism(opts.KafkaSASLMechanism)
		config.Net.SASL.User = opts.KafkaSASLUsername
		config.Net.SASL.Password = opts.KafkaSASLPassword

		switch opts.KafkaSASLMechanism {
		case "SCRAM-SHA-256", "SCRAM-SHA-512":
			config.Net.SASL.Handshake = true
		default:
			config.Net.SASL.Handshake = false
		}
	}

	// 配置 TLS
	if opts.KafkaEnableTLS {
		config.Net.TLS.Enable = true
	}

	// 生产者配置
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = 10 * time.Second

	// 创建 Kafka 客户端
	client, err := sarama.NewClient(opts.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// 测试连接
	brokers := client.Brokers()
	if len(brokers) == 0 {
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to any kafka broker")
	}

	log.Debug("Kafka client connected",
		zap.Int("broker_count", len(brokers)),
	)

	// 创建生产者
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	log.Info("Kafka producer created successfully",
		zap.Strings("brokers", opts.KafkaBrokers),
		zap.String("client_id", opts.KafkaClientID),
	)

	return &kafkaProducer{
		producer:    producer,
		enableTrace: opts.EnableTrace,
	}, nil
}

// Send 发送消息（自动处理 trace）
func (p *kafkaProducer) Send(ctx context.Context, topic string, body []byte, opts ...SendOption) error {
	// 解析选项
	sendOpts := &SendOptions{}
	for _, opt := range opts {
		opt(sendOpts)
	}

	// 创建消息
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(body),
	}

	if sendOpts.Key != "" {
		msg.Key = sarama.ByteEncoder(sendOpts.Key)
	}

	// 添加消息头
	if len(sendOpts.Headers) > 0 {
		msg.Headers = make([]sarama.RecordHeader, 0, len(sendOpts.Headers))
		for k, v := range sendOpts.Headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	// 注入 trace context
	if p.enableTrace {
		// 先创建 span（这样 context 包含 span）
		var span trace.Span
		ctx, span = createSpan(ctx, "kafka.send", "kafka",
			attribute.String("messaging.destination", topic),
			attribute.Int("messaging.message_size", len(body)),
		)
		// 从包含 span 的 context 中注入 trace context
		injectTraceContext(ctx, msg, "kafka")
		defer func() {
			finishSpan(span, nil)
		}()

		startTime := time.Now()
		partition, offset, err := p.producer.SendMessage(msg)
		duration := time.Since(startTime)

		if span != nil {
			span.SetAttributes(
				attribute.Int64("messaging.kafka.partition", int64(partition)),
				attribute.Int64("messaging.kafka.offset", offset),
				attribute.Float64("messaging.duration_ms", float64(duration.Milliseconds())),
			)
		}

		if err != nil {
			log.FromContext(ctx).Error("Kafka send message failed",
				zap.String("topic", topic),
				zap.String("key", sendOpts.Key),
				zap.Int("message_size", len(body)),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			log.FromContext(ctx).Debug("Kafka send message succeeded",
				zap.String("topic", topic),
				zap.String("key", sendOpts.Key),
				zap.Int("partition", int(partition)),
				zap.Int64("offset", offset),
				zap.Int("message_size", len(body)),
				zap.Duration("duration", duration),
			)
		}

		return err
	}

	startTime := time.Now()
	partition, offset, err := p.producer.SendMessage(msg)
	duration := time.Since(startTime)

	if err != nil {
		log.FromContext(ctx).Error("Kafka send message failed",
			zap.String("topic", topic),
			zap.String("key", sendOpts.Key),
			zap.Int("message_size", len(body)),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("Kafka send message succeeded",
			zap.String("topic", topic),
			zap.String("key", sendOpts.Key),
			zap.Int("partition", int(partition)),
			zap.Int64("offset", offset),
			zap.Int("message_size", len(body)),
			zap.Duration("duration", duration),
		)
	}

	return err
}

// SendString 发送字符串消息
func (p *kafkaProducer) SendString(ctx context.Context, topic string, body string, opts ...SendOption) error {
	return p.Send(ctx, topic, []byte(body), opts...)
}

// Close 关闭生产者
func (p *kafkaProducer) Close() error {
	return p.producer.Close()
}

// newKafkaConsumer 创建 Kafka 消费者
func newKafkaConsumer(opts *Options) (Consumer, error) {
	if len(opts.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}

	log.Info("Creating Kafka consumer",
		zap.Strings("brokers", opts.KafkaBrokers),
		zap.String("client_id", opts.KafkaClientID),
		zap.String("version", opts.KafkaVersion),
		zap.Bool("enable_sasl", opts.KafkaEnableSASL),
		zap.Bool("enable_tls", opts.KafkaEnableTLS),
		zap.Bool("enable_trace", opts.EnableTrace),
	)

	// 解析 Kafka 版本
	var version sarama.KafkaVersion
	if opts.KafkaVersion != "" {
		var err error
		version, err = sarama.ParseKafkaVersion(opts.KafkaVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid kafka version %s: %w", opts.KafkaVersion, err)
		}
	} else {
		version = sarama.V2_8_0_0
	}

	// 配置 Kafka 客户端
	config := sarama.NewConfig()
	config.Version = version
	config.ClientID = opts.KafkaClientID
	if config.ClientID == "" {
		config.ClientID = "ai-api-market"
	}

	// 设置超时
	if opts.KafkaDialTimeout > 0 {
		config.Net.DialTimeout = opts.KafkaDialTimeout
	}
	if opts.KafkaReadTimeout > 0 {
		config.Net.ReadTimeout = opts.KafkaReadTimeout
	}
	if opts.KafkaWriteTimeout > 0 {
		config.Net.WriteTimeout = opts.KafkaWriteTimeout
	}

	// 配置 SASL 认证
	if opts.KafkaEnableSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLMechanism(opts.KafkaSASLMechanism)
		config.Net.SASL.User = opts.KafkaSASLUsername
		config.Net.SASL.Password = opts.KafkaSASLPassword

		switch opts.KafkaSASLMechanism {
		case "SCRAM-SHA-256", "SCRAM-SHA-512":
			config.Net.SASL.Handshake = true
		default:
			config.Net.SASL.Handshake = false
		}
	}

	// 配置 TLS
	if opts.KafkaEnableTLS {
		config.Net.TLS.Enable = true
	}

	// 消费者配置
	config.Consumer.Return.Errors = true

	// 创建 Kafka 客户端
	client, err := sarama.NewClient(opts.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// 测试连接
	brokers := client.Brokers()
	if len(brokers) == 0 {
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to any kafka broker")
	}

	log.Debug("Kafka client connected",
		zap.Int("broker_count", len(brokers)),
	)

	// 创建消费者
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	log.Info("Kafka consumer created successfully",
		zap.Strings("brokers", opts.KafkaBrokers),
		zap.String("client_id", opts.KafkaClientID),
	)

	return &kafkaConsumer{
		consumer:    consumer,
		client:      client,
		enableTrace: opts.EnableTrace,
	}, nil
}

// Subscribe 订阅主题并处理消息（自动处理 trace）
func (c *kafkaConsumer) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	log.FromContext(ctx).Info("Subscribing to Kafka topic",
		zap.String("topic", topic),
	)

	// 获取主题的所有分区
	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		log.FromContext(ctx).Error("Failed to get Kafka partitions",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	if len(partitions) == 0 {
		log.FromContext(ctx).Error("Kafka topic has no partitions",
			zap.String("topic", topic),
		)
		return fmt.Errorf("topic %s has no partitions", topic)
	}

	log.FromContext(ctx).Debug("Kafka topic partitions found",
		zap.String("topic", topic),
		zap.Int("partition_count", len(partitions)),
	)

	// 为每个分区创建消费者
	successCount := 0
	for _, partition := range partitions {
		partitionConsumer, err := c.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			// 记录错误但继续处理其他分区
			continue
		}

		successCount++

		// 为每个分区启动一个 goroutine 处理消息
		go func(p int32, pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					if msg == nil {
						return
					}
					// 提取 trace context 并创建 span
					msgCtx := extractTraceContext(msg, "kafka")
					if c.enableTrace {
						var span trace.Span
						msgCtx, span = createSpan(msgCtx, "kafka.consume", "kafka",
							attribute.String("messaging.destination", msg.Topic),
							attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
							attribute.Int64("messaging.kafka.offset", msg.Offset),
							attribute.Int("messaging.message_size", len(msg.Value)),
						)
						// 处理消息
						startTime := time.Now()
						err := handler(msgCtx, msg.Value)
						duration := time.Since(startTime)

						if err != nil {
							log.FromContext(msgCtx).Error("Kafka message handler failed",
								zap.String("topic", msg.Topic),
								zap.Int32("partition", msg.Partition),
								zap.Int64("offset", msg.Offset),
								zap.Int("message_size", len(msg.Value)),
								zap.Duration("duration", duration),
								zap.Error(err),
							)
							finishSpan(span, err)
						} else {
							log.FromContext(msgCtx).Debug("Kafka message processed",
								zap.String("topic", msg.Topic),
								zap.Int32("partition", msg.Partition),
								zap.Int64("offset", msg.Offset),
								zap.Int("message_size", len(msg.Value)),
								zap.Duration("duration", duration),
							)
							finishSpan(span, nil)
						}
					} else {
						// 处理消息
						startTime := time.Now()
						err := handler(msgCtx, msg.Value)
						duration := time.Since(startTime)

						if err != nil {
							log.FromContext(msgCtx).Error("Kafka message handler failed",
								zap.String("topic", msg.Topic),
								zap.Int32("partition", msg.Partition),
								zap.Int64("offset", msg.Offset),
								zap.Int("message_size", len(msg.Value)),
								zap.Duration("duration", duration),
								zap.Error(err),
							)
						} else {
							log.FromContext(msgCtx).Debug("Kafka message processed",
								zap.String("topic", msg.Topic),
								zap.Int32("partition", msg.Partition),
								zap.Int64("offset", msg.Offset),
								zap.Int("message_size", len(msg.Value)),
								zap.Duration("duration", duration),
							)
						}
					}

				case err := <-pc.Errors():
					if err != nil {
						// 记录错误但不中断消费
						// 错误已通过错误通道返回，由上层处理
					}

				case <-ctx.Done():
					return
				}
			}
		}(partition, partitionConsumer)
	}

	if successCount == 0 {
		log.FromContext(ctx).Error("Failed to create consumer for any partition",
			zap.String("topic", topic),
			zap.Int("partition_count", len(partitions)),
		)
		return fmt.Errorf("failed to create consumer for any partition of topic %s", topic)
	}

	log.FromContext(ctx).Info("Kafka consumer subscribed successfully",
		zap.String("topic", topic),
		zap.Int("partition_count", len(partitions)),
		zap.Int("success_count", successCount),
	)

	return nil
}

// Close 关闭消费者
func (c *kafkaConsumer) Close() error {
	var errs []error
	if c.consumer != nil {
		if err := c.consumer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka consumer: %v", errs)
	}
	return nil
}
