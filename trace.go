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
	pkgtrace "github.com/go-anyway/framework-trace"

	"github.com/IBM/sarama"
	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// extractTraceContext 从消息中提取 trace context（统一入口）
// 支持 Kafka、RabbitMQ 两种消息队列
func extractTraceContext(msg interface{}, system string) context.Context {
	ctx := context.Background()
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		return ctx
	}

	switch system {
	case "kafka":
		if kafkaMsg, ok := msg.(*sarama.ConsumerMessage); ok {
			carrier := &kafkaCarrier{headers: &kafkaMsg.Headers}
			ctx = propagator.Extract(ctx, carrier)
		}
	case "rabbitmq":
		if delivery, ok := msg.(amqp091.Delivery); ok {
			carrier := &rabbitMQCarrier{headers: delivery.Headers}
			ctx = propagator.Extract(ctx, carrier)
		}
	}

	return ctx
}

// injectTraceContext 向消息中注入 trace context（统一入口）
// 支持 Kafka、RabbitMQ 两种消息队列
func injectTraceContext(ctx context.Context, msg interface{}, system string) {
	propagator := otel.GetTextMapPropagator()
	if propagator == nil || ctx == nil {
		return
	}

	switch system {
	case "kafka":
		if kafkaMsg, ok := msg.(*sarama.ProducerMessage); ok {
			if kafkaMsg.Headers == nil {
				kafkaMsg.Headers = make([]sarama.RecordHeader, 0)
			}
			carrier := &kafkaProducerCarrier{headers: &kafkaMsg.Headers}
			propagator.Inject(ctx, carrier)
		}
	case "rabbitmq":
		if headers, ok := msg.(map[string]interface{}); ok {
			carrier := &rabbitMQCarrier{headers: headers}
			propagator.Inject(ctx, carrier)
		}
	}
}

// createSpan 创建统一的 span
func createSpan(ctx context.Context, operation string, system string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	allAttrs := []attribute.KeyValue{
		attribute.String("messaging.system", system),
	}
	allAttrs = append(allAttrs, attrs...)

	ctx, span := pkgtrace.StartSpan(ctx, operation, trace.WithAttributes(allAttrs...))
	return ctx, span
}

// finishSpan 结束 span 并设置状态
func finishSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// ============================================================================
// Kafka Carrier 实现
// ============================================================================

// kafkaCarrier 实现 OpenTelemetry 的 TextMapCarrier 接口（消费者）
type kafkaCarrier struct {
	headers *[]*sarama.RecordHeader
}

func (c *kafkaCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for i := len(*c.headers) - 1; i >= 0; i-- {
		if (*c.headers)[i] != nil && string((*c.headers)[i].Key) == key {
			return string((*c.headers)[i].Value)
		}
	}
	return ""
}

func (c *kafkaCarrier) Set(key, value string) {
	// 消费者只读
}

func (c *kafkaCarrier) Keys() []string {
	if c.headers == nil {
		return []string{}
	}
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		if header != nil {
			keys = append(keys, string(header.Key))
		}
	}
	return keys
}

// kafkaProducerCarrier 实现 OpenTelemetry 的 TextMapCarrier 接口（生产者）
type kafkaProducerCarrier struct {
	headers *[]sarama.RecordHeader
}

func (c *kafkaProducerCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for i := len(*c.headers) - 1; i >= 0; i-- {
		if string((*c.headers)[i].Key) == key {
			return string((*c.headers)[i].Value)
		}
	}
	return ""
}

func (c *kafkaProducerCarrier) Set(key, value string) {
	if c.headers == nil {
		*c.headers = make([]sarama.RecordHeader, 0)
	}
	keyBytes := []byte(key)
	for i := range *c.headers {
		if string((*c.headers)[i].Key) == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, sarama.RecordHeader{
		Key:   keyBytes,
		Value: []byte(value),
	})
}

func (c *kafkaProducerCarrier) Keys() []string {
	if c.headers == nil {
		return []string{}
	}
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, string(header.Key))
	}
	return keys
}

// ============================================================================
// RabbitMQ Carrier 实现
// ============================================================================

// rabbitMQCarrier 实现 OpenTelemetry 的 TextMapCarrier 接口
type rabbitMQCarrier struct {
	headers map[string]interface{}
}

func (c *rabbitMQCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	val, ok := c.headers[key]
	if !ok {
		return ""
	}
	// RabbitMQ headers 可能是 string、[]byte 或其他类型
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		// 尝试转换为字符串
		if str, ok := val.(fmt.Stringer); ok {
			return str.String()
		}
		return fmt.Sprintf("%v", val)
	}
}

func (c *rabbitMQCarrier) Set(key, value string) {
	if c.headers == nil {
		c.headers = make(map[string]interface{})
	}
	c.headers[key] = value
}

func (c *rabbitMQCarrier) Keys() []string {
	if c.headers == nil {
		return []string{}
	}
	keys := make([]string, 0, len(c.headers))
	for k := range c.headers {
		keys = append(keys, k)
	}
	return keys
}

// ============================================================================
// 统一的追踪和日志记录函数
// ============================================================================

// sendWithTrace 带追踪的发送消息包装器
func sendWithTrace(
	ctx context.Context,
	system string,
	topic string,
	operation string,
	handler func(context.Context) error,
	enableTrace bool,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "messagequeue."+operation,
			trace.WithAttributes(
				attribute.String("messaging.system", system),
				attribute.String("messaging.operation", operation),
				attribute.String("messaging.destination", topic),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Message queue send failed",
			zap.String("system", system),
			zap.String("operation", operation),
			zap.String("topic", topic),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("Message queue send completed",
			zap.String("system", system),
			zap.String("operation", operation),
			zap.String("topic", topic),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(
			attribute.Float64("messaging.duration_ms", float64(duration.Milliseconds())),
		)
	}

	return err
}

// consumeWithTrace 带追踪的消费消息包装器
func consumeWithTrace(
	ctx context.Context,
	system string,
	topic string,
	operation string,
	handler func(context.Context) error,
	enableTrace bool,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "messagequeue."+operation,
			trace.WithAttributes(
				attribute.String("messaging.system", system),
				attribute.String("messaging.operation", operation),
				attribute.String("messaging.destination", topic),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("Message queue consume failed",
			zap.String("system", system),
			zap.String("operation", operation),
			zap.String("topic", topic),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Debug("Message queue consume completed",
			zap.String("system", system),
			zap.String("operation", operation),
			zap.String("topic", topic),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(
			attribute.Float64("messaging.duration_ms", float64(duration.Milliseconds())),
		)
	}

	return err
}

// logMessageCost 记录消息耗时（统一函数）
func logMessageCost(ctx context.Context, system, topic string, startTime time.Time, attrs ...zap.Field) {
	if startTime.IsZero() {
		return
	}
	cost := time.Since(startTime)
	allAttrs := []zap.Field{
		zap.Float64("cost_ms", float64(cost.Microseconds())/1000.0),
		zap.String("system", system),
		zap.String("topic", topic),
	}
	allAttrs = append(allAttrs, attrs...)
	log.FromContext(ctx).Info("Message processed", allAttrs...)
}
