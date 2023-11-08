// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using AWS.Messaging.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Moq;
using AWS.Messaging.Publishers;
using System.Threading.Tasks;
using AWS.Messaging.UnitTests.Models;
using Amazon.SQS;
using AWS.Messaging.Serialization;
using System.Threading;
using Amazon.SQS.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using AWS.Messaging.Publishers.EventBridge;
using AWS.Messaging.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using AWS.Messaging.Publishers.SQS;
using AWS.Messaging.Publishers.SNS;

namespace AWS.Messaging.UnitTests;

public class MessagePublisherTests
{
    private readonly Mock<IMessageConfiguration> _messageConfiguration;
    private readonly Mock<ILogger<IMessagePublisher>> _logger;
    private readonly Mock<IAmazonSQS> _sqsClient;
    private readonly Mock<IAmazonSimpleNotificationService> _snsClient;
    private readonly Mock<IAmazonEventBridge> _eventBridgeClient;
    private readonly Mock<IEnvelopeSerializer> _envelopeSerializer;
    private readonly ChatMessage _chatMessage;

    public MessagePublisherTests()
    {
        _messageConfiguration = new Mock<IMessageConfiguration>();
        _logger = new Mock<ILogger<IMessagePublisher>>();
        _sqsClient = new Mock<IAmazonSQS>();
        _snsClient = new Mock<IAmazonSimpleNotificationService>();
        _eventBridgeClient = new Mock<IAmazonEventBridge>();
        _envelopeSerializer = new Mock<IEnvelopeSerializer>();

        _envelopeSerializer.SetReturnsDefault<ValueTask<MessageEnvelope<ChatMessage>>> (ValueTask.FromResult(new MessageEnvelope<ChatMessage>()
        {
            Id = "1234",
            Source = new Uri("/aws/messaging/unittest", UriKind.Relative)
        }));


        _chatMessage = new ChatMessage { MessageDescription = "Test Description" };
    }

    [Fact]
    public async Task SQSPublisher_HappyPath()
    {
        var serviceProvider = SetupSQSPublisherDIServices();

        _sqsClient.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await messagePublisher.PublishAsync(_chatMessage);

        _sqsClient.Verify(x =>
            x.SendMessageAsync(
                It.Is<SendMessageRequest>(request =>
                    request.QueueUrl.Equals("endpoint")),
                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task RoutingPublisher_TelemetryHappyPath()
    {
        var serviceProvider = SetupSQSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _sqsClient.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            telemetryFactory.Object
            );

        await messagePublisher.PublishAsync(_chatMessage);

        telemetryFactory.Verify(x =>
            x.Trace(
                It.Is<string>(request =>
                    request.Equals("Routing message to AWS service"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.ObjectType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));
    }

    [Fact]
    public async Task RoutingPublisher_TelemetryThrowsException()
    {
        var serviceProvider = SetupSQSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _sqsClient.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>())).ThrowsAsync(new Exception("Telemetry exception"));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            telemetryFactory.Object
            );

        await Assert.ThrowsAsync<Exception>(() => messagePublisher.PublishAsync(_chatMessage));

        telemetryTrace.Verify(x =>
            x.AddException(
                It.Is<Exception>(request =>
                    request.Message.Equals("Telemetry exception")),
                It.IsAny<bool>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SQSPublisher_TelemetryHappyPath()
    {
        var serviceProvider = SetupSQSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _sqsClient.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new SQSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await messagePublisher.PublishAsync(_chatMessage);

        telemetryFactory.Verify(x =>
            x.Trace(
                It.Is<string>(request =>
                    request.Equals("Publish to AWS SQS"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.ObjectType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.QueueUrl)),
                It.Is<string>(request =>
                    request.Equals("endpoint"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageId)),
                It.Is<string>(request =>
                    request.Equals("1234"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.RecordTelemetryContext(
                It.IsAny<MessageEnvelope>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SQSPublisher_TelemetryThrowsException()
    {
        var serviceProvider = SetupSQSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _sqsClient.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>())).ThrowsAsync(new Exception("Telemetry exception"));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new SQSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await Assert.ThrowsAsync<Exception>(() => messagePublisher.PublishAsync(_chatMessage));

        telemetryTrace.Verify(x =>
            x.AddException(
                It.Is<Exception>(request =>
                    request.Message.Equals("Telemetry exception")),
                It.IsAny<bool>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SQSPublisher_MappingNotFound()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<MissingMessageTypeConfigurationException>(() => messagePublisher.PublishAsync(_chatMessage));
    }

    [Fact]
    public async Task SQSPublisher_InvalidMessage()
    {
        var serviceProvider = SetupSQSPublisherDIServices();

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<InvalidMessageException>(() => messagePublisher.PublishAsync<ChatMessage?>(null));
    }

    private IServiceProvider SetupSQSPublisherDIServices(string queueUrl = "endpoint")
    {
        var publisherConfiguration = new SQSPublisherConfiguration(queueUrl);
        var publisherMapping = new PublisherMapping(typeof(ChatMessage), publisherConfiguration, PublisherTargetType.SQS_PUBLISHER);

        _messageConfiguration.Setup(x => x.GetPublisherMapping(typeof(ChatMessage))).Returns(publisherMapping);

        var services = new ServiceCollection();
        services.AddSingleton<IAmazonSQS>(_sqsClient.Object);
        services.AddSingleton<ILogger<IMessagePublisher>>(_logger.Object);
        services.AddSingleton<IMessageConfiguration>(_messageConfiguration.Object);
        services.AddSingleton<IEnvelopeSerializer>(_envelopeSerializer.Object);
        services.AddSingleton<IAWSClientProvider, AWSClientProvider>();
        services.AddSingleton<ITelemetryFactory, DefaultTelemetryFactory>();

        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task SQSPublisher_UnsupportedPublisher()
    {
        var serviceProvider = SetupSQSPublisherDIServices();

        var publisherMapping = new PublisherMapping(typeof(ChatMessage), null!, "NEW_PUBLISHER");
        _messageConfiguration.Setup(x => x.GetPublisherMapping(typeof(ChatMessage))).Returns(publisherMapping);

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<UnsupportedPublisherException>(() => messagePublisher.PublishAsync(_chatMessage));
    }

    private IServiceProvider SetupSNSPublisherDIServices(string topicArn = "endpoint")
    {
        var publisherConfiguration = new SNSPublisherConfiguration(topicArn);
        var publisherMapping = new PublisherMapping(typeof(ChatMessage), publisherConfiguration, PublisherTargetType.SNS_PUBLISHER);

        _messageConfiguration.Setup(x => x.GetPublisherMapping(typeof(ChatMessage))).Returns(publisherMapping);

        var services = new ServiceCollection();
        services.AddSingleton<IAmazonSimpleNotificationService>(_snsClient.Object);
        services.AddSingleton<ILogger<IMessagePublisher>>(_logger.Object);
        services.AddSingleton<IMessageConfiguration>(_messageConfiguration.Object);
        services.AddSingleton<IEnvelopeSerializer>(_envelopeSerializer.Object);
        services.AddSingleton<IAWSClientProvider, AWSClientProvider>();
        services.AddSingleton<ITelemetryFactory, DefaultTelemetryFactory>();

        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task SNSPublisher_HappyPath()
    {
        var serviceProvider = SetupSNSPublisherDIServices();

        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await messagePublisher.PublishAsync(_chatMessage);

        _snsClient.Verify(x =>
            x.PublishAsync(
                It.Is<PublishRequest>(request =>
                    request.TopicArn.Equals("endpoint")),
                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SNSPublisher_TelemetryHappyPath()
    {
        var serviceProvider = SetupSNSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new SNSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await messagePublisher.PublishAsync(_chatMessage);

        telemetryFactory.Verify(x =>
            x.Trace(
                It.Is<string>(request =>
                    request.Equals("Publish to AWS SNS"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.ObjectType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.TopicUrl)),
                It.Is<string>(request =>
                    request.Equals("endpoint"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageId)),
                It.Is<string>(request =>
                    request.Equals("1234"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.RecordTelemetryContext(
                It.IsAny<MessageEnvelope>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SNSPublisher_TelemetryThrowsException()
    {
        var serviceProvider = SetupSNSPublisherDIServices();
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>())).ThrowsAsync(new Exception("Telemetry exception"));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new SNSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await Assert.ThrowsAsync<Exception>(() => messagePublisher.PublishAsync(_chatMessage));

        telemetryTrace.Verify(x =>
            x.AddException(
                It.Is<Exception>(request =>
                    request.Message.Equals("Telemetry exception")),
                It.IsAny<bool>()), Times.Exactly(1));
    }

    [Fact]
    public async Task SNSPublisher_InvalidMessage()
    {
        var serviceProvider = SetupSNSPublisherDIServices();

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<InvalidMessageException>(() => messagePublisher.PublishAsync<ChatMessage?>(null));
    }

    private IServiceProvider SetupEventBridgePublisherDIServices(string eventBusName, string? endpointID = null)
    {
        var publisherConfiguration = new EventBridgePublisherConfiguration(eventBusName)
        {
            EndpointID = endpointID
        };

        var publisherMapping = new PublisherMapping(typeof(ChatMessage), publisherConfiguration, PublisherTargetType.EVENTBRIDGE_PUBLISHER);

        _messageConfiguration.Setup(x => x.GetPublisherMapping(typeof(ChatMessage))).Returns(publisherMapping);

        var services = new ServiceCollection();
        services.AddSingleton<IAmazonEventBridge>(_eventBridgeClient.Object);
        services.AddSingleton<ILogger<IMessagePublisher>>(_logger.Object);
        services.AddSingleton<IMessageConfiguration>(_messageConfiguration.Object);
        services.AddSingleton<IEnvelopeSerializer>(_envelopeSerializer.Object);
        services.AddSingleton<IAWSClientProvider, AWSClientProvider>();
        services.AddSingleton<ITelemetryFactory, DefaultTelemetryFactory>();

        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task EventBridgePublisher_HappyPath()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await messagePublisher.PublishAsync(_chatMessage);

        _eventBridgeClient.Verify(x =>
            x.PutEventsAsync(
                It.Is<PutEventsRequest>(request =>
                    request.Entries[0].EventBusName.Equals("event-bus-123") && string.IsNullOrEmpty(request.EndpointId)
                    && request.Entries[0].DetailType.Equals("AWS.Messaging.UnitTests.Models.ChatMessage") && request.Entries[0].Source.Equals("/aws/messaging/unittest")),
                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_TelemetryHappyPath()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>()));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new EventBridgePublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await messagePublisher.PublishAsync(_chatMessage);

        telemetryFactory.Verify(x =>
            x.Trace(
                It.Is<string>(request =>
                    request.Equals("Publish to AWS EventBridge"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.ObjectType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageType)),
                It.Is<string>(request =>
                    request.Equals("AWS.Messaging.UnitTests.Models.ChatMessage"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.EventBusName)),
                It.Is<string>(request =>
                    request.Equals("event-bus-123"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.AddMetadata(
                It.Is<string>(request =>
                    request.Equals(TelemetryKeys.MessageId)),
                It.Is<string>(request =>
                    request.Equals("1234"))), Times.Exactly(1));

        telemetryTrace.Verify(x =>
            x.RecordTelemetryContext(
                It.IsAny<MessageEnvelope>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_TelemetryThrowsException()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");
        var telemetryFactory = new Mock<ITelemetryFactory>();
        var telemetryTrace = new Mock<ITelemetryTrace>();

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>())).ThrowsAsync(new Exception("Telemetry exception"));
        telemetryFactory.Setup(x => x.Trace(It.IsAny<string>())).Returns(telemetryTrace.Object);
        telemetryTrace.Setup(x => x.AddMetadata(It.IsAny<string>(), It.IsAny<string>()));

        var messagePublisher = new EventBridgePublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            telemetryFactory.Object
            );

        await Assert.ThrowsAsync<Exception>(() => messagePublisher.PublishAsync(_chatMessage));

        telemetryTrace.Verify(x =>
            x.AddException(
                It.Is<Exception>(request =>
                    request.Message.Equals("Telemetry exception")),
                It.IsAny<bool>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_GlobalEP()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123", "endpoint.123");

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await messagePublisher.PublishAsync(_chatMessage);

        _eventBridgeClient.Verify(x =>
            x.PutEventsAsync(
                It.Is<PutEventsRequest>(request =>
                    request.Entries[0].EventBusName.Equals("event-bus-123") && request.EndpointId.Equals("endpoint.123")
                    && request.Entries[0].DetailType.Equals("AWS.Messaging.UnitTests.Models.ChatMessage") && request.Entries[0].Source.Equals("/aws/messaging/unittest")),
                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_OptionSource()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new EventBridgePublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await messagePublisher.PublishAsync(_chatMessage, new EventBridgeOptions
        {
            Source = "/aws/custom"
        });

        _eventBridgeClient.Verify(x =>
            x.PutEventsAsync(
                It.Is<PutEventsRequest>(request =>
                    request.Entries[0].EventBusName.Equals("event-bus-123") && string.IsNullOrEmpty(request.EndpointId)
                    && request.Entries[0].DetailType.Equals("AWS.Messaging.UnitTests.Models.ChatMessage") && request.Entries[0].Source.Equals("/aws/custom")),
                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_SetOptions()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");

        _eventBridgeClient.Setup(x => x.PutEventsAsync(It.IsAny<PutEventsRequest>(), It.IsAny<CancellationToken>()));

        var messagePublisher = new EventBridgePublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        DateTimeOffset dateTimeOffset = new DateTimeOffset(2015, 2, 17, 0, 0, 0, TimeSpan.Zero);

        await messagePublisher.PublishAsync(_chatMessage, new EventBridgeOptions
        {
            TraceHeader = "trace-header1",
            Time = dateTimeOffset
        });

        _eventBridgeClient.Verify(x =>
            x.PutEventsAsync(
                It.Is<PutEventsRequest>(request =>
                    request.Entries[0].EventBusName.Equals("event-bus-123") && string.IsNullOrEmpty(request.EndpointId)
                    && request.Entries[0].TraceHeader.Equals("trace-header1") && request.Entries[0].Time.Year == dateTimeOffset.Year),

                It.IsAny<CancellationToken>()), Times.Exactly(1));
    }

    [Fact]
    public async Task EventBridgePublisher_InvalidMessage()
    {
        var serviceProvider = SetupEventBridgePublisherDIServices("event-bus-123");

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<InvalidMessageException>(() => messagePublisher.PublishAsync<ChatMessage?>(null));
    }

    [Fact]
    public async Task PublishToFifoQueue_WithoutMessageGroupId_ThrowsException()
    {
        var serviceProvider = SetupSQSPublisherDIServices("endpoint.fifo");

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        var sqsMessagePublisher = new SQSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<InvalidFifoPublishingRequestException>(() => messagePublisher.PublishAsync<ChatMessage?>(new ChatMessage()));
        await Assert.ThrowsAsync<InvalidFifoPublishingRequestException>(() => sqsMessagePublisher.PublishAsync<ChatMessage?>(new ChatMessage(), new SQSOptions()));
    }

    [Fact]
    public async Task PublishToFifoTopic_WithoutMessageGroupId_ThrowsException()
    {
        var serviceProvider = SetupSNSPublisherDIServices("endpoint.fifo");

        var messagePublisher = new MessageRoutingPublisher(
            serviceProvider,
            _messageConfiguration.Object,
            _logger.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        var snsMessagePublisher = new SNSPublisher(
            (IAWSClientProvider)serviceProvider.GetService(typeof(IAWSClientProvider))!,
            _logger.Object,
            _messageConfiguration.Object,
            _envelopeSerializer.Object,
            new DefaultTelemetryFactory(serviceProvider)
            );

        await Assert.ThrowsAsync<InvalidFifoPublishingRequestException>(() => messagePublisher.PublishAsync<ChatMessage?>(new ChatMessage()));
        await Assert.ThrowsAsync<InvalidFifoPublishingRequestException>(() => snsMessagePublisher.PublishAsync<ChatMessage?>(new ChatMessage(), new SNSOptions()));
    }
}
