// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using AWS.Messaging.IntegrationTests.Handlers;
using AWS.Messaging.IntegrationTests.Models;
using AWS.Messaging.Publishers.SQS;
using AWS.Messaging.Services;
using AWS.Messaging.Tests.Common.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace AWS.Messaging.IntegrationTests;

public class FifoSubscriberTests : IAsyncLifetime
{
    private readonly IAmazonSQS _sqsClient;
    private readonly IServiceCollection _serviceCollection;
    private string _sqsQueueUrl;

    public FifoSubscriberTests()
    {
        _sqsClient = new AmazonSQSClient();
        _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging(x => x.AddInMemoryLogger().SetMinimumLevel(LogLevel.Trace));
        _sqsQueueUrl = string.Empty;
    }

    public async Task InitializeAsync()
    {
        var queueName = $"MPFTest-{Guid.NewGuid().ToString().Split('-').Last()}.fifo";
        var createQueueResponse = await _sqsClient.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = queueName,
            Attributes = new()
            {
                { "FifoQueue", "true" },
                { "ContentBasedDeduplication", "true" }
            }
        });
        _sqsQueueUrl = createQueueResponse.QueueUrl;
    }

    [Theory]
    // Tests that the visibility is extended without needing multiple batch requests
    [InlineData(5, 50)]
    // Tests that the visibility is extended with the need for multiple batch requests
    //[InlineData(8, 50)]
    //// Increasing the number of messages processed to ensure stability at load
    //[InlineData(15, 15)]
    //// Increasing the number of messages processed with batching required to extend visibility
    //[InlineData(20, 15)]
    public async Task SendAndReceiveMultipleMessages_SameMessageGroupId(int numberOfMessages, int maxConcurrentMessages)
    {
        _serviceCollection.AddSingleton<TempStorage<OrderInfo>>();
        _serviceCollection.AddAWSMessageBus(builder =>
        {
            builder.AddSQSPublisher<OrderInfo>(_sqsQueueUrl);
            builder.AddSQSPoller(_sqsQueueUrl, options =>
            {
                options.MaxNumberOfConcurrentMessages = maxConcurrentMessages;
            });
            builder.AddMessageHandler<OrderInfoHandler_Fifo, OrderInfo>();
            builder.AddMessageSource("/aws/messaging");
        });
        var serviceProvider = _serviceCollection.BuildServiceProvider();

        var publishStartTime = DateTime.UtcNow;
        var sqsPublisher = serviceProvider.GetRequiredService<ISQSPublisher>();
        var messageGroupId = "1234";
        for (var i = 0; i < numberOfMessages; i++)
        {
            var orderInfo = new OrderInfo
            {
                OrderId = $"{i + 1}",
                UserId = messageGroupId
            };

            await sqsPublisher.PublishAsync(orderInfo, new SQSOptions
            {
                MessageGroupId = messageGroupId
            });
        }
        var publishEndTime = DateTime.UtcNow;

        var pump = serviceProvider.GetRequiredService<IHostedService>() as MessagePumpService;
        Assert.NotNull(pump);
        var source = new CancellationTokenSource();
        await pump.StartAsync(source.Token);

        source.CancelAfter((numberOfMessages+1) * 5000);

        while (!source.IsCancellationRequested) { }

        var orderInfoTempStorage = serviceProvider.GetRequiredService<TempStorage<OrderInfo>>();

        Assert.Equal(numberOfMessages, orderInfoTempStorage.MessagesFifo.Count);
        var orderInfoEnvelopes = orderInfoTempStorage.MessagesFifo.ToArray();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var orderInfoEnvelope = orderInfoEnvelopes[i];
            Assert.NotNull(orderInfoEnvelope);
            Assert.False(string.IsNullOrEmpty(orderInfoEnvelope.Id));
            Assert.Equal("/aws/messaging", orderInfoEnvelope.Source.ToString());
            Assert.True(orderInfoEnvelope.TimeStamp > publishStartTime);
            Assert.True(orderInfoEnvelope.TimeStamp < publishEndTime);

            var orderInfo = orderInfoEnvelope.Message;
            Assert.Equal($"{i+1}", orderInfo.OrderId);
            Assert.Equal(messageGroupId, orderInfoEnvelope.SQSMetadata!.MessageGroupId);
        }
    }

    public async Task DisposeAsync()
    {
        try
        {
            await _sqsClient.DeleteQueueAsync(_sqsQueueUrl);
        }
        catch { }
    }
}
