// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Threading.Tasks.Dataflow;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using AWS.Messaging.Configuration;
using AWS.Messaging.Serialization;
using AWS.Messaging.Services;
using AWS.Messaging.SQS;
using Microsoft.Extensions.Logging;

namespace AWS.Messaging.Lambda.Services;

internal class DefaultLambdaMessageProcessor : ILambdaMessageProcessor, ISQSMessageCommunication
{
    private readonly IAmazonSQS _sqsClient;
    private readonly ILogger<DefaultLambdaMessageProcessor> _logger;
    private readonly IMessageManager _messageManager;
    private readonly IEnvelopeSerializer _envelopeSerializer;
    private readonly LambdaMessageProcessorConfiguration _configuration;

    private readonly SQSBatchResponse _sqsBatchResponse;

    // this is used to safely delete messages from _configuration.SQSBatchResponse.
    private readonly object _sqsBatchResponseLock = new object();

    /// <summary>
    /// Creates instance of <see cref="DefaultLambdaMessageProcessor" />
    /// </summary>
    /// <param name="logger">Logger for debugging information.</param>
    /// <param name="messageManagerFactory">The factory to create the message manager for processing messages.</param>
    /// <param name="awsClientProvider">Provides the AWS service client from the DI container.</param>
    /// <param name="configuration">The Lambda message processor configuration.</param>
    /// <param name="envelopeSerializer">Serializer used to deserialize the SQS messages</param>
    public DefaultLambdaMessageProcessor(
        ILogger<DefaultLambdaMessageProcessor> logger,
        IMessageManagerFactory messageManagerFactory,
        IAWSClientProvider awsClientProvider,
        LambdaMessageProcessorConfiguration configuration,
        IEnvelopeSerializer envelopeSerializer)
    {
        _logger = logger;
        _sqsClient = awsClientProvider.GetServiceClient<IAmazonSQS>();
        _envelopeSerializer = envelopeSerializer;
        _configuration = configuration;
        _messageManager = messageManagerFactory.CreateMessageManager(this, new MessageManagerConfiguration
        {
            SupportExtendingVisibilityTimeout = false,
            MaxNumberOfConcurrentMessages = _configuration.MaxNumberOfConcurrentMessages,
            FifoProcessing = _configuration.SubscriberEndpoint.EndsWith(".fifo")
        });

        _sqsBatchResponse = new SQSBatchResponse();
    }


    public async Task<SQSBatchResponse?> ProcessMessagesAsync(CancellationToken token = default)
    {
        var sqsEvent = _configuration.SQSEvent;
        if (sqsEvent is null || !sqsEvent.Records.Any())
        {
            return _sqsBatchResponse;
        }

        var index = 0;
        try
        {
            var processingQueue = new List<Task>();
            while (!token.IsCancellationRequested && index < sqsEvent.Records.Count)
            {

                var message = ConvertToStandardSQSMessage(sqsEvent.Records[index]);
                var messageEnvelopeResult = await _envelopeSerializer.ConvertToEnvelopeAsync(message);

                var result = await _envelopeSerializer.ConvertToEnvelopeAsync(message);
                processingQueue.Add(_messageManager.AddToProcessingQueueAsync(new MessageProcessingTask(result.Envelope, result.Mapping, token)));
                index++;
            }

            await Task.WhenAll(processingQueue);
            await _messageManager.WaitForCompletionAsync(token);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An unknown exception initiating message handlers for incoming SQS messages.");

            // If there are any errors queuing messages into the handlers then let the exception bubble up to allow Lambda to report a function invocation failure.
            throw;
        }

        // If partial failure mode is not enabled then if there are any errors reported from the message handlers we
        // need to communicate up to Lambda via an exception that the invocation failed.
        if(!_configuration.UseBatchResponse && _sqsBatchResponse.BatchItemFailures?.Count > 0)
        {
            throw new LambdaInvocationFailureException($"Lambda invocation failed because {_sqsBatchResponse.BatchItemFailures.Count} message reported failures during handling");
        }

        return _configuration.UseBatchResponse ? _sqsBatchResponse : null;
    }

    /// <inheritdoc/>
    public async Task DeleteMessagesAsync(IEnumerable<MessageEnvelope> messages, CancellationToken token = default)
    {
        // If batch response is enabled then rely on Lambda to delete the messages that are not in the SQSBatchResponse returned for the Lambda function.
        if (!_configuration.DeleteMessagesWhenCompleted || _configuration.UseBatchResponse)
        {
            return;
        }

        if(!messages.Any())
        {
            return;
        }    

        var request = new DeleteMessageBatchRequest
        {
            QueueUrl = _configuration.SubscriberEndpoint
        };

        foreach (var message in messages)
        {
            if (!string.IsNullOrEmpty(message.SQSMetadata?.ReceiptHandle))
            {
                _logger.LogTrace("Preparing to delete message {MessageId} with SQS receipt handle {ReceiptHandle} from queue {SubscriberEndpoint}",
                    message.Id, message.SQSMetadata.ReceiptHandle, _configuration.SubscriberEndpoint);
                request.Entries.Add(new DeleteMessageBatchRequestEntry()
                {
                    Id = message.Id,
                    ReceiptHandle = message.SQSMetadata.ReceiptHandle
                });
            }
            else
            {
                _logger.LogError("Attempted to delete message {MessageId} from {SubscriberEndpoint} without an SQS receipt handle.", message.Id, _configuration.SubscriberEndpoint);
                throw new MissingSQSMetadataException($"Attempted to delete message {message.Id} from {_configuration.SubscriberEndpoint} without an SQS receipt handle.");
            }
        }

        var response = await _sqsClient.DeleteMessageBatchAsync(request, token);

        foreach (var successMessage in response.Successful)
        {
            _logger.LogTrace("Deleted message {MessageId} from queue {SubscriberEndpoint} successfully", successMessage.Id, _configuration.SubscriberEndpoint);
        }

        foreach (var failedMessage in response.Failed)
        {
            _logger.LogError("Failed to delete message {FailedMessageId} from queue {SubscriberEndpoint}: {FailedMessage}",
                failedMessage.Id, _configuration.SubscriberEndpoint, failedMessage.Message);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// This is a no-op since visibility should match the length of the Lambda function timeout.
    /// </remarks>
    public Task ExtendMessageVisibilityTimeoutAsync(IEnumerable<MessageEnvelope> messages, CancellationToken token = default)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// This is a no-op when SQS event source mapping is not configured to use <see href="https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting">partial batch responses.</see>
    /// </remarks>
    public ValueTask ReportMessageFailureAsync(MessageEnvelope message, CancellationToken token = default)
    {
        lock (_sqsBatchResponseLock)
        {
            if (string.IsNullOrEmpty(message.SQSMetadata?.MessageID))
            {
                _logger.LogError("The message envelope with ID {MessageEnvelopeID} was not added to the batchFailureItems list since it did not have a valid SQS message ID.", message.Id);
                throw new MissingSQSMetadataException($"The message envelope with ID {message.Id} was not added to the batchFailureItems list since it did not have a valid SQS message ID.");
            }

            var batchItemFailure = new SQSBatchResponse.BatchItemFailure
            {
                ItemIdentifier = message.SQSMetadata.MessageID
            };
            _sqsBatchResponse.BatchItemFailures.Add(batchItemFailure);
        }

        return ValueTask.CompletedTask;
    }

    private Message ConvertToStandardSQSMessage(SQSEvent.SQSMessage sqsEventMessage)
    {
        var sqsMessage = new Message
        {
            Attributes = sqsEventMessage.Attributes,
            Body = sqsEventMessage.Body,
            MD5OfBody = sqsEventMessage.Md5OfBody,
            MD5OfMessageAttributes = sqsEventMessage.Md5OfMessageAttributes,
            MessageId = sqsEventMessage.MessageId,
            ReceiptHandle = sqsEventMessage.ReceiptHandle,
        };

        foreach (var attr in sqsEventMessage.MessageAttributes)
        {
            sqsMessage.MessageAttributes.Add(attr.Key, new MessageAttributeValue
            {
                BinaryListValues = attr.Value.BinaryListValues,
                BinaryValue = attr.Value.BinaryValue,
                DataType = attr.Value.DataType,
                StringListValues = attr.Value.StringListValues,
                StringValue = attr.Value.StringValue,
            });
        }

        return sqsMessage;
    }
}

