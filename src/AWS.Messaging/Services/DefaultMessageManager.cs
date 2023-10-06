// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using AWS.Messaging.Configuration;
using AWS.Messaging.SQS;
using Microsoft.Extensions.Logging;

namespace AWS.Messaging.Services;

/// <inheritdoc cref="IMessageManager"/>
public class DefaultMessageManager : IMessageManager
{
    private readonly ISQSMessageCommunication _sqsMessageCommunication;
    private readonly IHandlerInvoker _handlerInvoker;
    private readonly ILogger<DefaultMessageManager> _logger;
    private readonly MessageManagerConfiguration _configuration;

    private readonly ConcurrentDictionary<MessageEnvelope, InFlightMetadata> _inFlightMessageMetadata = new();
    private readonly object _visibilityTimeoutExtensionTaskLock = new object();
    private Task? _visibilityTimeoutExtensionTask;

    private readonly ActionBlock<MessageProcessingTask> _messageProcessingBlock;
    private readonly List<ActionBlock<MessageProcessingTask>> _fifoProcessingBlocks = new();

    /// <summary>
    /// Constructs an instance of <see cref="DefaultMessageManager"/>
    /// </summary>
    /// <param name="sqsMessageCommunication">Provides APIs to communicate back to SQS and the associated Queue for incoming messages.</param>
    /// <param name="handlerInvoker">Used to look up and invoke the correct handler for each message</param>
    /// <param name="logger">Logger for debugging information</param>
    /// <param name="configuration">The configuration for the message manager</param>
    public DefaultMessageManager(ISQSMessageCommunication sqsMessageCommunication, IHandlerInvoker handlerInvoker, ILogger<DefaultMessageManager> logger, MessageManagerConfiguration configuration)
    {
        _sqsMessageCommunication = sqsMessageCommunication;
        _handlerInvoker = handlerInvoker;
        _logger = logger;
        _configuration = configuration;

        _messageProcessingBlock = CreateMessageProcessingBlock(_configuration.MaxNumberOfConcurrentMessages, _configuration.MaxNumberOfConcurrentMessages);

        for (var i = 0; i <  _configuration.MaxNumberOfConcurrentMessages; i++)
        {
            _fifoProcessingBlocks.Add(CreateMessageProcessingBlock(1, 10));
        }
    }

    /// <inheritdoc/>
    public async Task AddToProcessingQueueAsync(MessageProcessingTask messageProcessingTask)
    {
        _inFlightMessageMetadata.TryAdd(messageProcessingTask.MessageEnvelope, new InFlightMetadata(
            DateTimeOffset.UtcNow + TimeSpan.FromSeconds(_configuration.VisibilityTimeout)));

        if (_configuration.SupportExtendingVisibilityTimeout)
            StartMessageVisibilityExtensionTaskIfNotRunning(messageProcessingTask.CancellationToken);

        if (!_configuration.FifoProcessing)
        {
            await _messageProcessingBlock.SendAsync(messageProcessingTask);
            return;
        }

        var messageGroupId = messageProcessingTask.MessageEnvelope.SQSMetadata?.MessageGroupId;
        if (string.IsNullOrEmpty(messageGroupId))
        {
            _logger.LogError("Message with ID {messageProcessingTask.MessageEnvelope.Id} could not be added for Fifo processing since it had an empty message group ID", messageProcessingTask.MessageEnvelope.Id);
            throw new InvalidOperationException($"Message with ID {messageProcessingTask.MessageEnvelope.Id} could not be added for Fifo processing since it had an empty message group ID");
        }

        var totalFifoProcessingBlocks = _fifoProcessingBlocks.Count;
        await _fifoProcessingBlocks[messageGroupId.GetHashCode() % totalFifoProcessingBlocks].SendAsync(messageProcessingTask);   
    }

    /// <inheritdoc/>
    public async Task WaitForCompletionAsync(CancellationToken token = default)
    {
        if (!_configuration.FifoProcessing)
        {
            _messageProcessingBlock.Complete();
            await _messageProcessingBlock.Completion.WaitAsync(token);
            return;
        }

        var fifoProcessingBlockCompletion = new List<Task>();
        foreach (var fifoProcessingBlock in _fifoProcessingBlocks)
        {
            fifoProcessingBlock.Complete();
            fifoProcessingBlockCompletion.Add(fifoProcessingBlock.Completion.WaitAsync(token));
        }
        await Task.WhenAll(fifoProcessingBlockCompletion);
    }

    /// <inheritdoc/>
    private async Task ProcessMessageAsync(MessageEnvelope messageEnvelope, SubscriberMapping subscriberMapping, CancellationToken token = default)
    {
        var handlerTask = _handlerInvoker.InvokeAsync(messageEnvelope, subscriberMapping, token);

        // Wait for the handler to finish processing the message
        try
        {
            await handlerTask;
        }
        catch (AWSMessagingException)
        {
            // Swallow exceptions thrown by the framework, and rely on the thrower to log
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An unknown exception occurred while processing message ID {SubscriberEndpoint}", messageEnvelope.Id);
        }

        _inFlightMessageMetadata.Remove(messageEnvelope, out _);

        if (handlerTask.IsCompletedSuccessfully)
        {
            if (handlerTask.Result.IsSuccess)
            {
                // Delete the message from the queue if it was processed successfully
                await _sqsMessageCommunication.DeleteMessagesAsync(new MessageEnvelope[] { messageEnvelope });
            }
            else // the handler still finished, but returned MessageProcessStatus.Failed
            {
                _logger.LogError("Message handling completed unsuccessfully for message ID {MessageId}", messageEnvelope.Id);
                await _sqsMessageCommunication.ReportMessageFailureAsync(messageEnvelope);
            }
        }
        else if (handlerTask.IsFaulted)
        {
            _logger.LogError(handlerTask.Exception, "Message handling failed unexpectedly for message ID {MessageId}", messageEnvelope.Id);
            await _sqsMessageCommunication.ReportMessageFailureAsync(messageEnvelope);
        }
    }

    /// <summary>
    /// Starts the task that extends the visibility timeout of in-flight messages
    /// </summary>
    /// <param name="token">Cancellation token to stop the visibility timeout extension task</param>
    private void StartMessageVisibilityExtensionTaskIfNotRunning(CancellationToken token)
    {
        // It may either have been never started, or previously started and completed because there were no more in flight messages
        if (_visibilityTimeoutExtensionTask == null || _visibilityTimeoutExtensionTask.IsCompleted)
        {
            lock(_visibilityTimeoutExtensionTaskLock)
            {
                if (_visibilityTimeoutExtensionTask == null || _visibilityTimeoutExtensionTask.IsCompleted)
                {
                    _visibilityTimeoutExtensionTask = ExtendUnfinishedMessageVisibilityTimeoutBatch(token);

                    _logger.LogTrace("Started task with id {id} to extend the visibility timeout of in flight messages", _visibilityTimeoutExtensionTask.Id);
                }
            }
        }
    }

    /// <summary>
    /// Extends the visibility timeout periodically for messages whose corresponding handler task is not yet complete
    /// </summary>
    /// <param name="token">Cancellation token to stop the visibility timeout extension loop</param>
    private async Task ExtendUnfinishedMessageVisibilityTimeoutBatch(CancellationToken token)
    {
        IEnumerable<KeyValuePair<MessageEnvelope, InFlightMetadata>> unfinishedMessages;

        do
        {
            // Wait for the configured interval before extending visibility
            await Task.Delay(_configuration.VisibilityTimeoutExtensionHeartbeatInterval, token);

            // Select the message envelopes whose corresponding handler task is not yet complete.
            //
            //   The .ToList() is important as we want to evaluate their expected expiration relative to the threshold at this instant,
            //   rather than lazily. Otherwise the batch of messages that are eligible for a visibility timeout extension may change between
            //   actually extending the timeout and recording that we've done so.
            unfinishedMessages = _inFlightMessageMetadata.Where(messageAndMetadata =>
                messageAndMetadata.Value.IsMessageVisibilityTimeoutExpiring(_configuration.VisibilityTimeoutExtensionThreshold)).ToList();

            // TODO: Handle the race condition where a message could have finished handling and be deleted concurrently
            if (unfinishedMessages.Any())
            {
                // Update the timestamp that the visibility timeout window is expected to expire
                // Per SQS documentation: "The new timeout period takes effect from the time you call the ChangeMessageVisibility action"
                foreach (var unfinishedMessage in unfinishedMessages)
                {
                    unfinishedMessage.Value.UpdateExpectedVisibilityTimeoutExpiration(_configuration.VisibilityTimeout);
                }

                await _sqsMessageCommunication.ExtendMessageVisibilityTimeoutAsync(unfinishedMessages.Select(messageAndMetadata => messageAndMetadata.Key), token);
            }
        } while (_inFlightMessageMetadata.Any() && !token.IsCancellationRequested);
    }

    private ActionBlock<MessageProcessingTask> CreateMessageProcessingBlock(int maxNumberofConcurrentMessages, int bufferCapacity)
    {
        return new ActionBlock<MessageProcessingTask>(
            async task =>
            {
                await ProcessMessageAsync(task.MessageEnvelope, task.SubscriberMapping, task.CancellationToken);
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxNumberofConcurrentMessages,
                BoundedCapacity = bufferCapacity,
                SingleProducerConstrained = true
            });
    }
}
