// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using AWS.Messaging.Configuration;

namespace AWS.Messaging.Services;

/// <summary>
/// Instances of <see cref="AWS.Messaging.Services.IMessageManager" /> manage the lifecycle of a message being processed.
/// </summary>
/// <remarks>
/// Responsibilities:
/// <list type="bullet">
/// <item><description>Start the async work of processing messages and add the task to collection of active tasks</description></item>
/// <item><description>Monitor the active processing message's task</description></item>
/// <item><description>If a task completes with success status code delete message</description></item>
/// <item><description>While the message tasks are working periodically inform the source the message is still being processed.</description></item>
/// </list>
/// </remarks>
public interface IMessageManager
{
    /// <summary>
    /// Schedules the async processing of message.
    /// </summary>
    /// <param name="messageProcessingTask">The input that is sent to the message processing task scheduler</param>
    Task AddToProcessingQueueAsync(MessageProcessingTask messageProcessingTask);

    /// <summary>
    /// Waits for the processing of all in-flight messages.
    /// </summary>
    /// <returns></returns>
    Task WaitForCompletionAsync(CancellationToken cancellationToken);
}
