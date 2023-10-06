// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using AWS.Messaging.Configuration;
using AWS.Messaging.Services;

namespace AWS.Messaging;

/// <summary>
/// This represents the input that is sent to the <see cref="IMessageManager"/>'s task scheduler
/// </summary>
public class MessageProcessingTask
{
    /// <inheritdoc cref="MessageEnvelope"/>
    public MessageEnvelope MessageEnvelope { get; set; }

    /// <inheritdoc cref="SubscriberMapping"/>
    public SubscriberMapping SubscriberMapping { get; set; }

    /// <summary>
    /// The cancellation token to stop the message processing.
    /// </summary>
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    /// Creates an instance of <see cref="MessageProcessingTask"/>
    /// </summary>
    /// <param name="messageEnvelope">The message to start processing</param>
    /// <param name="subscriberMapping">The mapping between the message's type and its handler</param>
    /// <param name="cancellationToken">Optional token to cancel the message processing</param>
    public MessageProcessingTask(MessageEnvelope messageEnvelope, SubscriberMapping subscriberMapping, CancellationToken cancellationToken = default)
    {
        MessageEnvelope = messageEnvelope;
        SubscriberMapping = subscriberMapping;
        CancellationToken = cancellationToken;
    }
}
