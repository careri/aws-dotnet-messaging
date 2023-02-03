// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

namespace AWS.Messaging.Configuration;

/// <summary>
/// EventBridge implementation of <see cref="IMessagePublisherConfiguration"/>
/// </summary>
public class EventBridgePublisherConfiguration : IMessagePublisherConfiguration
{
    private readonly string _eventBusUrl;

    /// <summary>
    /// Creates an instance of <see cref="EventBridgePublisherConfiguration"/>.
    /// </summary>
    /// <param name="eventBusUrl">The EventBus URL</param>
    public EventBridgePublisherConfiguration(string eventBusUrl)
    {
        if (string.IsNullOrEmpty(eventBusUrl))
            throw new InvalidPublisherEndpointException("The Event Bus URL cannot be empty.");

        _eventBusUrl = eventBusUrl;
    }

    /// <summary>
    /// Retrieves the EventBridge Event Bus URL which the publisher will use to route the message.
    /// </summary>
    /// <returns>SQS Queue URL</returns>
    public string GetPublisherEndpoint()
    {
        return _eventBusUrl;
    }
}
