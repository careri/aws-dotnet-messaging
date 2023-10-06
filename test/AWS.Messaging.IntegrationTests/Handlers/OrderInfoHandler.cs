// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Threading;
using System.Threading.Tasks;
using AWS.Messaging.IntegrationTests.Models;

namespace AWS.Messaging.IntegrationTests.Handlers;

public class OrderInfoHandler : IMessageHandler<OrderInfo>
{
    private readonly TempStorage<OrderInfo> _tempStorage;

    public OrderInfoHandler(TempStorage<OrderInfo> tempStorage)
    {
        _tempStorage = tempStorage;
    }

    public Task<MessageProcessStatus> HandleAsync(MessageEnvelope<OrderInfo> messageEnvelope, CancellationToken token = default)
    {
        _tempStorage.Messages.Add(messageEnvelope);

        return Task.FromResult(MessageProcessStatus.Success());
    }
}

public class OrderInfoHandler_Fifo : IMessageHandler<OrderInfo>
{
    private readonly TempStorage<OrderInfo> _tempStorage;

    private readonly Random _rnd = new();

    public OrderInfoHandler_Fifo(TempStorage<OrderInfo> tempStorage)
    {
        _tempStorage = tempStorage;
    }

    public async Task<MessageProcessStatus> HandleAsync(MessageEnvelope<OrderInfo> messageEnvelope, CancellationToken token = default)
    {
        var delayInMilliseconds = _rnd.Next(1000, 5001);
        await Task.Delay(delayInMilliseconds, token);

        _tempStorage.MessagesFifo.Enqueue(messageEnvelope);

        return await Task.FromResult(MessageProcessStatus.Success());
    }
}
