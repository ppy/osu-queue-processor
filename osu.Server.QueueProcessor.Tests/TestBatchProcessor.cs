// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;

namespace osu.Server.QueueProcessor.Tests;

public class TestBatchProcessor : QueueProcessor<FakeData>
{
    public TestBatchProcessor()
        : base(new QueueConfiguration
        {
            InputQueueName = "test-batch",
            BatchSize = 5,
        })
    {
    }

    protected override void ProcessResults(IEnumerable<FakeData> items)
    {
        foreach (var item in items)
        {
            Received?.Invoke(item);
        }
    }

    public Action<FakeData>? Received;
}
