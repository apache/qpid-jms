package org.apache.qpid.jms.test.testpeer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompositeAmqpPeerRunnable implements AmqpPeerRunnable
{
    private List<AmqpPeerRunnable> _list = new ArrayList<AmqpPeerRunnable>();

    public CompositeAmqpPeerRunnable(AmqpPeerRunnable... amqpPeerRunnables)
    {
        _list.addAll(Arrays.asList(amqpPeerRunnables));
    }

    public void add(AmqpPeerRunnable amqpPeerRunnable)
    {
        _list.add(amqpPeerRunnable);
    }

    @Override
    public void run()
    {
        for(AmqpPeerRunnable r : _list)
        {
            r.run();
        }
    }
}
