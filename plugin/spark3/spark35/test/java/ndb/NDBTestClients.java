/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.VastClient;

/**
 * Test access to the VAST client NDB hands to every catalog of the JVM, so
 * that a test can put a spied client in place before a session's catalog
 * initializes and restore the original one afterwards.
 */
public final class NDBTestClients
{
    private NDBTestClients()
    {
    }

    public static VastClient current()
    {
        return NDBCommon.vastClient;
    }

    public static void set(VastClient client)
    {
        NDBCommon.vastClient = client;
    }
}
