/*
 * Licensed to the Apache Software Foundation(ASF)under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License,Version2.0(the
 * "License");you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS"BASIS,WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND,either express or implied.See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.maven.plugin.announcer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.search.Query;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.ArtifactInfoGroup;
import org.apache.maven.index.GroupedSearchRequest;
import org.apache.maven.index.GroupedSearchResponse;
import org.apache.maven.index.Grouping;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.MAVEN;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.expr.SourcedSearchExpression;
import org.apache.maven.index.search.grouping.GAGrouping;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.apache.maven.index.updater.WagonHelper;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.util.StringUtils;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Main
    implements AutoCloseable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );

    private static final Option helpOpt =
        Option.builder( "?" ).longOpt( "help" ).desc( "Print this help message" ).build();

    private final Options options;

    private final CommandLine commandLine;

    private final DefaultPlexusContainer plexusContainer;

    private final Indexer indexer;

    private final IndexUpdater indexUpdater;

    private final Wagon httpWagon;

    private IndexingContext centralContext;

    private DB db;

    public Main( String... args )
        throws Exception
    {
        options = new Options();
        for ( Option option : Arrays.asList( helpOpt ) )
        {
            options.addOption( option );
        }
        CommandLineParser parser = new DefaultParser();
        try
        {
            commandLine = parser.parse( options, args );
        }
        catch ( ParseException e )
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "announcer", options );
            throw e;
        }
        final DefaultContainerConfiguration config = new DefaultContainerConfiguration();
        config.setClassPathScanning( PlexusConstants.SCANNING_INDEX );
        this.plexusContainer = new DefaultPlexusContainer( config );

        // lookup the indexer components from plexus
        this.indexer = plexusContainer.lookup( Indexer.class );
        this.indexUpdater = plexusContainer.lookup( IndexUpdater.class );
        // lookup wagon used to remotely fetch index
        this.httpWagon = plexusContainer.lookup( Wagon.class, "http" );
    }

    public static void main( String[] args )
        throws Exception
    {
        try (Main main = new Main( args ))
        {
            main.run();
        }
    }

    public void run()
        throws ComponentLookupException, IOException
    {
        for ( Option o : commandLine.getOptions() )
        {
            if ( o.equals( helpOpt ) )
            {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "announcer", options );
                return;
            }
        }
        File centralLocalCache = new File( "central-cache" );
        File centralIndexDir = new File( "central-index" );
        db = DBMaker.fileDB( new File( "work-db" ) ).transactionEnable().fileMmapEnableIfSupported().make();

        // Creators we want to use (search for fields it defines)
        List<IndexCreator> indexers = new ArrayList<IndexCreator>();
        indexers.add( plexusContainer.lookup( IndexCreator.class, "min" ) );
        indexers.add( plexusContainer.lookup( IndexCreator.class, "maven-plugin" ) );

        // Create context for central repository index
        centralContext =
            indexer.createIndexingContext( "central-context", "central", centralLocalCache, centralIndexDir,
                                           "http://repo1.maven.org/maven2", null, true, true, indexers );

        httpWagon.addTransferListener( new AbstractTransferListener()
        {
            public void transferStarted( TransferEvent transferEvent )
            {
                LOGGER.info( "Downloading {}", transferEvent.getResource().getName() );
            }

            public void transferProgress( TransferEvent transferEvent, byte[] buffer, int length )
            {
            }

            public void transferCompleted( TransferEvent transferEvent )
            {
                LOGGER.info( "Done" );
            }
        } );
        long nextUpdate = System.nanoTime();
        boolean first = true;

        try
        {
            while ( true )
            {
                boolean canAnnounce = false;
                if ( first || nextUpdate - System.nanoTime() <= 0 )
                {
                    if ( updateIndex() )
                    {
                        canAnnounce = true;
                    }
                    canAnnounce |= first;
                    nextUpdate = System.nanoTime() + TimeUnit.HOURS.toNanos( 1 );
                    first = false;
                }
                if ( canAnnounce )
                {
                    findAndAnnounce();
                }
                long sleep = nextUpdate - System.nanoTime();
                if ( sleep >= TimeUnit.MINUTES.toNanos( 1 ) )
                {
                    LOGGER.info( "Sleeping for {} minutes", TimeUnit.NANOSECONDS.toMinutes( sleep ) );
                }
                else if ( sleep >= TimeUnit.SECONDS.toNanos( 1 ) )
                {
                    LOGGER.info( "Sleeping for {} seconds", TimeUnit.NANOSECONDS.toSeconds( sleep ) );
                }
                else if ( sleep > 0 )
                {
                    LOGGER.info( "Sleeping for less than a second" );
                }
                if ( sleep > 0 )
                {
                    Thread.sleep( TimeUnit.NANOSECONDS.toMillis( sleep ), Math.max( 0, (int) ( sleep % 1000000L ) ) );
                }
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.info( "Interrupted", e );
        }
    }

    private void findAndAnnounce()
        throws IOException, InterruptedException
    {
        Query q = indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "maven-plugin" ) );
        Grouping g = new GAGrouping( ArtifactInfo.VERSION_COMPARATOR.reversed() );

        GroupedSearchResponse response = indexer.searchGrouped( new GroupedSearchRequest( q, g, centralContext ) );

        Atomic.Boolean seeded = db.atomicBoolean( "seeded" ).createOrOpen();
        Set<String> knownPlugins = db.hashSet( "knownPlugins", Serializer.STRING ).createOrOpen();
        Set<String> knownVersions = db.hashSet( "knownVersions", Serializer.STRING ).createOrOpen();

        boolean canAnnounce = Boolean.TRUE.equals( seeded.get() );
        int urlLength = 25;
        Twitter twitter;
        if ( canAnnounce )
        {
            String consumerKey = System.getenv( "CONSUMER_KEY" );
            String consumerSecret = System.getenv( "CONSUMER_SECRET" );
            String accessToken = System.getenv( "ACCESS_TOKEN" );
            String accessTokenSecret = System.getenv( "ACCESS_TOKEN_SECRET" );
            if ( StringUtils.isBlank( consumerKey ) || StringUtils.isBlank( consumerSecret ) || StringUtils.isBlank(
                accessToken ) || StringUtils.isBlank( accessTokenSecret ) )
            {
                LOGGER.warn(
                    "You must provide all four environment variables (CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN"
                        + " and ACCESS_TOKEN_SECRET) to enable tweeting" );
                twitter = null;
            }
            else
            {
                ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setOAuthConsumerKey( consumerKey );
                cb.setOAuthConsumerSecret( consumerSecret );
                cb.setOAuthAccessToken( accessToken );
                cb.setOAuthAccessTokenSecret( accessTokenSecret );
                twitter = new TwitterFactory( cb.build() ).getInstance();
                try
                {
                    urlLength = twitter.getAPIConfiguration().getShortURLLengthHttps();
                }
                catch ( TwitterException e )
                {
                    LOGGER.warn( "Could not get the Twitter API configuration", e );
                }
            }
        }
        else
        {
            twitter = null;
        }

        try
        {
            int pluginCount = 0;
            int announceCount = 0;
            int versionCount = 0;
            LOGGER.info( canAnnounce ? "Looking for plugins to announce" : "Recording initial list of plugins" );
            for ( Map.Entry<String, ArtifactInfoGroup> entry : response.getResults().entrySet() )
            {
                pluginCount++;
                versionCount += entry.getValue().getArtifactInfos().size();
                ArtifactInfo ai = entry.getValue().getArtifactInfos().iterator().next();
                String ga = ai.groupId + ":" + ai.artifactId;
                if ( !knownPlugins.contains( ga ) )
                {
                    if ( canAnnounce )
                    {
                        if ( ai.name != null )
                        {
                            announceCount++;
                            announce( "New plugin!", urlLength, twitter, ai );
                        }
                    }
                    knownPlugins.add( ga );
                    for ( ArtifactInfo a : entry.getValue().getArtifactInfos() )
                    {
                        knownVersions.add( ga + ":" + a.version );
                    }
                }
                else
                {
                    for ( ArtifactInfo a : entry.getValue().getArtifactInfos() )
                    {
                        String gav = ga + ":" + a.version;
                        if ( !knownVersions.contains( gav ) )
                        {
                            if ( canAnnounce )
                            {
                                announceCount++;
                                announce( "New release!", urlLength, twitter, a );
                            }
                            knownVersions.add( gav );
                        }
                    }
                }
                if ( pluginCount % 100 == 0 )
                {
                    LOGGER.info( "Found {} plugins with a total of {} versions so far", pluginCount, versionCount );
                    db.commit();
                }
            }
            LOGGER.info( "Processed {} plugins with a total of {} versions and announced {} releases", pluginCount,
                         versionCount, announceCount );
            seeded.set( true );
        }
        finally
        {
            db.commit();
        }
    }

    private void announce( String prefix, int urlLength, Twitter twitter, ArtifactInfo ai )
        throws InterruptedException
    {
        String ga = ai.groupId + ":" + ai.artifactId;
        String url = url( ai );
        StringBuilder tweet = new StringBuilder( 140 );
        String shortVersion = StringUtils.abbreviate( ai.version, 50 );
        int count = prefix.length() + " ".length() + " version ".length() + shortVersion.length() + " at ".length()
            + " ".length() + Math.min( url.length(), urlLength );
        int remaining = 140 - count;
        String name = StringUtils.defaultString( ai.name, ai.artifactId );
        int index1 = name.indexOf( "${" );
        int index2 = index1 == -1 ? -1 : name.indexOf( "}", index1 + 2 );
        StringBuilder nameBuilder = new StringBuilder( name.length() );
        int last = 0;
        while ( index1 != -1 && index2 != -1 )
        {
            nameBuilder.append( name.substring( last, index1 ) );
            String expr = name.substring( index1 + 2, index2 );
            switch ( expr.trim() )
            {
                case "groupId":
                case "project.groupId":
                case "pom.groupId":
                    nameBuilder.append( ai.groupId );
                    break;
                case "artifactId":
                case "project.artifactId":
                case "pom.artifactId":
                    nameBuilder.append( ai.artifactId );
                    break;
                default:
                    nameBuilder.append( "${" );
                    nameBuilder.append( expr );
                    nameBuilder.append( "}" );
                    break;
            }
            last = index2 + 1;
            index1 = name.indexOf( "${", last );
            index2 = index1 == -1 ? -1 : name.indexOf( "}", index1 + 2 );
        }
        if ( last > 0 )
        {
            nameBuilder.append( name.substring( last ) );
            name = nameBuilder.toString();
        }
        int gaLength = Math.max( 4, Math.min( ga.length(), Math.min( remaining / 2, remaining - name.length() ) ) );
        remaining = Math.max( 4, remaining - gaLength );
        tweet.append( prefix );
        tweet.append( " " );
        tweet.append( StringUtils.abbreviate( name, remaining ) );
        tweet.append( " version " );
        tweet.append( shortVersion );
        tweet.append( " at " );
        tweet.append( StringUtils.abbreviate( ga, gaLength ) );
        tweet.append( " " );
        tweet.append( url );
        if ( twitter != null )
        {
            StatusUpdate status = new StatusUpdate( tweet.toString() );
            status.setDisplayCoordinates( false );
            status.setPossiblySensitive( false );
            try
            {
                twitter.updateStatus( status );
            }
            catch ( TwitterException e )
            {
                LOGGER.warn( "Could not announce {}", ai, e );
            }
            // always wait 1 minute after tweeting
            TimeUnit.MINUTES.sleep( 1 );
        }
        else
        {
            LOGGER.info( "Tweet: {}", tweet );
        }
    }

    private boolean updateIndex()
        throws IOException
    {
        LOGGER.info( "Updating Index..." );
        // Create ResourceFetcher implementation to be used with IndexUpdateRequest
        // Here, we use Wagon based one as shorthand, but all we need is a ResourceFetcher implementation
        ResourceFetcher resourceFetcher = new WagonHelper.WagonFetcher( httpWagon, null, null, null );

        Date centralContextCurrentTimestamp = centralContext.getTimestamp();
        IndexUpdateRequest updateRequest = new IndexUpdateRequest( centralContext, resourceFetcher );
        IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex( updateRequest );
        if ( updateResult.isFullUpdate() )
        {
            LOGGER.info( "Full update completed" );
            return true;
        }
        else if ( updateResult.getTimestamp().equals( centralContextCurrentTimestamp ) )
        {
            LOGGER.info( "No update needed, index is up to date!" );
            return false;
        }
        else
        {
            LOGGER.info( "Incremental update completed, change covered {} - {} period.", centralContextCurrentTimestamp,
                         updateResult.getTimestamp() );
            return true;
        }
    }

    public String url( ArtifactInfo ai )
    {
        return "https://search.maven.org/#artifactdetails%7C" + ai.groupId + "%7C" + ai.artifactId + "%7C" + ai.version
            + "%7Cpom";
    }

    @Override
    public void close()
        throws Exception
    {
        if ( db != null )
        {
            db.close();
        }
    }
}
