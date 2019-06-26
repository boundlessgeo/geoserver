/* (c) 2016 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geogig.geoserver.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import com.google.common.collect.Lists;
import org.locationtech.geogig.repository.*;
import org.locationtech.geogig.storage.*;

/** Specialized RepositoryResolver for GeoServer manager Geogig Repositories. */
public class GeoServerGeoGigRepositoryResolver implements RepositoryResolver {

    /**
     * System property key for specifying disabled resolvers.
     */
    private static final String DISABLE_RESOLVER_KEY = "disableResolvers";

    /**
     * List of disabled resolver class names.
     */
    private static final List<String> DISABLED_RESOLVERS = new ArrayList<>();

    public static final String GEOSERVER_URI_SCHEME = "geoserver";

    public static final int SCHEME_LENGTH = GEOSERVER_URI_SCHEME.length() + "://".length();

    public static String getURI(String repoName) {
        return String.format("%s://%s", GEOSERVER_URI_SCHEME, repoName);
    }

    @Override
    public boolean canHandle(URI repoURI) {
        return repoURI != null && canHandleURIScheme(repoURI.getScheme());
    }

    @Override
    public boolean canHandleURIScheme(String scheme) {
        return scheme != null && GEOSERVER_URI_SCHEME.equals(scheme);
    }

    @Override
    public boolean repoExists(URI repoURI) throws IllegalArgumentException {
        String name = getName(repoURI);
        RepositoryManager repoMgr = RepositoryManager.get();
        // get the repo by name
        RepositoryInfo repoInfo = repoMgr.getByRepoName(name);
        return repoInfo != null;
    }

    @Override
    public String getName(URI repoURI) {
        checkArgument(canHandle(repoURI), "Not a GeoServer GeoGig repository URI: %s", repoURI);
        // valid looking URI, strip the name part out and get everything after the scheme
        // "geoserver" and the "://"
        String name = repoURI.toString().substring(SCHEME_LENGTH);
        // if it's empty, they didn't provide a name or Id
        checkArgument(!Strings.isNullOrEmpty(name), "No GeoGig repository Name or ID specified");
        return name;
    }

    @Override
    public void initialize(URI repoURI, Context repoContext) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    public ConfigDatabase getConfigDatabase(URI repoURI, Context repoContext, boolean rootUri) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Repository open(URI repositoryLocation) throws RepositoryConnectionException {
        String name = getName(repositoryLocation);
        // get a handle to the RepositoryManager
        RepositoryManager repoMgr = RepositoryManager.get();
        // get the repo by name
        RepositoryInfo info = repoMgr.getByRepoName(name);
        if (info != null) {
            // get the native RepositoryResolver for the location and open it directly
            // Using the RepositryManager to get the repo would cause the repo to be managed by the
            // RepositoryManager,
            // when this repo should be managed by the DataStore. The DataStore will close this repo
            // instance when
            // GeoServer decides to dispose the DataStore.
            Repository repo = load(info.getLocation());
            checkState(
                    repo.isOpen(), "RepositoryManager returned a closed repository for %s", name);
            return repo;
        } else {
            // didn't find a repo
            RepositoryConnectionException rce =
                    new RepositoryConnectionException(
                            "No GeoGig repository found with NAME or ID: " + name);
            throw rce;
        }
    }

    @Override
    public Repository open(URI repositoryLocation, Hints hints) throws RepositoryConnectionException {
        return null;
    }

    @Override
    public boolean delete(URI repositoryLocation) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConfigDatabase resolveConfigDatabase(URI repoURI, Context repoContext, boolean rootUri) {
        return null;
    }

    @Override
    public ObjectDatabase resolveObjectDatabase(URI repoURI, Hints hints) {
        return null;
    }

    @Override
    public IndexDatabase resolveIndexDatabase(URI repoURI, Hints hints) {
        return null;
    }

    @Override
    public RefDatabase resolveRefDatabase(URI repoURI, Hints hints) {
        return null;
    }

    @Override
    public ConflictsDatabase resolveConflictsDatabase(URI repoURI, Hints hints) {
        return null;
    }

    @Override
    public URI getRootURI(URI repoURI) {
        return null;
    }

    @Override
    public URI buildRepoURI(URI rootRepoURI, String repoName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> listRepoNamesUnderRootURI(URI rootRepoURI) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * @param repositoryLocation the URI with the location of the repository to load
     * @return a {@link Repository} loaded from the given URI, already {@link Repository#open()
     *         open}
     * @throws IllegalArgumentException if no registered {@link RepositoryResolver} implementation
     *         can load the repository at the given location
     * @throws RepositoryConnectionException if the repository can't be opened
     */
    public static Repository load(URI repositoryLocation) throws RepositoryConnectionException {
        RepositoryResolver initializer = lookup(repositoryLocation);
        Repository repository = initializer.open(repositoryLocation);
        return repository;
    }
    /**
     * Finds a {@code RepositoryResolver} that {@link #canHandle(URI) can handle} the given URI, or
     * throws an {@code IllegalArgumentException} if no such initializer can be found.
     * <p>
     * The lookup method uses the standard JAVA SPI (Service Provider Interface) mechanism, by which
     * all the {@code META-INF/services/org.locationtech.geogig.repository.RepositoryResolver} files
     * in the classpath will be scanned for fully qualified names of implementing classes.
     *
     * @param repoURI Repository location URI
     * @return A RepositoryResolver that can handle the supplied URI.
     * @throws IllegalArgumentException if no repository resolver is found capable of handling the
     *         given URI
     */
    public static RepositoryResolver lookup(URI repoURI) throws IllegalArgumentException {

        Preconditions.checkNotNull(repoURI, "Repository URI is null");

        List<RepositoryResolver> resolvers = lookupResolvers();
        RepositoryResolver resolver = null;
        for (RepositoryResolver resolverImpl : resolvers) {
            final String resolverClassName = resolverImpl.getClass().getName();
            if (!DISABLED_RESOLVERS.contains(resolverClassName)
                    && resolverImpl.canHandle(repoURI)) {
                resolver = resolverImpl;
                break;
            }
        }
        Preconditions.checkArgument(resolver != null,
                "No repository initializer found capable of handling this kind of URI: %s",
                repoURI.getScheme());
        return resolver;
    }

    public static List<RepositoryResolver> lookupResolvers() {

        List<RepositoryResolver> resolvers;
        resolvers = Lists.newArrayList(ServiceLoader.load(RepositoryResolver.class).iterator());
        if (resolvers.isEmpty()) {
            ClassLoader classLoader = RepositoryResolver.class.getClassLoader();
            ServiceLoader<RepositoryResolver> serviceLoader = ServiceLoader
                    .load(RepositoryResolver.class, classLoader);
            resolvers = Lists.newArrayList(serviceLoader.iterator());
        }
        return resolvers;
    }
}
