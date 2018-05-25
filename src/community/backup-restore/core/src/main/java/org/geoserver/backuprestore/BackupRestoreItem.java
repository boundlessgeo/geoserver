/* (c) 2016 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.backuprestore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CatalogException;
import org.geoserver.catalog.ValidationResult;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.catalog.impl.CoverageStoreInfoImpl;
import org.geoserver.catalog.impl.StoreInfoImpl;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.config.util.XStreamPersisterFactory;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.util.Assert;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;

/**
 * @author Alessio Fabiani, GeoSolutions S.A.S.
 *
 */
public abstract class BackupRestoreItem<T> {

    /**
     * logger
     */
    private static final Logger LOGGER = Logging.getLogger(BackupRestoreItem.class);
    
    protected Backup backupFacade;

    private Catalog catalog;

    protected XStreamPersister xstream;

    private XStream xp;

    private boolean isNew;

    private AbstractExecutionAdapter currentJobExecution;

    private boolean dryRun;

    private boolean bestEffort;

    private XStreamPersisterFactory xStreamPersisterFactory;

    private Filter filter;

    public static final String ENCRYPTED_FIELDS_KEY = "backupRestoreParameterizedFields";

    public BackupRestoreItem(Backup backupFacade, XStreamPersisterFactory xStreamPersisterFactory) {
        this.backupFacade = backupFacade;
        this.xStreamPersisterFactory = xStreamPersisterFactory;
    }

    /**
     * @return the xStreamPersisterFactory
     */
    public XStreamPersisterFactory getxStreamPersisterFactory() {
        return xStreamPersisterFactory;
    }

    /**
     * @return the xp
     */
    public XStream getXp() {
        return xp;
    }

    /**
     * @param xp the xp to set
     */
    public void setXp(XStream xp) {
        this.xp = xp;
    }

    /**
     * @return the catalog
     */
    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * @return the isNew
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * @return the currentJobExecution
     */
    public AbstractExecutionAdapter getCurrentJobExecution() {
        return currentJobExecution;
    }

    /**
     * @return the dryRun
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * @return the bestEffort
     */
    public boolean isBestEffort() {
        return bestEffort;
    }

    /**
     * @return the filter
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * @param filter the filter to set
     */
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @BeforeStep
    public void retrieveInterstepData(StepExecution stepExecution) {
        // Accordingly to the running execution type (Backup or Restore) we
        // need to validate resources against the official GeoServer Catalog (Backup)
        // or the temporary one (Restore).
        //
        // For restore operations the order matters.
        JobExecution jobExecution = stepExecution.getJobExecution();
        this.xstream = xStreamPersisterFactory.createXMLPersister();
        if (backupFacade.getRestoreExecutions() != null
                && !backupFacade.getRestoreExecutions().isEmpty()
                && backupFacade.getRestoreExecutions().containsKey(jobExecution.getId())) {
            this.currentJobExecution = backupFacade.getRestoreExecutions()
                    .get(jobExecution.getId());
            this.catalog = ((RestoreExecutionAdapter) currentJobExecution).getRestoreCatalog();
            this.isNew = true;
        } else {
            this.currentJobExecution = backupFacade.getBackupExecutions().get(jobExecution.getId());
            this.catalog = backupFacade.getCatalog();
            this.xstream.setExcludeIds();
            this.isNew = false;
        }

        Assert.notNull(this.catalog, "catalog must be set");

        this.xstream.setCatalog(this.catalog);
        this.xstream.setReferenceByName(true);
        this.xp = this.xstream.getXStream();

        Assert.notNull(this.xp, "xStream persister should not be NULL");

        JobParameters jobParameters = this.currentJobExecution.getJobParameters();

        boolean parameterizePasswords = Boolean.parseBoolean(
            jobParameters.getString(Backup.PARAM_PARAMETERIZE_PASSWDS, "false"));
        String replacementSeparator = jobParameters.getString(Backup.REPLACEMENT_SEPARATOR, ",");

        if (parameterizePasswords) {

            //here we set some customized XML handling code. For backups, we add a converter that tokenizes
            //outgoing passwords. for restores, a handler for those tokenized backups.
            if (!isNew) {

            } else {
                String concatenatedPasswordTokens = jobParameters.getString(Backup.PARAM_PASSWORD_TOKENS);
                Map<String, String> passwordTokens = parseConcatenatedPasswordTokens(
                    concatenatedPasswordTokens, replacementSeparator);
                this.xp.registerLocalConverter(StoreInfoImpl.class, "connectionParameters",
                    new TokenizedFieldConverter(passwordTokens, xstream.getXStream().getMapper()));
                this.xp.registerLocalConverter(CoverageStoreInfoImpl.class, "url", new TokenizedValueConverter(passwordTokens));
            }


        }

        this.dryRun = Boolean
                .parseBoolean(jobParameters.getString(Backup.PARAM_DRY_RUN_MODE, "false"));
        this.bestEffort = Boolean
                .parseBoolean(jobParameters.getString(Backup.PARAM_BEST_EFFORT_MODE, "false"));

        final String cql = jobParameters.getString("filter", null);
        if (cql != null && cql.contains("name")) {
            try {
                this.filter = ECQL.toFilter(cql);
            } catch (CQLException e) {
                throw new IllegalArgumentException("Filter is not valid!", e);
            }
        } else {
            this.filter = null;
        }
        
        initialize(stepExecution);
    }

    private Map<String, String> parseConcatenatedPasswordTokens(String concatenatedPasswordTokens,
        String replacementSeparator) {
        Map<String, String> tokenMap = new HashMap<>();
        if (concatenatedPasswordTokens != null) {
            Arrays.stream(concatenatedPasswordTokens.split(replacementSeparator)).forEach(tokenPair -> {
                String[] tokenPairSplit = tokenPair.split("=");
                if (tokenPairSplit.length == 2) {
                    tokenMap.put(tokenPairSplit[0], tokenPairSplit[1]);
                }
            });
        }
        return tokenMap;
    }

    /**
     * 
     */
    protected abstract void initialize(StepExecution stepExecution);

    /**
     * @param result
     * @param e
     * @return
     * @throws Exception
     */
    protected boolean logValidationExceptions(ValidationResult result, Exception e) throws Exception {
        CatalogException validationException = new CatalogException(e);
        if(!isBestEffort()) {
            if (result != null) {
                result.throwIfInvalid();
            } else {
                throw e;
            }
        }

        if(!isBestEffort()) {
            getCurrentJobExecution().addFailureExceptions(Arrays.asList(validationException));
        }
        return false;
    }

    /**
     * @param resource
     */
    protected boolean logValidationExceptions(T resource, Throwable e) {
        CatalogException validationException = e != null ? new CatalogException(e) : 
            new CatalogException("Invalid resource: " + resource);
        if (!isBestEffort()) {
            getCurrentJobExecution().addFailureExceptions(Arrays.asList(validationException));
            throw validationException;
        } else {
            getCurrentJobExecution().addWarningExceptions(Arrays.asList(validationException));
        }
        return false;
    }
    
    /**
     * @param resource
     * @param ws
     * @return
     */
    protected boolean filteredResource(T resource, WorkspaceInfo ws, boolean strict) {
        // Filtering Resources
        if (getFilter() != null) {
            if ((strict && ws == null) || (ws != null && !getFilter().evaluate(ws))) {
                LOGGER.info("Skipped filtered resource: " + resource);
                return true;
            }
        }

        return false;
    }

    public Converter getTokenizedPasswordConverter() {
        return new Converter() {
            @Override
            public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
            }

            @Override
            public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
                String tokenizedValue = reader.getValue();
                String replacedValue = this.replaceTokenizedValue(tokenizedValue);
                return replacedValue;
            }

            private String replaceTokenizedValue(String tokenizedValue) {
                return "foo";
            }

            @Override
            public boolean canConvert(Class type) {
                if (BackupRestoreItem.class.equals(type)) {
                    return true;
                } else {
                    return false;
                }
            }
        };
    }

    private static class TokenizedFieldConverter extends MapConverter {

        Map<String, String> properties = new HashMap<>();

        public TokenizedFieldConverter(Map<String, String> passwordTokens, Mapper mapper) {
            super(mapper);
            this.properties = passwordTokens;
        }

        @Override
        public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
            context.convertAnother(source);
        }

        @Override
        protected void populateMap(
                HierarchicalStreamReader reader, UnmarshallingContext context, Map map) {

            while (reader.hasMoreChildren()) {
                reader.moveDown();

                // we support four syntaxes here:
                // 1) <key>value</key>
                // 2) <key><type>value</type></key>
                // 3) <entry key="">value</entry>
                // 4) <entry>
                //      <type>key</type>
                //      <type>value</type>
                //    </entry>
                String key = reader.getNodeName();
                Object value = null;
                if ("entry".equals(key)) {
                    if (reader.getAttribute("key") != null) {
                        // this is case 3
                        key = reader.getAttribute("key");
                        value = reader.getValue();
                    } else if (reader.hasMoreChildren()) {
                        // this is case 4
                        reader.moveDown();
                        key = reader.getValue();
                        reader.moveUp();
                        reader.moveDown();
                        value = reader.getValue();
                        reader.moveUp();
                    }

                } else {
                    boolean old = false;
                    if (reader.hasMoreChildren()) {
                        // this handles case 2
                        old = true;
                        reader.moveDown();
                    }

                    value = readItem(reader, context, map);

                    if (old) {
                        reader.moveUp();
                    }
                }

                if (value instanceof String) {
                    value = replaceTokenizedValue((String)value);
                }
                map.put(key, value);
                reader.moveUp();
            }
        }

        private String replaceTokenizedValue(String tokenizedValue) {
            return properties.getOrDefault(tokenizedValue, tokenizedValue);
        }

        @Override
        public boolean canConvert(Class type) {
            return Map.class.isAssignableFrom(type);
        }
    }

    private static class TokenizedValueConverter implements Converter {
        private final Map<String, String> tokenReplacements;

        public TokenizedValueConverter(Map<String, String> passwordTokens) {
            this.tokenReplacements = passwordTokens;
        }

        @Override
        public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {

        }

        @Override
        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
            String value = reader.getValue();
            return tokenReplacements.getOrDefault(value, value);
        }

        @Override
        public boolean canConvert(Class type) {
            return String.class.isAssignableFrom(type);
        }
    }
}