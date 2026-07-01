package com.striim.util;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
//import java.nio.ByteBuffer;
//import java.nio.Buffer;
//import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;

//import com.sun.org.apache.xpath.internal.operations.Bool;
//import com.sun.org.apache.xpath.internal.operations.Bool;
import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.event.Event;
import com.webaction.metaRepository.MetadataRepository;
//import com.webaction.metering.common.EnumTypes;
//import com.webaction.proc.BaseProcess;
import com.webaction.proc.SourceProcess;
import com.webaction.proc.events.WAEvent;
import com.webaction.runtime.Context;
//import com.webaction.runtime.LagMarker;
import com.webaction.runtime.TypeGenerator;
import com.webaction.runtime.compiler.Compiler;
import com.webaction.runtime.compiler.TypeDefOrName;
import com.webaction.runtime.compiler.TypeField;
import com.webaction.runtime.compiler.TypeName;
import com.webaction.runtime.compiler.Compiler.ExecutionCallback;
import com.webaction.runtime.compiler.stmts.CreateTypeStmt;
import com.webaction.runtime.compiler.stmts.Stmt;
//import com.webaction.runtime.containers.ITaskEvent;
//import com.webaction.runtime.containers.TaskEvent;
import com.webaction.uuid.UUID;
import com.webaction.runtime.meta.MetaInfo;
import com.webaction.runtime.meta.MetaInfo.Type;
import com.webaction.runtime.components.EntityType;

import com.webaction.uuid.AuthToken;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.Seconds;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.appender.RollingFileAppender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import com.webaction.runtime.components.EntityType;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.core.type.TypeReference;

//import com.webaction.metering.persistence.MeteringDb;

@PropertyTemplate(name = "StriimWatcher", version = "4.2.4", type = AdapterType.source, properties = {
        @PropertyTemplateProperty(name = "RepeatInSeconds",                                 type = String.class,  required = true, defaultValue = "300"),
        @PropertyTemplateProperty(name = "IncludeNodeMonitor",                              type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeNodeCluster",                              type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeNodeES",                                   type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeAppDetail",                                type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeAppDescribeDetail",                        type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeAppStatusDetail",                          type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeLee",                                      type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeTableComparisonDetail",                    type = Boolean.class, required = true, defaultValue = "true"),
        @PropertyTemplateProperty(name = "IncludeTableComparisonDetail_SinceLastInterval",  type = Boolean.class, required = true, defaultValue = "true"),
        // Not Required:
        @PropertyTemplateProperty(name = "IncludeCreatedApplicationDetail",                 type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "IncludeDeployedApplicationDetail",                type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "StartOn",                                         type = String.class,  required = false, defaultValue = "2020-12-31T08:00"),
        @PropertyTemplateProperty(name = "EndOn",                                           type = String.class,  required = false, defaultValue = ""),
        @PropertyTemplateProperty(name = "PreserveStriimWatcherPosition",                   type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "AdditionalCommandList",                           type = String.class,  required = false, defaultValue = ""),
        @PropertyTemplateProperty(name = "IncludeComponentDetailsAsOutput",                 type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "IncludeTypeDetailsAsOutput",                      type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "IncludeLogWatcher",                               type = Boolean.class, required = false, defaultValue = "false"),
        @PropertyTemplateProperty(name = "IncludeDataWarehouseDetails",                     type = Boolean.class, required = false, defaultValue = "false")
}, inputType = WAEvent.class)

public class StriimWatcher extends SourceProcess {

    private static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger();
    private static HashMap<String, MetaInfo.Type> typeCache = new HashMap<String, MetaInfo.Type>();

    private static String thisNamespace = "";
    private static String SWhistoryFilename = "StriimWatcher.hist";
    private static String SWLoghistoryFilename = "StriimWatcher_log.hist";
    private static String StriimWatcherAppName = "StriimWatcher";

    private static AuthToken TOKEN = null;

    //Tracking Variables
    private static DateTime currentrun = DateTime.now();
    HashMap<String, String> currentBatch;
    Integer interval = null;
    private Object running = new Object();
    private static DateTime nextStart = null;
    private static DateTime lastStart = null;
    private static Integer totalRuns = 0;
    private static int counter = 0;

    private static DateTime startOn = null;
    private static DateTime endOn = null;
    private static Integer repeatInSeconds = 0;

    // Settings Variables
    private static Boolean includeNodeMonitor = true;
    private static Boolean includeNodeCluster = true;
    private static Boolean includeNodeES = true;
    private static Boolean includeAppDetail = true;
    private static Boolean includeCreatedApplicationDetail = true;
    private static Boolean includeDeployedApplicationDetail = true;
    private static Boolean includeTableComparisonDetail = true;
    private static Boolean includeTableComparisonDetail_SinceLastInterval = true;
    private static Boolean includeAppDescribeDetail = true;
    private static Boolean includeAppStatusDetail = true;
    private static Boolean includeLee = true;
    private static Boolean includeComponentDetailsAsOutput = false;
    private static Boolean includeTypeDetailsAsOutput = false;
    private static Boolean includeLogWatcher = false;
    private static Boolean includeDataWarehouseDetails = false;
    private static Boolean hasAdditionalCommandsToRun = false;
    private static Boolean preserveStriimWatcherPosition = false;
    private static Boolean includeAdvancedAnalytics = false;

    private static LocalDateTime lastSeenLogEntry;

    private static String localNamespace = "";
    private static String logPath = "";

    private static URL savedURL = null;

    static AuthManager am = new AuthManager();
    /**
     * This provided a static list of table names, to prevent typos. These should be referenced rather than anything hardcoded.
     */
    private static class watcherTableName {
        public static final String adapterName = StriimWatcherAppName;
        public static final String adapterDesc = "";
        public static final String prefix = "striim_mon_";
        public static final String mon_namespace = "mon";
        public static final String mon_appdetail = prefix + "appdetail";
        public static final String mon_appdetail_pk = "monid";
        public static final String mon_table_runhistory = prefix + "table_runhistory";
        public static final String mon_table_runhistory_pk = "runid";
        public static final String mon_node_applications = prefix + "node_applications";
        public static final String mon_node_applications_pk = "monappid";
        public static final String mon_node_cluster = prefix + "node_cluster";
        public static final String mon_node_cluster_pk = "monnodeclusterid";
        public static final String mon_node_elasticsearch = prefix + "node_elasticsearch";
        public static final String mon_node_elasticsearch_pk = "monnodesid";
        public static final String mon_table_comparison = prefix + "table_comparison";
        public static final String mon_table_comparison_pk = "tblcompareid";
        public static final String mon_table_comparison_sli = prefix + "table_comparison_sli";
        public static final String mon_table_comparison_sli_pk = "tblcomparesliid";
        public static final String mon_table_datawarehouse_detail = prefix + "datawarehouse_detail";
        public static final String mon_table_datawarehouse_detail_pk = "dwdid";
        public static final String mon_lee = prefix + "lee";
        public static final String mon_lee_pk = "monleeid";
        public static final String mon_appcomponentoutput = prefix + "component_output";
        public static final String mon_appcomponentoutput_pk = "moncomoutid";
        public static final String mon_apptablecolumndetail = prefix + "table_column_detail";
        public static final String mon_apptablecolumndetail_pk = "montblcoldtlid";
        public static final String mon_logwatcher = prefix + "log_watcher";
        public static final String mon_logwatcher_pk = "errorid";

    }

    private static class saveType {
        public static final String srctgtmap_prior = "srctgtmap_prior";
        public static final String lastStart = "lastStart";
        public static final String lastSeenLogEntry = "lastSeenLogEntry";
    }

    private static class TypeNameKey {
        private final String sourceName;
        private final String nameSpace;
        private final String tableName;

        public TypeNameKey(String sourceName, String nameSpace, String tableName) {
            this.sourceName = sourceName;
            this.nameSpace = nameSpace;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypeNameKey key = (TypeNameKey) o;
            return sourceName.equals(key.sourceName) && nameSpace.equals(key.nameSpace) && tableName.equals(key.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceName, nameSpace, tableName);
        }
    }

    private static Map<TypeNameKey, String> typeNames = new HashMap<>();

    private static List<ComponentDescribe> cmpdesc = new ArrayList<>();
    private static class ComponentDescribe {
        String appName;
        String componentName;
        DateTime createdDate;

        // constructor
        public ComponentDescribe(String appName, String componentName, DateTime createdDate) {
            this.appName = appName;
            this.componentName = componentName;
            this.createdDate = createdDate;
        }
    }

    private static List<CommandList> cmndlist = new ArrayList<>();
    private static class CommandList {
        String command;
        String commandType;
        Integer intervalSeconds;
        DateTime nextRun;

        // constructor
        public CommandList(String command, String commandType, Integer intervalSeconds, DateTime nextRun) {
            this.command = command;
            this.commandType = commandType;
            this.intervalSeconds = intervalSeconds;
            this.nextRun = nextRun;
        }
    }

    //Data Mapping Variables
    private static List<SourceEntry> srcmap = new ArrayList<>();
    private static class SourceEntry {
        String appName;
        String componentName;
        String sourceName;
        String src_adapterType;
        String src_databaseProviderType;
        String src_properties;
        int numOfDeletes;
        int numOfDdls;
        int numOfPkupdates;
        int numOfUpdates;
        int numOfInserts;
        String schemaGenerationStatus;
        String dataReadStatus;

        // constructor
        public SourceEntry(String appName, String componentName, String sourceName, String adapterType, String databaseProviderType, String properties, int numOfDeletes, int numOfDdls, int numOfPkupdates, int numOfUpdates, int numOfInserts, String schemaGenerationStatus, String dataReadStatus) {
            this.appName = appName;
            this.componentName = componentName;
            this.sourceName = sourceName;
            this.src_adapterType = adapterType;
            this.src_databaseProviderType = databaseProviderType;
            this.src_properties = properties;
            this.numOfDeletes = numOfDeletes;
            this.numOfDdls = numOfDdls;
            this.numOfPkupdates = numOfPkupdates;
            this.numOfUpdates = numOfUpdates;
            this.numOfInserts = numOfInserts;
            this.schemaGenerationStatus = schemaGenerationStatus;
            this.dataReadStatus = dataReadStatus;
        }
    }

    private static List<TargetEntry> tgtmap = new ArrayList<>();
    private static class TargetEntry {
        String appName;
        String componentName;
        String sourceName;
        String targetName;
        String tgt_adapterType;
        String tgt_databaseProviderType;
        String tgt_properties;
        int numOfDeletes;
        int numOfDdls;
        int numOfPkupdates;
        int numOfUpdates;
        int numOfInserts;
        String lastBatchExecutionTime;
        String lastCommitExecutionTime;

        // constructor
        public TargetEntry(String appName, String componentName, String sourceName, String targetName, String adapterType, String databaseProviderType, String properties, int numOfDeletes, int numOfDdls, int numOfPkupdates, int numOfUpdates, int numOfInserts, String lastBatchExecutionTime, String lastCommitExecutionTime) {
            this.appName = appName;
            this.componentName = componentName;
            this.sourceName = sourceName;
            this.targetName = targetName;
            this.tgt_adapterType = adapterType;
            this.tgt_databaseProviderType = databaseProviderType;
            this.tgt_properties = properties;
            this.numOfDeletes = numOfDeletes;
            this.numOfDdls = numOfDdls;
            this.numOfPkupdates = numOfPkupdates;
            this.numOfUpdates = numOfUpdates;
            this.numOfInserts = numOfInserts;
            this.lastBatchExecutionTime = lastBatchExecutionTime;
            this.lastCommitExecutionTime = lastCommitExecutionTime;
        }
    }

    private static List<TargetDWDetail> tgtdwmap = new ArrayList<>();

    private static class TargetDWDetail {
        String appName;
        String componentName;
        String sourceName;
        String targetName;
        String tgt_adapterType;
        String tgt_databaseProviderType;
        int totalBatchesCreated;
        int partitionPrunedBatches;
        String lastSuccessfulMergeTime;
        int totalBatchesIgnored;
        int maxIntegrationTimeMs;
        double avgInMemCompactionTimeMs;
        double avgBatchSizeMb;
        double avgEventCountPerBatch;
        int minIntegrationTimeMs;
        String mappedSourceTable;
        int totalBatchesQueued;
        double avgCompactionTimeMs;
        double avgWaitingTimeInQueueMs;
        double avgIntegrationTimeMs;
        int totalBatchesUploaded;
        double avgMergeTimeMs;
        double avgStageResourcesMgmtTimeMs;
        double avgUploadTimeMs;

        // Fields from "Last batch info"
        int lastBatchUpdates;
        int lastBatchEventCount;
        int lastBatchInserts;
        double lastBatchMaxRecordSizeMb;
        int lastBatchTotalEventsMerged;
        int lastBatchDdls;
        int lastBatchSequenceNumber;
        double lastBatchSizeMb;
        int lastBatchDeletes;
        int lastBatchPkUpdates;
        int lastBatchAccumulationTimeMs;

        // Fields from "Integration Task Time" (within "Last batch info")
        int lastBatchCompactionTimeMs;
        int lastBatchStageResourcesMgmtTimeMs;
        int lastBatchUploadTimeMs;
        int lastBatchMergeTimeMs;
        int lastBatchInMemCompactionTimeMs;
        int lastBatchPkUpdateTimeMs;
        int lastBatchDdlExecutionTimeMs;
        int lastBatchTotalIntegrationTimeMs;

        // Constructor
        public TargetDWDetail(String appName, String componentName, String sourceName, String targetName, String adapterType, String databaseProviderType,
                              int totalBatchesCreated,
                              int partitionPrunedBatches,
                              String lastSuccessfulMergeTime,
                              int totalBatchesIgnored,
                              int maxIntegrationTimeMs,
                              double avgInMemCompactionTimeMs,
                              double avgBatchSizeMb,
                              double avgEventCountPerBatch,
                              int minIntegrationTimeMs,
                              String mappedSourceTable,
                              int totalBatchesQueued,
                              double avgCompactionTimeMs,
                              double avgWaitingTimeInQueueMs,
                              double avgIntegrationTimeMs,
                              int totalBatchesUploaded,
                              double avgMergeTimeMs,
                              double avgStageResourcesMgmtTimeMs,
                              double avgUploadTimeMs,
                              int lastBatchUpdates,
                              int lastBatchEventCount,
                              int lastBatchInserts,
                              double lastBatchMaxRecordSizeMb,
                              int lastBatchTotalEventsMerged,
                              int lastBatchDdls,
                              int lastBatchSequenceNumber,
                              double lastBatchSizeMb,
                              int lastBatchDeletes,
                              int lastBatchPkUpdates,
                              int lastBatchAccumulationTimeMs,
                              int lastBatchCompactionTimeMs,
                              int lastBatchStageResourcesMgmtTimeMs,
                              int lastBatchUploadTimeMs,
                              int lastBatchMergeTimeMs,
                              int lastBatchInMemCompactionTimeMs,
                              int lastBatchPkUpdateTimeMs,
                              int lastBatchDdlExecutionTimeMs,
                              int lastBatchTotalIntegrationTimeMs
        ) {
            this.appName = appName;
            this.componentName = componentName;
            this.sourceName = sourceName;
            this.targetName = targetName;
            this.tgt_adapterType = adapterType;
            this.tgt_databaseProviderType = databaseProviderType;

            // Initialize all the fields
            this.totalBatchesCreated = totalBatchesCreated;
            this.partitionPrunedBatches = partitionPrunedBatches;
            this.lastSuccessfulMergeTime = lastSuccessfulMergeTime;
            this.totalBatchesIgnored = totalBatchesIgnored;
            this.maxIntegrationTimeMs = maxIntegrationTimeMs;
            this.avgInMemCompactionTimeMs = avgInMemCompactionTimeMs;
            this.avgBatchSizeMb = avgBatchSizeMb;
            this.avgEventCountPerBatch = avgEventCountPerBatch;
            this.minIntegrationTimeMs = minIntegrationTimeMs;
            this.mappedSourceTable = mappedSourceTable;
            this.totalBatchesQueued = totalBatchesQueued;
            this.avgCompactionTimeMs = avgCompactionTimeMs;
            this.avgWaitingTimeInQueueMs = avgWaitingTimeInQueueMs;
            this.avgIntegrationTimeMs = avgIntegrationTimeMs;
            this.totalBatchesUploaded = totalBatchesUploaded;
            this.avgMergeTimeMs = avgMergeTimeMs;
            this.avgStageResourcesMgmtTimeMs = avgStageResourcesMgmtTimeMs;
            this.avgUploadTimeMs = avgUploadTimeMs;
            this.lastBatchUpdates = lastBatchUpdates;
            this.lastBatchEventCount = lastBatchEventCount;
            this.lastBatchInserts = lastBatchInserts;
            this.lastBatchMaxRecordSizeMb = lastBatchMaxRecordSizeMb;
            this.lastBatchTotalEventsMerged = lastBatchTotalEventsMerged;
            this.lastBatchDdls = lastBatchDdls;
            this.lastBatchSequenceNumber = lastBatchSequenceNumber;
            this.lastBatchSizeMb = lastBatchSizeMb;
            this.lastBatchDeletes = lastBatchDeletes;
            this.lastBatchPkUpdates = lastBatchPkUpdates;
            this.lastBatchAccumulationTimeMs = lastBatchAccumulationTimeMs;
            this.lastBatchCompactionTimeMs = lastBatchCompactionTimeMs;
            this.lastBatchStageResourcesMgmtTimeMs = lastBatchStageResourcesMgmtTimeMs;
            this.lastBatchUploadTimeMs = lastBatchUploadTimeMs;
            this.lastBatchMergeTimeMs = lastBatchMergeTimeMs;
            this.lastBatchInMemCompactionTimeMs = lastBatchInMemCompactionTimeMs;
            this.lastBatchPkUpdateTimeMs = lastBatchPkUpdateTimeMs;
            this.lastBatchDdlExecutionTimeMs = lastBatchDdlExecutionTimeMs;
            this.lastBatchTotalIntegrationTimeMs = lastBatchTotalIntegrationTimeMs;
        }
    }

    private static List<SmartAlertDetails> smartAlertDetailsList = new ArrayList<>();

    private static class SmartAlertDetails {
        LocalDateTime date;
        String server;
        String app;
        String logLevel;
        String alertMatched;
        String message;
        String entityName;
        String metricValue;
        String metricUnit;
        String sourceOrTarget;
        String type;

        // Constructor
        public SmartAlertDetails(LocalDateTime date, String server, String app, String logLevel,
                                 String alertMatched, String message, String entityName,
                                 String metricValue, String metricUnit, String sourceOrTarget, String type) {
            this.date = date;
            this.server = server;
            this.app = app;
            this.logLevel = logLevel;
            this.alertMatched = alertMatched;
            this.message = message;
            this.entityName = entityName;
            this.metricValue = metricValue;
            this.metricUnit = metricUnit;
            this.sourceOrTarget = sourceOrTarget;
            this.type = type;
        }
    }

    private static List<ErrorDetails> errorDetailsList = new ArrayList<>();

    private static class ErrorDetails {
        LocalDateTime date;
        String server;
        String app;
        String logLevel;
        String message;
        String contextBufferText; // To store the accumulated context

        // Constructor
        public ErrorDetails(LocalDateTime date, String server, String app, String logLevel, String message, String contextBufferText) {
            this.date = date;
            this.server = server;
            this.app = app;
            this.logLevel = logLevel;
            this.message = message;
            this.contextBufferText = contextBufferText;
        }
    }


    private static List<SrcTgtEntry> srctgtmap = new ArrayList<>();
    private static List<SrcTgtEntry> srctgtmap_prior = new ArrayList<>();
    private static class SrcTgtEntry {
        String appName;
        String sourceName;
        String targetName;
        int srcNumOfDeletes;
        int tgtNumOfDeletes;
        int diffNumOfDeletes;
        int srcNumOfDdls;
        int tgtNumOfDdls;
        int diffNumOfDdls;
        int srcNumOfPkupdates;
        int tgtNumOfPkupdates;
        int diffNumOfPkupdates;
        int srcNumOfUpdates;
        int tgtNumOfUpdates;
        int diffNumOfUpdates;
        int srcNumOfInserts;
        int tgtNumOfInserts;
        int diffNumOfInserts;

        // constructor
        public SrcTgtEntry(String appName, String sourceName, String targetName, int srcNumOfDeletes, int tgtNumOfDeletes, int srcNumOfDdls, int tgtNumOfDdls, int srcNumOfPkupdates, int tgtNumOfPkupdates, int srcNumOfUpdates, int tgtNumOfUpdates, int srcNumOfInserts, int tgtNumOfInserts) {
            this.appName = appName;
            this.sourceName = sourceName;
            this.targetName = targetName;
            this.srcNumOfDeletes = srcNumOfDeletes;
            this.tgtNumOfDeletes = tgtNumOfDeletes;
            this.diffNumOfDeletes = srcNumOfDeletes - tgtNumOfDeletes;
            this.srcNumOfDdls = srcNumOfDdls;
            this.tgtNumOfDdls = tgtNumOfDdls;
            this.diffNumOfDdls = srcNumOfDdls - tgtNumOfDdls;
            this.srcNumOfPkupdates = srcNumOfPkupdates;
            this.tgtNumOfPkupdates = tgtNumOfPkupdates;
            this.diffNumOfPkupdates = srcNumOfPkupdates - tgtNumOfPkupdates;
            this.srcNumOfUpdates = srcNumOfUpdates;
            this.tgtNumOfUpdates = tgtNumOfUpdates;
            this.diffNumOfUpdates = srcNumOfUpdates - tgtNumOfUpdates;
            this.srcNumOfInserts = srcNumOfInserts;
            this.tgtNumOfInserts = tgtNumOfInserts;
            this.diffNumOfInserts = srcNumOfInserts - tgtNumOfInserts;
        }
    }

    private static List<SrcTgtEntryDiff> srctgtmap_diff = new ArrayList<>();
    private static class SrcTgtEntryDiff {
        String appName;
        String sourceName;
        String targetName;
        int srcNumOfDeletes_sli;
        int tgtNumOfDeletes_sli;
        int diffNumOfDeletes_sli;
        int srcNumOfDdls_sli;
        int tgtNumOfDdls_sli;
        int diffNumOfDdls_sli;
        int srcNumOfPkupdates_sli;
        int tgtNumOfPkupdates_sli;
        int diffNumOfPkupdates_sli;
        int srcNumOfUpdates_sli;
        int tgtNumOfUpdates_sli;
        int diffNumOfUpdates_sli;
        int srcNumOfInserts_sli;
        int tgtNumOfInserts_sli;
        int diffNumOfInserts_sli;
    }

    private static List<LeeRecord> leerecord_list = new ArrayList<>();
    private static class LeeRecord {
        private String source;
        private String target;
        private String lagEndToEnd;
        private String measuredAt;
        private String sourceTime;

        // Constructor
        public LeeRecord(String source, String target, String lagEndToEnd, String measuredAt, String sourceTime) {
            this.source = source;
            this.target = target;
            this.lagEndToEnd = lagEndToEnd;
            this.measuredAt = measuredAt;
            this.sourceTime = sourceTime;
        }

    }

    private static List<LeePlusRecord> leeplusrecord_list = new ArrayList<>();
    private static class LeePlusRecord {
        private String source;
        private String target;
        private String lagEndToEnd;
        private String measuredAt;
        private String sourceTime;
        private String minLEE;
        private String maxLEE;
        private String avgLEE;
        private String sampleSize;

        // Constructor
        public LeePlusRecord(String source, String target, String lagEndToEnd, String measuredAt, String sourceTime, String minLEE, String maxLEE, String avgLEE, String sampleSize) {
            this.source = source;
            this.target = target;
            this.lagEndToEnd = lagEndToEnd;
            this.measuredAt = measuredAt;
            this.sourceTime = sourceTime;
            this.minLEE = minLEE;
            this.maxLEE = maxLEE;
            this.avgLEE = avgLEE;
            this.sampleSize = sampleSize;
        }

    }

    private static List<ComponentOutput> cmpntOutput = new ArrayList<>();
    private static class ComponentOutput {
        String appName;
        String componentName;
        String command;
        String type;
        String jsondata;

        // constructor
        public ComponentOutput(String appName, String componentName, String command, String type, String jsondata) {
            this.appName = appName;
            this.componentName = componentName;
            this.command = command;
            this.type = type;
            this.jsondata = jsondata;
        }
    }

    private static List<StriimTypeList> typeListOutput = new ArrayList<>();
    private static List<StriimTypeList> striimTypeList = new ArrayList<>();
    private static class StriimTypeList {
        private String typeName;
        private String appName;
        private String tableName;
        private DateTime createdDate;
        private String columnName;
        private String columnType;
        private Boolean isPK;

        public StriimTypeList(String typeName, String appName, String tableName, DateTime createdDate, String columnName, String columnType, Boolean isPK) {
            this.typeName = typeName;
            this.appName = appName;
            this.tableName = tableName;
            this.createdDate = createdDate;
            this.columnName = columnName;
            this.columnType = columnType;
            this.isPK = isPK;
        }

    }

    private static List<AppSTMap> appstmaplist = new ArrayList<>();

    private static class AppSTMap {
        String stname;
        String appname;

        // constructor
        public AppSTMap(String stname, String appname) {
            this.stname = stname;
            this.appname = appname;
        }
    }

    //         _    _ _______ _    _   __  __          _   _          _____ ______ _____
    //    /\  | |  | |__   __| |  | | |  \/  |   /\   | \ | |   /\   / ____|  ____|  __ \
    //   /  \ | |  | |  | |  | |__| | | \  / |  /  \  |  \| |  /  \ | |  __| |__  | |__) |
    //  / /\ \| |  | |  | |  |  __  | | |\/| | / /\ \ | . ` | / /\ \| | |_ |  __| |  _  /
    // / ____ \ |__| |  | |  | |  | | | |  | |/ ____ \| |\  |/ ____ \ |__| | |____| | \ \
    ///_/    \_\____/   |_|  |_|  |_| |_|  |_/_/    \_\_| \_/_/    \_\_____|______|_|  \_\

    public static class AuthManager {
        private Context ctx;
        private String authToken;
        AuthToken myToken;

        public String getAuthString() {
            if (authToken == null || authToken.isEmpty()) {
                // Retrieve the auth token (e.g., from an API call or other source)
                try {
                    logger.debug("AuthManager - getAuthString: Auth String unavailable; attempting to retrieve.");
                    authToken = fetchAuthToken();
                } catch (Exception ex) {
                    logger.warn("AuthManager - getAuthString: Unable to fetch authToken.");
                }
            }
            return authToken;
        }

        public AuthToken getAuthToken() {
            if (authToken == null || authToken.isEmpty()) {
                // Retrieve the auth token (e.g., from an API call or other source)
                try {
                    logger.debug("AuthManager - getAuthToken: Token unavailable; attempting to retrieve.");
                    authToken = fetchAuthToken();
                } catch (Exception ex) {
                    logger.warn("AuthManager - getAuthToken: Unable to fetch authToken.");
                }
            }
            return myToken;
        }

        public Context getAuthContext() {
            if (authToken == null || authToken.isEmpty()) {
                // Retrieve the auth token (e.g., from an API call or other source)
                try {
                    logger.debug("AuthManager - getAuthContext: Context unavailable; attempting to retrieve.");
                    authToken = fetchAuthToken();
                } catch (Exception ex) {
                    logger.warn("AuthManager - getAuthContext: Unable to fetch authToken.");
                }
            }
            return ctx;
        }

        public void clearAuthToken() {
            authToken = null;
        }

        private AuthToken getTOKEN() throws Exception {

            long maxWaitHours = 24; // Replace X with the desired maximum wait time in hours
            long retryIntervalSeconds = repeatInSeconds; // Replace Y with the desired retry interval in seconds
            long startTime = System.currentTimeMillis();
            int executionCount = 0;

            while ((System.currentTimeMillis() - startTime) < maxWaitHours * 3600000) {
                try {
                    TOKEN = getToken();
                    return TOKEN;
                } catch (Exception ex) {
                    logger.warn("AuthManager - getTOKEN: Unable to retrieve token.");
                }

                try {
                    long waitTimeMillis;
                    if (executionCount < 10) {
                        waitTimeMillis = 100 + (long) ((retryIntervalSeconds * 1000 - 100) * (executionCount / 9.0));
                        executionCount++;
                    } else {
                        waitTimeMillis = retryIntervalSeconds * 1000;
                    }
                    Thread.sleep(waitTimeMillis); // Wait for calculated or retry interval

                    if (Thread.interrupted()) { // Check for interruption during sleep
                        logger.warn("AuthManager - getTOKEN: Thread interrupted while waiting for token.");
                        break; // Exit the loop if interrupted
                    }
                } catch (InterruptedException e) {
                    logger.warn("AuthManager - getTOKEN: Thread interrupted while waiting for token.");
                    Thread.currentThread().interrupt(); // Restore interrupted status
                    break; // Exit the loop if interrupted
                }
            }

            if ((System.currentTimeMillis() - startTime) >= maxWaitHours * 3600000) {
                logger.warn("AuthManager - getTOKEN: Maximum wait time reached. Unable to retrieve token.");
                // Handle the failure (e.g., throw an exception, retry later, etc.)
            }

            return TOKEN;
        }

        private AuthToken getToken() throws Exception {
            // This is the new method for getting auth
            final String token = getAuthToken("admin", System.getProperty("striim.cluster.adminPassword"));
            return new AuthToken(token);
        }

        private String getAuthToken(final String uname, final String pwd) throws Exception {
            String token = "";
            try {
                final String urlParameters = "username=" + uname + "&password=" + pwd;
                final byte[] postData = urlParameters.getBytes("UTF-8");
                final int postDataLength = postData.length;
                final String request = "http://localhost:9080/security/authenticate";
                // logger.info(request);
                final URL url = new URL(request);
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setInstanceFollowRedirects(false);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                conn.setRequestProperty("charset", "utf-8");
                conn.setRequestProperty("Content-Length", Integer.toString(postDataLength));
                conn.setUseCaches(false);
                try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
                    wr.write(postData);
                    wr.flush();
                    wr.close();

                    final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    String inputLine;
                    final StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                    in.close();

                    token = response.toString().substring(10, response.length() - 2);

                    return token;
                }
            } catch (final Exception ex) {
                ex.printStackTrace();
                throw ex;

            }
        }

        private String fetchAuthToken() {
            try {
                myToken = getTOKEN();
                ctx = Context.createContext(myToken);
                authToken = ctx.getAuthToken().toString();
            } catch (Exception ex) {
                logger.warn("AuthManager - fetchAuthToken: Unable to fetch Auth Token.");
            }

            return authToken; // Replace with the actual token
        }
    }

    // _____ _   _ _____ _______
    //|_   _| \ | |_   _|__   __|
    //  | | |  \| | | |    | |
    //  | | | . ` | | |    | |
    // _| |_| |\  |_| |_   | |
    //|_____|_| \_|_____|  |_|

    /**
     * Init Method: Assigns all the property values from settings, initalizes components
     */
    @Override
    public synchronized void init(Map<String, Object> properties) throws Exception {
        currentBatch = new HashMap<String, String>();

        //Assign values from Property Set
        if (properties.get("RepeatInSeconds") != null && !properties.get("RepeatInSeconds").toString().equals(""))
            repeatInSeconds = Integer.parseInt(properties.get("RepeatInSeconds").toString());
        else
            repeatInSeconds = 300;

        try {
            String providedStartOn = properties.get("StartOn").toString();
            startOn = providedStartOn.isEmpty() || providedStartOn.equalsIgnoreCase("2020-12-31T08:00") ? DateTime.now() : DateTime.parse(providedStartOn);
        } catch (Exception startOnE) {
            startOn = DateTime.now();
            logger.debug("init: Unable to parse StartOn: ", startOnE);
        }

        includeNodeMonitor = Boolean.parseBoolean(properties.get("IncludeNodeMonitor").toString());
        includeNodeCluster = Boolean.parseBoolean(properties.get("IncludeNodeCluster").toString());
        includeNodeES = Boolean.parseBoolean(properties.get("IncludeNodeES").toString());
        includeCreatedApplicationDetail = Boolean.parseBoolean(properties.get("IncludeCreatedApplicationDetail").toString());
        includeDeployedApplicationDetail = Boolean.parseBoolean(properties.get("IncludeDeployedApplicationDetail").toString());
        includeTableComparisonDetail = Boolean.parseBoolean(properties.get("IncludeTableComparisonDetail").toString());
        includeTableComparisonDetail_SinceLastInterval = Boolean.parseBoolean(properties.get("IncludeTableComparisonDetail_SinceLastInterval").toString());
        includeAppDetail = Boolean.parseBoolean(properties.get("IncludeAppDetail").toString());

        includeAppDescribeDetail = Boolean.parseBoolean(properties.get("IncludeAppDescribeDetail").toString());
        includeAppStatusDetail = Boolean.parseBoolean(properties.get("IncludeAppStatusDetail").toString());

        includeLee = Boolean.parseBoolean(properties.get("IncludeLee").toString());

        includeComponentDetailsAsOutput = Boolean.parseBoolean(properties.get("IncludeComponentDetailsAsOutput").toString());
        includeTypeDetailsAsOutput = Boolean.parseBoolean(properties.get("IncludeTypeDetailsAsOutput").toString());
        includeLogWatcher = Boolean.parseBoolean(properties.get("IncludeLogWatcher").toString());
        includeDataWarehouseDetails = Boolean.parseBoolean(properties.get("IncludeDataWarehouseDetails").toString());

        //includeAdvancedAnalytics = Boolean.parseBoolean(properties.get("IncludeAdvancedAnalytics").toString());

        preserveStriimWatcherPosition = Boolean.parseBoolean(properties.get("PreserveStriimWatcherPosition").toString());

        //includeComponentOutput = Boolean.parseBoolean(properties.get("IncludeComponentOutputDetail").toString());

        if (properties.get("endOn") != null && !properties.get("endOn").toString().equals(""))
            endOn = DateTime.parse(properties.get("endOn").toString());

        getNextStart();

        //AdditionalCommandList - Needs to be last to access other values, including what's in getNextStart()
        try {
            String CL = properties.containsKey("AdditionalCommandList") ? properties.get("AdditionalCommandList").toString() : "";
            if (!CL.isEmpty()) {
                parseAdditionalCommandList(CL);
            }
        } catch (Exception noACL) {
            logger.debug("init: Unable to parse AdditionalCommandList:", noACL);
        }

        this.interval = 5000;
        super.init(properties);
    }

    private void getNextStart() {

        if (nextStart == null)
        {
            nextStart = startOn;
            lastStart = null;
        }
        else {
            nextStart = DateTime.now().plusSeconds(repeatInSeconds);
        }
        return;
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();
    }

    /**
     *
     * @Override
     * @param channel
     * @param in
     * @throws Exception
     */
    public synchronized void receiveImpl(int channel, Event in) throws Exception {
        try {

            synchronized (running) {
                running = true;
                List<WAEvent> actions = checkTimer();
                for (WAEvent e : actions)
                    send(e, channel);

                if (this.interval != null) {
                    Thread.sleep(this.interval);
                }

            }

        }catch (Exception ex) {
            logger.debug("receiveImpl: ", ex);
        }
    }

    static class CallBackExecutor implements ExecutionCallback {
        UUID uuid;

        @Override
        public void execute(final Stmt stmt, final Compiler compiler) throws Exception {
            uuid = (UUID) compiler.compileStmt(stmt);
        }
    }

    private List<WAEvent> checkTimer() throws Exception {
        List<WAEvent> actions = new ArrayList<WAEvent>();

        thisNamespace = getLocalNamespaceForSW();

        if (nextStart.isBeforeNow()) {

            //Time since last run
            long timeSinceLastRun = 0;

            if (!(lastStart == null))
            {
                timeSinceLastRun = Seconds.secondsBetween(lastStart, DateTime.now()).getSeconds();
            }

            lastStart = nextStart;

            getNextStart();

            currentrun = DateTime.now();

            // Reset this list
            //cmpntOutput.clear();
            cmpntOutput = new ArrayList<>();

            totalRuns++;

            Boolean firstRun = totalRuns == 1 ? true : false;

            AuthToken myToken = am.getAuthToken();

            Context ctx = Context.createContext(myToken);

            MetadataRepository mdr = MetadataRepository.getINSTANCE();

            if (firstRun && preserveStriimWatcherPosition) {
                srctgtmap_prior = loadHistory();
                //Reset this variable, used for table comparison SLI
                timeSinceLastRun = Seconds.secondsBetween(lastStart, DateTime.now()).getSeconds();
            }

            // This section controls where mon; is executed
            if (includeNodeMonitor){
                try {
                    String command = "mon;";
                    String nodeHealthData = runCommand(command);
                    List<WAEvent> monresults = createNodeHealth(nodeHealthData);
                    actions.addAll(monresults);

                } catch (Exception e) {
                    //System.out.println("StriimWatcher: There was an issue parsing node health." + e.toString());
                    String eMsg = String.format("StriimWatcher: There was an issue parsing node health.") + e;
                    logger.debug(eMsg);
                }
            }

            // Need to gather types
            if (includeTypeDetailsAsOutput) {
                String typeList = runCommand("list types;");

                JSONArray jsonArray = new JSONArray(typeList);

                if (jsonArray.length() > 0) {

                    JSONObject jsonObject = jsonArray.getJSONObject(0);

                    if (jsonObject.has("output")) {

                        Object jsonObjectOutputObject = jsonObject.opt("output");

                        if (jsonObjectOutputObject instanceof JSONArray) {
                            JSONArray outputArray = jsonObject.getJSONArray("output");

                            for (int i = 0; i < outputArray.length(); i++) {
                                JSONObject outputObject = outputArray.getJSONObject(i);

                                if (outputObject.keys().hasNext()) {
                                    String typeName = outputObject.keys().next().toString();
                                    JSONObject typeObject = outputObject.getJSONObject(typeName);

                                    if (typeObject != null && typeObject.has("name")) {
                                        String name = typeObject.getString("name");

                                        if (!(  name.toLowerCase().startsWith("Global".toLowerCase()) ||
                                                name.toLowerCase().startsWith("System$Notification".toLowerCase()) ||
                                                name.toLowerCase().contains("mon_striim_mon".toLowerCase()) //All of StriimWatcher should have the mon namespace and striim_mon as a prefix
                                        )) {
                                            String describeCommand = "describe " + name + ";";

                                            String jsonDescribeType = runCommand(describeCommand);

                                            //describe
                                            JSONArray descjsonArray = new JSONArray(jsonDescribeType);

                                            if (descjsonArray.length() > 0) {
                                                JSONObject descjsonObject = descjsonArray.getJSONObject(0);

                                                if (descjsonObject.has("output")) {
                                                    JSONArray descoutputArray = descjsonObject.getJSONArray("output");

                                                    Boolean didTypeUpdate = false;

                                                    if (descoutputArray.length() > 0) {
                                                        JSONObject descoutputObject = descoutputArray.getJSONObject(0);

                                                        //String desctypeName = descoutputObject.has("name") ? descoutputObject.getString("name") : null;
                                                        DateTime createdDate = descoutputObject.has("created") ? convertStringToDateTime(descoutputObject.getString("created")) : null;

                                                        JSONArray attributesArray = descoutputObject.getJSONArray("attributes");
                                                        for (int j = 0; j < attributesArray.length(); j++) {
                                                            JSONObject attributeObject = attributesArray.getJSONObject(j);

                                                            if (attributeObject.has("fieldAlias") && attributeObject.has("isKey") && attributeObject.length() == 3) {

                                                                if (attributeObject.has("isKey")) {
                                                                    String columnType = "";
                                                                    String columnName = "";
                                                                    Boolean isPK = attributeObject.has("isKey") ? attributeObject.getBoolean("isKey") : false;

                                                                    String key1 = attributeObject.keys().next().toString();
                                                                    String value1 = key1.equalsIgnoreCase("isKey") ? "" : attributeObject.getString(key1);

                                                                    attributeObject.remove(key1); // Remove the processed key

                                                                    String key2 = attributeObject.keys().next().toString();
                                                                    String value2 = key2.equalsIgnoreCase("isKey") ? "" : attributeObject.getString(key2);

                                                                    attributeObject.remove(key2); // Remove the processed key

                                                                    String key3 = attributeObject.keys().next().toString();
                                                                    String value3 = key3.equalsIgnoreCase("isKey") ? "" : attributeObject.getString(key3);

                                                                    if (value1.startsWith("java.") ||
                                                                            value1.startsWith("org."))   {
                                                                        columnType = value1;
                                                                        columnName = key1;
                                                                    }

                                                                    if (value2.startsWith("java.") ||
                                                                            value2.startsWith("org."))   {
                                                                        columnType = value2;
                                                                        columnName = key2;
                                                                    }

                                                                    if (value3.startsWith("java.") ||
                                                                            value3.startsWith("org."))   {
                                                                        columnType = value3;
                                                                        columnName = key3;
                                                                    }

                                                                    if (upsertTypeList(name, "", "", createdDate, columnName, columnType, isPK)) {
                                                                        createTypeOutput(name, "","", createdDate, columnName, columnType, isPK);
                                                                        didTypeUpdate = true;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                    if (didTypeUpdate) {
                                                        createComponentOutput("", name, "describe", "TYPE", jsonDescribeType, includeTypeDetailsAsOutput);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // This section controls where mon app; is executed
            // It also populates source and target maps inside monApplication method
            // This section needs to run in order for table comparison to run
            try {
                List<MetaInfo.Flow> apps = mdr.getAllApplications(am.getAuthToken());

                for (MetaInfo.Flow app : apps) {

                    String appStatus = app.flowStatus.name().toString();

                    if ((appStatus.equalsIgnoreCase("CREATED") && includeCreatedApplicationDetail) ||
                            (appStatus.equalsIgnoreCase("DEPLOYED") && includeDeployedApplicationDetail) ||
                            (!appStatus.equalsIgnoreCase("DEPLOYED") && !appStatus.equalsIgnoreCase("CREATED"))) {
                        //All apps need their mon data added to the result
                        List<WAEvent> results = monApplication(app);

                        // We need to run the results list, but not necessarily include in the output
                        // We will check if we unclude AppDetail in the output and then only add it
                        if (includeAppDetail) {
                            actions.addAll(results);
                        }
                    }
                }
            } catch (Exception e) {
                //System.out.println("StriimWatcher: There was an error while monitoring the app." + e.toString());
                String eMsg = "StriimWatcher: There was an error while monitoring the app." + e;
                logger.debug(eMsg);
                e.printStackTrace();
            }

            if (includeLee) {
                try {
                    String commandrl = "report lee;";
                    String sourceJson = runCommand(commandrl);

                    JSONArray lee_jsonArray = new JSONArray(sourceJson);

                    for (int k = 0; k < lee_jsonArray.length(); k++) {
                        JSONObject lee_jsonObject = lee_jsonArray.getJSONObject(k);
                        if (lee_jsonObject.has("output")) {

                            Object lee_jsonObjectOutputObject = lee_jsonObject.opt("output");

                            if (lee_jsonObjectOutputObject instanceof JSONArray) {
                                JSONArray lee_outputArray = lee_jsonObject.getJSONArray("output");

                                for (int i = 0; i < lee_outputArray.length(); i++) {
                                    JSONObject lee_output = lee_outputArray.getJSONObject(i);

                                    // Variables for data under output
                                    String lee_source = lee_output.has("source") ? lee_output.getString("source") : null;
                                    String lee_target = lee_output.has("target") ? lee_output.getString("target") : null;
                                    String lee_lagEndToEnd = lee_output.has("lagEndToEnd") ? lee_output.getString("lagEndToEnd") : null;
                                    String lee_measuredAt = lee_output.has("measuredAt") ? lee_output.getString("measuredAt") : null;
                                    String lee_sourceTime = lee_output.has("sourceTime") ? lee_output.getString("sourceTime") : null;

                                    leerecord_list.add(new LeeRecord(lee_source, lee_target, lee_lagEndToEnd, lee_measuredAt, lee_sourceTime));
                                }
                            }
                        }
                    }

                    sourceJson = runCommand("report lee+;");

                    JSONArray leep_jsonArray = new JSONArray(sourceJson);

                    for (int k = 0; k < leep_jsonArray.length(); k++) {
                        JSONObject leep_jsonObject = leep_jsonArray.getJSONObject(k);
                        if (leep_jsonObject.has("output")) {

                            Object leep_jsonObjectOutputObject = leep_jsonObject.opt("output");

                            if (leep_jsonObjectOutputObject instanceof JSONArray) {
                                JSONArray leep_outputArray = leep_jsonObject.getJSONArray("output");

                                for (int j = 0; j < leep_outputArray.length(); j++) {
                                    JSONObject leep_output = leep_outputArray.getJSONObject(j);

                                    // Variables for data under output
                                    String leep_source = leep_output.has("source") ? leep_output.getString("source") : null;
                                    String leep_target = leep_output.has("target") ? leep_output.getString("target") : null;
                                    String leep_lagEndToEnd = leep_output.has("lagEndToEnd") ? leep_output.getString("lagEndToEnd") : null;
                                    String leep_measuredAt = leep_output.has("measuredAt") ? leep_output.getString("measuredAt") : null;
                                    String leep_sourceTime = leep_output.has("sourceTime") ? leep_output.getString("sourceTime") : null;
                                    String leep_minLEE = leep_output.has("minLEE") ? leep_output.getString("minLEE") : null;
                                    String leep_maxLEE = leep_output.has("maxLEE") ? leep_output.getString("maxLEE") : null;
                                    String leep_avgLEE = leep_output.has("avgLEE") ? leep_output.getString("avgLEE") : null;
                                    String leep_sampleSize = leep_output.has("sampleSize") ? leep_output.getString("sampleSize") : null;

                                    leeplusrecord_list.add(new LeePlusRecord(leep_source, leep_target, leep_lagEndToEnd, leep_measuredAt, leep_sourceTime, leep_minLEE, leep_maxLEE, leep_avgLEE, leep_sampleSize));
                                }
                            }
                        }
                    }

                    //Create Lee Output Record
                    for (LeeRecord leerecord : leerecord_list) {
                        for (LeePlusRecord leeplusrecord : leeplusrecord_list) {
                            if (leerecord.source.equals(leeplusrecord.source) && leerecord.target.equals(leeplusrecord.target)) {
                                logger.debug("Match found: " + leerecord.source + " -> " + leerecord.target);

                                String leeSourceApp = "";
                                String leeSourceName = "";
                                String leeSourceType = "";
                                String leeTargetApp = "";
                                String leeTargetName = "";
                                String leeTargetType = "";

                                try {
                                    String data = leerecord.source;

                                    // Find the index of '(' and ')'
                                    int openParenIndex = data.indexOf('(');
                                    int closeParenIndex = data.indexOf(')');

                                    if (openParenIndex != -1 && closeParenIndex != -1 && openParenIndex < closeParenIndex) {
                                        // Extract the content inside the parentheses
                                        String type = data.substring(openParenIndex + 1, closeParenIndex).trim();

                                        // Extract the content outside the parentheses
                                        String sn = data.substring(0, openParenIndex).trim();

                                        leeSourceName = sn;
                                        leeSourceType = type;

                                        final String cmptName = leeSourceName;

                                        Optional<String> appNameOptional = srcmap.stream()
                                                .filter(entry -> entry.componentName.equals(cmptName))
                                                .map(entry -> entry.appName)
                                                .findFirst();
                                        leeSourceApp = appNameOptional.orElse(null);

                                    } else {
                                        logger.debug("Error: Incorrect format or missing parentheses.");
                                    }
                                } catch (Exception ex) {
                                    leeSourceName = leerecord.source;
                                    leeSourceType = "";
                                }

                                try {
                                    String data = leerecord.target;

                                    // Find the index of '(' and ')'
                                    int openParenIndex = data.indexOf('(');
                                    int closeParenIndex = data.indexOf(')');

                                    if (openParenIndex != -1 && closeParenIndex != -1 && openParenIndex < closeParenIndex) {
                                        // Extract the content inside the parentheses
                                        String type = data.substring(openParenIndex + 1, closeParenIndex).trim();

                                        // Extract the content outside the parentheses
                                        String sn = data.substring(0, openParenIndex).trim();

                                        leeTargetName = sn;
                                        leeTargetType = type;

                                        final String cmptName = leeTargetName;

                                        Optional<String> appNameOptional = tgtmap.stream()
                                                .filter(entry -> entry.componentName.equals(cmptName))
                                                .map(entry -> entry.appName)
                                                .findFirst();

                                        leeTargetApp = appNameOptional.orElse(null);

                                    } else {
                                        logger.debug("Error: Incorrect format or missing parentheses.");
                                    }
                                } catch (Exception ex) {
                                    leeTargetName = leerecord.target;
                                    leeTargetType = "";
                                }

                                // If not the lag for StriimWatcher, log it
                                if (!(leeSourceType.equalsIgnoreCase("StriimWatcher") ||
                                        leeTargetType.equalsIgnoreCase("StriimWatcher"))) {
                                    // Create WAEvent
                                    String tableName = watcherTableName.mon_lee;
                                    String pkid = watcherTableName.mon_lee_pk;
                                    String namespace = watcherTableName.mon_namespace;

                                    WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                                    // Use event.setData(eventIndex++, "abc"); to set the next value
                                    int eventIndex = 0;

                                    long monnodesid = generateUniqueInteger();

                                    event.setData(eventIndex++, monnodesid);
                                    event.setData(eventIndex++, currentrun);
                                    event.setData(eventIndex++, leeSourceApp);
                                    event.setData(eventIndex++, leeSourceName);
                                    event.setData(eventIndex++, leeSourceType);
                                    event.setData(eventIndex++, leeTargetApp);
                                    event.setData(eventIndex++, leeTargetName);
                                    event.setData(eventIndex++, leeTargetType);
                                    event.setData(eventIndex++, getNumericOnly(leerecord.lagEndToEnd));
                                    event.setData(eventIndex++, parseAndConvertDateTimeToNull(leerecord.measuredAt));
                                    event.setData(eventIndex++, leerecord.sourceTime);
                                    event.setData(eventIndex++, getNumericOnly(leeplusrecord.minLEE));
                                    event.setData(eventIndex++, getNumericOnly(leeplusrecord.maxLEE));
                                    event.setData(eventIndex++, getNumericOnly(leeplusrecord.avgLEE));
                                    event.setData(eventIndex++, getNumericOnly(leeplusrecord.sampleSize));

                                    event = addMetaRelated(event, leeSourceApp, leeTargetApp);

                                    actions.add(event);
                                }

                                // Once the work is done for the current match, you can break out of the inner loop
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    String eMsg = "Unable to add lee information." + e;
                    logger.debug(eMsg);
                }

            }

            if (includeTableComparisonDetail) {

                // Loop through each entry in tgtmap
                for (TargetEntry tgt : tgtmap) {
                    // Loop through each entry in srcmap
                    for (SourceEntry src : srcmap) {
                        // Check if appName and sourceName match
                        if (tgt.appName.equals(src.appName) && tgt.sourceName.equals(src.sourceName)) {
                            // Check if an entry already exists in srctgtmap
                            boolean exists = false;
                            for (SrcTgtEntry st : srctgtmap) {
                                if (st.appName.equals(tgt.appName) && st.sourceName.equals(tgt.sourceName) && st.targetName.equals(tgt.targetName)) {
                                    // Update the existing entry
                                    st.srcNumOfDeletes += src.numOfDeletes;
                                    st.tgtNumOfDeletes += tgt.numOfDeletes;
                                    st.srcNumOfDdls += src.numOfDdls;
                                    st.tgtNumOfDdls += tgt.numOfDdls;
                                    st.srcNumOfInserts += src.numOfInserts;
                                    st.tgtNumOfInserts += tgt.numOfInserts;
                                    st.srcNumOfPkupdates += src.numOfPkupdates;
                                    st.tgtNumOfPkupdates += tgt.numOfPkupdates;
                                    st.srcNumOfUpdates += src.numOfUpdates;
                                    st.tgtNumOfUpdates += tgt.numOfUpdates;
                                    exists = true;
                                    break;
                                }
                            }
                            // If an entry does not exist, create a new one
                            if (!exists) {
                                srctgtmap.add(new SrcTgtEntry(tgt.appName, src.sourceName, tgt.targetName.replace("\\", ""), src.numOfDeletes, tgt.numOfDeletes, src.numOfDdls, tgt.numOfDdls, src.numOfPkupdates, tgt.numOfPkupdates, src.numOfUpdates, tgt.numOfUpdates, src.numOfInserts, tgt.numOfInserts));
                            }
                        }
                    }
                }

                // Loop through sources and targets, compare with what exists in srctgtmap, and add one-sided ones to the output
                //for (TargetEntry tgt : tgtmap) {
                // If tgt does not exist in  srctgtmap, then add it
                // SrcTgtEntry newEntry = new SrcTgtEntry(tgt.appName, src.sourceName, tgt.targetName, src.numOfDeletes, tgt.numOfDeletes, src.numOfDdls, tgt.numOfDdls, src.numOfPkupdates, tgt.numOfPkupdates, src.numOfUpdates, tgt.numOfUpdates, src.numOfInserts, tgt.numOfInserts);
                // srctgtmap.add(newEntry);
                //}

                // create newEntry first
                for (TargetEntry tgt : tgtmap) {
                    // Check if tgt does not exist in srctgtmap
                    boolean exists = srctgtmap.stream().anyMatch(srcTgt -> srcTgt.appName.equals(tgt.appName) && srcTgt.targetName.equals(tgt.targetName));
                    if (!exists) {
                        // If tgt does not exist in srctgtmap, then add it
                        String sourceName = tgt.sourceName;

                        // If it is not from the monitoring app, then add it to table compare
                        if (!sourceName.startsWith("mon.striim"))
                        {
                            sourceName = "NONE:" + sourceName;

                            srctgtmap.add(new SrcTgtEntry(tgt.appName, sourceName, tgt.targetName.replace("\\", ""), 0, tgt.numOfDeletes, 0, tgt.numOfDdls, 0, tgt.numOfPkupdates, 0, tgt.numOfUpdates, 0, tgt.numOfInserts));
                        }
                    }
                }

                for (SourceEntry src : srcmap) {
                    // Check if tgt does not exist in srctgtmap
                    boolean exists = srctgtmap.stream().anyMatch(srcTgt -> srcTgt.appName.equals(src.appName) && srcTgt.sourceName.equals(src.sourceName));
                    if (!exists) {
                        // If tgt does not exist in srctgtmap, then add it

                        srctgtmap.add(new SrcTgtEntry(src.appName, src.sourceName, "NONE", src.numOfDeletes, 0, src.numOfDdls, 0, src.numOfPkupdates, 0, src.numOfUpdates, 0, src.numOfInserts, 0));
                    }
                }

                //Output WAEvent
                for (SrcTgtEntry entity : srctgtmap) {
                    String tableName = watcherTableName.mon_table_comparison;
                    String pkid = watcherTableName.mon_table_comparison_pk;

                    String namespace = watcherTableName.mon_namespace;

                    WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                    // Use event.setData(eventIndex++, "abc"); to set the next value
                    int eventIndex = 0;

                    long tblcompareid = generateUniqueInteger();

                    //event.setBefore(0, monappid);

                    event.setData(eventIndex++, tblcompareid);
                    event.setData(eventIndex++, currentrun);
                    event.setData(eventIndex++, entity.appName);
                    event.setData(eventIndex++, entity.sourceName);
                    event.setData(eventIndex++, entity.targetName);
                    event.setData(eventIndex++, entity.srcNumOfDeletes);
                    event.setData(eventIndex++, entity.tgtNumOfDeletes);
                    event.setData(eventIndex++, entity.diffNumOfDeletes);
                    event.setData(eventIndex++, entity.srcNumOfDdls);
                    event.setData(eventIndex++, entity.tgtNumOfDdls);
                    event.setData(eventIndex++, entity.diffNumOfDdls);
                    event.setData(eventIndex++, entity.srcNumOfPkupdates);
                    event.setData(eventIndex++, entity.tgtNumOfPkupdates);
                    event.setData(eventIndex++, entity.diffNumOfPkupdates);
                    event.setData(eventIndex++, entity.srcNumOfUpdates);
                    event.setData(eventIndex++, entity.tgtNumOfUpdates);
                    event.setData(eventIndex++, entity.diffNumOfUpdates);
                    event.setData(eventIndex++, entity.srcNumOfInserts);
                    event.setData(eventIndex++, entity.tgtNumOfInserts);
                    event.setData(eventIndex++, entity.diffNumOfInserts);

                    event = addMetaRelated(event, entity.appName);

                    actions.add(event);

                }

            }

            if (includeTableComparisonDetail_SinceLastInterval)  {
                try {
                    //Check if we have an old one
                    if (srctgtmap_prior.size() > 0)
                    {
                        //Output comparison WAEvent
                        for (SrcTgtEntry entry : srctgtmap) {
                            for (SrcTgtEntry priorEntry : srctgtmap_prior) {
                                if (entry.appName.equals(priorEntry.appName) &&
                                        entry.sourceName.equals(priorEntry.sourceName) &&
                                        entry.targetName.equals(priorEntry.targetName)) {

                                    SrcTgtEntryDiff diff = new SrcTgtEntryDiff();

                                    diff.appName = entry.appName;
                                    diff.sourceName = entry.sourceName;
                                    diff.targetName = entry.targetName;
                                    diff.srcNumOfDeletes_sli = entry.srcNumOfDeletes - priorEntry.srcNumOfDeletes;
                                    diff.tgtNumOfDeletes_sli = entry.tgtNumOfDeletes - priorEntry.tgtNumOfDeletes;
                                    diff.diffNumOfDeletes_sli = entry.diffNumOfDeletes - priorEntry.diffNumOfDeletes;
                                    diff.srcNumOfDdls_sli = entry.srcNumOfDdls - priorEntry.srcNumOfDdls;
                                    diff.tgtNumOfDdls_sli = entry.tgtNumOfDdls - priorEntry.tgtNumOfDdls;
                                    diff.diffNumOfDdls_sli = entry.diffNumOfDdls - priorEntry.diffNumOfDdls;
                                    diff.srcNumOfPkupdates_sli = entry.srcNumOfPkupdates - priorEntry.srcNumOfPkupdates;
                                    diff.tgtNumOfPkupdates_sli = entry.tgtNumOfPkupdates - priorEntry.tgtNumOfPkupdates;
                                    diff.diffNumOfPkupdates_sli = entry.diffNumOfPkupdates - priorEntry.diffNumOfPkupdates;
                                    diff.srcNumOfUpdates_sli = entry.srcNumOfUpdates - priorEntry.srcNumOfUpdates;
                                    diff.tgtNumOfUpdates_sli = entry.tgtNumOfUpdates - priorEntry.tgtNumOfUpdates;
                                    diff.diffNumOfUpdates_sli = entry.diffNumOfUpdates - priorEntry.diffNumOfUpdates;
                                    diff.srcNumOfInserts_sli = entry.srcNumOfInserts - priorEntry.srcNumOfInserts;
                                    diff.tgtNumOfInserts_sli = entry.tgtNumOfInserts - priorEntry.tgtNumOfInserts;
                                    diff.diffNumOfInserts_sli = entry.diffNumOfInserts - priorEntry.diffNumOfInserts;

                                    srctgtmap_diff.add(diff);
                                }
                            }
                        }

                        //Cycle through and create events
                        for (SrcTgtEntryDiff entity : srctgtmap_diff) {

                            String tableName = watcherTableName.mon_table_comparison_sli;
                            String pkid = watcherTableName.mon_table_comparison_sli_pk;
                            String namespace = watcherTableName.mon_namespace;

                            WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                            // Use event.setData(eventIndex++, "abc"); to set the next value
                            int eventIndex = 0;

                            long tblcompareid = generateUniqueInteger();

                            //event.setBefore(0, monappid);

                            event.setData(eventIndex++, tblcompareid);
                            event.setData(eventIndex++, currentrun);

                            //columns.put("timesincelastbatch", "Long");
                            event.setData(eventIndex++, timeSinceLastRun);

                            event.setData(eventIndex++, entity.appName);
                            event.setData(eventIndex++, entity.sourceName);
                            event.setData(eventIndex++, entity.targetName);
                            event.setData(eventIndex++, entity.srcNumOfDeletes_sli);
                            event.setData(eventIndex++, entity.tgtNumOfDeletes_sli);
                            event.setData(eventIndex++, entity.diffNumOfDeletes_sli);
                            event.setData(eventIndex++, entity.srcNumOfDdls_sli);
                            event.setData(eventIndex++, entity.tgtNumOfDdls_sli);
                            event.setData(eventIndex++, entity.diffNumOfDdls_sli);
                            event.setData(eventIndex++, entity.srcNumOfPkupdates_sli);
                            event.setData(eventIndex++, entity.tgtNumOfPkupdates_sli);
                            event.setData(eventIndex++, entity.diffNumOfPkupdates_sli);
                            event.setData(eventIndex++, entity.srcNumOfUpdates_sli);
                            event.setData(eventIndex++, entity.tgtNumOfUpdates_sli);
                            event.setData(eventIndex++, entity.diffNumOfUpdates_sli);
                            event.setData(eventIndex++, entity.srcNumOfInserts_sli);
                            event.setData(eventIndex++, entity.tgtNumOfInserts_sli);
                            event.setData(eventIndex++, entity.diffNumOfInserts_sli);

                            event = addMetaRelated(event, entity.appName);

                            actions.add(event);
                        }

                    }

                } catch (Exception e) {
                    String eMsg = "Unable to compare tables: " + e;
                    logger.debug(eMsg);
                }
                srctgtmap_prior = srctgtmap;
            }

            if (includeDataWarehouseDetails) {
                for (TargetDWDetail entity : tgtdwmap) {
                    String tableName = watcherTableName.mon_table_datawarehouse_detail;
                    String pkid = watcherTableName.mon_table_datawarehouse_detail_pk;

                    String namespace = watcherTableName.mon_namespace;

                    WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                    int eventIndex = 0;

                    long tbluniqueid = generateUniqueInteger(); // Assuming you have this function

                    event.setData(eventIndex++, tbluniqueid);
                    event.setData(eventIndex++, currentrun);

//                    String appName;
//                    String componentName;
//                    String sourceName;
//                    String targetName;
//                    String tgt_adapterType;
//                    String tgt_databaseProviderType;

                    event.setData(eventIndex++, entity.appName);
                    event.setData(eventIndex++, entity.sourceName);
                    event.setData(eventIndex++, entity.targetName);

                    event.setData(eventIndex++, entity.tgt_adapterType);

                    String tgtProperties = null;

                    // Find matching TargetEntry in tgtmap
                    for (TargetEntry tgtEntry : tgtmap) {
                        if (tgtEntry.appName.equals(entity.appName) &&
                                tgtEntry.sourceName.equals(entity.sourceName) &&
                                tgtEntry.targetName.equals(entity.targetName)) {

                            tgtProperties = tgtEntry.tgt_properties;
                            break; // Exit the inner loop once a match is found
                        }
                    }

                    int eventCount = -1;
                    int interval = -1;

                    boolean streamingUpload = false;
                    boolean optimizedMerge = false;
                    String Mode = "APPENDONLY";
                    String projectId = null;
                    String StreamingConfiguration = null;

                    if (tgtProperties != null) {
                        try {
                            JSONObject properties = new JSONObject(tgtProperties);

                            // Convert all keys to lowercase for case-insensitive retrieval (applied to all properties)
                            JSONObject lowercaseProperties = new JSONObject();
                            for (Object key : properties.keySet()) {
                                lowercaseProperties.put(key.toString().toLowerCase(), properties.get(key.toString()));
                            }

                            properties = lowercaseProperties;

                            if (properties.has("batchpolicy")) {
                                String batchPolicy = properties.getString("batchpolicy");
                                Map<String, String> batchPolicyMap = parsePolicy(batchPolicy);

                                // Convert all keys to lowercase for case-insensitive retrieval
                                batchPolicyMap = batchPolicyMap.entrySet().stream()
                                        .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));

                                if (batchPolicyMap.containsKey("eventcount")) {
                                    try {
                                        eventCount = Integer.parseInt(batchPolicyMap.get("eventcount"));
                                    } catch (NumberFormatException e) {
                                        //System.err.println("Invalid EventCount format in batchpolicy: " + batchPolicyMap.get("eventcount"));
                                    }
                                }

                                if (batchPolicyMap.containsKey("interval")) {
                                    try {
                                        String intervalValue = batchPolicyMap.get("interval");
                                        interval = parseInterval(intervalValue);
                                    } catch (NumberFormatException e) {
                                        //System.err.println("Invalid Interval format in batchpolicy: " + batchPolicyMap.get("interval"));
                                    }
                                }
                            }


                            if (properties.has("streamingupload")) {
                                try {
                                    streamingUpload = properties.getBoolean("streamingupload");
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                            if (properties.has("optimizedmerge")) {
                                try {
                                    optimizedMerge = properties.getBoolean("optimizedmerge");
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                            if (properties.has("mode")) {
                                try {
                                    Mode = properties.getString("mode");
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                            if (properties.has("projectid")) {

                                try {
                                    projectId = properties.getString("projectid");
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                            if (properties.has("streamingconfiguration")) {
                                try {
                                    StreamingConfiguration = properties.getString("streamingconfiguration");
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                            // Snowflake has a Boolean for this property
                            if (properties.has("appendonly")) {
                                try {
                                    Mode = properties.getBoolean("appendonly") ? "APPENDONLY" : "MERGE";
                                } catch (Exception e) {
                                    //System.err.println("");
                                }
                            }

                        } catch (JSONException e) {
                            //System.err.println("Error parsing properties JSON: " + e.getMessage());
                        }
                    }

                    event.setData(eventIndex++, projectId);
                    event.setData(eventIndex++, Mode);
                    event.setData(eventIndex++, streamingUpload);
                    event.setData(eventIndex++, StreamingConfiguration);
                    event.setData(eventIndex++, optimizedMerge);

                    event.setData(eventIndex++, eventCount != -1 ? eventCount : null);
                    event.setData(eventIndex++, interval != -1 ? interval : null);

                    // Populate fields based on TargetDWDetail and BigQuery DDL order
                    event.setData(eventIndex++, entity.totalBatchesCreated);
                    event.setData(eventIndex++, entity.partitionPrunedBatches);
                    event.setData(eventIndex++, parseAndConvertDateTimeToNull(entity.lastSuccessfulMergeTime)); // Assuming this is already in a format suitable for BigQuery TIMESTAMP
                    event.setData(eventIndex++, entity.totalBatchesIgnored);
                    event.setData(eventIndex++, entity.maxIntegrationTimeMs);
                    event.setData(eventIndex++, entity.avgInMemCompactionTimeMs);
                    event.setData(eventIndex++, entity.avgBatchSizeMb);
                    event.setData(eventIndex++, entity.avgEventCountPerBatch);
                    event.setData(eventIndex++, entity.minIntegrationTimeMs);
                    event.setData(eventIndex++, entity.totalBatchesQueued);
                    event.setData(eventIndex++, entity.avgCompactionTimeMs);
                    event.setData(eventIndex++, entity.avgWaitingTimeInQueueMs);
                    event.setData(eventIndex++, entity.avgIntegrationTimeMs);
                    event.setData(eventIndex++, entity.totalBatchesUploaded);
                    event.setData(eventIndex++, entity.avgMergeTimeMs);
                    event.setData(eventIndex++, entity.avgStageResourcesMgmtTimeMs);
                    event.setData(eventIndex++, entity.avgUploadTimeMs);

                    // Fields from "Last batch info"
                    event.setData(eventIndex++, entity.lastBatchUpdates);
                    event.setData(eventIndex++, entity.lastBatchEventCount);
                    event.setData(eventIndex++, entity.lastBatchInserts);
                    event.setData(eventIndex++, entity.lastBatchMaxRecordSizeMb);
                    event.setData(eventIndex++, entity.lastBatchTotalEventsMerged);
                    event.setData(eventIndex++, entity.lastBatchDdls);
                    event.setData(eventIndex++, entity.lastBatchSequenceNumber);
                    event.setData(eventIndex++, entity.lastBatchSizeMb);
                    event.setData(eventIndex++, entity.lastBatchDeletes);
                    event.setData(eventIndex++, entity.lastBatchPkUpdates);
                    event.setData(eventIndex++, entity.lastBatchAccumulationTimeMs);

                    // Fields from "Integration Task Time"
                    event.setData(eventIndex++, entity.lastBatchCompactionTimeMs);
                    event.setData(eventIndex++, entity.lastBatchStageResourcesMgmtTimeMs);
                    event.setData(eventIndex++, entity.lastBatchUploadTimeMs);
                    event.setData(eventIndex++, entity.lastBatchMergeTimeMs);
                    event.setData(eventIndex++, entity.lastBatchInMemCompactionTimeMs);
                    event.setData(eventIndex++, entity.lastBatchPkUpdateTimeMs);
                    event.setData(eventIndex++, entity.lastBatchDdlExecutionTimeMs);
                    event.setData(eventIndex++, entity.lastBatchTotalIntegrationTimeMs);

                    // Add any additional fields from TargetDWDetail here, following the BigQuery DDL order

                    event = addMetaRelated(event, entity.appName); // Assuming you have this function

                    actions.add(event);
                }
            }

            if (includeComponentDetailsAsOutput || includeTypeDetailsAsOutput) {
                for (ComponentOutput cmpt : cmpntOutput) {
                    actions.add(getComponentOutputEvent(cmpt));
                }
            }

            if (includeTypeDetailsAsOutput) {

                for (StriimTypeList stl : typeListOutput) {

                    Boolean doInsert = false;

                    if (firstRun) {
                        //This is our first run, we'll expose all
                        doInsert = true;
                    } else {
                        //This is on-going, only provide any that have changed since last run
                        if (stl.createdDate.isAfter(lastStart)) {
                            //Insert rows
                            doInsert = true;
                        }
                    }

                    if (doInsert) {
                        String tableName = watcherTableName.mon_apptablecolumndetail;
                        String pkid = watcherTableName.mon_apptablecolumndetail_pk;

                        String namespace = watcherTableName.mon_namespace;

                        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                        int eventIndex = 0;

                        long monappcolid = generateUniqueInteger();

                        event.setData(eventIndex++, monappcolid);
                        event.setData(eventIndex++, currentrun);
                        event.setData(eventIndex++, stl.typeName);
                        event.setData(eventIndex++, stl.appName);
                        event.setData(eventIndex++, stl.tableName);
                        event.setData(eventIndex++, stl.createdDate);
                        event.setData(eventIndex++, stl.columnName);
                        event.setData(eventIndex++, stl.columnType);
                        event.setData(eventIndex++, stl.isPK);

                        event = addMetaRelated(event, stl.appName);

                        actions.add(event);
                    }
                }
            }

            if (hasAdditionalCommandsToRun) {
                DateTime currentNow = DateTime.now();
                for (CommandList commandList : cmndlist) {
                    if (currentNow.isAfter(commandList.nextRun) || commandList.intervalSeconds == repeatInSeconds) {
                        String providedCommand = commandList.command;

                        if (providedCommand.contains("%source-all%") ||
                                providedCommand.contains("%source-running%") ||
                                providedCommand.contains("%target-all%") ||
                                providedCommand.contains("%target-running%") ||
                                providedCommand.contains("%app-all%") ||
                                providedCommand.contains("%app-running%")){

                            try {

                                if (providedCommand.contains("%source-all%")) {

                                    // Use MDR to go through sources
                                    for (Object srses : mdr.getByEntityType(EntityType.SOURCE, am.getAuthToken()))
                                    {
                                        if (srses instanceof MetaInfo.Source)
                                        {
                                            MetaInfo.Source mis = (MetaInfo.Source) srses;

                                            String newCommand = providedCommand.replace("%source-all%", mis.getFullName());

                                            String output = runCommand(newCommand);

                                            if (!output.isEmpty()) {
                                                actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                            }
                                        }
                                    }
                                }

                                if (providedCommand.contains("%target-all%")) {
                                    for (Object tgts : mdr.getByEntityType(EntityType.TARGET, am.getAuthToken()))
                                    {
                                        if (tgts instanceof MetaInfo.Target)
                                        {
                                            MetaInfo.Target mis = (MetaInfo.Target) tgts;

                                            String newCommand = providedCommand.replace("%target-all%", mis.getFullName());

                                            String output = runCommand(newCommand);

                                            if (!output.isEmpty()) {
                                                actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                            }
                                        }
                                    }
                                }

                                if (providedCommand.contains("%app-all%")) {

                                    List<MetaInfo.Flow> apps = mdr.getAllApplications(am.getAuthToken());

                                    for (MetaInfo.Flow app : apps) {

                                        String appStatus = app.flowStatus.name().toString();
                                        String appName = app.getFullName();

                                        String newCommand = providedCommand.replace("%app-all%", appName);

                                        String output = runCommand(newCommand);

                                        if (!output.isEmpty()) {
                                            actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                        }
                                    }
                                }

                                if (providedCommand.contains("%source-running%") ||
                                        providedCommand.contains("%target-running%") ||
                                        providedCommand.contains("%app-running%")){

                                    List<MetaInfo.Flow> apps = mdr.getAllApplications(am.getAuthToken());

                                    for (MetaInfo.Flow app : apps) {

                                        String appStatus = app.flowStatus.name().toString();
                                        String appName = app.getFullName();

                                        if (appStatus.equalsIgnoreCase("RUNNING"))
                                        {
                                            if (providedCommand.contains("%source-running%")) {
                                                for (SourceEntry src : srcmap) {
                                                    if (src.appName == appName) {
                                                        String newCommand = providedCommand.replace("%source-running%", src.componentName);

                                                        String output = runCommand(newCommand);

                                                        if (!output.isEmpty()) {
                                                            actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                                        }
                                                    }
                                                }
                                            }

                                            if (providedCommand.contains("%target-running%")) {
                                                for (TargetEntry tgt : tgtmap) {
                                                    if (tgt.appName == appName) {
                                                        String newCommand = providedCommand.replace("%target-running%", tgt.componentName);

                                                        String output = runCommand(newCommand);

                                                        if (!output.isEmpty()) {
                                                            actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                                        }
                                                    }
                                                }
                                            }

                                            if (providedCommand.contains("%app-running%")) {

                                                String newCommand = providedCommand.replace("%app-running%", appName);

                                                String output = runCommand(newCommand);

                                                if (!output.isEmpty()) {
                                                    actions.add(getComponentOutputEvent(new ComponentOutput("", newCommand, commandList.commandType, "CUSTOM", output)));
                                                }
                                            }
                                        }
                                    }

                                }


                                commandList.nextRun = currentNow.plusSeconds(commandList.intervalSeconds);

                            } catch (Exception e) {
                                //System.out.println("StriimWatcher: There was an error while monitoring the app." + e.toString());
                                String eMsg = "StriimWatcher: There was an error while running custom commands:" + e;
                                logger.debug(eMsg);
                            }
                        } else {
                            // No looping, no alterations, just run command
                            String output = runCommand(providedCommand);

                            if (!output.isEmpty()) {
                                commandList.nextRun = currentNow.plusSeconds(commandList.intervalSeconds);
                                actions.add(getComponentOutputEvent(new ComponentOutput("", commandList.command, commandList.commandType, "CUSTOM", output)));
                            }
                        }
                    }
                }
            }

            // Add here Log Watcher and/or Exception watcher
            if (includeLogWatcher) {
                readNewLogLines(60, lastKnownLogPosition);

                for (ErrorDetails errorDetails : errorDetailsList) {
                    String tableName = watcherTableName.mon_logwatcher;
                    String pkid = watcherTableName.mon_logwatcher_pk;

                    String namespace = watcherTableName.mon_namespace;

                    WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                    int eventIndex = 0;

                    // Assuming you have a way to generate unique integer IDs
                    long errorid = generateUniqueInteger();

                    // Set event data based on the ErrorDetails object and BigQuery DDL
                    event.setData(eventIndex++, errorid);
                    event.setData(eventIndex++, currentrun); // Assuming 'batchdate' is the current timestamp
                    event.setData(eventIndex++, errorDetails.date);
                    event.setData(eventIndex++, errorDetails.server);
                    event.setData(eventIndex++, errorDetails.app); // Using 'app' from ErrorDetails for 'appName' in BigQuery
                    event.setData(eventIndex++, errorDetails.logLevel);
                    event.setData(eventIndex++, errorDetails.message);
                    event.setData(eventIndex++, errorDetails.contextBufferText);

                    // Add any meta-related data (if applicable)
                    event = addMetaRelated(event, errorDetails.app);

                    event.metadata.put("AlertType", "ERROR");

                    actions.add(event);
                }

                for (SmartAlertDetails alert : smartAlertDetailsList) {
                    logger.debug("SmartAlertDetails - Date: " + alert.date);
                    logger.debug("SmartAlertDetails - Server: " + alert.server);
                    logger.debug("SmartAlertDetails - App: " + alert.app);
                    logger.debug("SmartAlertDetails - Log Level: " + alert.logLevel);
                    logger.debug("SmartAlertDetails - Alert Matched: " + alert.alertMatched);
                    logger.debug("SmartAlertDetails - Message: " + alert.message);
                    logger.debug("SmartAlertDetails - Type: " + alert.type);

                    String SAName = "";
                    if (alert.alertMatched != null && alert.alertMatched.contains(",")) {
                        SAName = alert.alertMatched.split(",")[0];
                    }

                    String newLogLevel;

                    newLogLevel = (SAName.isEmpty() ? alert.sourceOrTarget : SAName + ":" + alert.sourceOrTarget);

                    //Do not consider System$Alerts in output
                    if (alert.message.contains("Application System$Notification")) {
                        logger.debug("SmartAlertDetails - Detected System$Notification alert, not reporting.");
                    } else {
                        if (alert.entityName != null) {
                            logger.debug("SmartAlertDetails - Entity Name: " + alert.entityName);
                        }
                        if (alert.metricValue != null) {
                            logger.debug("SmartAlertDetails - Metric Value: " + alert.metricValue);
                        }
                        if (alert.metricUnit != null) {
                            logger.debug("SmartAlertDetails - Metric Unit: " + alert.metricUnit);
                        }
                        if (alert.sourceOrTarget != null) {
                            logger.debug("SmartAlertDetails - Source/Target: " + alert.sourceOrTarget);
                        }

                        String tableName = watcherTableName.mon_logwatcher;
                        String pkid = watcherTableName.mon_logwatcher_pk;

                        String namespace = watcherTableName.mon_namespace;

                        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                        int eventIndex = 0;

                        // Assuming you have a way to generate unique integer IDs
                        long errorid = generateUniqueInteger();

                        // Set event data based on the ErrorDetails object and BigQuery DDL
                        event.setData(eventIndex++, errorid);
                        event.setData(eventIndex++, currentrun); // Assuming 'batchdate' is the current timestamp
                        event.setData(eventIndex++, alert.date);
                        event.setData(eventIndex++, alert.server);
                        if (alert.type.equalsIgnoreCase("application")) {
                            if (alert.entityName == null) {
                                try {
                                    String relatedApp = getFromSTmap(alert.sourceOrTarget);
                                    if (relatedApp != null) {
                                        event = addMetaRelated(event, relatedApp);
                                        event.setData(eventIndex++, relatedApp);
                                    } else {
                                        event.setData(eventIndex++, alert.sourceOrTarget);
                                        event = addMetaRelated(event, alert.sourceOrTarget);
                                    }
                                } catch (Exception ex) {
                                    logger.debug("SmartAlertList: Unable to get relatedApp: " + ex);
                                }
                            } else {
                                event = addMetaRelated(event, alert.entityName);
                                event.setData(eventIndex++, alert.entityName);
                            }
                        } else if (alert.type.equalsIgnoreCase("source") || alert.type.equalsIgnoreCase("target")) {
                            String relatedApp = getFromSTmap(alert.sourceOrTarget);
                            if (relatedApp != null) {
                                event = addMetaRelated(event, relatedApp);
                                event.setData(eventIndex++, relatedApp);
                            } else {
                                event.setData(eventIndex++, alert.entityName != null ? alert.entityName : alert.sourceOrTarget);
                            }
                        } else if (alert.type.equalsIgnoreCase("node")) {
                            event.setData(eventIndex++, alert.entityName != null ? alert.entityName : alert.sourceOrTarget);
                        } else {
                            event.setData(eventIndex++, alert.app);
                        }
                        event.setData(eventIndex++, newLogLevel);
                        event.setData(eventIndex++, alert.message);
                        event.setData(eventIndex++, alert.alertMatched);

                        event.metadata.put("AlertType", "SmartAlert");
                        event.metadata.put("SmartAlertName", alert.alertMatched);
                        event.metadata.put("SmartAlertType", SAName);

                        if (alert.sourceOrTarget != null) {
                            event.metadata.put("SmartAlertObject", alert.sourceOrTarget);
                        }

                        if (alert.metricValue != null) {
                            event.metadata.put("SmartAlertMetricValue", alert.metricValue);
                        }

                        if (alert.metricUnit != null) {
                            event.metadata.put("SmartAlertMetricUnit", alert.metricUnit);
                        }

                        actions.add(event);
                    }
                }
            }

            //Create run history as final item, to track how long this all took
            try {
                WAEvent runHistoryResult = createRunHistory();

                actions.add(runHistoryResult);
            } catch (Exception e) {
                //System.out.println("Error creating run history: " + e.toString());
                String eMsg = String.format("StriimWatcher: There was an issue parsing run history.") + e;
                logger.debug(eMsg);
            }

            if (preserveStriimWatcherPosition) {
                saveHistory();
            }

            //Clear our data map
            clearGlobalArrays();
        }

        return actions;
    }


    // _________     _______  ______  _____
    //|__   __\ \   / /  __ \|  ____|/ ____|
    //   | |   \ \_/ /| |__) | |__  | (___
    //   | |    \   / |  ___/|  __|  \___ \
    //   | |     | |  | |    | |____ ____) |
    //   |_|     |_|  |_|    |______|_____/
    private static Type resolveType(final String ns, final String sourceName, final String tableName,
                                    final Map<String, String> columns, final String pkName, AuthToken myToken) throws Exception {
        final String typeName = createTypeName(sourceName, ns, tableName);
        return resolveType(typeName, columns, pkName, myToken);
    }

    private static Type resolveType(final String typeName, final Map<String, String> columns, final String pkName, AuthToken myToken)
            throws Exception {
        if (!typeCache.containsKey(typeName)) {

            if (columns == null)
                return null;

            //Let's try to delete the type first, in case it exists, before we create it. That way, if it changed, we can use the right one.
            try {
                runCommand("drop type " + typeName + ";");
            } catch (Exception ex) {
                logger.debug("Unable to delete existing type: " + typeName + "| Error: " + ex);
            }

            final Type t = createType(typeName, columns, pkName, myToken);
            typeCache.put(typeName, t);
        }
        return typeCache.get(typeName);
    }

    private static String createTypeName(final String sourceName, final String nameSpace, final String tableName) {
        //final String typeName = TypeGenerator.getTypeName(nameSpace, sourceName, tableName);// pass the source's
        // namespace, source name,
        // fully qualified table
        // name
        //return typeName;
        TypeNameKey key = new TypeNameKey(sourceName, nameSpace, tableName);
        if (!typeNames.containsKey(key)) {
            final String typeName = TypeGenerator.getTypeName(nameSpace, sourceName, tableName);
            typeNames.put(key, typeName);
        }
        return typeNames.get(key);
    }

    private static Type createType(final String typeName, final Map<String, String> columns, final String pkName, AuthToken myToken)
            throws Exception {

        //System.out.println("type name detected:" + typeName);

        TypeDefOrName typeDef = null;
        final ArrayList<TypeField> fields = new ArrayList<TypeField>();

        final String[] typeParts = typeName.split("\\.");
        Type type = (Type) MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.TYPE, typeParts[0],
                typeParts[1], 1, myToken);
        if (type == null) {
            for (final String column : columns.keySet()) {
                final String colName = column;
                final String colType = columns.get(column);
                final TypeName newtype = new TypeName(colType, 0);
                TypeField col = null;

                if (pkName.toLowerCase().contains(column.toLowerCase())) {
                    col = new TypeField(colName, newtype, true);
                } else {
                    col = new TypeField(colName, newtype, false);
                }
                fields.add(col);
            }

            typeDef = new TypeDefOrName(typeName, fields);
            //final Context ctx = Context.createContext(myToken);
            final CreateTypeStmt ctStmt = new CreateTypeStmt(typeDef.typeName, false, typeDef);
            final CallBackExecutor cb = new CallBackExecutor();
            com.webaction.runtime.compiler.Compiler.compile(ctStmt, am.getAuthContext(), cb);

            type = (Type) MetadataRepository.getINSTANCE().getMetaObjectByUUID(cb.uuid, myToken);
            return type;
        } else {
            //logger.info("Type {" + typeName + "} already exists, reusing");
            return type;

        }
    }


    // ____           _____ ______  __          __     ________      ________ _   _ _______
    //|  _ \   /\    / ____|  ____| \ \        / /\   |  ____\ \    / /  ____| \ | |__   __|
    //| |_) | /  \  | (___ | |__     \ \  /\  / /  \  | |__   \ \  / /| |__  |  \| |  | |
    //|  _ < / /\ \  \___ \|  __|     \ \/  \/ / /\ \ |  __|   \ \/ / |  __| | . ` |  | |
    //| |_) / ____ \ ____) | |____     \  /\  / ____ \| |____   \  /  | |____| |\  |  | |
    //|____/_/    \_\_____/|______|     \/  \/_/    \_\______|   \/   |______|_| \_|  |_|

    private static Map<String, String> getColumnMap(String mapName) {
        final Map<String, String> columns = new LinkedHashMap<String, String>();

        switch (mapName) {
            case watcherTableName.mon_table_runhistory:
                columns.put(watcherTableName.mon_table_runhistory_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("runtimeEnd", "DateTime");
                columns.put("runtimeDurationMS", "Integer");
                columns.put("clusterName", "String");
                columns.put("companyName", "String");
                columns.put("lastrun", "DateTime");
                columns.put("nextrun", "DateTime");
                break;
            case watcherTableName.mon_appdetail:
                columns.put(watcherTableName.mon_appdetail_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("command", "String");
                columns.put("appName", "String");
                columns.put("appStatus", "String");
                columns.put("totalInput", "Integer");
                columns.put("totalOutput", "Integer");
                columns.put("isBackpressured", "Boolean");
                columns.put("isRecoveryEnabled", "Boolean");
                columns.put("recoverySetting", "String");
                columns.put("checkpointStatus", "String");
                columns.put("checkpointDetail", "String");
                columns.put("isEncryptionEnabled", "Boolean");
                columns.put("deploymentOn", "String");
                columns.put("deploymentIn", "String");
                columns.put("appCreatedDate", "DateTime");
                columns.put("latestActivity", "DateTime");
                break;
            case watcherTableName.mon_node_applications:
                columns.put(watcherTableName.mon_node_applications_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("command", "String");
                columns.put("montype", "String");
                columns.put("appname", "String");
                columns.put("status", "String");
                columns.put("rate", "Double");
                columns.put("sourcerate", "Integer");
                columns.put("cpurate", "Double");
                columns.put("numservers", "Integer");
                columns.put("latestActivity", "DateTime");
                break;
            case watcherTableName.mon_node_cluster:
                columns.put(watcherTableName.mon_node_cluster_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("command", "String");
                columns.put("montype", "String");
                columns.put("nodename", "String");
                columns.put("striimversion", "String");
                columns.put("freemem", "String");
                columns.put("cpurate", "Integer");
                columns.put("uptime", "String");
                break;
            case watcherTableName.mon_node_elasticsearch:
                columns.put(watcherTableName.mon_node_elasticsearch_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("command", "String");
                columns.put("montype", "String");
                columns.put("elasticsearchReceiveThroughput", "Long");
                columns.put("elasticsearchTransmitThroughput", "Long");
                columns.put("elasticsearchClusterStorageFree", "Long");
                columns.put("elasticsearchClusterStorageTotal", "Long");
                break;
            case watcherTableName.mon_table_comparison:
                columns.put(watcherTableName.mon_table_comparison_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("appName", "String");
                columns.put("sourceName", "String");
                columns.put("targetName", "String");
                columns.put("srcNumOfDeletes", "Integer");
                columns.put("tgtNumOfDeletes", "Integer");
                columns.put("diffNumOfDeletes", "Integer");
                columns.put("srcNumOfDdls", "Integer");
                columns.put("tgtNumOfDdls", "Integer");
                columns.put("diffNumOfDdls", "Integer");
                columns.put("srcNumOfPkupdates", "Integer");
                columns.put("tgtNumOfPkupdates", "Integer");
                columns.put("diffNumOfPkupdates", "Integer");
                columns.put("srcNumOfUpdates", "Integer");
                columns.put("tgtNumOfUpdates", "Integer");
                columns.put("diffNumOfUpdates", "Integer");
                columns.put("srcNumOfInserts", "Integer");
                columns.put("tgtNumOfInserts", "Integer");
                columns.put("diffNumOfInserts", "Integer");
                break;
            case watcherTableName.mon_table_comparison_sli:
                columns.put(watcherTableName.mon_table_comparison_sli_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("timesincelastbatch", "Long");
                columns.put("appName", "String");
                columns.put("sourceName", "String");
                columns.put("targetName", "String");
                columns.put("srcNumOfDeletes_sli", "Integer");
                columns.put("tgtNumOfDeletes_sli", "Integer");
                columns.put("diffNumOfDeletes_sli", "Integer");
                columns.put("srcNumOfDdls_sli", "Integer");
                columns.put("tgtNumOfDdls_sli", "Integer");
                columns.put("diffNumOfDdls_sli", "Integer");
                columns.put("srcNumOfPkupdates_sli", "Integer");
                columns.put("tgtNumOfPkupdates_sli", "Integer");
                columns.put("diffNumOfPkupdates_sli", "Integer");
                columns.put("srcNumOfUpdates_sli", "Integer");
                columns.put("tgtNumOfUpdates_sli", "Integer");
                columns.put("diffNumOfUpdates_sli", "Integer");
                columns.put("srcNumOfInserts_sli", "Integer");
                columns.put("tgtNumOfInserts_sli", "Integer");
                columns.put("diffNumOfInserts_sli", "Integer");
                break;
            case watcherTableName.mon_lee:
                columns.put(watcherTableName.mon_lee_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("sourceApp", "String");
                columns.put("sourceName", "String");
                columns.put("sourceType", "String");
                columns.put("targetApp", "String");
                columns.put("targetName", "String");
                columns.put("targetType", "String");
                columns.put("lagEndToEnd", "String");
                columns.put("measuredAt", "DateTime");
                columns.put("sourceTime", "String");
                columns.put("minLEE", "Double");
                columns.put("maxLEE", "Double");
                columns.put("avgLEE", "Double");
                columns.put("sampleSize", "Double");
                break;
            case watcherTableName.mon_appcomponentoutput:
                columns.put(watcherTableName.mon_appcomponentoutput_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("appName", "String");
                columns.put("componentName", "String");
                columns.put("command", "String");
                columns.put("type", "String");
                columns.put("jsondata", "String");
                break;
            case watcherTableName.mon_apptablecolumndetail:
                columns.put(watcherTableName.mon_apptablecolumndetail_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("typeName", "String");
                columns.put("appName", "String");
                columns.put("tableName", "String");
                columns.put("createdDate", "DateTime");
                columns.put("columnName", "String");
                columns.put("columnType", "String");
                columns.put("isPK", "Boolean");
                break;
            case watcherTableName.mon_table_datawarehouse_detail:
                columns.put(watcherTableName.mon_table_datawarehouse_detail_pk, "Long"); // Assuming dwdid is the PK
                columns.put("batchdate", "DateTime");
                columns.put("appName", "String");
                columns.put("sourceName", "String");
                columns.put("targetName", "String");
                columns.put("target_adaptername", "String");

                columns.put("projectId", "String");
                columns.put("Mode", "String");
                columns.put("streamingUpload", "Boolean");
                columns.put("StreamingConfiguration", "String");
                columns.put("optimizedMerge", "Boolean");

                columns.put("batch_event_count", "Integer");
                columns.put("batch_interval", "Integer");

                columns.put("total_batches_created", "Integer");
                columns.put("partition_pruned_batches", "Integer");
                columns.put("last_successful_merge_time", "DateTime");
                columns.put("total_batches_ignored", "Integer");
                columns.put("max_integration_time_ms", "Integer");
                columns.put("avg_in_mem_compaction_time_ms", "Double");
                columns.put("avg_batch_size_bytes", "Integer");
                columns.put("avg_event_count_per_batch", "Double");
                columns.put("min_integration_time_ms", "Integer");
                columns.put("total_batches_queued", "Integer");
                columns.put("avg_compaction_time_ms", "Double");
                columns.put("avg_waiting_time_in_queue_ms", "Double");
                columns.put("avg_integration_time_ms", "Double");
                columns.put("total_batches_uploaded", "Integer");
                columns.put("avg_merge_time_ms", "Double");
                columns.put("avg_stage_resources_mgmt_time_ms", "Double");
                columns.put("avg_upload_time_ms", "Double");

                // Fields from "Last batch info"
                columns.put("last_batch_updates", "Integer");
                columns.put("last_batch_event_count", "Integer");
                columns.put("last_batch_inserts", "Integer");
                columns.put("last_batch_max_record_size_bytes", "Integer");
                columns.put("last_batch_total_events_merged", "Integer");
                columns.put("last_batch_ddls", "Integer");
                columns.put("last_batch_sequence_number", "Integer");
                columns.put("last_batch_size_bytes", "Integer");
                columns.put("last_batch_deletes", "Integer");
                columns.put("last_batch_pk_updates", "Integer");
                columns.put("last_batch_accumulation_time_ms", "Integer");

                // Fields from "Integration Task Time"
                columns.put("last_batch_compaction_time_ms", "Integer");
                columns.put("last_batch_stage_resources_mgmt_time_ms", "Integer");
                columns.put("last_batch_upload_time_ms", "Integer");
                columns.put("last_batch_merge_time_ms", "Integer");
                columns.put("last_batch_in_mem_compaction_time_ms", "Integer");
                columns.put("last_batch_pk_update_time_ms", "Integer");
                columns.put("last_batch_ddl_execution_time_ms", "Integer");
                columns.put("last_batch_total_integration_time_ms", "Integer");
                break;
            case watcherTableName.mon_logwatcher:
                columns.put(watcherTableName.mon_logwatcher_pk, "Long");
                columns.put("batchdate", "DateTime");
                columns.put("log_date", "DateTime");
                columns.put("server", "String");
                columns.put("appName", "String");
                columns.put("log_level", "DateTime");
                columns.put("message", "String");
                columns.put("contextbuffertext", "String");
                break;
        }
        return columns;
    }

    private static WAEvent getBaseWAEvent(String namespace, String tableName, String pkid) throws Exception {
        final Map<String, String> WAColumns = getColumnMap(tableName);
        int numOfColumns = WAColumns.size();

        AuthToken myToken = am.getAuthToken();

        if (thisNamespace == "" || thisNamespace == null || thisNamespace.isEmpty()) {
            thisNamespace = "admin";
        }

        MetaInfo.Type type = resolveType(thisNamespace, namespace, tableName, WAColumns, pkid, myToken);

        WAEvent event = new WAEvent(numOfColumns, null);
        event.metadata = new HashMap<String, Object>();
        event.userdata = new HashMap<String, Object>();
        event.metadata.put("TimeStamp", DateTime.now());
        event.metadata.put("NextRun", nextStart);
        event.metadata.put("TotalRuns", totalRuns);
        event.metadata.put("TableName", namespace + "." + tableName);
        event.metadata.put("OperationName", "INSERT");
        event.metadata.put("ColumnCount", numOfColumns);
        event.metadata.put("OPERATION_TS", DateTime.now().getMillis() / 1000);

        event.data = new Object[numOfColumns];
        event.before = new Object[numOfColumns];

        event.typeUUID = type.getUuid();

        return event;
    }

    private static WAEvent addMetaRelated(WAEvent incomingWA, String appName) {
        incomingWA.metadata.put("RelatedAppName", appName);
        return incomingWA;
    }

    private static WAEvent addMetaRelated(WAEvent incomingWA, String sourceAppName, String targetAppName) {
        incomingWA.metadata.put("RelatedAppName", sourceAppName); //This can be default for consistency for filtering
        incomingWA.metadata.put("RelatedSourceAppName", sourceAppName);
        incomingWA.metadata.put("RelatedTargetAppName", targetAppName);
        return incomingWA;
    }

    //  _____ ______ _   _ ______ _____  _____ _____   _____  _    _ _   _
    // / ____|  ____| \ | |  ____|  __ \|_   _/ ____| |  __ \| |  | | \ | |
    //| |  __| |__  |  \| | |__  | |__) | | || |      | |__) | |  | |  \| |
    //| | |_ |  __| | . ` |  __| |  _  /  | || |      |  _  /| |  | | . ` |
    //| |__| | |____| |\  | |____| | \ \ _| || |____  | | \ \| |__| | |\  |
    // \_____|______|_| \_|______|_|  \_\_____\_____| |_|  \_\\____/|_| \_|
    /**
     * Runs any command against the Striim Tungsten API
     *
     * This method takes a context and the command
     *
     * @param command The string of the command to execute (i.e. 'mon;').
     * @return Returns a JSON String output of the command
     */
    private static String runCommand(String command) throws Exception {
        String responseOutput = "";

        // Check to confirm that the command ends in semi-colon:
        command = command.endsWith(";") ? command : command + ";";

        long maxWaitHours = 24; // Replace X with the desired maximum wait time in hours
        long retryIntervalSeconds = repeatInSeconds; // Replace Y with the desired retry interval in seconds
        long startTime = System.currentTimeMillis();
        int executionCount = 0;

        while ((System.currentTimeMillis() - startTime) < maxWaitHours * 3600000) {
            try {
                responseOutput = runCOMMAND(command);
                return responseOutput;
            } catch (EmptyStackException esx) {
                throw esx;
            }
            catch (Exception ex) {
                logger.debug("runCommand: Unable to run command:" + command);
            }

            try {
                long waitTimeMillis;
                if (executionCount < 10) {
                    waitTimeMillis = 100 + (long) ((retryIntervalSeconds * 1000 - 100) * (executionCount / 9.0));
                    executionCount++;
                } else {
                    waitTimeMillis = retryIntervalSeconds * 1000;
                }
                Thread.sleep(waitTimeMillis); // Wait for calculated or retry interval

                if (Thread.interrupted()) { // Check for interruption during sleep
                    logger.debug("runCommand: Thread interrupted while waiting for next command retry:" + command);
                    break; // Exit the loop if interrupted
                }
            } catch (InterruptedException e) {
                logger.debug("runCommand: Thread interrupted while waiting for next command retry:" + command);
                Thread.currentThread().interrupt(); // Restore interrupted status
                break; // Exit the loop if interrupted
            }
        }

        if ((System.currentTimeMillis() - startTime) >= maxWaitHours * 3600000) {
            logger.debug("runCommand: Maximum wait time reached. Unable to run command:" + command);
            // Handle the failure (e.g., throw an exception, retry later, etc.)
        }

        return responseOutput;
    }

    private static String runCOMMAND(String command) throws Exception {

        String responseOutput = "";

        if (Thread.interrupted()) {
            logger.debug("runCommand: Thread interrupted before running command:" + command);
            throw new EmptyStackException();
        }

        // Allow for up to 5 recursive calls, by using try-> catch -> recursion logic
        try {
            if (savedURL == null) {
                String StriimURLBase = "http://localhost:9080";

                // Construct the URL
                String apiUrl = StriimURLBase + "/api/v2/tungsten";

                URL url = new URL(apiUrl);
                savedURL = url;
            }

            // Open a connection to the URL
            HttpURLConnection connection = (HttpURLConnection) savedURL.openConnection();

            // Set the request method to POST
            connection.setRequestMethod("POST");

            // Set request headers
            connection.setRequestProperty("authorization", "STRIIM-TOKEN " + am.getAuthString());
            connection.setRequestProperty("content-type", "text/plain");

            // Enable input/output streams
            connection.setDoOutput(true);

            // Write the command to the output stream
            try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                wr.writeBytes(command);
                wr.flush();
            }

            // Read the response from the input stream
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }

                responseOutput = response.toString();
            }

            // Close the connection
            connection.disconnect();

        } catch (Exception e) {
            String errorMsg = "Tried to run API call (" + command + "), got exception but will retry:" + e;
            logger.debug(errorMsg);
        }

        // logger.debug("Run command (" + command + ") successful: " + responseOutput);
        return responseOutput;
    }

    private static void createComponentOutput(String appName, String componentName, String command, String type, String jsonData, Boolean forceOutput) throws Exception {
        // Skip this if we are not including component output
        if (includeComponentDetailsAsOutput || forceOutput){
            cmpntOutput.add(new ComponentOutput(appName, componentName, command, type, jsonData));
        }
    }

    private static void createTypeOutput(String name, String appName, String tableName, DateTime createdDate, String columnName, String columnType, Boolean isPK) throws Exception {
        // Skip this if we are not including component output
        if (!includeTypeDetailsAsOutput){
            return;
        } else {
            upsertTypeOutputList(name, appName, tableName, createdDate, columnName, columnType, isPK);
        }
    }

    // __  __  ____  _   _   ______ _    _ _   _  _____ _______ _____ ____  _   _  _____
    //|  \/  |/ __ \| \ | | |  ____| |  | | \ | |/ ____|__   __|_   _/ __ \| \ | |/ ____|
    //| \  / | |  | |  \| | | |__  | |  | |  \| | |       | |    | || |  | |  \| | (___
    //| |\/| | |  | | . ` | |  __| | |  | | . ` | |       | |    | || |  | | . ` |\___ \
    //| |  | | |__| | |\  | | |    | |__| | |\  | |____   | |   _| || |__| | |\  |____) |
    //|_|  |_|\____/|_| \_| |_|     \____/|_| \_|\_____|  |_|  |_____\____/|_| \_|_____/
    private static WAEvent createMonWae(String command, String jsonMonData) throws Exception {
        //String jsonMonData = runCommand(ctx, command);

        String tableName = watcherTableName.mon_appdetail;
        String pkid = watcherTableName.mon_appdetail_pk;

        String namespace = watcherTableName.mon_namespace;

        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

        int eventIndex = 0;

        long monid = generateUniqueInteger();

        boolean isBackpressured = false;
        boolean isRecoveryEnabled = false;
        String checkpointStatus = null;
        String recoverySetting = null;
        String checkpointDetail = null;
        boolean isEncryptionEnabled = false;
        String appCreatedDate = null;
        String deplyomentOn = null;
        String deploymentIn = null;

        Integer sourceInputTotal = 0;
        Integer targetOutputTotal = 0;

        //Set the before if needed
        //event.setBefore(0, monid);

        event.setData(eventIndex++, monid);
        event.setData(eventIndex++, currentrun);
        event.setData(eventIndex++, command);

        JSONArray jsonArray = new JSONArray(jsonMonData);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);

            if (jsonObject.has("output")) {
                Object outputObject = jsonObject.opt("output");
                if (outputObject instanceof JSONObject) {
                    JSONObject output = jsonObject.getJSONObject("output");
                    //String entityType = output.has("entityType") ? output.getString("entityType") : null;
                    String fullName = output.has("fullName") ? output.getString("fullName") : null;
                    String statusChange = output.has("statusChange") ? output.getString("statusChange") : null;
                    //String rate = output.has("rate") ? output.getString("rate") : null;
                    //String sourceRate = output.has("sourceRate") ? output.getString("sourceRate") : null;
                    //String cpuRate = output.has("cpuRate") ? output.getString("cpuRate") : null;
                    //String numServers = output.has("numServers") ? output.getString("numServers") : null;
                    String latestActivity = output.has("latestActivity") ? output.getString("latestActivity") : null;

                    event.setData(eventIndex++, fullName);

                    event = addMetaRelated(event, fullName);

                    event.setData(eventIndex++, statusChange);

                    if (output.has("applicationComponents")) {
                        JSONArray applicationComponents = output.getJSONArray("applicationComponents");
                        for (int j = 0; j < applicationComponents.length(); j++) {
                            JSONObject component = applicationComponents.getJSONObject(j);

                            String ac_entityType = component.has("entityType") ? component.getString("entityType") : null;
                            String ac_fullName = component.has("fullName") ? component.getString("fullName") : null;
                            //String ac_statusChange = component.has("statusChange") ? component.getString("statusChange") : null;
                            //String ac_rate = component.has("rate") ? component.getString("rate") : null;
                            //String ac_sourceRate = component.has("sourceRate") ? component.getString("sourceRate") : null;
                            //String ac_cpuRate = component.has("cpuRate") ? component.getString("cpuRate") : null;
                            //String ac_numServers = component.has("numServers") ? component.getString("numServers") : null;
                            //String ac_latestActivity = component.has("latestActivity") ? component.getString("latestActivity") : null;

                            if (ac_entityType.equals("SOURCE")) {
                                sourceInputTotal = addSource(ac_fullName, sourceInputTotal, fullName);
                            }

                            // Need to identify backpressure
                            if (ac_entityType.equals("STREAM")) {
                                isBackpressured = addStream(ac_fullName, isBackpressured, fullName);
                            }

                            if (ac_entityType.equals("TARGET")) {
                                targetOutputTotal = addTarget(ac_fullName, targetOutputTotal, fullName);
                            }

                            // Flows can have nested equivalent to applicationComponents
                            if (ac_entityType.equals("FLOW") && component.has("flowComponents")) {
                                JSONArray flowComponents = component.getJSONArray("flowComponents");

                                for (int t = 0; t < flowComponents.length(); t++) {
                                    JSONObject fcomponent = flowComponents.getJSONObject(t);

                                    String f_ac_entityType = fcomponent.has("entityType") ? fcomponent.getString("entityType") : null;
                                    String f_ac_fullName = fcomponent.has("fullName") ? fcomponent.getString("fullName") : null;

                                    ac_entityType = f_ac_entityType;
                                    ac_fullName = f_ac_fullName;

                                    // Duplicate to logic above -- need to create functions for pieces
                                    if (ac_entityType.equals("SOURCE")) {
                                        sourceInputTotal = addSource(ac_fullName, sourceInputTotal, fullName);
                                    }

                                    if (ac_entityType.equals("STREAM")) {
                                        isBackpressured = addStream(ac_fullName, isBackpressured, fullName);
                                    }

                                    if (ac_entityType.equals("TARGET")) {
                                        targetOutputTotal = addTarget(ac_fullName, targetOutputTotal, fullName);
                                    }
                                }
                            }

                        }
                    }

                    if (output.has("CheckpointInformation")) {
                        JSONObject checkpointInfo = output.getJSONObject("CheckpointInformation");

                        //int ac_numberOfNormalCheckpointsInjected = checkpointInfo.has("numberOfNormalCheckpointsInjected") ? checkpointInfo.getInt("numberOfNormalCheckpointsInjected") : 0;
                        //int ac_numberOfNormalCheckpointsRecorded = checkpointInfo.has("numberOfNormalCheckpointsRecorded") ? checkpointInfo.getInt("numberOfNormalCheckpointsRecorded") : 0;
                        //String ac_timeAtWhichLastNormalCheckpointWasRecorded = checkpointInfo.has("timeAtWhichLastNormalCheckpointWasRecorded") ? checkpointInfo.getString("timeAtWhichLastNormalCheckpointWasRecorded") : null;
                        //String ac_timeTakenForLastCheckpoint = checkpointInfo.has("timeTakenForLastCheckpoint") ? checkpointInfo.getString("timeTakenForLastCheckpoint") : null;
                        //int ac_numberOfLateCheckpointsInjected = checkpointInfo.has("numberOfLateCheckpointsInjected") ? checkpointInfo.getInt("numberOfLateCheckpointsInjected") : 0;
                        //int ac_numberOfLateCheckpointsRecorded = checkpointInfo.has("numberOfLateCheckpointsRecorded") ? checkpointInfo.getInt("numberOfLateCheckpointsRecorded") : 0;
                        //String ac_timeAtWhichLastLateCheckpointWasRecorded = checkpointInfo.has("timeAtWhichLastLateCheckpointWasRecorded") ? checkpointInfo.getString("timeAtWhichLastLateCheckpointWasRecorded") : null;
                        //String ac_timeTakenForLastLateCheckpoint = checkpointInfo.has("timeTakenForLastLateCheckpoint") ? checkpointInfo.getString("timeTakenForLastLateCheckpoint") : null;
                        String ac_statusOfCheckpoint = checkpointInfo.has("statusOfCheckpoint") ? checkpointInfo.getString("statusOfCheckpoint") : null;

                        isRecoveryEnabled = true;
                        checkpointStatus = ac_statusOfCheckpoint;

                    }

                    if (includeAppDescribeDetail)
                    {
                        //Wrap in a try/catch in order to not prevent other data points from being recorded
                        try
                        {
                            command = "describe " + fullName;
                            String jsonDescribeDetail = runCommand(command);

                            JSONArray jsonDescribeArray = new JSONArray(jsonDescribeDetail);

                            for (int d = 0; d < jsonDescribeArray.length(); d++) {
                                JSONObject jsonDescribeObject = jsonDescribeArray.getJSONObject(d);

                                if (jsonDescribeObject.has("output")) {
                                    Object jsonDescribeOutputObject = jsonDescribeObject.opt("output");

                                    if (jsonDescribeOutputObject instanceof JSONArray) {
                                        JSONArray outputArray = jsonDescribeObject.getJSONArray("output");

                                        JSONObject firstOutputObject = outputArray.getJSONObject(0);

                                        String appDescCreatedDate = firstOutputObject.has("created") ? firstOutputObject.getString("created") : null;
                                        String appRecoverySetting = firstOutputObject.has("recovery") ? firstOutputObject.getString("recovery") : null;
                                        Boolean appEncryption = firstOutputObject.has("encryption") ? firstOutputObject.getBoolean("encryption") : null;
                                        JSONArray appCheckpointDetail = firstOutputObject.has("Checkpoint") ? firstOutputObject.getJSONArray("Checkpoint") : null;

                                        recoverySetting = appRecoverySetting;
                                        checkpointDetail = appCheckpointDetail == null ? null : appCheckpointDetail.toString();
                                        isEncryptionEnabled = appEncryption;
                                        appCreatedDate = appDescCreatedDate;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            String eMsg = "Unable to capture describe detail: " + e;
                            logger.debug(eMsg);
                        }

                    }

                    if (includeAppStatusDetail)
                    {
                        //Wrap in a try/catch in order to not prevent other data points from being recorded
                        try
                        {
                            command = "status " + fullName;
                            String jsonStatusDetail = runCommand(command);

                            JSONArray jsonStatusArray = new JSONArray(jsonStatusDetail);

                            for (int d = 0; d < jsonStatusArray.length(); d++) {
                                JSONObject jsonDescribeObject = jsonStatusArray.getJSONObject(d);

                                if (jsonDescribeObject.has("output")) {
                                    JSONObject firstOutputObject = jsonDescribeObject.getJSONObject("output");

                                    if (firstOutputObject.has("deploymentInfo"))
                                    {
                                        JSONArray deploymentInfoArray = firstOutputObject.getJSONArray("deploymentInfo");
                                        JSONObject appDeploymentInfo = deploymentInfoArray.getJSONObject(0);

                                        String deployOn = appDeploymentInfo.has("on") ? appDeploymentInfo.getString("on") : null;
                                        String deployIn = appDeploymentInfo.has("in") ? appDeploymentInfo.getJSONArray("in").join(", ") : null;

                                        deplyomentOn = deployOn;
                                        deploymentIn = deployIn;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            String eMsg = "Unable to capture status detail: " + e;
                            logger.debug(eMsg);
                        }

                    }

                    //Final Columns
                    event.setData(eventIndex++, sourceInputTotal);
                    event.setData(eventIndex++, targetOutputTotal);
                    event.setData(eventIndex++, isBackpressured);
                    event.setData(eventIndex++, isRecoveryEnabled);
                    event.setData(eventIndex++, recoverySetting);
                    event.setData(eventIndex++, checkpointStatus);
                    event.setData(eventIndex++, checkpointDetail);
                    event.setData(eventIndex++, isEncryptionEnabled);
                    event.setData(eventIndex++, deplyomentOn);
                    event.setData(eventIndex++, deploymentIn);
                    event.setData(eventIndex++, parseAndConvertDateTimeToNull(appCreatedDate));
                    event.setData(eventIndex++, parseAndConvertDateTimeToNull(latestActivity));
                }

            }
        }

        return event;
    }

    private static List<WAEvent> createNodeHealth(String jsonMonData) throws Exception {

        //String jsonMonData = runCommand(command);

        String command = "mon;";

        ArrayList<WAEvent> results = new ArrayList<WAEvent>();

        JSONArray jsonArray = new JSONArray(jsonMonData);

        String storedCommand = command;
        String monValType = "";

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);

            //output element of JSON Response Object
            if (jsonObject.has("output")) {
                JSONObject output = jsonObject.getJSONObject("output");

                try {
                    //output.striimApplications element of JSON Response Object
                    if (output.has("striimApplications")) {

                        Object striimAppObject = output.opt("striimApplications");
                        if (striimAppObject instanceof JSONArray) {
                            JSONArray striimApplications = output.getJSONArray("striimApplications");

                            monValType = "Striim Applications";

                            for (int j = 0; j < striimApplications.length(); j++) {
                                JSONObject application = striimApplications.getJSONObject(j);

                                String entityType = application.has("entityType") ? application.getString("entityType") : null;
                                String fullName = application.has("fullName") ? application.getString("fullName") : null;
                                String statusChange = application.has("statusChange") ? application.getString("statusChange") : null;
                                String rate = application.has("rate") ? application.getString("rate") : null;
                                String sourceRate = application.has("sourceRate") ? application.getString("sourceRate") : null;
                                String cpuRate = application.has("cpuRate") ? application.getString("cpuRate") : null;
                                String numServers = application.has("numServers") ? application.getString("numServers") : null;
                                String latestActivity = application.has("latestActivity") ? application.getString("latestActivity") : null;

                                //Create striimApplications WAEvent
                                String tableName = watcherTableName.mon_node_applications;
                                String pkid = watcherTableName.mon_node_applications_pk;

                                String namespace = watcherTableName.mon_namespace;

                                WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                                // Use event.setData(eventIndex++, "abc"); to set the next value
                                int eventIndex = 0;

                                long monappid = generateUniqueInteger();

                                //event.setBefore(0, monappid);

                                event.setData(eventIndex++, monappid);
                                event.setData(eventIndex++, currentrun);
                                event.setData(eventIndex++, storedCommand);
                                event.setData(eventIndex++, entityType);
                                event.setData(eventIndex++, fullName);
                                event.setData(eventIndex++, statusChange);
                                event.setData(eventIndex++, getNumericOnly(rate));
                                event.setData(eventIndex++, getNumericOnly(sourceRate));
                                event.setData(eventIndex++, getNumericOnly(cpuRate));
                                event.setData(eventIndex++, getNumericOnly(numServers));
                                event.setData(eventIndex++, parseAndConvertDateTimeToNull(latestActivity));

                                event = addMetaRelated(event, fullName);

                                results.add(event);

                            }
                        }
                    }
                }
                catch (Exception ex) {
                    logger.debug("Unable to parse striimApplications data:" + ex);
                }

                try {

                    //output.striimClusterNodes element of JSON Response Object
                    if (output.has("striimClusterNodes") && includeNodeCluster) {

                        Object striimClusterNodesOutputObject = output.opt("striimClusterNodes");
                        if (striimClusterNodesOutputObject instanceof JSONArray) {
                            JSONArray striimClusterNodes = output.getJSONArray("striimClusterNodes");

                            monValType = "Striim Cluster Nodes";

                            for (int j = 0; j < striimClusterNodes.length(); j++) {
                                JSONObject node = striimClusterNodes.getJSONObject(j);

                                String entityType = node.has("entityType") ? node.getString("entityType") : null;
                                String name = node.has("name") ? node.getString("name") : null;
                                String version = node.has("version") ? node.getString("version") : null;
                                String freeMemory = node.has("freeMemory") ? node.getString("freeMemory") : null;
                                String cpuRate = node.has("cpuRate") ? node.getString("cpuRate") : null;
                                String uptime = node.has("uptime") ? node.getString("uptime") : null;

                                //Create striimApplications WAEvent
                                String tableName = watcherTableName.mon_node_cluster;
                                String pkid = watcherTableName.mon_node_cluster_pk;
                                String namespace = watcherTableName.mon_namespace;

                                WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                                int eventIndex = 0;

                                long monnodeclusterid = generateUniqueInteger();

                                //event.setBefore(0, monnodeclusterid);

                                event.setData(eventIndex++, monnodeclusterid);
                                event.setData(eventIndex++, currentrun);
                                event.setData(eventIndex++, storedCommand);
                                event.setData(eventIndex++, entityType);
                                event.setData(eventIndex++, name);
                                event.setData(eventIndex++, version);
                                event.setData(eventIndex++, freeMemory);
                                event.setData(eventIndex++, getNumericOnly(cpuRate));
                                event.setData(eventIndex++, uptime);

                                results.add(event);

                            }
                        }
                    }
                } catch (Exception ex) {
                    logger.debug("Unable to parse striimClusterNodes data: " + ex);
                }

                try {
                    //output.elasticsearch element of JSON Response Object
                    if (output.has("elasticsearch") && includeNodeES) {
                        JSONObject elasticsearch = output.getJSONObject("elasticsearch");

                        monValType = "elasticsearch";

                        String tableName = watcherTableName.mon_node_elasticsearch;
                        String pkid = watcherTableName.mon_node_elasticsearch_pk;

                        String namespace = watcherTableName.mon_namespace;

                        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

                        // Use event.setData(eventIndex++, "abc"); to set the next value
                        int eventIndex = 0;

                        long monnodesid = generateUniqueInteger();

                        //event.setBefore(0, monnodesid);

                        String elasticsearchReceiveThroughput = elasticsearch.has("elasticsearchReceiveThroughput") ? elasticsearch.getString("elasticsearchReceiveThroughput") : null;
                        String elasticsearchTransmitThroughput = elasticsearch.has("elasticsearchTransmitThroughput") ? elasticsearch.getString("elasticsearchTransmitThroughput") : null;
                        String elasticsearchClusterStorageFree = elasticsearch.has("elasticsearchClusterStorageFree") ? elasticsearch.getString("elasticsearchClusterStorageFree") : null;
                        String elasticsearchClusterStorageTotal = elasticsearch.has("elasticsearchClusterStorageTotal") ? elasticsearch.getString("elasticsearchClusterStorageTotal") : null;

                        event.setData(eventIndex++, monnodesid);
                        event.setData(eventIndex++, currentrun);
                        event.setData(eventIndex++, storedCommand);
                        event.setData(eventIndex++, monValType);
                        event.setData(eventIndex++, getNumericOnly(elasticsearchReceiveThroughput));
                        event.setData(eventIndex++, getNumericOnly(elasticsearchTransmitThroughput));
                        event.setData(eventIndex++, getNumericOnly(elasticsearchClusterStorageFree));
                        event.setData(eventIndex++, getNumericOnly(elasticsearchClusterStorageTotal));


                        event.setBefore(0, monnodesid);

                        results.add(event);

                    }
                } catch (Exception ex) {
                    logger.debug("Unable to parse elasticsearch data: " + ex);
                }

            }
        }

        return results;
    }

    private static List<WAEvent> monApplication(MetaInfo.Flow app) throws Exception {

        // Get result
        ArrayList<WAEvent> result = new ArrayList<WAEvent>();
        try {
            Context ctx = am.getAuthContext();

            //logger.warn("App Monitor: The Application " + app.name + " was monitored.");
            String appNameFull = app.nsName + "." + app.name;
            String command = "mon " + appNameFull + ";";
            String monAppResult = runCommand(command);

            createComponentOutput(appNameFull, appNameFull, "mon", "APP", monAppResult, includeComponentDetailsAsOutput);

            result.add(createMonWae(command, monAppResult));


        } catch (Exception e) {
            String eMsg = String.format("StriimWatcher: There was an error while issuing start for the app %s.", app.name) + e;
            logger.debug(eMsg);
            return result;
        }
        return result;
    }

    private static WAEvent getComponentOutputEvent(ComponentOutput cmpt) throws Exception {
        String tableName = watcherTableName.mon_appcomponentoutput;
        String pkid = watcherTableName.mon_appcomponentoutput_pk;

        String namespace = watcherTableName.mon_namespace;

        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

        int eventIndex = 0;

        long monid = generateUniqueInteger();

        event.setData(eventIndex++, monid);
        event.setData(eventIndex++, currentrun);
        event.setData(eventIndex++, cmpt.appName);
        event.setData(eventIndex++, cmpt.componentName);
        event.setData(eventIndex++, cmpt.command);
        event.setData(eventIndex++, cmpt.type);
        event.setData(eventIndex++, cmpt.jsondata);

        event = addMetaRelated(event, cmpt.appName);

        return event;
    }

    private static void mapTableNameToType(String sourceName, String appName, String tableName) {
        if (includeTypeDetailsAsOutput)
        {
            try {
                String typePredicted = sourceName + "_" + tableName.replace(".", "_") + "_";

                for (StriimTypeList c : typeListOutput) {
                    if (c.typeName.toLowerCase().contains(typePredicted.toLowerCase())) {
                        if (upsertTypeList(c.typeName, appName, tableName, c.createdDate, c.columnName, c.columnType, c.isPK)) {
                            upsertTypeOutputList(c.typeName, appName, tableName, c.createdDate, c.columnName, c.columnType, c.isPK);
                        }
                    }
                }
            }
            catch (Exception ex) {
                logger.debug("Unable to add table map: " + ex);
            }

        }
    }

    private static Integer addSource(String ac_fullName, Integer sourceInputTotal, String appName) {
        try {
            insertIntoSTmap(ac_fullName, appName);
        } catch (Exception ex) {
            logger.debug("Unable to map into STMap: Source: " + ac_fullName + " | App: " + appName + "- ex:" + ex);
        }

        try {

            // Need to get source information
            String command = "mon " + ac_fullName + ";";
            String sourceInfo = runCommand(command);

            // Mon Source - To add sourceInput and TableInformation

            createComponentOutput(appName, ac_fullName, "mon", "SOURCE", sourceInfo, includeComponentDetailsAsOutput);

            JSONArray src_jsonArray = new JSONArray(sourceInfo);

            for (int k = 0; k < src_jsonArray.length(); k++) {
                JSONObject src_jsonObject = src_jsonArray.getJSONObject(k);
                if (src_jsonObject.has("output")) {
                    JSONObject src_output = src_jsonObject.getJSONObject("output");

                    //String src_numberOfEventsSeenPerMonitorSnapshotInterval = src_output.has("numberOfEventsSeenPerMonitorSnapshotInterval") ? src_output.getString("numberOfEventsSeenPerMonitorSnapshotInterval") : null;
                    //String src_ignoredTablesList = src_output.has("ignoredTablesList") ? src_output.getString("ignoredTablesList") : null;
                    //String src_input = src_output.has("input") ? src_output.getString("input") : null;
                    //String src_inputRate = src_output.has("inputRate") ? src_output.getString("inputRate") : null;
                    //String src_latestActivity = src_output.has("latestActivity") ? src_output.getString("latestActivity") : null;
                    //String src_numServers = src_output.has("numServers") ? src_output.getString("numServers") : null;
                    //String src_rate = src_output.has("rate") ? src_output.getString("rate") : null;
                    String src_sourceInput = src_output.has("sourceInput") ? src_output.getString("sourceInput") : null;
                    //String src_sourceRate = src_output.has("sourceRate") ? src_output.getString("sourceRate") : null;
                    String src_tableInformation = src_output.has("tableInformation") ? src_output.getString("tableInformation") : null;
                    //String src_timestamp = src_output.has("timestamp") ? src_output.getString("timestamp") : null;
                    //String src_rowCount = src_output.has("rowCount") ? src_output.getString("rowCount") : null;

                    //logger.debug("Createmonawe.tableInformation=" + src_tableInformation);

                    try
                    {
                        sourceInputTotal += Integer.parseInt(src_sourceInput.replace(",", ""));
                    } catch (Exception e) {
                        String eMsg = "For app (" + ac_fullName + ") Unable to add count for (Source Input) value: " + e;
                        logger.debug(eMsg);
                    }

                    // Describe Source - To add adapterName and databaseprovidertype
                    String src_adapterName = "";
                    String src_databaseprovidertype = "";
                    String src_properties = "";

                    // IBR does not contain table-level info, so this will not help
                    // Boolean isIBR = false;

                    // Describe to get source type
                    try {
                        String strdescribeCommand = "describe " + ac_fullName + ";";
                        String describeCommand = runCommand(strdescribeCommand);

                        JSONArray desc_jsonArray = new JSONArray(describeCommand);

                        for (int m = 0; m < desc_jsonArray.length(); m++) {

                            JSONObject des_jsonObject = desc_jsonArray.getJSONObject(m);

                            if (des_jsonObject.has("output")) {

                                Object des_jsonObjectOutputObject = des_jsonObject.opt("output");

                                if (des_jsonObjectOutputObject instanceof JSONArray) {
                                    JSONArray des_output = des_jsonObject.getJSONArray("output");

                                    for (int n = 0; n < des_output.length(); n++)
                                    {
                                        JSONObject des_jobj = des_output.getJSONObject(n);

                                        src_adapterName = des_jobj.has("adapterName") ? des_jobj.getString("adapterName") : "";
                                        String src_created = des_jobj.has("created") ? des_jobj.getString("created") : "";

                                        DateTime dte_created = convertStringToDateTime(src_created);

                                        if (upsertComponentDescribe(appName, ac_fullName, dte_created))
                                        {
                                            createComponentOutput(appName, ac_fullName, "describe", "SOURCE", describeCommand, includeComponentDetailsAsOutput);
                                        }

                                        JSONObject desc_properties = des_jobj.has("properties") ? des_jobj.getJSONObject("properties") : null;

                                                        /*if (src_adapterName.equalsIgnoreCase("IncrementalBatchReader"))
                                                        {
                                                            isIBR = true;
                                                        }*/

                                        src_properties = desc_properties.toString();

                                        src_databaseprovidertype = desc_properties.has("databaseprovidertype") ? desc_properties.getString("databaseprovidertype") : "";
                                    }
                                }
                            }
                        }

                    } catch (Exception e) {
                        String eMsg = "Unable to add source type information: " + ac_fullName;
                        logger.debug(eMsg);
                    }

                    // Try to parse TableInformation
                    try {

                        // If the table information is not empty or null, we can parse
                        if (!(isStringEmpty(src_tableInformation)))
                        {
                            //Need to ensure that our JSON Object is well-formatted (starts and ends with [])
                            if (!src_tableInformation.startsWith("[")) {
                                src_tableInformation = "[" + src_tableInformation;
                            }
                            if (!src_tableInformation.endsWith("]")) {
                                src_tableInformation = src_tableInformation + "]";
                            }

                            JSONArray jsonTblArray = new JSONArray(src_tableInformation);

                            for (int l = 0; l < jsonTblArray.length(); l++) {
                                JSONObject jsonTblObject = jsonTblArray.getJSONObject(l);

                                Iterator<String> keys = jsonTblObject.keys();

                                while(keys.hasNext()) {
                                    String sourceName = keys.next();

                                    JSONObject targetObject = null;

                                    //It can either be a String returned value, or an Object.
                                    try {
                                        targetObject = jsonTblObject.getJSONObject(sourceName);

                                    } catch (JSONException e) {
                                        //logger.info("Input is not JSONObject:" + e);
                                        //System.out.println("Input is not String:" + e);
                                        try {
                                            targetObject = new JSONObject(jsonTblObject.getString(sourceName));
                                        } catch (Exception ex) {
                                            logger.debug("Input is not JSONObject or String (Source: " + ac_fullName + "); Unable to parse detailed table information." + e);
                                            //throw new RuntimeException(ex);
                                        }
                                    }

                                    int numOfDeletes = 0;
                                    int numOfDdls = 0;
                                    int numOfPkupdates = 0;
                                    int numOfUpdates = 0;
                                    int numOfInserts = 0;

                                    // Optional values
                                    int totalRows = 0;
                                    String schemaGenerationStatus = "";
                                    String dataReadStatus = "";

                                    if (targetObject.has("No of Inserts")) {
                                        numOfInserts = targetObject.getInt("No of Inserts");
                                        numOfDeletes = targetObject.has("No of Deletes") ? targetObject.getInt("No of Deletes") : 0;
                                        numOfDdls = targetObject.has("No of DDLs") ? targetObject.getInt("No of DDLs") : 0;
                                        numOfPkupdates = targetObject.has("No of PKUpdates") ? targetObject.getInt("No of PKUpdates") : 0;
                                        numOfUpdates = targetObject.has("No of Updates") ? targetObject.getInt("No of Updates") : 0;
                                        schemaGenerationStatus = "";
                                        dataReadStatus = "CDC";
                                    } else {
                                        numOfInserts = targetObject.has("RowsRead") ? targetObject.getInt("RowsRead") : 0;
                                        totalRows = targetObject.has("TotalRows") ? targetObject.getInt("TotalRows") : 0;
                                        schemaGenerationStatus = targetObject.has("schemaGenerationStatus") ? targetObject.getString("schemaGenerationStatus") : null;
                                        dataReadStatus = targetObject.has("dataReadStatus") ? targetObject.getString("dataReadStatus") : null;

                                    }

                                    Boolean exists = false;

                                    //Same App, Same Table gets updated; source component name is not saved.
                                    for (SourceEntry st : srcmap) {
                                        if (st.appName.equals(appName) &&
                                                !st.componentName.equals(ac_fullName) &&
                                                st.sourceName.equals(sourceName))
                                        {
                                            // Update the existing entry
                                            st.numOfDeletes += numOfDeletes;
                                            st.numOfDdls += numOfDdls;
                                            st.numOfInserts += numOfInserts;
                                            st.numOfPkupdates += numOfPkupdates;
                                            st.numOfUpdates += numOfUpdates;
                                            exists = true;
                                            break;
                                        }
                                    }

                                    mapTableNameToType(ac_fullName, appName, sourceName);

                                    // If we didn't upgrade an entry, we can insert a new one
                                    if (!exists)
                                    {
                                        srcmap.add(new SourceEntry(appName, ac_fullName, sourceName, src_adapterName, src_databaseprovidertype, src_properties, numOfDeletes, numOfDdls, numOfPkupdates, numOfUpdates, numOfInserts, schemaGenerationStatus, dataReadStatus));
                                    }
                                }

                            }

                        }
                    } catch (JSONException e) {
                        //System.out.println("Exception in adding source:" + e);
                        String eMsg = "Unable to parse table information: " + e;
                        logger.debug(eMsg);
                    }

                }
            }
        }
        catch (Exception e) {
            String eMsg = "Unable to add source: (" + ac_fullName + ") " + e;
            logger.debug(eMsg);
        }

        return sourceInputTotal;
    }

    private static Integer addTarget(String ac_fullName, Integer targetOutputTotal, String appName) {
        try {
            insertIntoSTmap(ac_fullName, appName);
        } catch (Exception ex) {
            logger.debug("Unable to map into STMap: Target: " + ac_fullName + " | App: " + appName + "- ex:" + ex);
        }
        try {
            // Need to get source information
            String command = "mon " + ac_fullName + ";";
            String sourceInfo = runCommand(command);

            createComponentOutput(appName, ac_fullName, "mon", "TARGET", sourceInfo, includeComponentDetailsAsOutput);

            JSONArray src_jsonArray = new JSONArray(sourceInfo);

            for (int k = 0; k < src_jsonArray.length(); k++) {
                JSONObject src_jsonObject = src_jsonArray.getJSONObject(k);
                if (src_jsonObject.has("output")) {
                    JSONObject src_output = src_jsonObject.getJSONObject("output");

                    //String tgt_accepted = src_output.has("accepted") ? src_output.getString("accepted") : null;
                    //String tgt_noOfEventsAcceptedPerInterval = src_output.has("no.ofEventsAcceptedPerInterval") ? src_output.getString("no.ofEventsAcceptedPerInterval") : null;
                    //String tgt_acceptedRate = src_output.has("acceptedRate") ? src_output.getString("acceptedRate") : null;
                    //String tgt_commitLag = src_output.has("commitLag") ? src_output.getString("commitLag") : null;
                    //String tgt_lastCommitLatency = src_output.has("lastCommitLatency") ? src_output.getString("lastCommitLatency") : null;
                    //String tgt_cpu = src_output.has("cpu") ? src_output.getString("cpu") : null;
                    //String tgt_cpuRatePerNode = src_output.has("cpuRatePerNode") ? src_output.getString("cpuRatePerNode") : null;
                    //String tgt_cpuRate = src_output.has("cpuRate") ? src_output.getString("cpuRate") : null;
                    //String tgt_ddlInformation = src_output.has("ddlInformation") ? src_output.getString("ddlInformation") : null;
                    //String tgt_discardedEventCount = src_output.has("discardedEventCount") ? src_output.getString("discardedEventCount") : null;
                    //String tgt_numberOfEventsSeenPerMonitorSnapshotInterval = src_output.has("numberOfEventsSeenPerMonitorSnapshotInterval") ? src_output.getString("numberOfEventsSeenPerMonitorSnapshotInterval") : null;
                    //String tgt_externalIoLatency = src_output.has("externalI\\/oLatency") ? src_output.getString("externalI\\/oLatency") : null;
                    //String tgt_input = src_output.has("input") ? src_output.getString("input") : null;
                    //String tgt_inputRate = src_output.has("inputRate") ? src_output.getString("inputRate") : null;
                    //String tgt_lastCommitTime = src_output.has("lastCommitTime") ? src_output.getString("lastCommitTime") : null;
                    //String tgt_lastIoTime = src_output.has("lastI\\/oTime") ? src_output.getString("lastI\\/oTime") : null;
                    //String tgt_lastEventWriteAge = src_output.has("lastEventWriteAge") ? src_output.getString("lastEventWriteAge") : null;
                    //String tgt_latestActivity = src_output.has("latestActivity") ? src_output.getString("latestActivity") : null;
                    //String tgt_maxLeeFromAllSources = src_output.has("maxLeeFromAllSources") ? src_output.getString("maxLeeFromAllSources") : null;
                    //String tgt_noOpOperations = src_output.has("no-opOperations") ? src_output.getString("no-opOperations") : null;
                    //String tgt_numServers = src_output.has("numServers") ? src_output.getString("numServers") : null;
                    //String tgt_exceptionsIgnored = src_output.has("exceptionsIgnored") ? src_output.getString("exceptionsIgnored") : null;
                    //String tgt_individualOperationCount = src_output.has("individualOperationCount") ? src_output.getString("individualOperationCount") : null;
                    //String tgt_output = src_output.has("output") ? src_output.getString("output") : null;
                    //String tgt_processed = src_output.has("processed") ? src_output.getString("processed") : null;
                    //String tgt_rate = src_output.has("rate") ? src_output.getString("rate") : null;
                    String tgt_tableInformation = src_output.has("tableInformation") ? src_output.getString("tableInformation") : null;
                    //String tgt_targetAcked = src_output.has("targetAcked") ? src_output.getString("targetAcked") : null;
                    //String tgt_targetCommitPosition = src_output.has("targetCommitPosition") ? src_output.getString("targetCommitPosition") : null;
                    String tgt_targetOutput = src_output.has("targetOutput") ? src_output.getString("targetOutput") : null;
                    //String tgt_targetRate = src_output.has("targetRate") ? src_output.getString("targetRate") : null;
                    //String tgt_timestamp = src_output.has("timestamp") ? src_output.getString("timestamp") : null;
                    //String tgt_totalEventsInLastCommit = src_output.has("totalEventsInLastCommit") ? src_output.getString("totalEventsInLastCommit") : null;
                    //String tgt_totalEventsInLastIo = src_output.has("totalEventsInLastI\\/o") ? src_output.getString("totalEventsInLastI\\/o") : null;
                    //String tgt_totalNumberOfReconnects = src_output.has("totalNumberOfReconnects") ? src_output.getString("totalNumberOfReconnects") : null;
                    //String tgt_writeBytes = src_output.has("writeBytes") ? src_output.getString("writeBytes") : null;
                    String tgt_tableWriteInformation = src_output.has("tableWriteInformation") ? src_output.getString("tableWriteInformation") : null;

                    try
                    {
                        targetOutputTotal += Integer.parseInt(tgt_targetOutput.replace(",", ""));

                    } catch (Exception e) {
                        String eMsg = "For app (" + ac_fullName + ") Unable to add count for (Target Output) value " + e;
                        logger.debug(eMsg);
                    }

                    // Describe Target - To add adapterName, databaseprovidertype

                    String tgt_adapterName = "";
                    String tgt_databaseprovidertype = "";
                    String tgt_properties = "";

                    // Describe to get source type
                    try {
                        String strdescribeCommand = "describe " + ac_fullName + ";";
                        String describeCommand = runCommand(strdescribeCommand);


                        JSONArray desc_jsonArray = new JSONArray(describeCommand);

                        for (int m = 0; m < desc_jsonArray.length(); m++) {

                            JSONObject des_jsonObject = desc_jsonArray.getJSONObject(m);

                            if (des_jsonObject.has("output")) {
                                JSONArray des_output = des_jsonObject.getJSONArray("output");

                                for (int n = 0; n < des_output.length(); n++)
                                {
                                    JSONObject des_jobj = des_output.getJSONObject(n);

                                    tgt_adapterName = des_jobj.has("adapterName") ? des_jobj.getString("adapterName") : "";
                                    JSONObject desc_properties = des_jobj.has("properties") ? des_jobj.getJSONObject("properties") : null;

                                    String src_created = des_jobj.has("created") ? des_jobj.getString("created") : "";

                                    DateTime dte_created = convertStringToDateTime(src_created);

                                    if (upsertComponentDescribe(appName, ac_fullName, dte_created))
                                    {
                                        createComponentOutput(appName, ac_fullName, "describe", "TARGET", describeCommand, includeComponentDetailsAsOutput);
                                    }

                                    tgt_properties = desc_properties.toString();

                                    tgt_databaseprovidertype = desc_properties.has("databaseprovidertype") ? desc_properties.getString("databaseprovidertype") : "";
                                }

                            }
                        }

                    } catch (Exception e) {
                        String eMsg = "Unable to add source type information: " + ac_fullName;
                        logger.debug(eMsg);
                    }

                    logger.debug("src_tableInformation: " + tgt_tableInformation);

                    try {
                        // If the table information is not empty or null, we can parse
                        if (!(isStringEmpty(tgt_tableInformation)))
                        {
                            //Need to ensure that our JSON Object is well-formatted (starts and ends with [])
                            if (!tgt_tableInformation.startsWith("[")) {
                                tgt_tableInformation = "[" + tgt_tableInformation;
                            }
                            if (!tgt_tableInformation.endsWith("]")) {
                                tgt_tableInformation = tgt_tableInformation + "]";
                            }

                            JSONArray jsonTblArray = new JSONArray(tgt_tableInformation);

                            for (int l = 0; l < jsonTblArray.length(); l++) {
                                JSONObject jsonTblObject = jsonTblArray.getJSONObject(l);

                                Iterator<String> keys = jsonTblObject.keys();

                                while(keys.hasNext()) {
                                    String targetName = keys.next();

                                    JSONObject targetObject = null;

                                    //It can either be a String returned value, or an Object.
                                    try {
                                        targetObject = jsonTblObject.getJSONObject(targetName);
                                    } catch (JSONException e) {
                                        //logger.info("Unable to parse JSONObject from targetObject: " + e);
                                        try {
                                            targetObject = new JSONObject(jsonTblObject.getString(targetName));
                                        } catch (Exception ex) {
                                            logger.debug("Input is not JSONObject or String (Target: " + ac_fullName + "); Unable to parse detailed table information." + e);
                                            //throw new RuntimeException(ex);
                                        }
                                    }

                                    if (targetObject.has("Sources"))
                                    {
                                        String sourceName = targetObject.getJSONArray("Sources").getString(0);

                                        int numOfDeletes = 0;
                                        int numOfDdls = 0;
                                        int numOfPkupdates = 0;
                                        int numOfUpdates = 0;
                                        int numOfInserts = 0;

                                        if (targetObject.has("No of Inserts"))
                                        {
                                            numOfDeletes = targetObject.has("No of Deletes") ? targetObject.getInt("No of Deletes") : 0;
                                            numOfDdls = targetObject.has("No of DDLs") ? targetObject.getInt("No of DDLs") : 0;
                                            numOfPkupdates = targetObject.has("No of PKUpdates") ? targetObject.getInt("No of PKUpdates") : 0;
                                            numOfUpdates = targetObject.has("No of Updates") ? targetObject.getInt("No of Updates") : 0;
                                            numOfInserts = targetObject.has("No of Inserts") ? targetObject.getInt("No of Inserts") : 0;
                                        }

                                        String lastBatchExecutionTime = targetObject.has("Last Batch Execution Time") ? targetObject.getString("Last Batch Execution Time") : null;
                                        String lastCommitExecutionTime = targetObject.has("Last Commit Execution Time") ? targetObject.getString("Last Commit Execution Time") : null;

                                        Boolean exists = false;

                                        //Same App, Same Table gets updated; target component name is not saved.
                                        for (TargetEntry st : tgtmap) {
                                            if (st.appName.equals(appName) &&
                                                    !st.componentName.equals(ac_fullName) &&
                                                    st.sourceName.equals(sourceName))
                                            {
                                                // Update the existing entry
                                                st.numOfDeletes += numOfDeletes;
                                                st.numOfDdls += numOfDdls;
                                                st.numOfInserts += numOfInserts;
                                                st.numOfPkupdates += numOfPkupdates;
                                                st.numOfUpdates += numOfUpdates;
                                                exists = true;
                                                break;
                                            }
                                        }

                                        // If we didn't upgrade an entry, we can insert a new one
                                        if (!exists)
                                        {
                                            tgtmap.add(new TargetEntry(appName, ac_fullName, sourceName, targetName, tgt_adapterName, tgt_databaseprovidertype, tgt_properties, numOfDeletes, numOfDdls, numOfPkupdates, numOfUpdates, numOfInserts, lastBatchExecutionTime, lastCommitExecutionTime));
                                        }
                                    }
                                }
                            }
                        }


                        if (!(isStringEmpty(tgt_tableWriteInformation)))
                        {
                            //Need to ensure that our JSON Object is well-formatted (starts and ends with [])
                            if (!tgt_tableWriteInformation.startsWith("[")) {
                                tgt_tableWriteInformation = "[" + tgt_tableWriteInformation;
                            }
                            if (!tgt_tableWriteInformation.endsWith("]")) {
                                tgt_tableWriteInformation = tgt_tableWriteInformation + "]";
                            }

                            JSONArray jsonTblArray = new JSONArray(tgt_tableWriteInformation);

                            for (int l = 0; l < jsonTblArray.length(); l++) {
                                JSONObject jsonTblObject = jsonTblArray.getJSONObject(l);

                                Iterator<String> keys = jsonTblObject.keys();

                                while(keys.hasNext()) {
                                    String targetName = keys.next();

                                    JSONObject initialObject = null;

                                    //It can either be a String returned value, or an Object.
                                    try {
                                        initialObject = jsonTblObject.getJSONObject(targetName);
                                    } catch (JSONException e) {
                                        //logger.info("Unable to parse JSONObject from targetObject: " + e);
                                        try {
                                            initialObject = new JSONObject(jsonTblObject.getString(targetName));
                                        } catch (Exception ex) {
                                            logger.debug("Input is not JSONObject or String (Target: " + ac_fullName + "); Unable to parse detailed table information." + e);
                                            //throw new RuntimeException(ex);
                                        }
                                    }

                                    Boolean hasSourceTable = false;

                                    String sourceName = "";

                                    if (initialObject.has("Mapped Source Table")) {
                                        sourceName = initialObject.getString("Mapped Source Table");
                                        hasSourceTable = true;
                                    } else
                                    {
                                        sourceName = "";
                                        hasSourceTable = true;
                                    }

                                    if (hasSourceTable) {
                                        try {
                                            if (initialObject.has("Total event info")) {

                                                Object IL = initialObject.getJSONObject("Total event info");

                                                if (IL != null) {

                                                    if (IL instanceof JSONObject) {
                                                        JSONObject targetObject = initialObject.getJSONObject("Total event info");

                                                        int numOfDeletes = 0;
                                                        int numOfDdls = 0;
                                                        int numOfPkupdates = 0;
                                                        int numOfUpdates = 0;
                                                        int numOfInserts = 0;

                                                        if (targetObject.has("No of inserts"))
                                                        {
                                                            numOfDeletes = targetObject.has("No of deletes") ? targetObject.getInt("No of deletes") : 0;
                                                            numOfDdls = targetObject.has("No of DDLs") ? targetObject.getInt("No of DDLs") : 0;
                                                            numOfPkupdates = targetObject.has("No of pkupdates") ? targetObject.getInt("No of pkupdates") : 0;
                                                            numOfUpdates = targetObject.has("No of updates") ? targetObject.getInt("No of updates") : 0;
                                                            numOfInserts = targetObject.has("No of inserts") ? targetObject.getInt("No of inserts") : 0;
                                                        }

                                                        String lastBatchExecutionTime = initialObject.has("Last successful merge time") ? initialObject.getString("Last successful merge time") : null;
                                                        String lastCommitExecutionTime = initialObject.has("Last successful merge time") ? initialObject.getString("Last successful merge time") : null;

                                                        Boolean exists = false;

                                                        //Same App, Same Table gets updated; target component name is not saved.
                                                        for (TargetEntry st : tgtmap) {
                                                            if (st.appName.equals(appName) &&
                                                                    !st.componentName.equals(ac_fullName) &&
                                                                    st.sourceName.equals(sourceName))
                                                            {
                                                                // Update the existing entry
                                                                st.numOfDeletes += numOfDeletes;
                                                                st.numOfDdls += numOfDdls;
                                                                st.numOfInserts += numOfInserts;
                                                                st.numOfPkupdates += numOfPkupdates;
                                                                st.numOfUpdates += numOfUpdates;
                                                                exists = true;
                                                                break;
                                                            }
                                                        }

                                                        // If we didn't upgrade an entry, we can insert a new one
                                                        if (!exists)
                                                        {
                                                            tgtmap.add(new TargetEntry(appName, ac_fullName, sourceName, targetName, tgt_adapterName, tgt_databaseprovidertype, tgt_properties, numOfDeletes, numOfDdls, numOfPkupdates, numOfUpdates, numOfInserts, lastBatchExecutionTime, lastCommitExecutionTime));
                                                        }
                                                    }
                                                }

                                                if (includeDataWarehouseDetails) {
                                                    // Parse other metrics, checking for existence and converting bytes to MB
                                                    int totalBatchesCreated = initialObject.has("Total Batches Created") ? initialObject.getInt("Total Batches Created") : 0;
                                                    int partitionPrunedBatches = initialObject.has("Partition Pruned Batches") ? initialObject.getInt("Partition Pruned Batches") : 0;
                                                    String lastSuccessfulMergeTime = initialObject.has("Last successful merge time") ? initialObject.getString("Last successful merge time") : null;
                                                    int totalBatchesIgnored = initialObject.has("Total Batches Ignored") ? initialObject.getInt("Total Batches Ignored") : 0;
                                                    int maxIntegrationTimeMs = initialObject.has("Max Integration Time in ms") ? initialObject.getInt("Max Integration Time in ms") : 0;
                                                    double avgInMemCompactionTimeMs = initialObject.has("Avg In-Mem Compaction Time in ms") ? initialObject.getInt("Avg In-Mem Compaction Time in ms") : 0;
                                                    double avgBatchSizeMb = initialObject.has("Avg Batch Size in bytes") ? initialObject.getInt("Avg Batch Size in bytes") : 0;
                                                    double avgEventCountPerBatch = initialObject.has("Avg Event Count Per Batch") ? initialObject.getDouble("Avg Event Count Per Batch") : 0;
                                                    int minIntegrationTimeMs = initialObject.has("Min Integration Time in ms") ? initialObject.getInt("Min Integration Time in ms") : 0;
                                                    String mappedSourceTable = initialObject.has("Mapped Source Table") ? initialObject.getString("Mapped Source Table") : null;
                                                    int totalBatchesQueued = initialObject.has("Total Batches Queued") ? initialObject.getInt("Total Batches Queued") : 0;
                                                    double avgCompactionTimeMs = initialObject.has("Avg Compaction Time in ms") ? initialObject.getInt("Avg Compaction Time in ms") : 0;
                                                    double avgWaitingTimeInQueueMs = initialObject.has("Avg Waiting Time in Queue in ms") ? initialObject.getInt("Avg Waiting Time in Queue in ms") : 0;
                                                    double avgIntegrationTimeMs = initialObject.has("Avg Integration Time in ms") ? initialObject.getInt("Avg Integration Time in ms") : 0;
                                                    int totalBatchesUploaded = initialObject.has("Total Batches Uploaded") ? initialObject.getInt("Total Batches Uploaded") : 0;
                                                    double avgMergeTimeMs = initialObject.has("Avg Merge Time in ms") ? initialObject.getInt("Avg Merge Time in ms") : 0;

                                                    int lastBatchUpdates = 0;
                                                    int lastBatchEventCount = 0;
                                                    int lastBatchInserts = 0;
                                                    double lastBatchMaxRecordSizeMb = 0;
                                                    int lastBatchTotalEventsMerged = 0;
                                                    int lastBatchDdls = 0;
                                                    int lastBatchSequenceNumber = 0;
                                                    double lastBatchSizeMb = 0;
                                                    int lastBatchDeletes = 0;
                                                    int lastBatchPkUpdates = 0;
                                                    int lastBatchAccumulationTimeMs = 0;
                                                    int lastBatchCompactionTimeMs = 0;
                                                    int lastBatchStageResourcesMgmtTimeMs = 0;
                                                    int lastBatchUploadTimeMs = 0;
                                                    int lastBatchMergeTimeMs = 0;
                                                    int lastBatchInMemCompactionTimeMs = 0;
                                                    int lastBatchPkUpdateTimeMs = 0;
                                                    int lastBatchDdlExecutionTimeMs = 0;
                                                    int lastBatchTotalIntegrationTimeMs = 0;

                                                    // Parse nested "Last batch info" object
                                                    if (initialObject.has("Last batch info")) {
                                                        JSONObject lastBatchInfo = initialObject.getJSONObject("Last batch info");

                                                        lastBatchUpdates = lastBatchInfo.has("No of updates") ? lastBatchInfo.getInt("No of updates") : 0;
                                                        lastBatchEventCount = lastBatchInfo.has("Batch Event Count") ? lastBatchInfo.getInt("Batch Event Count") : 0;
                                                        lastBatchInserts = lastBatchInfo.has("No of inserts") ? lastBatchInfo.getInt("No of inserts") : 0;
                                                        lastBatchMaxRecordSizeMb = lastBatchInfo.has("Max Record Size in batch") ? lastBatchInfo.getInt("Max Record Size in batch") : 0;
                                                        lastBatchTotalEventsMerged = lastBatchInfo.has("Total events merged") ? lastBatchInfo.getInt("Total events merged") : 0;
                                                        lastBatchDdls = lastBatchInfo.has("No of DDLs") ? lastBatchInfo.getInt("No of DDLs") : 0;
                                                        lastBatchSequenceNumber = lastBatchInfo.has("Batch Sequence Number") ? lastBatchInfo.getInt("Batch Sequence Number") : 0;
                                                        lastBatchSizeMb = lastBatchInfo.has("Batch Size in bytes") ? lastBatchInfo.getInt("Batch Size in bytes") : 0;
                                                        lastBatchDeletes = lastBatchInfo.has("No of deletes") ? lastBatchInfo.getInt("No of deletes") : 0;
                                                        lastBatchPkUpdates = lastBatchInfo.has("No of pkupdates") ? lastBatchInfo.getInt("No of pkupdates") : 0;
                                                        lastBatchAccumulationTimeMs = lastBatchInfo.has("Batch Accumulation Time in ms") ? lastBatchInfo.getInt("Batch Accumulation Time in ms") : 0;

                                                        // Parse nested "Integration Task Time" object within "Last batch info"
                                                        if (lastBatchInfo.has("Integration Task Time")) {
                                                            JSONObject integrationTaskTime = lastBatchInfo.getJSONObject("Integration Task Time");

                                                            lastBatchCompactionTimeMs = integrationTaskTime.has("Compaction Time in ms") ? integrationTaskTime.getInt("Compaction Time in ms") : 0;
                                                            lastBatchStageResourcesMgmtTimeMs = integrationTaskTime.has("Stage Resources Management Time in ms") ? integrationTaskTime.getInt("Stage Resources Management Time in ms") : 0;
                                                            lastBatchUploadTimeMs = integrationTaskTime.has("Upload Time in ms") ? integrationTaskTime.getInt("Upload Time in ms") : 0;
                                                            lastBatchMergeTimeMs = integrationTaskTime.has("Merge Time in ms") ? integrationTaskTime.getInt("Merge Time in ms") : 0;
                                                            lastBatchInMemCompactionTimeMs = integrationTaskTime.has("In-Memory Compaction Time in ms") ? integrationTaskTime.getInt("In-Memory Compaction Time in ms") : 0;
                                                            lastBatchPkUpdateTimeMs = integrationTaskTime.has("pk Update Time in ms") ? integrationTaskTime.getInt("pk Update Time in ms") : 0;
                                                            lastBatchDdlExecutionTimeMs = integrationTaskTime.has("DDL Execution Time in ms") ? integrationTaskTime.getInt("DDL Execution Time in ms") : 0;
                                                            lastBatchTotalIntegrationTimeMs = integrationTaskTime.has("Total Integration Time in ms") ? integrationTaskTime.getInt("Total Integration Time in ms") : 0;

                                                        }

                                                        // Print or use the parsed values from "Last batch info" as needed
                                                        //System.out.println("Last Batch Updates: " + lastBatchUpdates);
                                                        // ... (print or use other parsed values from "Last batch info")
                                                    }

                                                    double avgStageResourcesMgmtTimeMs = initialObject.has("Avg Stage Resources Management Time in ms") ? initialObject.getInt("Avg Stage Resources Management Time in ms") : 0;
                                                    double avgUploadTimeMs = initialObject.has("Avg Upload Time in ms") ? initialObject.getInt("Avg Upload Time in ms") : 0;

                                                    // Add values to constructor
                                                    tgtdwmap.add(new TargetDWDetail(
                                                            appName, ac_fullName, sourceName, targetName.replace("\\", ""), tgt_adapterName, tgt_databaseprovidertype,
                                                            totalBatchesCreated,
                                                            partitionPrunedBatches,
                                                            lastSuccessfulMergeTime,
                                                            totalBatchesIgnored,
                                                            maxIntegrationTimeMs,
                                                            avgInMemCompactionTimeMs,
                                                            avgBatchSizeMb,
                                                            avgEventCountPerBatch,
                                                            minIntegrationTimeMs,
                                                            mappedSourceTable,
                                                            totalBatchesQueued,
                                                            avgCompactionTimeMs,
                                                            avgWaitingTimeInQueueMs,
                                                            avgIntegrationTimeMs,
                                                            totalBatchesUploaded,
                                                            avgMergeTimeMs,
                                                            avgStageResourcesMgmtTimeMs,
                                                            avgUploadTimeMs,
                                                            lastBatchUpdates,
                                                            lastBatchEventCount,
                                                            lastBatchInserts,
                                                            lastBatchMaxRecordSizeMb,
                                                            lastBatchTotalEventsMerged,
                                                            lastBatchDdls,
                                                            lastBatchSequenceNumber,
                                                            lastBatchSizeMb,
                                                            lastBatchDeletes,
                                                            lastBatchPkUpdates,
                                                            lastBatchAccumulationTimeMs,
                                                            lastBatchCompactionTimeMs,
                                                            lastBatchStageResourcesMgmtTimeMs,
                                                            lastBatchUploadTimeMs,
                                                            lastBatchMergeTimeMs,
                                                            lastBatchInMemCompactionTimeMs,
                                                            lastBatchPkUpdateTimeMs,
                                                            lastBatchDdlExecutionTimeMs,
                                                            lastBatchTotalIntegrationTimeMs
                                                    ));
                                                    // Print or use the parsed values as needed
                                                    //System.out.println("Total Batches Created: " + totalBatchesCreated);
                                                    // ... (print or use other parsed values)
                                                }

                                            }

                                        } catch (Exception ex) {
                                            logger.debug(ac_fullName + " (error) : " + ex);
                                            //throw new RuntimeException(ex);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (JSONException e) {
                        logger.debug("Exception in adding target" + e);
                    }
                }
            }
        }
        catch (Exception e) {
            String eMsg = "Unable to add target (" + ac_fullName + "): " + e;
            logger.debug(eMsg);
        }

        return targetOutputTotal;
    }

    private static Boolean addStream(String ac_fullName, Boolean isBackpressured, String appName) {
        try {
            String command = "mon " + ac_fullName + ";";
            String streamInfo = runCommand(command);

            createComponentOutput(appName, ac_fullName, "mon", "STREAM", streamInfo, includeComponentDetailsAsOutput);

            JSONArray src_jsonArray = new JSONArray(streamInfo);

            for (int k = 0; k < src_jsonArray.length(); k++) {
                JSONObject src_jsonObject = src_jsonArray.getJSONObject(k);
                if (src_jsonObject.has("output")) {
                    JSONObject src_output = src_jsonObject.getJSONObject("output");

                    //String src_cpu = src_output.has("cpu") ? src_output.getString("cpu") : null;
                    //String src_cpuRatePerNode = src_output.has("cpuRatePerNode") ? src_output.getString("cpuRatePerNode") : null;
                    //String src_cpuRate = src_output.has("cpuRate") ? src_output.getString("cpuRate") : null;
                    //String src_numberOfEventsSeenPerMonitorSnapshotInterval = src_output.has("numberOfEventsSeenPerMonitorSnapshotInterval") ? src_output.getString("numberOfEventsSeenPerMonitorSnapshotInterval") : null;
                    //String src_input = src_output.has("input") ? src_output.getString("input") : null;
                    //String src_inputRate = src_output.has("inputRate") ? src_output.getString("inputRate") : null;
                    //String src_latestActivity = src_output.has("latestActivity") ? src_output.getString("latestActivity") : null;
                    //String src_numServers = src_output.has("numServers") ? src_output.getString("numServers") : null;
                    //String src_rate = src_output.has("rate") ? src_output.getString("rate") : null;
                    String src_streamFull = src_output.has("streamFull") ? src_output.getString("streamFull") : null;
                    //String src_timestamp = src_output.has("timestamp") ? src_output.getString("timestamp") : null;

                    if (src_streamFull.equals("True"))
                    {
                        isBackpressured = true;
                    }

                }
            }
        }
        catch (Exception e) {
            String eMsg = "Unable to add stream (" + ac_fullName + "): " + e;
            logger.debug(eMsg);
        }
        return isBackpressured;
    }


    private static Boolean upsertComponentDescribe(String appName, String componentName, DateTime createdDate) {
        ComponentDescribe existingComponent = null;

        for (ComponentDescribe c : cmpdesc) {
            if (c.appName.equals(appName) && c.componentName.equals(componentName)) {
                existingComponent = c;
                break;
            }
        }

        if (existingComponent != null) {
            // If the existing component's createdDate is less than the new createdDate, update it
            if (existingComponent.createdDate.isBefore(createdDate)) {
                existingComponent.createdDate = createdDate;
                return true;
            }
        } else {
            // If no existing component is found, add a new one
            cmpdesc.add(new ComponentDescribe(appName, componentName, createdDate));
            return true;
        }
        return false;
    }

    private static void upsertTypeOutputList(String typeName, String appName, String tableName, DateTime createdDate, String columnName, String columnType, Boolean isPk) {
        StriimTypeList existingComponent = null;

        for (StriimTypeList c : typeListOutput) {
            if (c.typeName.equals(typeName) && c.columnName.equals(columnName)) {
                existingComponent = c;
                break;
            }
        }

        if (existingComponent != null) {
            // If the existing component's createdDate is less than the new createdDate, update it
            if (existingComponent.createdDate.isBefore(createdDate)) {
                existingComponent.createdDate = createdDate;
            }

            if (existingComponent.appName.isEmpty() && appName != "") {
                existingComponent.appName = appName;
            }

            if (existingComponent.tableName.isEmpty() && tableName != "") {
                existingComponent.tableName = tableName;
            }
        } else {
            // If no existing component is found, add a new one
            typeListOutput.add(new StriimTypeList(typeName, appName, tableName, createdDate, columnName, columnType, isPk));
        }
    }

    // Returns true if a change was detected, false if not
    private static Boolean upsertTypeList(String typeName, String appName, String tableName, DateTime createdDate, String columnName, String columnType, Boolean isPk) {
        StriimTypeList existingComponent = null;

        for (StriimTypeList c : striimTypeList) {
            if (c.typeName.equals(typeName) && c.columnName.equals(columnName)) {
                existingComponent = c;
                break;
            }
        }

        if (existingComponent != null) {
            // If the existing component's createdDate is less than the new createdDate, update it
            if (existingComponent.createdDate.isBefore(createdDate)) {
                existingComponent.createdDate = createdDate;

                Boolean didChange = false;

                if (existingComponent.appName.isEmpty() && appName != "") {
                    existingComponent.appName = appName;
                    didChange = true;
                }
                if (existingComponent.tableName.isEmpty() && tableName != "") {
                    existingComponent.tableName = tableName;
                }

                return didChange;
            } else if (existingComponent.appName.isEmpty() && appName != "") {
                existingComponent.appName = appName;
                if (existingComponent.tableName.isEmpty() && tableName != "") {
                    existingComponent.tableName = tableName;
                }
                return true;
            }
        } else {
            // If no existing component is found, add a new one
            striimTypeList.add(new StriimTypeList(typeName, appName, tableName, createdDate, columnName, columnType, isPk));
            return true;
        }
        return false;
    }

    private static void parseAdditionalCommandList(String commandList) {
        if (commandList == null || commandList.trim().isEmpty()) {
            logger.debug("Input is empty or null.");
            return;
        }

        //We need to parse by semicolon
        if (commandList.length() == 0) {
            return;
        }

        if (!commandList.contains(";")) {
            commandList += ";";
        }

        // Split the input by semicolon and trim each part
        String[] parts = commandList.split(";");

        for (String part : parts) {
            part = part.trim();

            String startsWith = "";

            if (part.startsWith("mon")) {
                startsWith = "mon";
            }
            if (part.startsWith("describe")) {
                startsWith = "describe";
            }
            if (part.startsWith("status")) {
                startsWith = "status";
            }
            if (part.startsWith("report")) {
                startsWith = "report";
            }
            if (part.startsWith("list")) {
                startsWith = "list";
            }
            if (part.startsWith("meter")) {
                startsWith = "meter";
            }
            if (part.startsWith("show")) {
                startsWith = "show";
            }
            if (part.startsWith("usage")) {
                startsWith = "usage";
            }

            if (!(startsWith.isEmpty())) {
                int bracketIndex = part.indexOf('{');
                String command;
                Integer value = null;

                if (bracketIndex != -1) {
                    int endBracketIndex = part.indexOf('}', bracketIndex);
                    if (endBracketIndex != -1) {
                        command = part.substring(0, bracketIndex).trim();
                        try {
                            value = Integer.parseInt(part.substring(bracketIndex + 1, endBracketIndex).trim());
                        } catch (NumberFormatException e) {
                            value = null; // In case the number inside {} is not valid
                        }
                    } else {
                        command = part.trim();
                    }
                } else {
                    command = part.trim();
                }

                //System.out.println(command + ", " + (value != null ? value : "null"));
                Integer runVal = value != null ? value : repeatInSeconds;

                // Default to run this command for the first time on startOn
                cmndlist.add(new CommandList(command, startsWith, runVal, startOn));

                //Have commands to run
                hasAdditionalCommandsToRun = true;
            }
        }
    }

    // _    _ ______ _      _____  ______ _____
    //| |  | |  ____| |    |  __ \|  ____|  __ \
    //| |__| | |__  | |    | |__) | |__  | |__) |
    //|  __  |  __| | |    |  ___/|  __| |  _  /
    //| |  | | |____| |____| |    | |____| | \ \
    //|_|  |_|______|______|_|    |______|_|  \_\

    private static synchronized long generateUniqueInteger() {
        // Get the current time in milliseconds using Instant
        long currentTimeMillis = Instant.now().toEpochMilli();

        // Append the counter value to make it unique for each call within the same millisecond
        long uniqueInteger = currentTimeMillis * 1000 + counter;

        // Increment the counter for the next call
        counter++;

        return uniqueInteger;
    }

    private static Boolean isStringEmpty(String val)
    {
        if (val == null) {
            return true;
        }

        if (val.trim().isEmpty() || val.trim() == "{ }") {
            return true;
        }

        return false;
    }

    private static DateTime parseAndConvertDateTimeToNull(String dateTimeString) {
        try {
            if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
                return null;
            }

            // Try parsing with the first format
            try {
                DateTimeFormatter formatter1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
                return formatter1.parseDateTime(dateTimeString.trim());
            } catch (Exception e1) {
                // If the first format fails, try the second format
                try {
                    DateTimeFormatter formatter2 = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss zzz yyyy").withLocale(Locale.ENGLISH);
                    return formatter2.parseDateTime(dateTimeString.trim());
                } catch (Exception e2) {
                    // If both formats fail, return null
                    return null;
                }
            }
        } catch (Exception e) {
            // Handle any other unexpected exceptions (optional)
            return null;
        }
    }

    private static DateTime convertStringToDateTime(String dateTimeStr) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dt = null;
        try {
            dt = formatter.parseDateTime(dateTimeStr);
        } catch (IllegalArgumentException e) {
            logger.debug("Unable to parse the string into DateTime: " + e.getMessage());
        }
        return dt;
    }

    private static String getNumericOnly(String inputVal) {
        if (inputVal == null || inputVal.trim().isEmpty()) {
            return "0";
        }
        String numericString = inputVal.replaceAll("[^\\d.]", "");
        if (numericString.isEmpty() || numericString.equals(".")) {
            return "0";
        }
        return numericString.startsWith(".") ? "0" + numericString : numericString;
    }

    private static String EncodingUtils_encode(String input) {
        return Base64.getEncoder().encodeToString(input.getBytes());
    }

    private static String EncodingUtils_decode(String input) {
        return new String(Base64.getDecoder().decode(input));
    }

    private static String EncodingUtils_encodeInt(int input) {
        return EncodingUtils_encode(String.valueOf(input));
    }

    private static int EncodingUtils_decodeInt(String input) {
        return Integer.parseInt(EncodingUtils_decode(input));
    }

    private static String EncodingUtils_encodeDateTime(DateTime dateTime) {
        String dateTimeString = dateTime.toString();
        return Base64.getEncoder().encodeToString(dateTimeString.getBytes());
    }

    private static DateTime EncodingUtils_decodeDateTime(String input) {
        String decodedString = new String(Base64.getDecoder().decode(input));
        return new DateTime(decodedString);
    }

    private static String EncodingUtils_encodeLDateTime(LocalDateTime dateTime) {
        String dateTimeString = dateTime.toString();
        return Base64.getEncoder().encodeToString(dateTimeString.getBytes());
    }

    private static LocalDateTime EncodingUtils_decodeLDateTime(String input) {
        String decodedString = new String(Base64.getDecoder().decode(input));
        return new LocalDateTime(decodedString);
    }

    public static double bq_numeric_truncate(double value) {
        String strValue = String.valueOf(value);
        int decimalIndex = strValue.indexOf(".");

        if (decimalIndex != -1) { // Decimal point exists
            String leftPart = strValue.substring(0, decimalIndex);
            String rightPart = strValue.substring(decimalIndex + 1); // Exclude the decimal point

            // Truncate right part to 9 digits
            if (rightPart.length() > 9) {
                rightPart = rightPart.substring(0, 9);
            }

            // Reconstruct the truncated value
            strValue = leftPart + "." + rightPart;
            return Double.parseDouble(strValue);
        }

        return value; // No decimal point, return original value
    }

    private static Map<String, String> parsePolicy(String policyString) {
        Map<String, String> policyMap = new HashMap<>();
        String[] parts = policyString.split(",");
        for (String part : parts) {
            String[] keyValue = part.split(":");
            if (keyValue.length == 2) {
                policyMap.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return policyMap;
    }

    private static void insertIntoSTmap(String stname, String appname) {
        try {
            boolean exists = appstmaplist.stream()
                    .anyMatch(map -> map.stname.equalsIgnoreCase(stname) && map.appname.equalsIgnoreCase(appname));
            if (!exists) {
                appstmaplist.add(new AppSTMap(stname, appname));
            }
        } catch (Exception ex) {
            logger.debug("insertIntoSTmap: Unable to insert into map:" + ex);
        }
    }

    private static String getFromSTmap(String stname) {
        return appstmaplist.stream()
                .filter(map -> map.stname.equalsIgnoreCase(stname))
                .findFirst()
                .map(map -> map.appname)
                .orElse(null);
    }

    private static void clearGlobalArrays() {
        srcmap.clear();
        tgtmap.clear();
        tgtdwmap.clear();
        errorDetailsList.clear();
        smartAlertDetailsList.clear();
        //srctgtmap.clear();
        //srctgtmap_diff.clear();
        leerecord_list.clear();
        leeplusrecord_list.clear();
        cmpntOutput.clear();
        typeListOutput.clear();
        appstmaplist.clear();

        srcmap = new ArrayList<>();
        tgtmap = new ArrayList<>();
        srctgtmap = new ArrayList<>();
        srctgtmap_diff = new ArrayList<>();
        leerecord_list = new ArrayList<>();
        leeplusrecord_list = new ArrayList<>();
        cmpntOutput = new ArrayList<>();
        typeListOutput = new ArrayList<>();
        appstmaplist = new ArrayList<>();
    }

    private String getLocalNamespaceForSW() {
        if (localNamespace.isEmpty())
        {
            String ns = getComponentFullName();
            localNamespace = ns.contains(".") ? ns.split("\\.")[0] : "";
        }
        return localNamespace;
    }

    private static int parseInterval(String intervalStr) {
        intervalStr = intervalStr.trim();
        int multiplier = 1; // Default to seconds

        if (intervalStr.endsWith("s")) {
            intervalStr = intervalStr.substring(0, intervalStr.length() - 1); // Remove 's'
        } else if (intervalStr.endsWith("m")) {
            intervalStr = intervalStr.substring(0, intervalStr.length() - 1);
            multiplier = 60; // Minutes to seconds
        } else if (intervalStr.endsWith("h")) {
            intervalStr = intervalStr.substring(0, intervalStr.length() - 1);
            multiplier = 3600; // Hours to seconds
        }

        return Integer.parseInt(intervalStr) * multiplier;
    }

    //  _    _ _____  _____ _______ ____  _______     __
    //| |  | |_   _|/ ____|__   __/ __ \|  __ \ \   / /
    //| |__| | | | | (___    | | | |  | | |__) \ \_/ /
    //|  __  | | |  \___ \   | | | |  | |  _  / \   /
    //| |  | |_| |_ ____) |  | | | |__| | | \ \  | |
    //|_|  |_|_____|_____/   |_|  \____/|_|  \_\ |_|

    private static WAEvent createRunHistory() throws Exception {
        long batchid = generateUniqueInteger();

        String tableName = watcherTableName.mon_table_runhistory;
        String pkid = watcherTableName.mon_table_runhistory_pk;
        String namespace = watcherTableName.mon_namespace;

        WAEvent event = getBaseWAEvent(namespace, tableName, pkid);

        // Use event.setData(eventIndex++, "abc"); to set the next value
        int eventIndex = 0;

        String clusterName = System.getProperty("striim.cluster.clusterName");
        String companyName = System.getProperty("striim.cluster.company_name");

        DateTime currentTime = DateTime.now();

        Duration duration = new Duration(currentrun, currentTime);
        long milliseconds = duration.getMillis();

        event.setData(eventIndex++, batchid);
        event.setData(eventIndex++, currentrun);
        event.setData(eventIndex++, currentTime);
        event.setData(eventIndex++, milliseconds);
        event.setData(eventIndex++, clusterName);
        event.setData(eventIndex++, companyName);
        event.setData(eventIndex++, lastStart);
        event.setData(eventIndex++, nextStart);

        return event;
    }

    private static void saveHistory() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(SWhistoryFilename))) {
            for (SrcTgtEntry entry : srctgtmap_prior) {
                writer.write(saveType.srctgtmap_prior + "," +
                        EncodingUtils_encode(entry.appName) + "," +
                        EncodingUtils_encode(entry.sourceName) + "," +
                        EncodingUtils_encode(entry.targetName) + "," +
                        EncodingUtils_encodeInt(entry.srcNumOfDeletes) + "," +
                        EncodingUtils_encodeInt(entry.tgtNumOfDeletes) + "," +
                        EncodingUtils_encodeInt(entry.diffNumOfDeletes) + "," +
                        EncodingUtils_encodeInt(entry.srcNumOfDdls) + "," +
                        EncodingUtils_encodeInt(entry.tgtNumOfDdls) + "," +
                        EncodingUtils_encodeInt(entry.diffNumOfDdls) + "," +
                        EncodingUtils_encodeInt(entry.srcNumOfPkupdates) + "," +
                        EncodingUtils_encodeInt(entry.tgtNumOfPkupdates) + "," +
                        EncodingUtils_encodeInt(entry.diffNumOfPkupdates) + "," +
                        EncodingUtils_encodeInt(entry.srcNumOfUpdates) + "," +
                        EncodingUtils_encodeInt(entry.tgtNumOfUpdates) + "," +
                        EncodingUtils_encodeInt(entry.diffNumOfUpdates) + "," +
                        EncodingUtils_encodeInt(entry.srcNumOfInserts) + "," +
                        EncodingUtils_encodeInt(entry.tgtNumOfInserts) + "," +
                        EncodingUtils_encodeInt(entry.diffNumOfInserts));
                writer.newLine();
            }

            writer.write(saveType.lastStart + "," + EncodingUtils_encodeDateTime(lastStart));
            writer.newLine();

        } catch (Exception e) {
            logger.debug("Unable to save StriimWatcher.hist (Config settings).", e);
        }

        // SWLoghistoryFilename
        if (includeLogWatcher) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(SWLoghistoryFilename))) {
                if (lastSeenLogEntry != null) {
                    writer.write(saveType.lastSeenLogEntry + "," + EncodingUtils_encodeLDateTime(lastSeenLogEntry));
                    writer.newLine();
                }
            } catch (Exception e) {
                logger.debug("Unable to save " + SWLoghistoryFilename + " (Config settings).", e);
            }
        }

    }

    private static List<SrcTgtEntry> loadHistory() {
        List<SrcTgtEntry> mySTE = new ArrayList<>();

        Path histPath = Paths.get(SWhistoryFilename);
        if (Files.exists(histPath)) {

            try (BufferedReader reader = new BufferedReader(new FileReader(SWhistoryFilename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");

                    String dataType = fields[0];

                    if (dataType.equalsIgnoreCase(saveType.srctgtmap_prior)) {
                        String appName = EncodingUtils_decode(fields[1]);
                        String sourceName = EncodingUtils_decode(fields[2]);
                        String targetName = EncodingUtils_decode(fields[3]);
                        int srcNumOfDeletes = EncodingUtils_decodeInt(fields[4]);
                        int tgtNumOfDeletes = EncodingUtils_decodeInt(fields[5]);
                        int srcNumOfDdls = EncodingUtils_decodeInt(fields[7]);
                        int tgtNumOfDdls = EncodingUtils_decodeInt(fields[8]);
                        int srcNumOfPkupdates = EncodingUtils_decodeInt(fields[10]);
                        int tgtNumOfPkupdates = EncodingUtils_decodeInt(fields[11]);
                        int srcNumOfUpdates = EncodingUtils_decodeInt(fields[13]);
                        int tgtNumOfUpdates = EncodingUtils_decodeInt(fields[14]);
                        int srcNumOfInserts = EncodingUtils_decodeInt(fields[16]);
                        int tgtNumOfInserts = EncodingUtils_decodeInt(fields[17]);
                        mySTE.add(new SrcTgtEntry(appName, sourceName, targetName, srcNumOfDeletes, tgtNumOfDeletes, srcNumOfDdls, tgtNumOfDdls, srcNumOfPkupdates, tgtNumOfPkupdates, srcNumOfUpdates, tgtNumOfUpdates, srcNumOfInserts, tgtNumOfInserts));
                    }

                    if (dataType.equalsIgnoreCase(saveType.lastStart)) {
                        lastStart = EncodingUtils_decodeDateTime(fields[1]);
                    }
                }
            } catch (Exception ex) {
                logger.debug("Unable to load history.", ex);
            }
        }

        Path logHistPath = Paths.get(SWLoghistoryFilename);

        if (Files.exists(logHistPath)) {
            // SWLoghistoryFilename
            if (includeLogWatcher) {
                try (BufferedReader reader = new BufferedReader(new FileReader(SWLoghistoryFilename))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split(",");

                        String dataType = fields[0];

                        if (dataType.equalsIgnoreCase(saveType.lastSeenLogEntry)) {
                            lastSeenLogEntry = EncodingUtils_decodeLDateTime(fields[1]);
                        }
                    }

                } catch (Exception ex) {
                    logger.debug("Unable to load history.", ex);
                }
            }
        }

        return mySTE;
    }

    //  _      ____   _____  __          __  _______ _____ _    _ ______ _____
    //| |    / __ \ / ____| \ \        / /\|__   __/ ____| |  | |  ____|  __ \
    //| |   | |  | | |  __   \ \  /\  / /  \  | | | |    | |__| | |__  | |__) |
    //| |   | |  | | | |_ |   \ \/  \/ / /\ \ | | | |    |  __  |  __| |  _  /
    //| |___| |__| | |__| |    \  /\  / ____ \| | | |____| |  | | |____| | \ \
    //|______\____/ \_____|     \/  \/_/    \_\_|  \_____|_|  |_|______|_|  \_\
    //
    // For reading from the Striim.server.log and gathering detailed output

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static final int CONTEXT_LINES = 20;
    private StringBuilder contextStringBuilder = new StringBuilder();

    private long lastKnownLogPosition = 0;

    private CircularBuffer recentLines = new CircularBuffer(CONTEXT_LINES * 2 + 1);

    private boolean trackingError = false;
    private int nextContextLinesRemaining = 0;


    LocalDateTime eDate;
    String eServer;
    String eApp;
    String eLogLevel;
    String eMessage;

    // Circular Buffer Implementation
    class CircularBuffer {
        private final String[] buffer;
        private int head = 0, tail = 0, size = 0;

        public CircularBuffer(int capacity) {
            buffer = new String[capacity];
        }

        public void add(String item) {
            buffer[tail] = item;
            tail = (tail + 1) % buffer.length;
            if (size == buffer.length) {
                head = (head + 1) % buffer.length;
            } else {
                size++;
            }
        }

        public String get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            return buffer[(head + index) % buffer.length];
        }

        public int size() {
            return size;
        }

        public Stream<String> stream() {
            return Stream.iterate(0, i -> i + 1)
                    .limit(size)
                    .map(this::get);
        }

        public Stream<String> streamFromEnd(int count) {
            int start = Math.max(0, size - count);
            return Stream.iterate(start, i -> i + 1)
                    .limit(size - start)
                    .map(this::get);
        }
    }

    public static String getLogPath() {
        if (logPath.isEmpty()) {
            // Attempt 1: Using the logger
            String logFilePath = getLogPathFromLogger();
            if (logFilePath != null && !logFilePath.isEmpty()) {
                logPath = logFilePath;
            }

            // Attempt 2: Parsing the properties file
            logFilePath = getLogPathFromPropertiesFile();
            if (logFilePath != null && !logFilePath.isEmpty()) {
                logPath = logFilePath;
            }

        }

        return logPath;
    }

    private static String getLogPathFromLogger() {
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration config = context.getConfiguration();

        // Get the root logger configuration
        final LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);

        if (rootLoggerConfig.getAppenderRefs().size() > 0) {
            final String appenderName = rootLoggerConfig.getAppenderRefs().get(0).getRef();
            final Appender appender = config.getAppender(appenderName);

            if (appender instanceof RollingFileAppender) {
                final RollingFileAppender rollingFileAppender = (RollingFileAppender) appender;
                return rollingFileAppender.getFileName();
            }
        }

        return null;
    }

    private static String getLogPathFromPropertiesFile() {
        final Path logFilePath = Paths.get("conf/log4j.server.properties").toAbsolutePath();
        final Pattern pattern = Pattern.compile("^appender\\.ServerFileAppender\\.fileName\\s*=\\s*(.*)$");

        try {
            for (final String line : Files.readAllLines(logFilePath)) {
                final Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    return matcher.group(1);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading log4j.server.properties file: {}", e.getMessage());
        }

        return null;
    }

    public void readNewLogLines(long timeoutInSeconds, long startFromPosition) {
        try {
            Path logFilePath = Paths.get(getLogPath()).toAbsolutePath();

            long startTime = System.currentTimeMillis();
            long fileSize = Files.size(logFilePath);

            if (startFromPosition > 0 && startFromPosition != lastKnownLogPosition) {
                // Start from the end (only new entries)
                lastKnownLogPosition = startFromPosition;
            }

            if (fileSize > lastKnownLogPosition) {
                try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath.toFile()))) {
                    if (startFromPosition == 0) {
                        // Skip to the last known position if not starting from the end
                        for (long i = 0; i < lastKnownLogPosition; i++) {
                            reader.readLine();
                        }
                    }

                    String line;
                    StringBuilder logEntryBuilder = new StringBuilder();
                    long currentLineNumber = lastKnownLogPosition;

                    while ((line = reader.readLine()) != null) {
                        // Timeout check
                        if (System.currentTimeMillis() - startTime > timeoutInSeconds * 1000) {
                            //System.out.println("Timeout reached. Interrupting log processing.");
                            break;
                        }

                        currentLineNumber++;
                        logEntryBuilder.append(line).append('\n');
                        processLogEntry(logEntryBuilder.toString(), currentLineNumber);
                        logEntryBuilder.setLength(0);

                        //System.out.println("Processed line: " + currentLineNumber); // Tracking
                    }

                    lastKnownLogPosition = currentLineNumber;
                    //System.out.println("Updated lastKnownLogPosition to: " + lastKnownLogPosition); // Confirmation
                }
            } else {
                //System.out.println("No new log lines to process."); // Confirmation
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void processLogEntry(String logEntry, long currentLogPosition) {
        recentLines.add(logEntry);

        if (trackingError) {
            contextStringBuilder.append(logEntry).append("\n");
            nextContextLinesRemaining--;

            if (nextContextLinesRemaining == 0) {
                trackingError = false;

                // Add this entry into a custom class that contains the details I need
                errorDetailsList.add(new ErrorDetails(eDate, eServer, eApp, eLogLevel, eMessage, contextStringBuilder.toString()));

                // Clear variables for usage
                eDate = null;
                eServer = null;
                eApp = null;
                eLogLevel = null;
                eMessage = null;
                contextStringBuilder.setLength(0);

            }
        }

        try {
            // Check if the log entry is long enough before substring
            if (logEntry.length() >= 23) {
                DATE_FORMAT.parseDateTime(logEntry.substring(0, 23));
                parseAndPrintLogEntry(logEntry, currentLogPosition);
            } else {
                //System.out.println("LogEntry too short for date parsing: " + logEntry);
            }
        } catch (IllegalArgumentException e) {
            int lastIndex = recentLines.size() - 1;
            if (lastIndex >= 0) { // Check if there's a previous line
                String lastLine = recentLines.get(lastIndex);
                recentLines.add(lastLine + logEntry);
            } else {
                //System.out.println("No previous line to append to: " + logEntry);
            }
        }
    }

    boolean pendingError = false;
    private long holdLastKnownLogPosition = 0;

    // Helper method to extract value after a specific keyword
    private String extractValueAfter(String text, String keyword) {
        try {
            int index = text.indexOf(keyword);
            if (index != -1) {
                return text.substring(index + keyword.length()).trim();
            }
        }
        catch (Exception ex) {
            logger.debug("extractValueAfter: text=" + text + "; keyword=" + keyword);
        }
        return null;
    }

    private String extractValueBefore(String text, String keyword) {
        try {
            int index = text.indexOf(keyword);
            if (index != -1) {
                return text.substring(0, index).trim();
            }
        }
            catch (Exception ex) {
            logger.debug("extractValueBefore: text=" + text + "; keyword=" + keyword);
        }
        return null;
    }

    // Helper method to extract value between two keywords
    private String extractValueBetween(String text, String keyword1, String keyword2) {
        int startIndex = text.indexOf(keyword1);
        if (startIndex != -1) {
            int endIndex = text.indexOf(keyword2, startIndex + keyword1.length());
            if (endIndex != -1) {
                return text.substring(startIndex + keyword1.length(), endIndex).trim();
            }
        }
        return null;
    }

    private void parseAndPrintLogEntry(String logEntry, long currentLogPosition) {
        String[] parts = logEntry.split("@", 3);

        // Check if we have enough parts after splitting
        if (parts.length >= 3 && logEntry.contains("-")) {
            LocalDateTime date = LocalDateTime.parse(parts[0].trim(), DATE_FORMAT);
            String server = parts[1].trim().isEmpty() ? "N/A" : parts[1].trim();

            String[] streamAndRest = parts[2].split("-", 2);

            Boolean skip = false;

            if (lastSeenLogEntry != null && lastSeenLogEntry.isAfter(date)) {
               skip = true;
            }

            // Check if we have enough parts after the second split
            if (!skip && streamAndRest.length >= 2) {
                String app = streamAndRest[0].trim().isEmpty() ? "N/A" : streamAndRest[0].trim();
                String[] logLevelAndMessage = streamAndRest[1].trim().split(" ", 2);

                // Check if we have at least the log level
                if (logLevelAndMessage.length > 0) {
                    String logLevel = logLevelAndMessage[0].trim();
                    String message = logLevelAndMessage.length > 1 ? logLevelAndMessage[1].trim() : "";

                    lastSeenLogEntry = date;

                    // We are checking if we are currently gathering error details. If so, we need to track this new error position
                    if (trackingError && logLevel.equals("ERROR")) {
                        // We should not capture this error
                        pendingError = true;
                        holdLastKnownLogPosition = currentLogPosition;
                    }

                    // We have an error waiting, but we finished tracking the prior error
                    if (pendingError && !trackingError) {
                        lastKnownLogPosition = holdLastKnownLogPosition;
                        holdLastKnownLogPosition = 0;
                        pendingError = false;
                    }

                    // WARN handling for "Alert Matched:"
                    // As long as we didn't switch to pending error mode, then we can read and report on these. Otherwise, wait until we aren't pending, so we don't repeat
                    if (!pendingError && logLevel.equals("WARN") && message.contains("Alert Matched:")) {
                        String alertMatched = extractValueAfter(message, "Alert Matched:");
                        String messageContent = extractValueAfter(message, "Message:");

                        logger.debug("messageContent: " + messageContent);

                        String sourceOrTarget = extractValueBefore(extractValueAfter(messageContent, "Source "), ":");
                        if (sourceOrTarget == null) {
                            sourceOrTarget = extractValueBefore(extractValueAfter(messageContent, "Target "), ":");
                        }

                        if (sourceOrTarget == null) {
                            sourceOrTarget = extractValueBefore(extractValueAfter(messageContent, "Application "), ":");
                        }

                        if (sourceOrTarget == null) {
                            sourceOrTarget = extractValueBefore(extractValueAfter(messageContent, "Node "), ":");
                        }

                        String entityName = null;
                        String metricValue = null;
                        String metricUnit = null;
                        String type = null;

                        String[] colonParts = messageContent.split(":", 2);

                        if (colonParts.length == 2) {
                            type = colonParts[0].trim().split(" ")[0].toLowerCase();
                            String rest = colonParts[1].trim();

                            // Further split the rest if it contains a colon
                            String[] spaceParts = rest.split(" ", 2);
                            //entityName = (spaceParts.length > 1) ? spaceParts[0].trim() : "";
                            String Z = (spaceParts.length > 1) ? spaceParts[1].trim() : rest;

                            logger.debug("sourceOrTarget: " + sourceOrTarget);
                            logger.debug("type: " + type);
                            logger.debug("entityName: " + sourceOrTarget);
                            logger.debug("Z: " + Z);
                            logger.debug("----");
                        } else {
                            logger.debug("No match found for: " + messageContent);
                        }

                        // Extract entityName, metricValue, metricUnit, and type based on message templates
                        /*if (messageContent.contains("CPU usage is") || messageContent.contains("Free memory is") ||
                                messageContent.contains("Left the cluster")) {
                            type = "node";
                            entityName = extractValueBefore(messageContent, ": ");
                            if (messageContent.contains("CPU usage is") || messageContent.contains("Free memory is")) {
                                metricValue = extractValueBetween(messageContent, " is ", " ");
                                metricUnit = extractValueAfter(messageContent, metricValue + " ");
                            }
                        } else if (messageContent.contains("Checkpoint did not progress in last") ||
                                messageContent.contains("Backpressured for last") ||
                                messageContent.startsWith("Application ") ||
                                messageContent.contains("Resumed automatically") ||
                                messageContent.contains("Rebalanced")) {
                            type = "application";
                            entityName = extractValueBefore(messageContent, ": ");
                            if (messageContent.contains("Checkpoint did not progress in last") || messageContent.contains("Backpressured for last")) {
                                metricValue = extractValueBetween(messageContent, " last ", " ");
                                metricUnit = extractValueAfter(messageContent, metricValue + " ");
                            }
                        } else if (messageContent.contains("No new event read in last")) {
                            type = "source";
                            entityName = sourceOrTarget;
                            metricValue = extractValueBetween(messageContent, " in last ", " ");
                            metricUnit = extractValueAfter(messageContent, metricValue + " ");
                        } else if (messageContent.contains("No new event written in last") ||
                                messageContent.contains("LEE is")) {
                            type = "target";
                            entityName = sourceOrTarget;
                            metricValue = extractValueBetween(messageContent, " is ", " ");
                            metricUnit = extractValueAfter(messageContent, metricValue + " ");
                        }*/

                        // Add to smartAlertDetailsList (include type in the constructor)
                        smartAlertDetailsList.add(new SmartAlertDetails(date, server, app, logLevel, alertMatched,
                                messageContent, entityName, metricValue,
                                metricUnit, sourceOrTarget, type));
                    }

                    // We're not tracking another error, and we have an error, let's track it
                    if (!trackingError && logLevel.equals("ERROR")) {
                        //contextStringBuilder.append("Date: " + date).append("\n");
                        eDate = date;
                        eServer = server;
                        eApp = app;
                        eLogLevel = logLevel;
                        eMessage = message;
//                        contextStringBuilder.append("Server: " + server).append("\n");
//                        contextStringBuilder.append("App: " + app).append("\n");
//                        contextStringBuilder.append("LogLevel: " + logLevel).append("\n");
//                        contextStringBuilder.append("Message: " + message).append("\n");

                        recentLines.streamFromEnd(CONTEXT_LINES)
                                .forEach(line -> contextStringBuilder.append(line).append("\n"));
                        contextStringBuilder.append(" ***************** ERROR SPOTTED *****************").append("\n");
                        trackingError = true;
                        nextContextLinesRemaining = CONTEXT_LINES;
                    }
                } else {
                    //System.out.println("Invalid log format (missing log level): " + logEntry);
                }
            } else {
                //System.out.println("Invalid log format (missing '-' after second '@'): " + logEntry);
            }
        } else {
            //System.out.println("Invalid log format (not enough '@' or missing '-'): " + logEntry);
        }
    }



}
