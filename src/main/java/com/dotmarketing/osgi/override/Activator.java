package com.dotmarketing.osgi.override;

import com.dotcms.repackage.org.osgi.framework.BundleContext;
import com.dotmarketing.osgi.GenericBundleActivator;
import com.dotmarketing.osgi.util.LegacyFilesMigrator;
import com.dotcms.repackage.org.apache.logging.log4j.LogManager;
import com.dotcms.repackage.org.apache.logging.log4j.core.LoggerContext;
import com.dotmarketing.loggers.Log4jUtil;

/**
 * Created by Jonathan Gamba
 * Date: 6/18/12
 */
public class Activator extends GenericBundleActivator {

    private LoggerContext pluginLoggerContext;
    
    @SuppressWarnings ("unchecked")
    public void start ( BundleContext context ) throws Exception {
        
        //Initializing log4j...
        LoggerContext dotcmsLoggerContext = Log4jUtil.getLoggerContext();
        
        //Initialing the log4j context of this plugin based on the dotCMS logger context
        pluginLoggerContext = (LoggerContext) LogManager.getContext(this.getClass().getClassLoader(),
                false,
                dotcmsLoggerContext,
                dotcmsLoggerContext.getConfigLocation());

        //Initializing services...
        initializeServices( context );

        //Expose bundle elements
        publishBundleServices( context );
        
        LegacyFilesMigrator leg = new LegacyFilesMigrator();

        leg.migrateLegacyFiles();
    }

    public void stop ( BundleContext context ) throws Exception {

        //Unpublish bundle services
        unpublishBundleServices();
        
        //Shutting down log4j in order to avoid memory leaks
        Log4jUtil.shutdown(pluginLoggerContext);
    }

}