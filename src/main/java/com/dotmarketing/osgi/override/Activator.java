package com.dotmarketing.osgi.override;

import com.dotcms.repackage.org.osgi.framework.BundleContext;
import com.dotmarketing.osgi.GenericBundleActivator;
import com.dotmarketing.osgi.util.LegacyFilesMigrator;

/**
 * Activator class for the Legacy File Migration plugin. The approach for the
 * migration process is to spawn a new thread that will be in charge of
 * migrating the legacy files and, therefore, keep the OSGi framework from
 * having to extend the start phase of the bundle because of such a heavy
 * process.
 * 
 * @author Jose Orsini, Jose Castro
 * @version 3.3
 * @since Aug 30th, 2017
 */
public class Activator extends GenericBundleActivator {

	@Override
	public void start(BundleContext context) throws Exception {
		// Initializing services...
		initializeServices(context);
		// Expose bundle elements
		publishBundleServices(context);
		final LegacyFilesMigrator migrator = new LegacyFilesMigrator();
		Thread migrationThread = new Thread(new Runnable() {

			@Override
			public void run() {
				migrator.migrateLegacyFiles();
			}

		});
		migrationThread.start();
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		// Unpublish bundle services
		unpublishBundleServices();
	}

}
