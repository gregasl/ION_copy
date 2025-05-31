/*
 * PropertyRegistration
 *
 * Handle configuration variable.
 * The VAR5 variable supports a custom logic to react on change requests.
 *
 * ION Trading U.K. Limited supplies this software code for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code. This software code has
 * not been thoroughly tested under all conditions. ION, therefore, cannot
 * guarantee or imply reliability, serviceability, or function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * Copyright 2005 ION Trading srl. All rights reserved.
 */
package com.iontrading.samples.advanced.byfeaturereport;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.bulkentitlements.IMkvESLibStatusListener;
import com.iontrading.mkv.bulkentitlements.IMkvByFeatureEntitlementReportListener;
import com.iontrading.mkv.bulkentitlements.IMkvESLibStatusListener;
import com.iontrading.mkv.bulkentitlements.IMkvByFeatureReport;
import com.iontrading.mkv.bulkentitlements.IMkvByFeatureUserReport;
import com.iontrading.mkv.bulkentitlements.IMkvByFeatureReportRule;
import com.iontrading.mkv.bulkentitlements.MkvESLibStatus;

public class ByFeatureReport {
    private Mkv mkv;

    public static void main(String commands[]) { new ByFeatureReport(commands); }

    public ByFeatureReport(String[] commands) {
        try {
            MkvQoS qos = new MkvQoS();
            qos.setArgs(commands);
            mkv = Mkv.start(qos);
            mkv.getEntitlementManager().addMkvESLibStatusListener(new MyESLibStatusListener());
        } catch (Exception e) { e.printStackTrace(); }
    }

    class MyESLibStatusListener implements IMkvESLibStatusListener {

        public void onStatusChange(MkvESLibStatus status) {
            if(status == MkvESLibStatus.READY) {

                /* As soon as the Entitlement Server Library is in READY state, it is safe to trigger report requests.
                 * Report requests arriving to the Entitlement Server Library before the READY state is reached
                 * are internally appended and will be served as soon as the library becomes READY.
                 *
                 * During this internal wait, a configurable timeout might expire:
                 * in this case, application-provided the report listener will be called on the onReportTimeout() event.
                 */

                /* ByFeature Report requests comes in three different flavors:
                 * - specifying just the Entitlement Namespace, to get the report for every user existing on the ION Platform:
                 *   mkv.getEntitlementManager().generateByFeatureReport("REFDATA", listener);
                 *
                 * - specifying the Entitlement Namespace and a user name, to get the report for the specified user only:
                 *   mkv.getEntitlementManager().generateByFeatureReport("REFDATA", "mkv", listener);
                 *
                 * - specifying the Entitlement Namespace, a user name and a feature, to get a report filtered on the specified feature:
                 *   mkv.getEntitlementManager().generateByFeatureReport("REFDATA", "mkv", "Trade/Edit", listener);
                 */
                mkv.getEntitlementManager().generateByFeatureReport("REFDATA", new IMkvByFeatureEntitlementReportListener() {

                    public void onReportCompleted(IMkvByFeatureReport report) {
                        System.out.println("ByFeature Report Generation completed for Namespace: " + report.getNamespace());

                        for(IMkvByFeatureUserReport userReport : report) { //or report.getUserReport(userName)
                            System.out.println("Report for User=" + userReport.getUserName() + ":");

                            for(IMkvByFeatureReportRule rule : userReport) {
                                System.out.println(
                                        "RuleType=" + rule.getRuleType() + " Name=" + rule.getName() + " RuleId=" + rule.getId() +
                                        " Resource=" + rule.getResource() + " Feature=" + rule.getFeature() +
                                        " Permission=" + rule.getPermission());
                            }
                        }
                    }

                    public void onReportTimeout(IMkvByFeatureReport report) {
                        System.out.println("ByFeature Report Generation has timeout while waiting for the Entitlement Server Library to become READY!");
                    }

                    public void onReportError(IMkvByFeatureReport report, String message) {
                        System.out.println("ByFeature Report Generation has failed with error: " + message);
                    }

                });
            }
        }

    }
}
