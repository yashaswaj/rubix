/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.common.metrics;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;

public class CustomMetricReporter {
  private static final Log log = LogFactory.getLog(CustomMetricReporter.class);

  private static CustomMetricReporter customMetricReporter;
  private static final Integer lock = 0;

  protected Configuration configuration;

  public CustomMetricReporter(Configuration configuration) {
      this.configuration = configuration;
  }

  public static CustomMetricReporter getMetricsCollectorInstance(Configuration conf) {
    if (customMetricReporter == null) {
      synchronized (lock) {
        if (customMetricReporter == null) {
          Class collectorClass;
          String className = CacheConfig.getRubixMetricCollectorImpl(conf);
          try {
            collectorClass = Class.forName(className);
            customMetricReporter = (CustomMetricReporter) collectorClass.getDeclaredConstructor(Configuration.class).newInstance(conf);
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error(String.format("External Metric Reporter class: %s can not be initialized", className), e);
            customMetricReporter = new CustomMetricReporter(conf);
          }
        }
      }
    }
    return customMetricReporter;
  }

  public void addMetric(CollectedMetric collectedMetric)
  {
    // Do nothing in default implementation.
  }

  public Configuration getConfiguration()
  {
    return this.configuration;
  }
}
