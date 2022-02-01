/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Collection;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * A special case that contains the resource calculation of the root queue.
 */
public final class RootCalculationDriver extends ResourceCalculationDriver {
  private final AbstractQueueCapacityCalculator rootCalculator;

  public RootCalculationDriver(CSQueue rootQueue, QueueCapacityUpdateContext updateContext,
                               AbstractQueueCapacityCalculator rootCalculator,
                               Collection<String> definedResources) {
    super(rootQueue, updateContext, Collections.emptyMap(), definedResources);
    this.rootCalculator = rootCalculator;
  }

  @Override
  public void calculateResources() {
    for (String label : parent.getConfiguredNodeLabels()) {
      for (QueueCapacityVector.QueueCapacityVectorEntry capacityVectorEntry : parent.getConfiguredCapacityVector(label)) {
        String resourceName = capacityVectorEntry.getResourceName();
        parent.getOrCreateAbsoluteMinCapacityVector(label).setValue(resourceName, 1);
        parent.getOrCreateAbsoluteMaxCapacityVector(label).setValue(resourceName, 1);

        CalculationContext context = new CalculationContext(resourceName, PERCENTAGE, parent);
        float minimumResource = rootCalculator.calculateMinimumResource(this, context, label);
        float maximumResource = rootCalculator.calculateMaximumResource(this, context, label);
        long roundedMinResource = (long) roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
        long roundedMaxResource = (long) roundingStrategy.getRoundedResource(maximumResource, parent.getConfiguredMaxCapacityVector(label).getResource(resourceName));
        parent.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
            resourceName, roundedMinResource);
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
            resourceName, roundedMaxResource);
      }
      rootCalculator.updateCapacitiesAfterCalculation(this, parent, label);
    }

    rootCalculator.calculateResourcePrerequisites(this);
  }
}
