/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.clerezza.rdf.cris;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.rdf.utils.GraphNode;

/**
 * A property that represents a path of properties.
 * 
 * @author rbn, daniel
 */
public class PathVirtualProperty extends VirtualProperty {

    /**
     * The properties representing the path.
     */
    List<IRI> properties;

    /**
     * Creates a new PathVirtualProperty with face search enabled
     * 
     * @param properties 
     */
    public PathVirtualProperty(List<IRI> properties) {
        this(properties, true);
    }
    /**
     * Creates a new PathVirtualProperty.
     * 
     * @param properties An ordered list specifying the path. 
     */
    public PathVirtualProperty(List<IRI> properties, boolean facetProperty) {
        super(facetProperty);
        this.properties = properties;
        List<VirtualProperty> list = new ArrayList<VirtualProperty>();
        for (IRI p : this.properties) {
            //facetProperty is not digest relevant
            list.add(new PropertyHolder(p,false));
        }
        this.baseProperties = new HashSet<IRI>(properties);
        this.stringKey = "P" + VirtualProperty.listDigest(list);
    }

    @Override
    protected List<String> value(GraphNode node) {
        List<String> list = new ArrayList<String>();
        getPathResults(node, this.properties, list);
        return list;
    }

    @Override
    protected List<IRI> pathToIndexedResource(IRI property) {

        List<IRI> list = new ArrayList<IRI>();
        for (IRI prop : this.properties) {
            if (!prop.equals(property)) {
                list.add(prop);
            }
        }
        return list;

    }

    private void getPathResults(GraphNode node, List<IRI> properties, List<String> list) {
        if (properties.size() == 1) {
            //being a facet property or not is irrelevant for the result
            list.addAll(new PropertyHolder(properties.get(0), false).value(node));
        } else {
            Lock lock = node.readLock();
            lock.lock();
            try {
                Iterator<GraphNode> iter = node.getObjectNodes(properties.get(0));
                while (iter.hasNext()) {
                    getPathResults(iter.next(), properties.subList(1, properties.size()), list);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
