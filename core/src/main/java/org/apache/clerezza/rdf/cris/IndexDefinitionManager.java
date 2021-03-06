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
import java.util.Set;
import org.apache.clerezza.commons.rdf.BlankNode;
import org.apache.clerezza.commons.rdf.BlankNodeOrIRI;
import org.apache.clerezza.commons.rdf.Graph;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.RDFTerm;
import org.apache.clerezza.commons.rdf.impl.utils.TripleImpl;
import org.apache.clerezza.rdf.cris.ontologies.CRIS;
import org.apache.clerezza.rdf.ontologies.RDF;
import org.apache.clerezza.rdf.utils.GraphNode;
import org.apache.clerezza.rdf.utils.RdfList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the definitions that specify which literals of a resource are
 * indexed by CRIS.
 * 
 * @author rbn, tio, daniel
 */
public class IndexDefinitionManager {

    private static final Logger log = LoggerFactory.getLogger(IndexDefinitionManager.class);
    
    private final Graph definitionGraph;

    /**
     * Creates a new IndexDefinitionManager.
     * 
     * @param definitionGraph    the graph into which the definitions are written. 
     */
    public IndexDefinitionManager(Graph definitionGraph) {
        this.definitionGraph = definitionGraph;
    }

    /**
     * Defines an index for the specified type and properties, removing
     * previous index definitions for that type (java friendly version) with
     * facet search enabled on all properties
     * 
     * @param rdfType The RDF type for which to build an index.
     * @param properties A list of RDF properties to index.
     */
    public void addDefinition(IRI rdfType, List<IRI> properties) {
        addDefinition(rdfType, properties, true);
    }
    /**
     * Defines an index for the specified type and properties, removing
     * previous index definitions for that type (java friendly version)
     * 
     * @param rdfType The RDF type for which to build an index.
     * @param properties A list of RDF properties to index.
     * @param facetSearch true if all properties shall be facet properties false if none
     */
    public void addDefinition(IRI rdfType, List<IRI> properties, boolean facetSearch) {

        List<VirtualProperty> list = new ArrayList<VirtualProperty>();
        for (IRI uri : properties) {
            list.add(new PropertyHolder(uri, facetSearch));
        }
        addDefinitionVirtual(rdfType, list);
    }

    /**
     * Defines an index for the specified types and virtual properties, removing
     * previous index definitions for that type (java friendly version)
     * 
     * @param rdfType The RDF type for which to build an index.
     * @param properties A list of properties to index.
     */
    public void addDefinitionVirtual(IRI rdfType, List<VirtualProperty> properties) {
        deleteDefinition(rdfType);
        GraphNode node = new GraphNode(new BlankNode(), definitionGraph);
        node.writeLock().lock();
        try {
            node.addProperty(RDF.type, CRIS.IndexDefinition);
            node.addProperty(CRIS.indexedType, rdfType);
            for (VirtualProperty p : properties) {
                node.addProperty(CRIS.indexedProperty, asResource(p));
            }
        } finally {
            node.writeLock().unlock();
        }
    }

    /**
     * Remove index definitions for the specified RDF type.
     * 
     * @param rdfType the RDF type
     */
    public void deleteDefinition(IRI rdfType) {
        GraphNode node = new GraphNode(rdfType, definitionGraph);
        Set<GraphNode> toDelete = new HashSet<GraphNode>();
        node.readLock().lock();
        try {    
            Iterator<GraphNode> iter = node.getSubjectNodes(CRIS.indexedType);
            while (iter.hasNext()) {
                toDelete.add(iter.next());
            }
        } finally {
            node.readLock().unlock();
        }
        node.writeLock().lock();
        try {
            for (GraphNode graphNode : toDelete) {
                graphNode.deleteNodeContext();
            }
        } finally {
            node.writeLock().unlock();
        }
    }

    private RDFTerm asResource(VirtualProperty vp) {
        final BlankNodeOrIRI vpResource = asResourceTypeSpecific(vp);
        if (vp.isFacetProperty()) {
            definitionGraph.add(new TripleImpl(vpResource, RDF.type, CRIS.FacetProperty));
        }
        return vpResource;
    }
    private BlankNodeOrIRI asResourceTypeSpecific(VirtualProperty vp) {
        if (vp instanceof PropertyHolder) {
            return ((PropertyHolder) vp).property;
        } else if (vp instanceof JoinVirtualProperty) {
            JoinVirtualProperty joinVirtualProperty = (JoinVirtualProperty) vp;
            if (joinVirtualProperty.properties.isEmpty()) {
                throw new RuntimeException("vp " + vp + " conatins an empty list");
            }

            BlankNode virtualProperty = new BlankNode();
            definitionGraph.add(new TripleImpl(virtualProperty, RDF.type, CRIS.JoinVirtualProperty));
            BlankNode listBlankNode = new BlankNode();
            definitionGraph.add(new TripleImpl(virtualProperty, CRIS.propertyList, listBlankNode));
            List rdfList = new RdfList(listBlankNode, definitionGraph);
            for (VirtualProperty uri : joinVirtualProperty.properties) {
                rdfList.add(asResource(uri));
            }
            return virtualProperty;
        } else if (vp instanceof PathVirtualProperty) {
            PathVirtualProperty pathVirtualProperty = (PathVirtualProperty) vp;
            if (pathVirtualProperty.properties.isEmpty()) {
                throw new RuntimeException("vp " + vp + " conatins an empty list");
            }
            BlankNode virtualProperty = new BlankNode();
            definitionGraph.add(new TripleImpl(virtualProperty, RDF.type, CRIS.PathVirtualProperty));
            BlankNode listBlankNode = new BlankNode();
            definitionGraph.add(new TripleImpl(virtualProperty, CRIS.propertyList, listBlankNode));
            List rdfList = new RdfList(listBlankNode, definitionGraph);
            for (IRI uri : pathVirtualProperty.properties) {
                rdfList.add(uri);
            }
            return virtualProperty;
        }

        throw new RuntimeException("Could not create resource.");

    }
}
