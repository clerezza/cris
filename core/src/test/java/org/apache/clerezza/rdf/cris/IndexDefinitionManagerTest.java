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
import java.util.Iterator;
import java.util.List;
import org.apache.clerezza.commons.rdf.BlankNode;
import org.apache.clerezza.commons.rdf.Graph;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.Triple;
import org.apache.clerezza.commons.rdf.impl.utils.TripleImpl;
import org.apache.clerezza.commons.rdf.impl.utils.simple.SimpleGraph;
import org.apache.clerezza.rdf.ontologies.FOAF;
import org.apache.clerezza.rdf.ontologies.RDF;
import org.apache.clerezza.rdf.cris.ontologies.CRIS;
import org.apache.clerezza.rdf.utils.GraphNode;
import org.junit.Assert;
import org.junit.Test;
/**
 *
 * @author tio
 */
public class IndexDefinitionManagerTest {


    private void createDefinition(IRI rdfType, List<IRI> properties, 
            Graph manuallyCreatedGraph, boolean facetProperty) {
            GraphNode node = new GraphNode(new BlankNode(), manuallyCreatedGraph);
            node.addProperty(RDF.type, CRIS.IndexDefinition);
            node.addProperty(CRIS.indexedType, rdfType);
            for (IRI p : properties) {
                node.addProperty(CRIS.indexedProperty, p);
                if (facetProperty) {
                    manuallyCreatedGraph.add(new TripleImpl(p, RDF.type, CRIS.FacetProperty));
                }
            }
        }

    @Test
    public void createDefinitionGraphWithoutFacetProperties() {
        createDefinitionGraph(false);
    }
    
    @Test
    public void createDefinitionGraphWithFacetProperties() {
        createDefinitionGraph(true);
    }
    
    public void createDefinitionGraph(boolean withFacetProperties) {
    Graph indexManagerGraph = new SimpleGraph();
    IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager(indexManagerGraph);
    List<IRI> properties = new java.util.ArrayList<IRI>();
    properties.add(FOAF.firstName);
    properties.add(FOAF.lastName);
    indexDefinitionManager.addDefinition(FOAF.Person, properties, withFacetProperties);
    List<IRI> list = new ArrayList<IRI>();
    list.add(FOAF.firstName);
    list.add(FOAF.lastName);

     Graph manuallyCreatedGraph = new SimpleGraph();
    createDefinition(FOAF.Person, list, manuallyCreatedGraph, withFacetProperties);
    Assert.assertEquals(manuallyCreatedGraph.getImmutableGraph(), indexManagerGraph.getImmutableGraph());
    }
    
  @Test
    public void createJoinIndexProperty() {
    //import VirtualProperties._
    Graph indexManagerGraph = new SimpleGraph();
    IndexDefinitionManager indexDefinitionManager = new IndexDefinitionManager(indexManagerGraph);
    List<VirtualProperty> predicates = new java.util.ArrayList<VirtualProperty>();
    predicates.add(new PropertyHolder(FOAF.firstName));
    predicates.add(new PropertyHolder(FOAF.lastName));

    List<VirtualProperty>  properties = new java.util.ArrayList<VirtualProperty>();
    properties.add(new JoinVirtualProperty(predicates));
    indexDefinitionManager.addDefinitionVirtual(FOAF.Person, properties);
    Iterator<Triple> typeStmtIter = indexManagerGraph.filter(null, RDF.type, CRIS.JoinVirtualProperty);
    Assert.assertTrue(typeStmtIter.hasNext());
        //Assert.assertEquals(manuallyCreatedGraph.getGraph, indexManagerGraph.getGraph)
    }
}
