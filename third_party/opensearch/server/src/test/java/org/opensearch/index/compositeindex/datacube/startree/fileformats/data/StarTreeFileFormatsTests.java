/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.compositeindex.datacube.startree.fileformats.data;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.density.index.compositeindex.datacube.startree.fileformats.StarTreeWriter;
import org.density.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.density.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.density.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.density.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.density.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.density.test.DensityTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeFileFormatsTests extends DensityTestCase {

    private IndexOutput dataOut;
    private IndexInput dataIn;
    private Directory directory;
    private Integer maxLevels;
    private static Integer dimensionValue;

    @Before
    public void setup() throws IOException {
        directory = newFSDirectory(createTempDir());
        maxLevels = randomIntBetween(2, 5);
        dimensionValue = 0;
    }

    public void test_StarTreeNode() throws IOException {

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap = new LinkedHashMap<>();
        InMemoryTreeNode root = generateSampleTree(inMemoryTreeNodeMap);
        StarTreeWriter starTreeWriter = new StarTreeWriter();
        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, root, inMemoryTreeNodeMap.size(), "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, (inMemoryTreeNodeMap.size() * 33L));
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);

        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        StarTreeNode starTreeNode = StarTreeFactory.createStarTree(dataIn, starTreeMetadata);
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeNode);

        while ((starTreeNode = queue.poll()) != null) {

            // verify the star node
            assertStarTreeNode(starTreeNode, inMemoryTreeNodeMap.get(starTreeNode.getDimensionValue()));

            Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

            if (starTreeNode.getChildDimensionId() != -1) {
                while (childrenIterator.hasNext()) {
                    StarTreeNode child = childrenIterator.next();
                    if (child.getStarTreeNodeType() == StarTreeNodeType.DEFAULT.getValue()) {
                        assertStarTreeNode(
                            starTreeNode.getChildForDimensionValue(child.getDimensionValue()),
                            inMemoryTreeNodeMap.get(child.getDimensionValue())
                        );
                        assertNull(starTreeNode.getChildStarNode());
                    }

                    queue.add(child);
                }
            } else {
                assertTrue(starTreeNode.isLeaf());
            }
        }

        dataIn.close();

    }

    public void test_starTreeSearch() throws IOException {

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap = new LinkedHashMap<>();
        InMemoryTreeNode root = generateSampleTree(inMemoryTreeNodeMap);
        StarTreeWriter starTreeWriter = new StarTreeWriter();
        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, root, inMemoryTreeNodeMap.size(), "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, (inMemoryTreeNodeMap.size() * 33L));
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);

        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        StarTreeNode starTreeNode = StarTreeFactory.createStarTree(dataIn, starTreeMetadata);
        InMemoryTreeNode inMemoryTreeNode = inMemoryTreeNodeMap.get(starTreeNode.getDimensionValue());
        assertNotNull(inMemoryTreeNode);

        for (int i = 0; i < maxLevels - 1; i++) {
            InMemoryTreeNode randomChildNode = randomFrom(inMemoryTreeNode.getChildren().values());
            StarTreeNode randomStarTreeChildNode = starTreeNode.getChildForDimensionValue(randomChildNode.getDimensionValue());

            assertNotNull(randomStarTreeChildNode);
            assertStarTreeNode(randomStarTreeChildNode, randomChildNode);

            starTreeNode = randomStarTreeChildNode;
            inMemoryTreeNode = randomChildNode;

        }
        dataIn.close();
    }

    private void assertStarTreeNode(StarTreeNode starTreeNode, InMemoryTreeNode treeNode) throws IOException {
        assertEquals(starTreeNode.getDimensionId(), treeNode.getDimensionId());
        assertEquals(starTreeNode.getDimensionValue(), treeNode.getDimensionValue());
        assertEquals(starTreeNode.getStartDocId(), treeNode.getStartDocId());
        assertEquals(starTreeNode.getEndDocId(), treeNode.getEndDocId());
        assertEquals(starTreeNode.getChildDimensionId(), treeNode.getChildDimensionId());
        assertEquals(starTreeNode.getAggregatedDocId(), treeNode.getAggregatedDocId());
        assertEquals(starTreeNode.getStarTreeNodeType(), treeNode.getNodeType());

        if (starTreeNode.getChildDimensionId() != -1) {
            assertFalse(starTreeNode.isLeaf());
            if (treeNode.getChildren() != null) {
                assertEquals(starTreeNode.getNumChildren(), treeNode.getChildren().values().size());
            }
        } else {
            assertTrue(starTreeNode.isLeaf());
        }

    }

    public InMemoryTreeNode generateSampleTree(Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap) {
        // Create the root node
        InMemoryTreeNode root = new InMemoryTreeNode(0, randomInt(), randomInt(), (byte) 0, -1);
        root.setChildDimensionId(1);
        root.setAggregatedDocId(randomInt());

        inMemoryTreeNodeMap.put(root.getDimensionValue(), root);

        // Generate the tree recursively
        generateTreeRecursively(root, 1, inMemoryTreeNodeMap);

        return root;
    }

    private void generateTreeRecursively(InMemoryTreeNode parent, int currentLevel, Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap) {
        if (currentLevel >= this.maxLevels) {
            return; // Maximum level reached, stop generating children
        }

        int numChildren = randomIntBetween(1, 10);

        for (int i = 0; i < numChildren; i++) {
            dimensionValue++;
            InMemoryTreeNode child = new InMemoryTreeNode(currentLevel, randomInt(), randomInt(), (byte) 0, dimensionValue);

            child.setChildDimensionId((currentLevel == this.maxLevels - 1) ? -1 : (currentLevel + 1));
            child.setAggregatedDocId(randomInt());

            parent.addChildNode(child, child.getDimensionValue());
            inMemoryTreeNodeMap.put(child.getDimensionValue(), child);

            generateTreeRecursively(child, currentLevel + 1, inMemoryTreeNodeMap);
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        dataIn.close();
        dataOut.close();
        directory.close();
    }

}
