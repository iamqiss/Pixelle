/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_OFFLOAD_ID;

import java.io.PrintStream;
import org.apache.commons.lang3.tuple.Pair;

public class PrintingGBPTreeVisitor<ROOT_KEY, KEY, VALUE> extends GBPTreeVisitor.Adaptor<ROOT_KEY, KEY, VALUE> {
    private final PrintStream out;
    private final boolean printValues;
    private final boolean printPosition;
    private final boolean printState;
    private final boolean printHeader;
    private final boolean printFreelist;
    private final boolean printOffload;
    private final boolean printHistory;

    /**
     * Prints a {@link GBPTree} in human readable form, very useful for debugging.
     * Will print sub-tree from that point. Leaves cursor at same page as when called. No guarantees on offset.
     * @param printConfig {@link PrintConfig} containing configurations for this printing.
     */
    public PrintingGBPTreeVisitor(PrintConfig printConfig) {
        this.out = printConfig.getPrintStream();
        this.printValues = printConfig.getPrintValues();
        this.printPosition = printConfig.getPrintPosition();
        this.printState = printConfig.getPrintState();
        this.printHeader = printConfig.getPrintHeader();
        this.printFreelist = printConfig.getPrintFreelist();
        this.printOffload = printConfig.getPrintOffload();
        this.printHistory = printConfig.getPrintHistory();
    }

    @Override
    public void treeState(Pair<TreeState, TreeState> statePair) {
        if (printState) {
            out.println("StateA: " + statePair.getLeft());
            out.println("StateB: " + statePair.getRight());
        }
    }

    @Override
    public void beginLevel(int level) {
        out.println("Level " + level);
    }

    @Override
    public void beginNode(long pageId, boolean isLeaf, long generation, int keyCount) {
        if (printHeader) {
            String treeNodeType = isLeaf ? "leaf" : "internal";
            out.printf("{%d,%s,generation=%d,keyCount=%d} ", pageId, treeNodeType, generation, keyCount);
        } else {
            out.print("{" + pageId + "} ");
        }
    }

    @Override
    public void position(int i) {
        if (printPosition) {
            out.print("#" + i + " ");
        }
    }

    @Override
    public void key(KEY key, boolean isLeaf, long offloadId) {
        boolean doPrintOffload = printOffload && offloadId != NO_OFFLOAD_ID;
        out.print(doPrintOffload ? "__" + offloadId + "__" + key : key);
    }

    @Override
    public void value(ValueHolder<VALUE> value) {
        if (printValues) {
            out.print("=" + value.value + (value.defined ? "[D]" : "[U]"));
        }
        out.print(" ");
    }

    @Override
    public boolean visitHistory() {
        return printHistory;
    }

    @Override
    public void historyStart() {
        if (printHistory) {
            out.print("History <");
        }
    }

    @Override
    public void historyEnd() {
        if (printHistory) {
            out.print(" > ");
        }
    }

    @Override
    public void historicalValue(long version, ValueHolder<VALUE> value) {
        if (printHistory) {
            out.print(" " + version + ":" + value.value + (value.defined ? "[D]" : "[U]"));
        }
    }

    @Override
    public void child(long child) {
        out.print(" /" + child + "\\ ");
    }

    @Override
    public void endNode(long pageId) {
        out.println();
    }

    @Override
    public void beginFreelistPage(long pageId) {
        if (printFreelist) {
            out.print("Freelist{" + pageId + "} ");
        }
    }

    @Override
    public void endFreelistPage(long pageId) {
        if (printFreelist) {
            out.println();
        }
    }

    @Override
    public void freelistEntry(long pageId, long generation, int pos) {
        if (printFreelist) {
            out.print("[" + generation + "," + pageId + "] ");
        }
    }
}
