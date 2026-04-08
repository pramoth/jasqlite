package com.jasqlite.store.btree;

import com.jasqlite.store.page.Page;
import com.jasqlite.store.page.Pager;
import com.jasqlite.store.record.Record;
import com.jasqlite.util.BinaryUtils;
import com.jasqlite.util.SQLiteValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * B-Tree operations for SQLite table and index B-trees.
 *
 * Table B-trees use 64-bit signed integer keys (rowid).
 * Index B-trees use arbitrary key values.
 *
 * Leaf Table B-Tree Cell Format:
 * - Varint: payload size
 * - Varint: rowid
 * - Payload (record format)
 * - Optional overflow page pointer (4 bytes)
 *
 * Interior Table B-Tree Cell Format:
 * - 4-byte left child page number
 * - Varint: rowid (key)
 *
 * Leaf Index B-Tree Cell Format:
 * - Varint: payload size
 * - Payload (record format)
 * - Optional overflow page pointer
 *
 * Interior Index B-Tree Cell Format:
 * - 4-byte left child page number
 * - Varint: payload size
 * - Payload
 * - Optional overflow page pointer
 */
public class BTree {

    private final Pager pager;
    private final int usableSize;

    // Maximum local payload for table leaf pages
    private final int maxLocalLeafTable;
    // Minimum local payload for table leaf pages
    private final int minLocalLeafTable;
    // Maximum local payload for index pages
    private final int maxLocalIndex;
    // Minimum local payload for index pages
    private final int minLocalIndex;

    public BTree(Pager pager) {
        this.pager = pager;
        this.usableSize = pager.getUsableSize();

        // From SQLite docs:
        // U = usable size, P = payload size
        // X = max local payload for table leaf = U - 35
        // M = min local payload = ((U - 12) * 32 / 255) - 23
        // K = M + ((P - M) % (U - 4))
        // If P <= X, store all locally. Else store K bytes locally with overflow.
        this.maxLocalLeafTable = usableSize - 35;
        this.minLocalLeafTable = ((usableSize - 12) * 32 / 255) - 23;

        // For index pages:
        // X = ((U - 12) * 64 / 255) - 23
        // M = ((U - 12) * 32 / 255) - 23
        this.maxLocalIndex = ((usableSize - 12) * 64 / 255) - 23;
        this.minLocalIndex = minLocalLeafTable;
    }

    /**
     * Result of a B-tree insert or delete.
     */
    public static class CellLocation {
        public int pageNumber;
        public int cellIndex;
        public long rowid;

        public CellLocation(int pageNumber, int cellIndex, long rowid) {
            this.pageNumber = pageNumber;
            this.cellIndex = cellIndex;
            this.rowid = rowid;
        }
    }

    /**
     * Cursor for traversing a B-tree.
     */
    public static class Cursor {
        public int pageNumber;
        public int cellIndex;
        public long rowid;
        public boolean eof;
        public boolean initialized;

        public Cursor() {
            this.pageNumber = 0;
            this.cellIndex = 0;
            this.rowid = 0;
            this.eof = false;
            this.initialized = false;
        }
    }

    /**
     * Create a new table B-tree (allocates a root page and initializes it as a leaf).
     * Returns the root page number.
     */
    public int createTable() throws IOException {
        int pageNum = pager.allocatePage();
        Page page = pager.getPageForWrite(pageNum);
        page.initAsLeafTable();
        return pageNum;
    }

    /**
     * Create a new index B-tree.
     * Returns the root page number.
     */
    public int createIndex() throws IOException {
        int pageNum = pager.allocatePage();
        Page page = pager.getPageForWrite(pageNum);
        page.initAsLeafIndex();
        return pageNum;
    }

    /**
     * Insert a record into a table B-tree with the given rowid.
     */
    public void insert(int rootPage, long rowid, Record record) throws IOException {
        byte[] payload = record.serialize();
        insertCell(rootPage, rowid, payload, true);
    }

    /**
     * Insert into an index B-tree.
     */
    public void insertIndex(int rootPage, byte[] keyPayload) throws IOException {
        insertIndexCell(rootPage, keyPayload);
    }

    /**
     * Delete a row from a table B-tree by rowid.
     */
    public boolean delete(int rootPage, long rowid) throws IOException {
        CellLocation loc = findCell(rootPage, rowid);
        if (loc == null) return false;
        return deleteCell(loc.pageNumber, loc.cellIndex, rowid);
    }

    /**
     * Find a cell in a table B-tree by rowid.
     */
    public CellLocation findCell(int rootPage, long rowid) throws IOException {
        return findCellInPage(rootPage, rowid);
    }

    private CellLocation findCellInPage(int pageNum, long rowid) throws IOException {
        Page page = pager.getPage(pageNum);

        if (page.isLeafTablePage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getCellPointer(i);
                long[] varIntResult = BinaryUtils.readVarInt(page.getData(), cellOffset);
                int payloadSize = (int) varIntResult[0];
                long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + (int) varIntResult[1]);
                long cellRowid = rowidResult[0];
                if (cellRowid == rowid) {
                    return new CellLocation(pageNum, i, rowid);
                }
            }
            return null;
        }

        if (page.isInteriorTablePage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getCellPointer(i);
                int leftChild = (int) BinaryUtils.readUInt32(page.getData(), cellOffset);
                long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + 4);
                long cellRowid = rowidResult[0];

                if (rowid <= cellRowid) {
                    return findCellInPage(leftChild, rowid);
                }
            }
            // Check rightmost pointer
            int rightMost = page.getRightMostPointer();
            if (rightMost > 0) {
                return findCellInPage(rightMost, rowid);
            }
            return null;
        }

        return null;
    }

    /**
     * Read a record from a table B-tree cell.
     */
    public Record readRecord(int pageNum, int cellIndex) throws IOException {
        Page page = pager.getPage(pageNum);
        int cellOffset = page.getCellPointer(cellIndex);

        long[] payloadSizeResult = BinaryUtils.readVarInt(page.getData(), cellOffset);
        int payloadSize = (int) payloadSizeResult[0];
        long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + (int) payloadSizeResult[1]);
        int payloadOffset = cellOffset + (int) payloadSizeResult[1] + (int) rowidResult[1];

        int localSize = localPayloadSize(payloadSize, true);
        byte[] payloadData = new byte[payloadSize];

        // Read local payload
        System.arraycopy(page.getData(), payloadOffset, payloadData, 0, Math.min(localSize, payloadSize));

        // Handle overflow pages
        if (localSize < payloadSize) {
            int overflowPtr = payloadOffset + localSize;
            int overflowPage = (int) BinaryUtils.readUInt32(page.getData(), overflowPtr);
            int bytesRead = localSize;
            while (bytesRead < payloadSize && overflowPage > 0) {
                Page op = pager.getPage(overflowPage);
                int chunkSize = Math.min(usableSize - 4, payloadSize - bytesRead);
                System.arraycopy(op.getData(), 4, payloadData, bytesRead, chunkSize);
                bytesRead += chunkSize;
                overflowPage = (int) BinaryUtils.readUInt32(op.getData(), 0);
            }
        }

        return Record.deserialize(payloadData, 0).record;
    }

    /**
     * Read the rowid from a table B-tree cell.
     */
    public long readRowid(int pageNum, int cellIndex) throws IOException {
        Page page = pager.getPage(pageNum);
        int cellOffset = page.getCellPointer(cellIndex);
        long[] payloadSizeResult = BinaryUtils.readVarInt(page.getData(), cellOffset);
        long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + (int) payloadSizeResult[1]);
        return rowidResult[0];
    }

    /**
     * Get all records from a table B-tree (full table scan).
     */
    public List<RecordWithRowid> scanTable(int rootPage) throws IOException {
        List<RecordWithRowid> results = new ArrayList<>();
        scanTablePage(rootPage, results);
        return results;
    }

    private void scanTablePage(int pageNum, List<RecordWithRowid> results) throws IOException {
        Page page = pager.getPage(pageNum);

        if (page.isLeafTablePage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                long rowid = readRowid(pageNum, i);
                Record record = readRecord(pageNum, i);
                results.add(new RecordWithRowid(record, rowid));
            }
        } else if (page.isInteriorTablePage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getCellPointer(i);
                int leftChild = (int) BinaryUtils.readUInt32(page.getData(), cellOffset);
                scanTablePage(leftChild, results);
            }
            int rightMost = page.getRightMostPointer();
            if (rightMost > 0) {
                scanTablePage(rightMost, results);
            }
        }
    }

    /**
     * Get all index entries from an index B-tree.
     */
    public List<byte[]> scanIndex(int rootPage) throws IOException {
        List<byte[]> results = new ArrayList<>();
        scanIndexPage(rootPage, results);
        return results;
    }

    private void scanIndexPage(int pageNum, List<byte[]> results) throws IOException {
        Page page = pager.getPage(pageNum);

        if (page.isLeafIndexPage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                byte[] payload = readIndexPayload(pageNum, i);
                results.add(payload);
            }
        } else if (page.isInteriorIndexPage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getCellPointer(i);
                int leftChild = (int) BinaryUtils.readUInt32(page.getData(), cellOffset);
                scanIndexPage(leftChild, results);
                byte[] payload = readIndexPayload(pageNum, i);
                results.add(payload);
            }
            int rightMost = page.getRightMostPointer();
            if (rightMost > 0) {
                scanIndexPage(rightMost, results);
            }
        }
    }

    private byte[] readIndexPayload(int pageNum, int cellIndex) throws IOException {
        Page page = pager.getPage(pageNum);
        int cellOffset = page.getCellPointer(cellIndex);
        int hdrSize = page.isLeafIndexPage() ? 0 : 4;

        long[] payloadSizeResult = BinaryUtils.readVarInt(page.getData(), cellOffset + hdrSize);
        int payloadSize = (int) payloadSizeResult[0];
        int payloadOffset = cellOffset + hdrSize + (int) payloadSizeResult[1];

        int localSize = localPayloadSize(payloadSize, false);
        byte[] payloadData = new byte[payloadSize];
        System.arraycopy(page.getData(), payloadOffset, payloadData, 0, Math.min(localSize, payloadSize));

        if (localSize < payloadSize) {
            int overflowPtr = payloadOffset + localSize;
            int overflowPage = (int) BinaryUtils.readUInt32(page.getData(), overflowPtr);
            int bytesRead = localSize;
            while (bytesRead < payloadSize && overflowPage > 0) {
                Page op = pager.getPage(overflowPage);
                int chunkSize = Math.min(usableSize - 4, payloadSize - bytesRead);
                System.arraycopy(op.getData(), 4, payloadData, bytesRead, chunkSize);
                bytesRead += chunkSize;
                overflowPage = (int) BinaryUtils.readUInt32(op.getData(), 0);
            }
        }
        return payloadData;
    }

    private void insertCell(int rootPage, long rowid, byte[] payload, boolean isTable) throws IOException {
        // Find the leaf page where the cell should be inserted
        InsertTarget target = findInsertTarget(rootPage, rowid);
        Page leaf = pager.getPageForWrite(target.pageNumber);

        // Compute local payload size
        int localSize = localPayloadSize(payload.length, isTable);
        boolean hasOverflow = localSize < payload.length;

        // Cell size: varint(payload_size) + varint(rowid) + local_payload + overflow_ptr
        int cellSize = BinaryUtils.varIntSize(payload.length) +
                       BinaryUtils.varIntSize(rowid) +
                       localSize +
                       (hasOverflow ? 4 : 0);

        // Check if we need to split
        int cellCount = leaf.getCellCount();
        int cellPtrAreaSize = (cellCount + 1) * 2;
        int hdrSize = leaf.headerOffset() + (leaf.isLeafTablePage() ? 8 : 12);
        int freeSpace = leaf.getCellContentOffset() - hdrSize - cellPtrAreaSize;

        if (cellSize > freeSpace) {
            // Need to split the page
            splitAndInsert(rootPage, target.pageNumber, rowid, payload, isTable);
            return;
        }

        // Allocate space for the cell
        int cellOffset = leaf.getCellContentOffset() - cellSize;
        leaf.setCellContentOffset(cellOffset);

        // Write cell data
        byte[] data = leaf.getData();
        int pos = cellOffset;
        pos += BinaryUtils.writeVarInt(data, pos, payload.length);
        pos += BinaryUtils.writeVarInt(data, pos, rowid);
        System.arraycopy(payload, 0, data, pos, localSize);
        pos += localSize;

        // Handle overflow
        if (hasOverflow) {
            int overflowPage = writeOverflowPages(payload, localSize);
            BinaryUtils.writeUInt32(data, pos, overflowPage);
        }

        // Update cell pointers - shift existing pointers and insert new one
        int insertIdx = target.cellIndex;
        // Shift cell pointers
        for (int i = cellCount; i > insertIdx; i--) {
            leaf.setCellPointer(i, leaf.getCellPointer(i - 1));
        }
        leaf.setCellPointer(insertIdx, cellOffset);
        leaf.setCellCount(cellCount + 1);
    }

    private void insertIndexCell(int rootPage, byte[] keyPayload) throws IOException {
        // For simplicity, always insert at the first available leaf
        // A production engine would do binary search to find the right position
        int leafPage = findIndexLeafPage(rootPage);
        Page leaf = pager.getPageForWrite(leafPage);

        int localSize = localPayloadSize(keyPayload.length, false);
        boolean hasOverflow = localSize < keyPayload.length;

        int cellSize = BinaryUtils.varIntSize(keyPayload.length) + localSize + (hasOverflow ? 4 : 0);
        int cellCount = leaf.getCellCount();
        int hdrSize = leaf.headerOffset() + 8;
        int freeSpace = leaf.getCellContentOffset() - hdrSize - (cellCount + 1) * 2;

        if (cellSize > freeSpace) {
            // Simple approach: just create a new leaf if this one is full
            // A production engine would properly split
            int newPage = pager.allocatePage();
            Page newLeaf = pager.getPageForWrite(newPage);
            newLeaf.initAsLeafIndex();

            int cellOffset = pager.getPageSize() - cellSize;
            newLeaf.setCellContentOffset(cellOffset);
            byte[] data = newLeaf.getData();
            int pos = cellOffset;
            pos += BinaryUtils.writeVarInt(data, pos, keyPayload.length);
            System.arraycopy(keyPayload, 0, data, pos, localSize);
            if (hasOverflow) {
                pos += localSize;
                int overflowPage = writeOverflowPages(keyPayload, localSize);
                BinaryUtils.writeUInt32(data, pos, overflowPage);
            }
            newLeaf.setCellPointer(0, cellOffset);
            newLeaf.setCellCount(1);
            return;
        }

        int cellOffset = leaf.getCellContentOffset() - cellSize;
        leaf.setCellContentOffset(cellOffset);

        byte[] data = leaf.getData();
        int pos = cellOffset;
        pos += BinaryUtils.writeVarInt(data, pos, keyPayload.length);
        System.arraycopy(keyPayload, 0, data, pos, localSize);
        if (hasOverflow) {
            pos += localSize;
            int overflowPage = writeOverflowPages(keyPayload, localSize);
            BinaryUtils.writeUInt32(data, pos, overflowPage);
        }

        leaf.setCellPointer(cellCount, cellOffset);
        leaf.setCellCount(cellCount + 1);
    }

    private int findIndexLeafPage(int pageNum) throws IOException {
        Page page = pager.getPage(pageNum);
        if (page.isLeafIndexPage()) return pageNum;
        int cellCount = page.getCellCount();
        if (cellCount > 0) {
            int cellOffset = page.getCellPointer(0);
            int leftChild = (int) BinaryUtils.readUInt32(page.getData(), cellOffset);
            return findIndexLeafPage(leftChild);
        }
        int rightMost = page.getRightMostPointer();
        if (rightMost > 0) return findIndexLeafPage(rightMost);
        return pageNum;
    }

    private InsertTarget findInsertTarget(int pageNum, long rowid) throws IOException {
        Page page = pager.getPage(pageNum);

        if (page.isLeafTablePage()) {
            int cellCount = page.getCellCount();
            int insertIdx = cellCount;
            for (int i = 0; i < cellCount; i++) {
                long cellRowid = readRowid(pageNum, i);
                if (rowid <= cellRowid) {
                    insertIdx = i;
                    break;
                }
            }
            return new InsertTarget(pageNum, insertIdx);
        }

        if (page.isInteriorTablePage()) {
            int cellCount = page.getCellCount();
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getCellPointer(i);
                int leftChild = (int) BinaryUtils.readUInt32(page.getData(), cellOffset);
                long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + 4);
                long cellRowid = rowidResult[0];
                if (rowid <= cellRowid) {
                    return findInsertTarget(leftChild, rowid);
                }
            }
            int rightMost = page.getRightMostPointer();
            if (rightMost > 0) {
                return findInsertTarget(rightMost, rowid);
            }
        }

        return new InsertTarget(pageNum, 0);
    }

    private void splitAndInsert(int rootPage, int pageNum, long rowid, byte[] payload, boolean isTable) throws IOException {
        // Allocate new page
        int newPageNum = pager.allocatePage();
        Page newPage = pager.getPageForWrite(newPageNum);
        newPage.initAsLeafTable();

        // Move half the cells to the new page
        Page oldPage = pager.getPageForWrite(pageNum);
        int cellCount = oldPage.getCellCount();
        int half = cellCount / 2;

        // For simplicity, we'll move the upper half to new page
        for (int i = half; i < cellCount; i++) {
            long oldRowid = readRowid(pageNum, i);
            Record oldRecord = readRecord(pageNum, i);
            byte[] oldPayload = oldRecord.serialize();

            int localSize = localPayloadSize(oldPayload.length, true);
            boolean hasOverflow = localSize < oldPayload.length;
            int cellSize = BinaryUtils.varIntSize(oldPayload.length) +
                           BinaryUtils.varIntSize(oldRowid) +
                           localSize + (hasOverflow ? 4 : 0);

            int newCellCount = newPage.getCellCount();
            int cellOffset = newPage.getCellContentOffset() - cellSize;
            newPage.setCellContentOffset(cellOffset);

            byte[] data = newPage.getData();
            int pos = cellOffset;
            pos += BinaryUtils.writeVarInt(data, pos, oldPayload.length);
            pos += BinaryUtils.writeVarInt(data, pos, oldRowid);
            System.arraycopy(oldPayload, 0, data, pos, localSize);
            if (hasOverflow) {
                pos += localSize;
                int overflowPage = writeOverflowPages(oldPayload, localSize);
                BinaryUtils.writeUInt32(data, pos, overflowPage);
            }

            newPage.setCellPointer(newCellCount, cellOffset);
            newPage.setCellCount(newCellCount + 1);
        }

        // Remove moved cells from old page (rebuild it)
        rebuildPageAfterSplit(pageNum, half);

        // If this was the root, create a new root
        if (pageNum == rootPage) {
            int newRootNum = pager.allocatePage();
            Page newRoot = pager.getPageForWrite(newRootNum);
            newRoot.initAsInteriorTable();

            byte[] rootData = newRoot.getData();
            int hdr = newRoot.headerOffset();
            BinaryUtils.writeUInt32(rootData, hdr + 8, newPageNum); // rightmost = new page

            // Add the old root's max rowid as a key pointing to old page
            long maxOldRowid = readRowid(pageNum, half > 0 ? half - 1 : 0);
            int cellOff = newRoot.getCellContentOffset() - 4 - BinaryUtils.varIntSize(maxOldRowid);
            newRoot.setCellContentOffset(cellOff);
            BinaryUtils.writeUInt32(rootData, cellOff, pageNum);
            BinaryUtils.writeVarInt(rootData, cellOff + 4, maxOldRowid);
            newRoot.setCellPointer(0, cellOff);
            newRoot.setCellCount(1);

            // Now insert the new row into the appropriate child
            if (rowid <= maxOldRowid) {
                insertCell(newRootNum, rowid, payload, isTable);
            } else {
                insertCell(newRootNum, rowid, payload, isTable);
            }
        } else {
            // Insert the new row into the appropriate page
            long splitRowid = readRowid(pageNum, half > 0 ? half - 1 : 0);
            if (rowid <= splitRowid) {
                insertCell(rootPage, rowid, payload, isTable);
            } else {
                insertCell(rootPage, rowid, payload, isTable);
            }
        }
    }

    private void rebuildPageAfterSplit(int pageNum, int keepCount) throws IOException {
        Page page = pager.getPageForWrite(pageNum);

        // Read the cells we want to keep
        List<byte[]> keptPayloads = new ArrayList<>();
        List<Long> keptRowids = new ArrayList<>();
        for (int i = 0; i < keepCount; i++) {
            keptRowids.add(readRowid(pageNum, i));
            keptPayloads.add(readRecord(pageNum, i).serialize());
        }

        // Reinitialize the page
        page.initAsLeafTable();

        // Re-insert kept cells
        byte[] data = page.getData();
        int currentOffset = pager.getPageSize();
        for (int i = keepCount - 1; i >= 0; i--) {
            byte[] payload = keptPayloads.get(i);
            long rowid = keptRowids.get(i);
            int localSize = localPayloadSize(payload.length, true);
            boolean hasOverflow = localSize < payload.length;
            int cellSize = BinaryUtils.varIntSize(payload.length) +
                           BinaryUtils.varIntSize(rowid) +
                           localSize + (hasOverflow ? 4 : 0);

            currentOffset -= cellSize;
            int pos = currentOffset;
            pos += BinaryUtils.writeVarInt(data, pos, payload.length);
            pos += BinaryUtils.writeVarInt(data, pos, rowid);
            System.arraycopy(payload, 0, data, pos, localSize);
            if (hasOverflow) {
                pos += localSize;
                int overflowPage = writeOverflowPages(payload, localSize);
                BinaryUtils.writeUInt32(data, pos, overflowPage);
            }
            page.setCellPointer(i, currentOffset);
        }
        page.setCellContentOffset(currentOffset);
        page.setCellCount(keepCount);
    }

    private boolean deleteCell(int pageNum, int cellIndex, long rowid) throws IOException {
        Page page = pager.getPageForWrite(pageNum);
        int cellCount = page.getCellCount();
        if (cellIndex < 0 || cellIndex >= cellCount) return false;

        // Get the cell pointer
        int cellOffset = page.getCellPointer(cellIndex);

        // Check for overflow pages and free them
        long[] payloadSizeResult = BinaryUtils.readVarInt(page.getData(), cellOffset);
        int payloadSize = (int) payloadSizeResult[0];
        long[] rowidResult = BinaryUtils.readVarInt(page.getData(), cellOffset + (int) payloadSizeResult[1]);
        int localSize = localPayloadSize(payloadSize, true);

        if (localSize < payloadSize) {
            int overflowPtrOffset = cellOffset + (int) payloadSizeResult[1] + (int) rowidResult[1] + localSize;
            int overflowPage = (int) BinaryUtils.readUInt32(page.getData(), overflowPtrOffset);
            freeOverflowPages(overflowPage);
        }

        // Add cell space to freeblock chain (simplified: just shift cell pointers)
        for (int i = cellIndex; i < cellCount - 1; i++) {
            page.setCellPointer(i, page.getCellPointer(i + 1));
        }
        page.setCellCount(cellCount - 1);

        return true;
    }

    private int writeOverflowPages(byte[] payload, int localSize) throws IOException {
        int firstOverflowPage = 0;
        int prevPage = 0;
        int offset = localSize;

        while (offset < payload.length) {
            int overflowPageNum = pager.allocatePage();
            if (firstOverflowPage == 0) {
                firstOverflowPage = overflowPageNum;
            }

            Page overflowPage = pager.getPageForWrite(overflowPageNum);
            byte[] data = overflowPage.getData();

            // Write next overflow page pointer (will be updated if more pages needed)
            BinaryUtils.writeUInt32(data, 0, 0);

            // Write payload chunk
            int chunkSize = Math.min(usableSize - 4, payload.length - offset);
            System.arraycopy(payload, offset, data, 4, chunkSize);
            offset += chunkSize;

            // Link previous overflow page to this one
            if (prevPage > 0) {
                Page prev = pager.getPageForWrite(prevPage);
                BinaryUtils.writeUInt32(prev.getData(), 0, overflowPageNum);
            }
            prevPage = overflowPageNum;
        }

        return firstOverflowPage;
    }

    private void freeOverflowPages(int overflowPageNum) throws IOException {
        while (overflowPageNum > 0) {
            Page page = pager.getPage(overflowPageNum);
            int nextPage = (int) BinaryUtils.readUInt32(page.getData(), 0);
            pager.freePage(overflowPageNum);
            overflowPageNum = nextPage;
        }
    }

    private int localPayloadSize(int payloadSize, boolean isTable) {
        int maxLocal = isTable ? maxLocalLeafTable : maxLocalIndex;
        int minLocal = isTable ? minLocalLeafTable : minLocalIndex;

        if (payloadSize <= maxLocal) {
            return payloadSize;
        }

        int k = minLocal + ((payloadSize - minLocal) % (usableSize - 4));
        if (k <= maxLocal) return k;
        return minLocal;
    }

    /**
     * Find the next available rowid for a table.
     */
    public long nextRowid(int rootPage) throws IOException {
        return findMaxRowid(rootPage) + 1;
    }

    private long findMaxRowid(int pageNum) throws IOException {
        Page page = pager.getPage(pageNum);
        if (page.isLeafTablePage()) {
            int cellCount = page.getCellCount();
            if (cellCount == 0) return 0;
            return readRowid(pageNum, cellCount - 1);
        }
        if (page.isInteriorTablePage()) {
            int rightMost = page.getRightMostPointer();
            if (rightMost > 0) return findMaxRowid(rightMost);
        }
        return 0;
    }

    /**
     * Get the rowid for a specific cell in a leaf table page.
     */
    public long getRowid(int pageNum, int cellIndex) throws IOException {
        return readRowid(pageNum, cellIndex);
    }

    /**
     * Get cell count for a page.
     */
    public int getCellCount(int pageNum) throws IOException {
        return pager.getPage(pageNum).getCellCount();
    }

    private static class InsertTarget {
        int pageNumber;
        int cellIndex;

        InsertTarget(int pageNumber, int cellIndex) {
            this.pageNumber = pageNumber;
            this.cellIndex = cellIndex;
        }
    }

    /**
     * Record with its rowid, used for table scans.
     */
    public static class RecordWithRowid {
        public final Record record;
        public final long rowid;

        public RecordWithRowid(Record record, long rowid) {
            this.record = record;
            this.rowid = rowid;
        }
    }
}
