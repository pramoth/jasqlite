package com.jasqlite.store.page;

/**
 * Represents a single database page.
 * Pages are the fundamental unit of I/O in SQLite.
 * Page numbers are 1-based (page 1 contains the 100-byte file header).
 */
public class Page {

    private final int pageNumber;
    private final byte[] data;
    private final int pageSize;

    public Page(int pageNumber, byte[] data, int pageSize) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageSize = pageSize;
        if (data.length != pageSize) {
            throw new IllegalArgumentException("Page data length must equal page size: "
                    + data.length + " != " + pageSize);
        }
    }

    public int getPageNumber() { return pageNumber; }
    public byte[] getData() { return data; }
    public int getPageSize() { return pageSize; }

    /**
     * Get the page type byte.
     * For page 1, the B-tree header starts at offset 100.
     */
    public int getPageType() {
        int offset = (pageNumber == 1) ? 100 : 0;
        return data[offset] & 0xFF;
    }

    /**
     * B-tree page types:
     * 2  - Interior index b-tree page
     * 5  - Interior table b-tree page
     * 10 - Leaf index b-tree page
     * 13 - Leaf table b-tree page
     */
    public boolean isLeafTablePage() { return getPageType() == 13; }
    public boolean isInteriorTablePage() { return getPageType() == 5; }
    public boolean isLeafIndexPage() { return getPageType() == 10; }
    public boolean isInteriorIndexPage() { return getPageType() == 2; }

    /**
     * Get the header offset for this page (100 for page 1, 0 otherwise).
     */
    public int headerOffset() {
        return (pageNumber == 1) ? 100 : 0;
    }

    /**
     * Get the cell pointer offset area start.
     * For leaf pages: headerOffset + 8
     * For interior pages: headerOffset + 12
     */
    public int cellPointerOffset() {
        int hdr = headerOffset();
        if (isInteriorTablePage() || isInteriorIndexPage()) {
            return hdr + 12;
        }
        return hdr + 8;
    }

    /**
     * Get number of cells on this page.
     */
    public int getCellCount() {
        int hdr = headerOffset();
        return ((data[hdr + 3] & 0xFF) << 8) | (data[hdr + 4] & 0xFF);
    }

    /**
     * Set number of cells on this page.
     */
    public void setCellCount(int count) {
        int hdr = headerOffset();
        data[hdr + 3] = (byte) ((count >> 8) & 0xFF);
        data[hdr + 4] = (byte) (count & 0xFF);
    }

    /**
     * Get the right-most pointer (interior pages only).
     */
    public int getRightMostPointer() {
        if (!isInteriorTablePage() && !isInteriorIndexPage()) {
            return 0;
        }
        int hdr = headerOffset();
        return (int) (((long)(data[hdr + 8] & 0xFF) << 24) |
                      ((data[hdr + 9] & 0xFF) << 16) |
                      ((data[hdr + 10] & 0xFF) << 8) |
                      (data[hdr + 11] & 0xFF));
    }

    /**
     * Set the right-most pointer (interior pages only).
     */
    public void setRightMostPointer(int ptr) {
        if (isInteriorTablePage() || isInteriorIndexPage()) {
            int hdr = headerOffset();
            data[hdr + 8] = (byte) ((ptr >> 24) & 0xFF);
            data[hdr + 9] = (byte) ((ptr >> 16) & 0xFF);
            data[hdr + 10] = (byte) ((ptr >> 8) & 0xFF);
            data[hdr + 11] = (byte) (ptr & 0xFF);
        }
    }

    /**
     * Get cell pointer for the given cell index.
     */
    public int getCellPointer(int cellIndex) {
        int offset = cellPointerOffset() + cellIndex * 2;
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    /**
     * Set cell pointer for the given cell index.
     */
    public void setCellPointer(int cellIndex, int pointer) {
        int offset = cellPointerOffset() + cellIndex * 2;
        data[offset] = (byte) ((pointer >> 8) & 0xFF);
        data[offset + 1] = (byte) (pointer & 0xFF);
    }

    /**
     * Get the first freeblock offset.
     */
    public int getFirstFreeBlock() {
        int hdr = headerOffset();
        return ((data[hdr + 1] & 0xFF) << 8) | (data[hdr + 2] & 0xFF);
    }

    /**
     * Set the first freeblock offset.
     */
    public void setFirstFreeBlock(int offset) {
        int hdr = headerOffset();
        data[hdr + 1] = (byte) ((offset >> 8) & 0xFF);
        data[hdr + 2] = (byte) (offset & 0xFF);
    }

    /**
     * Get cell content area start offset.
     */
    public int getCellContentOffset() {
        int hdr = headerOffset();
        return ((data[hdr + 5] & 0xFF) << 8) | (data[hdr + 6] & 0xFF);
    }

    /**
     * Set cell content area start offset.
     */
    public void setCellContentOffset(int offset) {
        int hdr = headerOffset();
        data[hdr + 5] = (byte) ((offset >> 8) & 0xFF);
        data[hdr + 6] = (byte) (offset & 0xFF);
    }

    /**
     * Get fragmented free bytes count.
     */
    public int getFragmentedFreeBytes() {
        int hdr = headerOffset();
        return data[hdr + 7] & 0xFF;
    }

    /**
     * Set fragmented free bytes count.
     */
    public void setFragmentedFreeBytes(int count) {
        int hdr = headerOffset();
        data[hdr + 7] = (byte) (count & 0xFF);
    }

    /**
     * Initialize a new b-tree page.
     */
    public void initAsLeafTable() {
        int hdr = headerOffset();
        data[hdr] = 13; // leaf table b-tree
        setFirstFreeBlock(0);
        setCellCount(0);
        setCellContentOffset(pageSize); // cells start at end of page
        setFragmentedFreeBytes(0);
    }

    public void initAsInteriorTable() {
        int hdr = headerOffset();
        data[hdr] = 5; // interior table b-tree
        setFirstFreeBlock(0);
        setCellCount(0);
        setRightMostPointer(0);
        setCellContentOffset(pageSize);
        setFragmentedFreeBytes(0);
    }

    public void initAsLeafIndex() {
        int hdr = headerOffset();
        data[hdr] = 10; // leaf index b-tree
        setFirstFreeBlock(0);
        setCellCount(0);
        setCellContentOffset(pageSize);
        setFragmentedFreeBytes(0);
    }

    public void initAsInteriorIndex() {
        int hdr = headerOffset();
        data[hdr] = 2; // interior index b-tree
        setFirstFreeBlock(0);
        setCellCount(0);
        setRightMostPointer(0);
        setCellContentOffset(pageSize);
        setFragmentedFreeBytes(0);
    }
}
