package com.jasqlite.store.page;

import com.jasqlite.JaSQLite;
import com.jasqlite.util.BinaryUtils;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages database pages - reads/writes pages from the database file.
 * Implements a page cache with LRU eviction.
 * This is the core I/O layer that directly manipulates the SQLite file format.
 *
 * SQLite file format (100-byte header):
 * Offset  Size  Description
 * 0       16    Header string: "SQLite format 3\000"
 * 16      2     Page size (1 means 65536)
 * 18      1     File format write version (1=legacy, 2=WAL)
 * 19      1     File format read version (1=legacy, 2=WAL)
 * 20      1     Reserved space at end of each page
 * 21      1     Maximum embedded payload fraction (must be 64)
 * 22      1     Minimum embedded payload fraction (must be 32)
 * 23      1     Leaf payload fraction (must be 32)
 * 24      4     File change counter
 * 28      4     Size of database file in pages
 * 32      4     Page number of first freelist trunk page
 * 36      4     Total number of freelist pages
 * 40      4     Schema cookie
 * 44      4     Schema format number
 * 48      4     Default page cache size
 * 52      4     Auto-vacuum (largest root b-tree page number)
 * 56      4     Database text encoding (1=UTF-8, 2=UTF-16le, 3=UTF-16be)
 * 60      4     User version
 * 64      4     Incremental vacuum mode
 * 68      4     Application ID
 * 72      20    Reserved for expansion (must be zero)
 * 92      4     Version-valid-for number
 * 96      4     SQLite version number
 */
public class Pager implements AutoCloseable {

    private final String filePath;
    private RandomAccessFile file;
    private final int pageSize;
    private final int usableSize; // pageSize - reservedSpace
    private final int reservedSpace;
    private int totalPages;
    private boolean readOnly;
    private boolean noSync;

    // Page cache
    private final Map<Integer, Page> cache;
    private final int cacheSize;
    private final LinkedList<Integer> lruList;

    // Dirty pages for transaction
    private final Set<Integer> dirtyPages;

    // Journal for rollback
    private RandomAccessFile journalFile;
    private boolean journalActive;

    // Database header fields
    private int fileChangeCounter;
    private int schemaCookie;
    private int schemaFormatNumber;
    private int defaultCacheSize;
    private int textEncoding; // 1=UTF-8, 2=UTF-16le, 3=UTF-16be
    private int userVersion;
    private int applicationId;
    private int freeListTrunkPage;
    private int freeListPageCount;
    private int autoVacuumTopPage;
    private int incrementalVacuum;

    // WAL support
    private WALFile walFile;
    private boolean walMode;

    // Lock
    private final Object lock = new Object();

    public Pager(String filePath, boolean readOnly) throws IOException {
        this.filePath = filePath;
        this.readOnly = readOnly;
        this.cacheSize = JaSQLite.DEFAULT_CACHE_SIZE;
        this.cache = new ConcurrentHashMap<>();
        this.lruList = new LinkedList<>();
        this.dirtyPages = new LinkedHashSet<>();

        File f = new File(filePath);
        boolean isNew = !f.exists() || f.length() == 0;

        if (isNew && !readOnly) {
            this.pageSize = JaSQLite.DEFAULT_PAGE_SIZE;
            this.reservedSpace = 0;
            this.usableSize = pageSize - reservedSpace;
            this.totalPages = 1;
            createNewDatabase();
        } else {
            // Read existing database
            String mode = readOnly ? "r" : "rw";
            this.file = new RandomAccessFile(filePath, mode);

            // Read header to get page size
            byte[] header = new byte[100];
            file.readFully(header);
            String headerStr = new String(header, 0, 16);
            if (!JaSQLite.SQLITE_HEADER_STRING.equals(headerStr)) {
                file.close();
                throw new IOException("Not a valid SQLite database file (or file is encrypted)");
            }

            int ps = BinaryUtils.readUInt16(header, 16);
            this.pageSize = (ps == 1) ? 65536 : ps;
            this.reservedSpace = header[20] & 0xFF;

            if (this.pageSize < JaSQLite.MIN_PAGE_SIZE || this.pageSize > JaSQLite.MAX_PAGE_SIZE) {
                throw new IOException("Invalid page size: " + this.pageSize);
            }

            this.usableSize = pageSize - reservedSpace;
            long fileSize = file.length();
            this.totalPages = (int) (fileSize / pageSize);
            if (fileSize % pageSize != 0) {
                throw new IOException("Database file size is not a multiple of page size");
            }

            // Read header fields
            this.fileChangeCounter = (int) BinaryUtils.readUInt32(header, 24);
            this.schemaCookie = (int) BinaryUtils.readUInt32(header, 40);
            this.schemaFormatNumber = (int) BinaryUtils.readUInt32(header, 44);
            this.defaultCacheSize = (int) BinaryUtils.readUInt32(header, 48);
            this.autoVacuumTopPage = (int) BinaryUtils.readUInt32(header, 52);
            this.textEncoding = (int) BinaryUtils.readUInt32(header, 56);
            this.userVersion = (int) BinaryUtils.readUInt32(header, 60);
            this.incrementalVacuum = (int) BinaryUtils.readUInt32(header, 64);
            this.applicationId = (int) BinaryUtils.readUInt32(header, 68);
            this.freeListTrunkPage = (int) BinaryUtils.readUInt32(header, 32);
            this.freeListPageCount = (int) BinaryUtils.readUInt32(header, 36);

            int writeVersion = header[18] & 0xFF;
            this.walMode = (writeVersion == 2);

            if (this.textEncoding == 0) {
                this.textEncoding = 1; // Default UTF-8
            }
        }
    }

    private void createNewDatabase() throws IOException {
        this.file = new RandomAccessFile(filePath, "rw");
        this.textEncoding = 1;
        this.fileChangeCounter = 1;
        this.schemaCookie = 0;
        this.schemaFormatNumber = 4;
        this.defaultCacheSize = JaSQLite.DEFAULT_CACHE_SIZE;
        this.walMode = false;

        // Write page 1 (contains the 100-byte header + btree page header for sqlite_master)
        byte[] page1 = new byte[pageSize];
        writeHeader(page1);
        // Page 1 is a b-tree leaf page for sqlite_master
        int hdrOffset = 100; // after the 100-byte database header
        page1[hdrOffset] = 13; // leaf table b-tree page
        BinaryUtils.writeUInt16(page1, hdrOffset + 1, 0); // first freeblock offset = 0
        BinaryUtils.writeUInt16(page1, hdrOffset + 3, 0); // number of cells = 0
        BinaryUtils.writeUInt16(page1, hdrOffset + 5, usableSize); // start of cell content area
        page1[hdrOffset + 7] = 0; // fragmented free bytes

        file.write(page1);
    }

    private void writeHeader(byte[] data) {
        // Header string
        byte[] headerStr = JaSQLite.SQLITE_HEADER_STRING.getBytes();
        System.arraycopy(headerStr, 0, data, 0, Math.min(headerStr.length, 16));

        BinaryUtils.writeUInt16(data, 16, pageSize == 65536 ? 1 : pageSize);
        data[18] = (byte) (walMode ? 2 : 1); // file format write version
        data[19] = (byte) (walMode ? 2 : 1); // file format read version
        data[20] = (byte) reservedSpace;
        data[21] = 64;  // max embedded payload fraction
        data[22] = 32;  // min embedded payload fraction
        data[23] = 32;  // leaf payload fraction
        BinaryUtils.writeUInt32(data, 24, fileChangeCounter);
        BinaryUtils.writeUInt32(data, 28, totalPages);
        BinaryUtils.writeUInt32(data, 32, freeListTrunkPage);
        BinaryUtils.writeUInt32(data, 36, freeListPageCount);
        BinaryUtils.writeUInt32(data, 40, schemaCookie);
        BinaryUtils.writeUInt32(data, 44, schemaFormatNumber);
        BinaryUtils.writeUInt32(data, 48, defaultCacheSize);
        BinaryUtils.writeUInt32(data, 52, autoVacuumTopPage);
        BinaryUtils.writeUInt32(data, 56, textEncoding);
        BinaryUtils.writeUInt32(data, 60, userVersion);
        BinaryUtils.writeUInt32(data, 64, incrementalVacuum);
        BinaryUtils.writeUInt32(data, 68, applicationId);
        // 72-91 reserved (zeros)
        BinaryUtils.writeUInt32(data, 92, fileChangeCounter);
        BinaryUtils.writeUInt32(data, 96, JaSQLite.SQLITE_VERSION_NUMBER);
    }

    /**
     * Read a page from the database file.
     * Page numbers are 1-based.
     */
    public Page getPage(int pageNumber) throws IOException {
        if (pageNumber < 1 || pageNumber > totalPages) {
            throw new IOException("Page number out of range: " + pageNumber + " (total: " + totalPages + ")");
        }
        synchronized (lock) {
            Page page = cache.get(pageNumber);
            if (page != null) {
                touchPage(pageNumber);
                return page;
            }

            // Check WAL first
            if (walFile != null) {
                byte[] walData = walFile.readPage(pageNumber);
                if (walData != null) {
                    page = new Page(pageNumber, walData, pageSize);
                    putCache(pageNumber, page);
                    return page;
                }
            }

            // Read from main file
            byte[] data = new byte[pageSize];
            file.seek((long) (pageNumber - 1) * pageSize);
            file.readFully(data);
            page = new Page(pageNumber, data, pageSize);
            putCache(pageNumber, page);
            return page;
        }
    }

    /**
     * Get a page for writing. Marks the page as dirty.
     */
    public Page getPageForWrite(int pageNumber) throws IOException {
        if (readOnly) {
            throw new IOException("Database is open in read-only mode");
        }
        Page page = getPage(pageNumber);
        dirtyPages.add(pageNumber);
        return page;
    }

    /**
     * Allocate a new page.
     */
    public int allocatePage() throws IOException {
        synchronized (lock) {
            // Try free list first
            if (freeListTrunkPage > 0) {
                int freed = allocateFromFreeList();
                if (freed > 0) return freed;
            }

            // Extend the file
            totalPages++;
            byte[] data = new byte[pageSize];
            Page page = new Page(totalPages, data, pageSize);
            dirtyPages.add(totalPages);
            putCache(totalPages, page);

            // Write the new page to file
            file.setLength((long) totalPages * pageSize);

            return totalPages;
        }
    }

    private int allocateFromFreeList() throws IOException {
        if (freeListTrunkPage == 0) return 0;

        Page trunk = getPage(freeListTrunkPage);
        int leafCount = (int) BinaryUtils.readUInt32(trunk.getData(), 8);

        if (leafCount > 0) {
            // Take a leaf from the trunk
            int leafOffset = 8 + 4 * leafCount;
            int leafPage = (int) BinaryUtils.readUInt32(trunk.getData(), leafOffset);
            trunk = getPageForWrite(freeListTrunkPage);
            BinaryUtils.writeUInt32(trunk.getData(), 8, leafCount - 1);
            freeListPageCount--;
            return leafPage;
        }

        // Use the trunk page itself
        int nextPage = (int) BinaryUtils.readUInt32(trunk.getData(), 4);
        freeListTrunkPage = nextPage;
        freeListPageCount--;
        return (int) BinaryUtils.readUInt32(trunk.getData(), 0); // return trunk as allocated
    }

    /**
     * Free a page by adding it to the free list.
     */
    public void freePage(int pageNumber) throws IOException {
        synchronized (lock) {
            if (freeListTrunkPage == 0) {
                // No trunk page yet, make this page the trunk
                Page trunk = getPageForWrite(pageNumber);
                BinaryUtils.writeUInt32(trunk.getData(), 0, 0); // next trunk = 0
                BinaryUtils.writeUInt32(trunk.getData(), 4, 0); // zero means this IS the trunk (next)
                BinaryUtils.writeUInt32(trunk.getData(), 8, 0); // leaf count = 0
                freeListTrunkPage = pageNumber;
            } else {
                // Add to existing trunk
                Page trunk = getPageForWrite(freeListTrunkPage);
                int leafCount = (int) BinaryUtils.readUInt32(trunk.getData(), 8);
                int maxLeaves = (usableSize - 8) / 4;

                if (leafCount < maxLeaves) {
                    // Add as leaf
                    BinaryUtils.writeUInt32(trunk.getData(), 8 + 4 * (leafCount + 1), pageNumber);
                    BinaryUtils.writeUInt32(trunk.getData(), 8, leafCount + 1);
                } else {
                    // Trunk is full, make freed page new trunk
                    Page newTrunk = getPageForWrite(pageNumber);
                    BinaryUtils.writeUInt32(newTrunk.getData(), 0, 0); // unused
                    BinaryUtils.writeUInt32(newTrunk.getData(), 4, freeListTrunkPage); // next trunk
                    BinaryUtils.writeUInt32(newTrunk.getData(), 8, 0); // leaf count = 0
                    freeListTrunkPage = pageNumber;
                }
            }
            freeListPageCount++;
            dirtyPages.add(pageNumber);
        }
    }

    private void putCache(int pageNumber, Page page) {
        if (cache.size() >= cacheSize) {
            evictPage();
        }
        cache.put(pageNumber, page);
        lruList.addFirst(pageNumber);
    }

    private void touchPage(int pageNumber) {
        lruList.remove(Integer.valueOf(pageNumber));
        lruList.addFirst(pageNumber);
    }

    private void evictPage() {
        if (lruList.isEmpty()) return;
        int victim = lruList.removeLast();
        Page page = cache.remove(victim);
        if (page != null && dirtyPages.contains(victim)) {
            try {
                writePageToDisk(page);
                dirtyPages.remove(victim);
            } catch (IOException e) {
                // Put it back
                cache.put(victim, page);
            }
        }
    }

    private void writePageToDisk(Page page) throws IOException {
        if (walFile != null && walMode) {
            walFile.writePage(page.getPageNumber(), page.getData());
        } else {
            file.seek((long) (page.getPageNumber() - 1) * pageSize);
            file.write(page.getData());
        }
    }

    /**
     * Begin a write transaction.
     */
    public void beginTransaction() throws IOException {
        if (journalActive) return;

        if (!walMode) {
            // Create rollback journal
            String journalPath = filePath + "-journal";
            journalFile = new RandomAccessFile(journalPath, "rw");
            journalFile.setLength(0);

            // Write journal header
            byte[] journalHeader = new byte[28];
            System.arraycopy("!JaSQL".getBytes(), 0, journalHeader, 0, 6);
            journalHeader[7] = 0; // no checksums for now
            BinaryUtils.writeUInt32(journalHeader, 8, pageSize);
            BinaryUtils.writeUInt32(journalHeader, 12, totalPages);
            journalFile.write(journalHeader);
            journalActive = true;
        }
    }

    /**
     * Commit the transaction.
     */
    public void commit() throws IOException {
        synchronized (lock) {
            if (walMode && walFile != null) {
                walFile.commit();
            }

            // Write dirty pages
            for (int pageNum : dirtyPages) {
                Page page = cache.get(pageNum);
                if (page != null) {
                    // Update page 1 header
                    if (pageNum == 1) {
                        writeHeader(page.getData());
                    }
                    writePageToDisk(page);
                }
            }
            dirtyPages.clear();

            if (!walMode && journalActive) {
                // Close and delete journal
                journalFile.close();
                new File(filePath + "-journal").delete();
                journalActive = false;
            }

            fileChangeCounter++;
            if (file != null) {
                file.getFD().sync();
            }
        }
    }

    /**
     * Rollback the transaction.
     */
    public void rollback() throws IOException {
        synchronized (lock) {
            if (!walMode && journalActive && journalFile != null) {
                // Read journal and restore pages
                journalFile.seek(28);
                while (journalFile.getFilePointer() < journalFile.length() - 4) {
                    int pageNum = journalFile.readInt();
                    int nPages = journalFile.readInt(); // always 1 for page data
                    byte[] pageData = new byte[pageSize];
                    journalFile.readFully(pageData);

                    // Restore page
                    Page page = new Page(pageNum, pageData, pageSize);
                    cache.put(pageNum, page);
                    file.seek((long) (pageNum - 1) * pageSize);
                    file.write(pageData);
                }
                journalFile.close();
                new File(filePath + "-journal").delete();
                journalActive = false;
            }

            // Discard dirty pages from cache
            for (int pageNum : dirtyPages) {
                cache.remove(pageNum);
            }
            dirtyPages.clear();
        }
    }

    /**
     * Save a page to the journal before modification (for rollback support).
     */
    public void saveToJournal(int pageNumber) throws IOException {
        if (!journalActive || walMode) return;
        synchronized (lock) {
            Page page = cache.get(pageNumber);
            if (page == null) {
                // Read from file
                byte[] data = new byte[pageSize];
                file.seek((long) (pageNumber - 1) * pageSize);
                file.readFully(data);
                page = new Page(pageNumber, data, pageSize);
            }
            journalFile.writeInt(pageNumber);
            journalFile.writeInt(1); // 1 page follows
            journalFile.write(page.getData());
        }
    }

    // Getters and setters for header fields
    public int getPageSize() { return pageSize; }
    public int getUsableSize() { return usableSize; }
    public int getTotalPages() { return totalPages; }
    public int getFileChangeCounter() { return fileChangeCounter; }
    public int getSchemaCookie() { return schemaCookie; }
    public void setSchemaCookie(int cookie) { this.schemaCookie = cookie; }
    public int getSchemaFormatNumber() { return schemaFormatNumber; }
    public int getTextEncoding() { return textEncoding; }
    public int getUserVersion() { return userVersion; }
    public void setUserVersion(int version) { this.userVersion = version; }
    public int getApplicationId() { return applicationId; }
    public void setApplicationId(int id) { this.applicationId = id; }
    public int getFreeListTrunkPage() { return freeListTrunkPage; }
    public int getFreeListPageCount() { return freeListPageCount; }
    public boolean isWalMode() { return walMode; }
    public void setTotalPages(int pages) { this.totalPages = pages; }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            // Flush dirty pages
            for (int pageNum : dirtyPages) {
                Page page = cache.get(pageNum);
                if (page != null) {
                    if (pageNum == 1) writeHeader(page.getData());
                    writePageToDisk(page);
                }
            }
            dirtyPages.clear();
            cache.clear();
            lruList.clear();

            if (journalFile != null) {
                journalFile.close();
            }
            if (walFile != null) {
                walFile.close();
            }
            if (file != null) {
                file.close();
            }
        }
    }
}
