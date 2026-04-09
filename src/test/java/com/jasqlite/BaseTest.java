package com.jasqlite;

import com.jasqlite.store.Database;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

abstract class BaseTest {

    static Path tempDir;
    Database db;

    @BeforeAll
    static void setupAll() throws Exception {
        tempDir = Files.createTempDirectory("jasqlite-test");
        Class.forName("com.jasqlite.jdbc.JaSQLiteDriver");
    }

    @BeforeEach
    void openDb() throws Exception {
        String path = tempDir.resolve("test-" + getClass().getSimpleName() + ".db").toString();
        new File(path).delete();
        db = new Database(path);
    }

    @AfterEach
    void closeDb() throws Exception {
        if (db != null) db.close();
    }
}
