package benchmark;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.VmOptions;
import com.google.caliper.runner.CaliperMain;
import org.apache.commons.lang3.RandomStringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.util.Random;

public class DBBenchmark {
    @VmOptions("-XX:-TieredCompilation")
    public static class insertBenchmark {
        @Param({"10", "100", "1000", "10000"}) private int batchSize;

        private int[] id;
        private int[] randomValue;
        private String[] randomString;
        private static int length = 100000;
        private static int stringLength = 20;

        static String url = "jdbc:postgresql://localhost/postgres";
        static String username = "tester";
        static String password = "test_password";

        Connection conn = null;

        @BeforeExperiment void setUp() throws SQLException {
            Random rand = new Random();
            id = new int[length];
            randomValue = new int[length];
            randomString = new String[length];
            for (int i = 0; i < length; i++) {
                id[i] = i;
                randomValue[i] = rand.nextInt();
                randomString[i] = RandomStringUtils.randomAlphabetic(stringLength);
            }
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS single_inserts (id int, num int, str varchar(20));");
            stmt.execute("CREATE TABLE IF NOT EXISTS batch_inserts (id int, num int, str varchar(20));");
            stmt.execute("CREATE TABLE IF NOT EXISTS bulk_load (id int, num int, str varchar(20));");
            stmt.execute("DELETE FROM single_inserts;");
            stmt.execute("DELETE FROM batch_inserts;");
            stmt.execute("DELETE FROM bulk_load;");
            conn.commit();
        }

        @Benchmark void singleInserts(int reps) throws SQLException {
            for (int i = 0; i < reps; i++) {
                PreparedStatement insert = conn.prepareStatement("INSERT INTO single_inserts (id, num, str) VALUES (?, ?, ?);");
                for (int j = 0; j < length; j++) {
                    insert.setInt(1, id[j]);
                    insert.setInt(2, randomValue[j]);
                    insert.setString(3, randomString[j]);
                    insert.execute();
                }
                conn.commit();
                conn.createStatement().execute("DELETE FROM single_inserts;");
                conn.commit();
            }
        }

        @Benchmark void batchInserts(int reps) throws SQLException {
            for (int i = 0; i < reps; i++) {
                PreparedStatement insert = conn.prepareStatement("INSERT INTO batch_inserts (id, num, str) VALUES (?, ?, ?);");
                for (int j = 0; j < length; j++) {
                    insert.setInt(1, id[j]);
                    insert.setInt(2, randomValue[j]);
                    insert.setString(3, randomString[j]);
                    insert.addBatch();
                    if ((j + 1) % batchSize == 0) {
                        insert.executeBatch();
                    }
                }
                conn.commit();
                conn.createStatement().execute("DELETE FROM batch_inserts;");
                conn.commit();
            }
        }

        @Benchmark void copy(int reps) throws SQLException, IOException {
            for (int i = 0; i < reps; i++) {
                StringBuilder sb = new StringBuilder();
                StringReader reader;
                CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
                for (int j = 0; j < length; j++) {
                    sb.append(id[j]).append(",").append(randomValue[j]).append(",").append(randomString[j]).append("\n");
                    if ((j + 1) % batchSize == 0) {
                        reader = new StringReader(sb.toString());
                        cpManager.copyIn("COPY bulk_load FROM STDIN WITH CSV", reader);
                        sb.delete(0, sb.length());
                    }
                }
                conn.commit();
                conn.createStatement().execute("DELETE FROM bulk_load;");
                conn.commit();
            }
        }

        @AfterExperiment void tearDown() throws SQLException {
            Statement stmt = conn.createStatement();
            stmt.execute("DELETE FROM single_inserts;");
            stmt.execute("DELETE FROM batch_inserts;");
            stmt.execute("DELETE FROM bulk_load;");
            conn.commit();
            conn.close();
        }
    }

    public static void main(String[] args) {
        CaliperMain.main(insertBenchmark.class, new String[] {"-i", "runtime"});
    }
}
