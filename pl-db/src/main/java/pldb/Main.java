package pldb;

import java.util.Scanner;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.exceptions.*;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.serializers.StringSerializer;

public class Main {
  private static Keyspace keyspace;
  private final static ColumnFamily<String, String> CF_LANGS = ColumnFamily.newColumnFamily("langs", StringSerializer.get(),
                                                                                                     StringSerializer.get());
  private final static ColumnFamily<String, String> CF_LANGUSAGE = ColumnFamily.newColumnFamily("langusage", StringSerializer.get(),
                                                                                                             StringSerializer.get());
  private final static ColumnFamily<String, String> CF_PROJECTS = ColumnFamily.newColumnFamily("projects", StringSerializer.get(),
                                                                                                           StringSerializer.get());
  private final static Scanner in = new Scanner(System.in);

  public static void main(String[] args) {
    try {
      AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster("Test Cluster")
        .forKeyspace("pldb")
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
            .setDiscoveryType(NodeDiscoveryType.NONE)
            .setCqlVersion("3.1.1")
            .setTargetCassandraVersion("2.0.4")
            )
        .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
            .setPort(9160)
            .setMaxConnsPerHost(1)
            .setSeeds("localhost:9160")
            )
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

      context.start();

      keyspace = context.getEntity();

      System.out.println("Welcome to Programming Languages Database");

      System.out.print("Do you want to populate the database with additional test data? ");
      String answer = in.next();
      if (answer.toLowerCase().charAt(0) == 'y')
        populateWithTestData();

      String input = "";
      do {
        System.out.println("What would you like to do?");
        System.out.println("1. See projects by language.");
        System.out.println("2. See languages by project.");
        System.out.println("3. See language average rating.");
        System.out.println("4. Nothing. Let me go, please.");

        input = in.next().trim();
        System.out.println();
        String lang = "";
        switch (input) {
          case "1":
            System.out.println("Projects by language");
            System.out.print("Enter a language: ");
            lang = in.next().trim();
            projectsByLanguage(lang);
            break;
          case "2":
            System.out.println("Languages by project");
            System.out.print("Enter a project: ");
            String project = in.next().trim();
            languagesByProject(project);
            break;
          case "3":
            System.out.println("Language average rating");
            System.out.print("Enter a language: ");
            lang = in.next().trim();
            languageAvgRating(lang);
            break;
        }

      } while (!input.equals("4"));

      System.out.println("Bye!");
    }
    catch (Throwable e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    }
  }

  private static void populateWithTestData() throws ConnectionException {
    MutationBatch m = keyspace.prepareMutationBatch();

    // GHC
    m.withRow(CF_PROJECTS, "GHC")
      .putColumn("projectdesc", "Glasgow Haskell Compiler", null)
      .putColumn("Haskell", "", null)
      .putColumn("C", "", null);

    m.withRow(CF_LANGS, "Haskell")
      .putColumn("GHC", "", null);
    m.withRow(CF_LANGS, "C")
      .putColumn("GHC", "", null);

    m.withRow(CF_LANGUSAGE, "Haskell")
      .incrementCounterColumn("usage", 1)
      .incrementCounterColumn("totalrating", 5);
    m.withRow(CF_LANGUSAGE, "C")
      .incrementCounterColumn("usage", 1)
      .incrementCounterColumn("totalrating", 3);

    // Cassandra
    m.withRow(CF_PROJECTS, "Cassandra")
      .putColumn("projectdesc", "Distributed database management system", null)
      .putColumn("Java", "", null);

    m.withRow(CF_LANGS, "Java")
      .putColumn("Cassandra", "", null);

    m.withRow(CF_LANGUSAGE, "Java")
      .incrementCounterColumn("usage", 1)
      .incrementCounterColumn("totalrating", 4);

    // Hadoop
    m.withRow(CF_PROJECTS, "Hadoop")
      .putColumn("projectdesc", "Distributed computing and data storage", null)
      .putColumn("Java", "", null);

    m.withRow(CF_LANGS, "Java")
      .putColumn("Hadoop", "", null);

    m.withRow(CF_LANGUSAGE, "Java")
      .incrementCounterColumn("usage", 1)
      .incrementCounterColumn("totalrating", 4);

    OperationResult<Void> result = m.execute();
  }

  private static void projectsByLanguage(String lang) throws ConnectionException {
    ColumnList<String> result = keyspace.prepareQuery(CF_LANGS)
      .getKey(lang)
      .execute()
      .getResult();
    if (!result.isEmpty()) {
      System.out.println("---------------");
      for (Column<String> projectCol : result) {
        if (!projectCol.getName().equals("langdesc"))
          System.out.println(projectCol.getName());
      }
      System.out.println("---------------");
    }
    else {
      System.out.println(lang + " is not found");
    }
    System.out.println();
  }

  private static void languagesByProject(String project) throws ConnectionException {
    ColumnList<String> result = keyspace.prepareQuery(CF_PROJECTS)
      .getKey(project)
      .execute()
      .getResult();
    if (!result.isEmpty()) {
      System.out.println("---------------");
      for (Column<String> langCol : result) {
        if (!langCol.getName().equals("projectdesc"))
          System.out.println(langCol.getName());
      }
      System.out.println("---------------");
    }
    else {
      System.out.println(project + " is not found");
    }
    System.out.println();
  }

  private static void languageAvgRating(String lang) throws ConnectionException {
    try {
      Column<String> result = keyspace.prepareQuery(CF_LANGUSAGE)
        .getKey(lang)
        .getColumn("usage")
        .execute()
        .getResult();

      Long usageValue = result.getLongValue();

      result = keyspace.prepareQuery(CF_LANGUSAGE)
        .getKey(lang)
        .getColumn("totalrating")
        .execute()
        .getResult();
      Long totalRatingValue = result.getLongValue();

      System.out.println("---------------");
      System.out.println(totalRatingValue / (double) usageValue);
      System.out.println("---------------");
    }
    catch (NotFoundException e) {
      System.out.println(lang + " is not found");
    }
    finally {
      System.out.println();
    }
  }
}

