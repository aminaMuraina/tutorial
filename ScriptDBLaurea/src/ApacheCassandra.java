import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public class ApacheCassandra {

	static Session session;
	static Cluster cluster;
	static final String tableNameImmobili = "immobili";
	static final String tableNameClienti = "clienti";
	static final String tableNameProposteDiVendita = "proposteDiVendita";

	static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

	public static void launch(String keyspaceName) {

		// create cluster and session instance fields to hold the references. A
		// session will manage the connections to our cluster.
		clusterSetUp(keyspaceName);

		// delete keyspace
		long startTime = System.nanoTime();
		deleteKeySpace(keyspaceName);
		long endTime = System.nanoTime();
		System.out.println("Cancellazione keyspace vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// create keyspace
		startTime = System.nanoTime();
		createKeySpace(keyspaceName);
		endTime = System.nanoTime();
		System.out.println("Creazione keyspace vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, "
				+ (endTime - startTime) / 1000000 + " millisecondi");

		// create tables
		startTime = System.nanoTime();
		createTypeIndirizzo(keyspaceName);
		createTypeProposta(keyspaceName);
		createTableImmobili(keyspaceName);
		createTableClienti(keyspaceName);
		endTime = System.nanoTime();
		System.out.println("Creazione types (indirizzo e proposta) e tabelle (immobili e clienti): "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// insert data into tables
		startTime = System.nanoTime();
		insertIntoTableImmobili(keyspaceName);
		insertIntoClienti(keyspaceName);
		endTime = System.nanoTime();
		System.out.println("Inserimento di 100.000 documenti per ogni tabella (immobili e clienti): "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// Read all row in table
		startTime = System.nanoTime();
		readData(keyspaceName, tableNameImmobili);
		endTime = System.nanoTime();
		System.out.println(
				"Lettura dei documenti nella tabella immobili: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		readData(keyspaceName, tableNameClienti);

		// Update row in table
		startTime = System.nanoTime();
		updateARowInClienti(keyspaceName);
		endTime = System.nanoTime();
		System.out.println(
				"Aggiornamento di una riga nella tabella clienti: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Delete row in table
		startTime = System.nanoTime();
		deleteARowInClienti(keyspaceName);
		endTime = System.nanoTime();
		System.out
				.println("Rimozione di una riga nella tabella clienti: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Delete tables
		startTime = System.nanoTime();
		deleteTable(keyspaceName, tableNameImmobili);
		deleteTable(keyspaceName, tableNameClienti);
		endTime = System.nanoTime();
		System.out.println(
				"Cancellazione delle tabelle (immobili e clienti):" + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// delete keyspace
		startTime = System.nanoTime();
		deleteKeySpace(keyspaceName);
		endTime = System.nanoTime();
		System.out.println("Cancellazione keyspace con 200000 righe: " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// close, to shut down the cluster instance
		closeClusterAndSession();
	}

	private static void deleteTable(String keyspaceName, String tableName) {
		session.execute("USE " + keyspaceName);
		session.execute("DROP TABLE " + tableName + ";");
	}

	private static void deleteARowInClienti(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		session.execute("DELETE FROM  " + tableNameClienti + " WHERE codiceFiscale='MRNASEFEFEDKED1' ;");
	}

	private static void updateARowInClienti(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		session.execute("UPDATE " + tableNameClienti + " SET nome= 'amina' WHERE codiceFiscale='MRNASEFEFEDKED1' ;");
	}

	private static void readData(String keyspaceName, String tableName) {
		session.execute("USE " + keyspaceName);
		ResultSet results = session.execute("SELECT * FROM " + tableName + ";");

		for (Row row : results.all()) {
			System.out.println("---" + row);
		}
	}

	private static void createTypeProposta(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		session.execute("CREATE TYPE IF NOT EXISTS proposta(" + "			    data timestamp,"
				+ "			    prezzo float," + "			    ggValidita int," + "			    id_Immobile uuid"
				+ "			);");
	}

	private static void createTypeIndirizzo(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		session.execute("CREATE TYPE IF NOT EXISTS indirizzo(" + "			    via text," + "			    civico int,"
				+ "			    cap int" + "			);");
	}

	private static void insertIntoTableImmobili(String keyspaceName) {
		for (int i = 0; i < 100000; i++) {

		String date = dateformat.format(new Date());

		UserType addressType = cluster.getMetadata().getKeyspace(keyspaceName).getUserType("indirizzo");
		UDTValue indirzzo = addressType.newValue().setString("via", " Ave NW").setInt("civico", 33).setInt("cap", 2500);

		// Insert one record into the table
		session.execute("USE " + keyspaceName);
		session.execute(
				"INSERT INTO " + tableNameImmobili + " (id_immobile, metratura, prezzo, dataDiVendita,indirizzo) VALUES"
						+ "( " + UUID.randomUUID() + "," + 100 + ", " + 34 + ", '" + date + "', " + indirzzo + ")");
		}
	}

	private static void insertIntoClienti(String keyspaceName) {
		for (int i = 0; i < 100000; i++) {

		String date = dateformat.format(new Date());

		UserType addressType = cluster.getMetadata().getKeyspace(keyspaceName).getUserType("indirizzo");
		UDTValue indirzzo = addressType.newValue().setString("via", "ennsylvania Ave NW").setInt("civico", 34)
				.setInt("cap", 20500);

		UserType propostaType = cluster.getMetadata().getKeyspace(keyspaceName).getUserType("proposta");
		UDTValue proposta = null;

		try {
			proposta = propostaType.newValue().setTimestamp("data", dateformat.parse(dateformat.format(new Date())))
					.setFloat("prezzo", 34).setUUID("id_Immobile", UUID.randomUUID()).setInt("ggValidita", 15);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<UDTValue> listaProposte = new ArrayList<UDTValue>();
		listaProposte.add(proposta);
		listaProposte.add(proposta);

		// Insert one record into the table
		session.execute("USE " + keyspaceName);

		session.execute("INSERT INTO " + tableNameClienti
				+ " (codiceFiscale, cognome, nome, datadinascita, indirizzoDiResidenza,proposteDiVendita) VALUES"
				+ "('MRNASEFEFEDKED"+i+"','Jones', 'Bob', '" + date + "', " + indirzzo + "," + listaProposte + ")");

		}
	}

	private static void createTableClienti(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		String query = "CREATE COLUMNFAMILY IF NOT EXISTS " + tableNameClienti + "("
				+ "codiceFiscale text PRIMARY KEY, " + "nome text, " + "cognome text, "
				+ "indirizzoDiResidenza indirizzo, " + "dataDiNascita date,"
				+ "proposteDiVendita list<frozen<proposta>>" + ");";
		session.execute(query);
	}

	private static void createTableImmobili(String keyspaceName) {
		session.execute("USE " + keyspaceName);
		String query = "CREATE COLUMNFAMILY IF NOT EXISTS " + tableNameImmobili + "(" + "id_immobile uuid PRIMARY KEY, "
				+ "indirizzo indirizzo, " + "prezzo float, " + "metratura bigint, " + "dataDiVendita date" + ");";
		session.execute(query);
	}

	private static void createKeySpace(String keyspaceName) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
				+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
	}

	private static void deleteKeySpace(String keyspaceName) {
		session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName + ";");
	}

	private static Cluster clusterSetUp(String keyspaceName) {

		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		session = cluster.connect();

		return cluster;
	}

	private static void closeClusterAndSession() {
		session.close();
		cluster.close();
	}
}
