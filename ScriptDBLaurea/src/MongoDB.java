import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static java.util.Arrays.asList;

public class MongoDB {

	static final String collectionImmobiliName = "Immobili";
	static final String collectionClientiName = "Clienti";
	static MongoDatabase db;
	static MongoClient mongoClient;

	public static void launch(String databaseName) {

		// Connessione al server di mongodb
		mongoClient = connessioneAlServer();

		// CRUD Operations

		// Delete database
		long startTime = System.nanoTime();
		dropDatabase(databaseName);
		long endTime = System.nanoTime();
		System.out.println("Cancellazione database vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");
		
		// Create Database
		startTime = System.nanoTime();
		db = createDatabase(databaseName);
		endTime = System.nanoTime();
		System.out.println("Creazione database vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, "
				+ (endTime - startTime) / 1000000 + " millisecondi");

		// Create Collections
		startTime = System.nanoTime();
		createCollection(collectionImmobiliName);
		createCollection(collectionClientiName);
		endTime = System.nanoTime();
		System.out.println("Creazione collezioni(immobili e clienti): " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Insert Documents in collection
		startTime = System.nanoTime();
		insertDocumentInImmobili();
		insertDocumentInClienti();
		endTime = System.nanoTime();
		System.out.println("Inserimento di 100.000 documenti per ogni collezione (immobili e clienti): "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// Read all row in collection
		startTime = System.nanoTime();
		readAllDocuments(db.getCollection(collectionImmobiliName));
		endTime = System.nanoTime();
		System.out.println(
				"Lettura dei documenti nella collezione immobili: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Upate document in collection
		startTime = System.nanoTime();
		updateFirstDocumentInClienti();
		endTime = System.nanoTime();
		System.out.println("Aggiornamento di un documento nella collezione clienti: "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// Remove document in collection
		startTime = System.nanoTime();
		removeFirstDocumentInClienti();
		endTime = System.nanoTime();
		System.out.println("Rimozione di un documento nella collezione clienti: "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// Drop all collections
		startTime = System.nanoTime();
		dropCollection(collectionImmobiliName);
		dropCollection(collectionClientiName);
		endTime = System.nanoTime();
		System.out.println("Cancellazione delle collezioni (immobili e clienti):"
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		startTime = System.nanoTime();
		dropDatabase(databaseName);
		endTime = System.nanoTime();
		System.out
				.println("Cancellazione database con 200000 documenti: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Chiusura connessione
		terminaConnessioneConIlServer();
	}

	private static void removeFirstDocumentInClienti() {
		db.getCollection(collectionClientiName)
				.findOneAndDelete(db.getCollection(collectionClientiName).find().first());
	}

	private static void updateFirstDocumentInClienti() {
		db.getCollection(collectionClientiName).findOneAndUpdate(db.getCollection(collectionClientiName).find().first(),
				new Document("$set", new Document("nome", "Aminatu (New)")));
	}

	private static void readAllDocuments(MongoCollection<Document> col) {
		try (MongoCursor<Document> cursor = col.find().iterator()) {
			while (cursor.hasNext()) {
				System.out.println("-" + cursor.next());
			}
		}
	}

	private static void insertDocumentInImmobili() {
		for (int i = 0; i < 100000; i++) {
			db.getCollection(collectionImmobiliName)
					.insertOne(new Document(new Document().append("_id", "wwwww" + i)
							.append("prezzo", "2.45").append("metratura", 100).append("dataVendita", "26/08/1994")
							.append("indirizzo", new Document().append("cap", "30112").append("civico", 150)
									.append("via", "emilia ponete"))));
		}
	}

	private static void insertDocumentInClienti() {
		for (int i = 0; i < 100000; i++) {

			db.getCollection(collectionClientiName)
					.insertOne(
							new Document(new Document().append("_id", "SFSSKEISDNSFS" + i).append("cognome", "Muraina")
									.append("nome", "Aminatu").append("dataNascita", "26/08/1994")
									.append("indirizzoResidenza",
											new Document().append("cap", "30112")
													.append("civico",
															150)
													.append("via", "emilia ponete"))
									.append("proposteDiAcquisto",
											asList(new Document().append("data", "01/02/1993").append("prezzo", 50)
													.append("ggValidita", 4).append("idImmobile", 1311221),
													new Document().append("data", "11/82/1983").append("prezzo", 150)
															.append("ggValidita", 9).append("idImmobile", 34568)))));
		}
	}

	private static void createCollection(String collectionName) {
		db.createCollection(collectionName);
	}

	private static MongoDatabase createDatabase(String databaseName) {
		return mongoClient.getDatabase(databaseName);
	}

	private static void dropDatabase(String databaseName) {
		mongoClient.getDatabase(databaseName).drop();
	}

	private static void dropCollection(String collectionName) {
		db.getCollection(collectionName).drop();
	}

	private static MongoClient connessioneAlServer() {
		MongoClient mongoClient = null;

		try {
			mongoClient = new MongoClient("localhost", 27017);
		} catch (Exception ex) {
			System.out.println("Errore durante la connessione al server");
		}

		return mongoClient;
	}

	private static void terminaConnessioneConIlServer() {
		try {
			mongoClient.close();
		} catch (Exception e) {
			System.out.println("Errore durante la chiusura di connection");
		}
	}
}
