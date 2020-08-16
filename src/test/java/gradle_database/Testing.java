package gradle_database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Testing
{
	private static Connection	connection;
	private static final String	url	= "jdbc:sqlite:D:\\EclipseWorkspace\\jdbc-practice\\src\\main\\java\\jdbc\\database.db";

	@BeforeClass
	public static void setConnection() throws Exception
	{
		connection = DriverManager.getConnection(url);
	}

	@Test
	public void testaddKunde() throws SQLException
	{
		Statement stmt = connection.createStatement();
		stmt.executeUpdate(
		        "INSERT INTO Benutzer(Email, Vorname, Nachname, Passwort) VALUES('newTest@test.de', 'Test', 'Kunde1', 'KsWdll3r');");
		stmt.executeUpdate(
		        "INSERT INTO Kunde(Email, Rueckrufnummer, Werbeeinwilligung, AdressID) VALUES('newTest@test.de', 021112211, 0, 3);");

		ResultSet rs = stmt.executeQuery("SELECT * FROM Kunde WHERE Email = 'newTest@test.de';");
		rs.next();
		System.out.println("Email: " + rs.getString("Email"));
		System.out.println("Rueckrufnummer: " + rs.getString("Rueckrufnummer"));
		System.out.println("Werbeeinwilligung: " + rs.getBoolean("Werbeeinwilligung"));
		System.out.println("AdressID: " + rs.getInt("AdressID"));
		rs.close();
		stmt.executeUpdate("DELETE FROM Kunde WHERE Email = 'newTest@test.de'");
		stmt.executeUpdate("DELETE FROM Benutzer WHERE Email = 'newTest@test.de'");
	}

	@Test
	public void testAddLieferant() throws SQLException
	{
		String email = "testLieferrant@liefer.de";
		String pwd = "hEllO1g";
		String vorname = "testUserr";
		String nachname = "testLiefer";
		String liefername = "TestLieferant";

		String addUsr = "INSERT INTO BENUTZER(Email, Vorname, Nachname, Passwort) VALUES(?, ?, ?, ?);";
		PreparedStatement addUserFirst = connection.prepareStatement(addUsr);
		addUserFirst.setObject(1, email);
		addUserFirst.setObject(2, vorname);
		addUserFirst.setObject(3, nachname);
		addUserFirst.setObject(4, pwd);
		addUserFirst.execute();

		String addLiefer = "INSERT INTO Lieferant(Email, Haendlername) VALUES(?, ?);";
		PreparedStatement addLief = connection.prepareStatement(addLiefer);
		addLief.setObject(1, email);
		addLief.setObject(2, liefername);
		addLief.execute();

		String output = "SELECT * FROM Lieferant WHERE Email = 'testLieferrant@liefer.de'";
		Statement out = connection.createStatement();
		ResultSet rs = out.executeQuery(output);
		rs.next();
		System.out.println("Email: " + rs.getString(1));
		System.out.println("Haendlername: " + rs.getString(2));
		rs.close();

		out.executeUpdate("DELETE FROM Lieferant WHERE Email = 'testLieferrant@liefer.de'");
		out.executeUpdate("DELETE FROM Benutzer WHERE Email = 'testLieferrant@liefer.de'");
	}

	@Test
	public void testSelectFahrzeugWithOptionalFilter() throws SQLException
	{

		String kennzeichen = "Kenn-Anspr-1";
		String erstzulassung = "2000-01-01 12:24:59";

		String sql = "SELECT * FROM Fahrzeug WHERE TRUE ";
		if(kennzeichen != null)
		{
			sql = sql + "AND Kennzeichen = '" + kennzeichen + "'";
		}
		if(erstzulassung != null)
		{
			sql = sql + "AND (julianday('" + erstzulassung + "') < julianday(Erstzulassung))";
		}
		sql = sql + ";";

		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next())
		{
			System.out.println("Kennzeichen: " + rs.getString(1));
			System.out.println("Erstzulassung: " + rs.getString(2));
			System.out.println("HU_Datum: " + rs.getString(3));
			System.out.println("Email: " + rs.getString(4));
			System.out.println("ModellID: " + rs.getInt(5));
		}
		rs.close();
	}

	@Test
	public void testGetErsatzteilFromAuftrag() throws SQLException
	{
		String getErsatzteilNr = "SELECT ErsatzteilNr from benoetigt WHERE AuftragID = 1;";
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(getErsatzteilNr);

		List<Integer> ersatzNr = new ArrayList<Integer>();

		while(rs.next())
		{
			ersatzNr.add(rs.getInt(1));
			// System.out.println("ErsatzteilNr: " + ersatzNr.toString());
			System.out.println("ErsatzteilNr: " + rs.getInt(1));
		}
		rs.close();

		int anzahlElemente = ersatzNr.size();

		String getErsatzteilInfo = "SELECT * FROM Ersatzteile WHERE ErsatzteilNr = ?;";
		PreparedStatement prp = connection.prepareStatement(getErsatzteilInfo);

		for(int i = 0; i < anzahlElemente; i++)
		{
			prp.setObject(1, ersatzNr.get(i));
			ResultSet newRs = prp.executeQuery();
			while(newRs.next())
			{
				System.out.println("ErsatzteilNr: " + newRs.getInt(1));
				System.out.println("Herstellerinfo: " + newRs.getString(2));
				System.out.println("Ersatz_Bezeichnung: " + newRs.getString(3));
				System.out.println("Bild: " + newRs.getString(4));
				System.out.println("KategorieBezeichnung: " + newRs.getString(5));
			}
		}

	}

	@Test
	public void testWildcard() throws SQLException
	{

		String sql = "INSERT INTO Benutzer(Email, Vorname, Nachname, Passwort) VALUES(?, ?, ?, ?)";
		PreparedStatement prp = connection.prepareStatement(sql);
		prp.setObject(1, "testbenutzer1@test123.de");
		prp.setObject(2, "Test1");
		prp.setObject(3, "Benutzer1");
		prp.setObject(4, "tEsT1e3");
		prp.addBatch();

		prp.setObject(1, "testbenutzer2@test123.de");
		prp.setObject(2, "Test2");
		prp.setObject(3, "Benutzer2");
		prp.setObject(4, "tEsT1e3zWei");
		prp.addBatch();

		connection.setAutoCommit(false);
		try
		{
			prp.executeBatch();
		}
		catch(SQLException e)
		{
			connection.rollback();
		}
		connection.setAutoCommit(true);

		String getBenutzerAll = "SELECT * FROM BENUTZER";
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(getBenutzerAll);
		while(rs.next())
		{
			System.out.println("Email: " + rs.getString(1));
			System.out.println("Vorname: " + rs.getString(2));
			System.out.println("Nachname: " + rs.getString(3));
			System.out.println("Passwort: " + rs.getString(4));
		}
		rs.close();

		stmt.executeUpdate("DELETE FROM Benutzer WHERE Email = 'testbenutzer1@test123.de'");
		stmt.executeUpdate("DELETE FROM Benutzer WHERE Email = 'testbenutzer2@test123.de'");
	}

	@Test
	public void testBatchStatement() throws SQLException
	{
		Statement s = connection.createStatement();
		s.addBatch("DELETE FROM Benutzer WHERE Email = 'tester1@benutzer.de';");
		s.addBatch("DELETE FROM Benutzer WHERE Email = 'tester2@benutzer.de';");
		s.addBatch("DELETE FROM Benutzer WHERE Email = 'tester3@benutzer.de';");
		s.addBatch("DELETE FROM Kategorie WHERE Kategoriebezeichnung = 'testbezeichnung';");

		s.addBatch("INSERT INTO Benutzer VALUES ('tester1@benutzer.de', 'tester1', 'benutzer1', 'BeNuTz3R1')");
		s.addBatch("INSERT INTO Benutzer VALUES ('tester2@benutzer.de', 'tester2', 'benutzer2', 'BeNuTz3R2')");
		s.addBatch("INSERT INTO Benutzer VALUES ('tester3@benutzer.de', 'tester3', 'benutzer3', 'BeNuTz3R3')");

		s.addBatch("INSERT INTO Kategorie VALUES ('testbezeichnung')");

		connection.setAutoCommit(false);
		int[] count;
		try
		{
			count = s.executeBatch();
			int elements = count.length;
			for(int i = 0; i < elements; i++)
			{
				System.out.println("i: " + count[i]);
			}
		}
		catch(SQLException e)
		{
			connection.rollback();
		}
		connection.setAutoCommit(true);
		String getBenutzerAll = "SELECT * FROM BENUTZER";
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(getBenutzerAll);
		while(rs.next())
		{
			System.out.println("Email: " + rs.getString(1));
			System.out.println("Vorname: " + rs.getString(2));
			System.out.println("Nachname: " + rs.getString(3));
			System.out.println("Passwort: " + rs.getString(4));
		}
		rs.close();

		String getKategorieAll = "SELECT * FROM Kategorie";
		Statement st = connection.createStatement();
		ResultSet rs1 = st.executeQuery(getKategorieAll);
		while(rs1.next())
		{
			System.out.println("KategorieBezeichnung: " + rs1.getString(1));
		}
		rs1.close();

	}

	@AfterClass
	public static void closeConnection() throws Exception
	{
		if(connection != null && !connection.isClosed())
		{
			connection.close();
		}
	}
}
