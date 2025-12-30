import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseManager {

    private static DatabaseManager instance;
    private Properties props;
    
    public enum Delegacion {
        MADRID, BARCELONA, CORUNA, SEVILLA
    }

    private DatabaseManager() {
        props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                System.out.println("ERROR: No encuentro db.properties en src/main/resources");
                return;
            }
            props.load(input);
            Class.forName(props.getProperty("db.driver"));
        } catch (Exception ex) {
            System.err.println("Error cargando configuración: " + ex.getMessage());
        }
    }

    public static DatabaseManager getInstance() {
        if (instance == null) instance = new DatabaseManager();
        return instance;
    }

    public Connection getConnection(Delegacion delegacion) throws SQLException {
        String prefix = "";
        switch (delegacion) {
            case MADRID: prefix = "node.madrid"; break;
            case BARCELONA: prefix = "node.bcn"; break;
            case CORUNA: prefix = "node.coruna"; break;
            case SEVILLA: prefix = "node.sevilla"; break;
        }
        return DriverManager.getConnection(
            props.getProperty(prefix + ".url"),
            props.getProperty(prefix + ".user"),
            props.getProperty(prefix + ".pass")
        );
    }
}