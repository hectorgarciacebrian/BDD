import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Permisos {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();
        System.out.println("--- APLICANDO PERMISOS (GRANTS) ---");

        String[] sentencias = {
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Sucursal TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Productor TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Vino TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Empleado TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Cliente TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Suministra TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Solicita TO cerveza1, cerveza2, cerveza3, cerveza4",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON Pide TO cerveza1, cerveza2, cerveza3, cerveza4"
        };

        // Ejecutar en todas las delegaciones
        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + d);
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                for (String sql : sentencias) {
                    try {
                        stmt.executeUpdate(sql);
                        System.out.println("   -> OK: " + sql.substring(0, 45) + "...");
                    } catch (SQLException e) {
                        System.err.println("   -> Error: " + e.getMessage());
                    }
                }

            } catch (SQLException e) {
                System.err.println("Error de conexión: " + e.getMessage());
            }
        }
        System.out.println("\nPERMISOS APLICADOS.");
    }
}